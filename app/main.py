import asyncio, json, os, queue, shutil, signal, threading, time, uuid
from pathlib import Path
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse

from .config import settings
from .ingest import ingest_file
from .job_runner import create_job, get_job, list_jobs_memory, start_job, cancel_job, Status
from . import database as db

BASE_DIR   = Path(__file__).parent.parent
UPLOAD_DIR = BASE_DIR / "data" / "uploads"
FRONTEND   = BASE_DIR / "frontend"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Magpie App")
app.mount("/static", StaticFiles(directory=str(FRONTEND / "static")), name="static")

_ingest_cache: dict = {}   # file_id -> (leads, report, file_name)


# ── Frontend ────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    return (FRONTEND / "index.html").read_text(encoding="utf-8")


# ── Upload ───────────────────────────────────────────────────────────────────

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    if not file.filename.endswith((".xlsx", ".csv")):
        raise HTTPException(400, "Only .xlsx and .csv files are supported.")
    file_id = str(uuid.uuid4())[:8]
    dest    = UPLOAD_DIR / f"{file_id}_{file.filename}"
    with open(dest, "wb") as f:
        shutil.copyfileobj(file.file, f)
    return {"file_id": file_id, "filename": file.filename}


# ── Ingest ───────────────────────────────────────────────────────────────────

@app.post("/api/ingest/{file_id}")
async def ingest(file_id: str):
    matches = list(UPLOAD_DIR.glob(f"{file_id}_*"))
    if not matches:
        raise HTTPException(404, "Uploaded file not found.")
    try:
        leads, report = ingest_file(str(matches[0]))
    except Exception as exc:
        raise HTTPException(500, f"Ingest failed: {exc}")
    _ingest_cache[file_id] = (leads, report, matches[0].name)
    return {
        "file_id":      file_id,
        "file_name":    matches[0].name,
        "sheets_found": report.sheets_found,
        "total_raw":    report.total_rows_raw,
        "total_valid":  report.total_rows_valid,
        "dropped":      report.dropped_rows,
        "drop_reasons": report.drop_reasons,
        "country_dist": report.country_distribution,
        "sample_drops": report.sample_drops,
        "sample_drops_by_reason": report.sample_drops_by_reason,
    }


# ── Job start ────────────────────────────────────────────────────────────────

@app.post("/api/job/start")
async def job_start(body: dict):
    """
    body:
      file_id          str
      limit            int | null
      experiment       {name, notes, parent_job_id, source_filter}
      pipeline         {haiku_enabled, haiku_validation_enabled, perplexity_enabled,
                        perplexity_validate, perplexity_trigger,
                        manual_associations_enabled, salesforce_enrichment_enabled,
                        legacy_enrichment_enabled, web_search_fallback_enabled,
                        bing_web_enabled, duckduckgo_web_enabled, bing_maps_enabled}
      job_settings     {thresholds, workers, etc.}
    """
    file_id     = body.get("file_id")
    limit       = body.get("limit")
    experiment  = body.get("experiment", {})
    pipeline    = body.get("pipeline", {})
    job_settings= body.get("job_settings", {})

    if file_id not in _ingest_cache:
        raise HTTPException(400, "Run /api/ingest/{file_id} first.")

    leads, _, file_name = _ingest_cache[file_id]
    if limit:
        leads = leads[:int(limit)]
    if not leads:
        raise HTTPException(400, "No valid leads to process.")

    # Apply pipeline defaults from current settings
    pipeline_cfg = {
        "haiku_enabled":             pipeline.get("haiku_enabled",             settings.haiku_enabled),
        "haiku_validation_enabled":  pipeline.get("haiku_validation_enabled",  settings.haiku_validation_enabled),
        "perplexity_enabled":        pipeline.get("perplexity_enabled",        settings.perplexity_enabled),
        "perplexity_validate":       pipeline.get("perplexity_validate",       settings.perplexity_validate),
        "perplexity_trigger":        pipeline.get("perplexity_trigger",        settings.perplexity_trigger),
        "manual_associations_enabled": pipeline.get("manual_associations_enabled", settings.manual_associations_enabled),
        "salesforce_enrichment_enabled": pipeline.get("salesforce_enrichment_enabled", settings.salesforce_enrichment_enabled),
        "legacy_enrichment_enabled":     pipeline.get("legacy_enrichment_enabled",     settings.legacy_enrichment_enabled),
        "web_search_fallback_enabled":   pipeline.get("web_search_fallback_enabled",   settings.web_search_fallback_enabled),
        "bing_web_enabled":              pipeline.get("bing_web_enabled",              settings.bing_web_enabled),
        "duckduckgo_web_enabled":        pipeline.get("duckduckgo_web_enabled",        settings.duckduckgo_web_enabled),
        "bing_maps_enabled":             pipeline.get("bing_maps_enabled",             settings.bing_maps_enabled),
    }

    job_id = create_job(leads, pipeline_cfg, job_settings, experiment, file_name)
    start_job(job_id)
    return {"job_id": job_id, "total": len(leads)}


# ── Follow-up job (run specific stage on subset of a previous job) ────────────

@app.post("/api/job/followup")
async def job_followup(body: dict):
    """
    Run a new job on a filtered subset of a completed job's records.
    body:
      parent_job_id   str
      source_filter   str  e.g. "status=low_confidence" | "confidence_tier=Low"
      pipeline        dict  (same as /api/job/start)
      experiment      dict
    """
    parent_id     = body.get("parent_job_id")
    source_filter = body.get("source_filter", "all")
    pipeline      = body.get("pipeline", {})
    experiment    = body.get("experiment", {})

    if not parent_id:
        raise HTTPException(400, "parent_job_id required.")

    # Get filtered records from DB
    source_rows = db.get_job_results_filtered(parent_id, source_filter)
    if not source_rows:
        raise HTTPException(404, f"No records match filter '{source_filter}' for job {parent_id}.")

    # Re-hydrate as Lead objects
    from .ingest import Lead
    leads = [
        Lead(company=r["company"], city=r["city"], state=r["state"],
             country=r["country"] or "United States",
             source_sheet=r["source_sheet"] or "", original_row_idx=0)
        for r in source_rows
        if r.get("company") and r.get("city") and r.get("state")
    ]
    if not leads:
        raise HTTPException(400, "No usable leads after filtering.")

    pipeline_cfg = {
        "haiku_enabled":            pipeline.get("haiku_enabled",            settings.haiku_enabled),
        "haiku_validation_enabled": pipeline.get("haiku_validation_enabled", settings.haiku_validation_enabled),
        "perplexity_enabled":       pipeline.get("perplexity_enabled",       settings.perplexity_enabled),
        "perplexity_validate":      pipeline.get("perplexity_validate",      settings.perplexity_validate),
        "perplexity_trigger":       pipeline.get("perplexity_trigger",       settings.perplexity_trigger),
        "manual_associations_enabled": pipeline.get("manual_associations_enabled", settings.manual_associations_enabled),
        "salesforce_enrichment_enabled": pipeline.get("salesforce_enrichment_enabled", settings.salesforce_enrichment_enabled),
        "legacy_enrichment_enabled":     pipeline.get("legacy_enrichment_enabled",     settings.legacy_enrichment_enabled),
        "web_search_fallback_enabled":   pipeline.get("web_search_fallback_enabled",   settings.web_search_fallback_enabled),
        "bing_web_enabled":              pipeline.get("bing_web_enabled",              settings.bing_web_enabled),
        "duckduckgo_web_enabled":        pipeline.get("duckduckgo_web_enabled",        settings.duckduckgo_web_enabled),
        "bing_maps_enabled":             pipeline.get("bing_maps_enabled",             settings.bing_maps_enabled),
    }
    experiment["parent_job_id"]  = parent_id
    experiment["source_filter"]  = source_filter

    parent_row = db.get_job_row(parent_id)
    file_name  = (parent_row or {}).get("file_name", "")

    job_id = create_job(leads, pipeline_cfg, body.get("job_settings", {}), experiment, file_name)
    start_job(job_id)
    return {"job_id": job_id, "total": len(leads), "parent_job_id": parent_id}


# ── Job status / report ───────────────────────────────────────────────────────

@app.get("/api/job/{job_id}/status")
async def job_status(job_id: str):
    job = get_job(job_id)
    if job:
        return {
            "job_id":    job.job_id,
            "status":    job.status,
            "total":     job.total,
            "completed": job.completed,
            "found":     job.found_count,
            "maps_found": getattr(job, "maps_found_count", 0),
            "errors":    job.error_count,
            "worker_errors": getattr(job, "worker_error_count", 0),
            "active_rows": job.active_rows_snapshot(),
            "recent_stage_events": job.stage_events_snapshot(limit=25),
            "haiku_cost_usd":      round(job.haiku_cost_usd, 6),
            "perplexity_cost_usd": round(job.perplexity_cost_usd, 6),
            "total_cost_usd":      round(job.haiku_cost_usd + job.perplexity_cost_usd, 6),
            "output":    job.output_file,
            "report":    job.report,
            "experiment":job.experiment,
            "runtime": {
                "started_at": job.started_at.isoformat() if job.started_at else None,
                "last_progress_at": job.last_progress_at.isoformat() if job.last_progress_at else None,
                "last_heartbeat_at": job.last_heartbeat_at.isoformat() if job.last_heartbeat_at else None,
                "max_heartbeat_gap_sec": round(job.max_heartbeat_gap_sec, 1),
                "stale_detected": job.stale_detected,
                "stale_reason": job.stale_reason,
                "peak_browser_count": getattr(job, "peak_browser_count", 0),
                "worker_error_count": getattr(job, "worker_error_count", 0),
                "active_rows": job.active_rows_snapshot(),
            },
        }
    row = db.get_job_row(job_id)
    if row:
        return row
    raise HTTPException(404, "Job not found.")


@app.get("/api/job/{job_id}/report")
async def job_report(job_id: str):
    job = get_job(job_id)
    if job and job.report:
        return job.report
    row = db.get_job_row(job_id)
    if row and row.get("report_json"):
        return json.loads(row["report_json"])
    raise HTTPException(404, "Report not available.")


@app.get("/api/job/{job_id}/api_calls")
async def job_api_calls(job_id: str):
    """All AI API calls for this job — provider, model, tokens, cost, citations, latency."""
    calls = db.get_api_calls(job_id)
    # Parse citations JSON for each call
    for c in calls:
        try:
            c["citations"] = json.loads(c.get("citations") or "[]")
        except Exception:
            c["citations"] = []
    return calls


@app.get("/api/job/{job_id}/search_candidates")
async def job_search_candidates(job_id: str, result_id: int | None = None):
    """All deterministic search candidates evaluated for this job."""
    return db.get_search_candidates(job_id, result_id)


@app.get("/api/job/{job_id}/stage_events")
async def job_stage_events(job_id: str, limit: int | None = None):
    """Worker/stage diagnostic events for a job."""
    job = get_job(job_id)
    if job:
        return job.stage_events_snapshot(limit=limit or 1000)
    return db.get_stage_events(job_id, limit=limit)


# ── SSE stream ────────────────────────────────────────────────────────────────

@app.get("/api/job/{job_id}/stream")
async def job_stream(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found.")

    async def generator():
        while True:
            try:
                event = job.event_queue.get_nowait()
                yield {"data": json.dumps(event)}
                if event.get("type") in ("done", "error"):
                    break
            except queue.Empty:
                if job.status in (Status.DONE, Status.CANCELLED, Status.ERROR):
                    break
                await asyncio.sleep(0.25)

    return EventSourceResponse(generator())


# ── Download ─────────────────────────────────────────────────────────────────

@app.get("/api/job/{job_id}/download")
async def job_download(job_id: str):
    job = get_job(job_id)
    out = job.output_file if job else None
    if not out:
        row = db.get_job_row(job_id)
        if row: out = row.get("output_file")
    if not out or not Path(out).exists():
        raise HTTPException(404, "Output file not ready.")
    path = Path(out)
    return FileResponse(str(path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=path.name)


# ── Job history ───────────────────────────────────────────────────────────────

@app.get("/api/jobs")
async def jobs_list():
    db_jobs  = db.get_all_jobs()
    db_ids   = {j["id"] for j in db_jobs}
    mem_jobs = [j for j in list_jobs_memory() if j["job_id"] not in db_ids]
    return db_jobs + mem_jobs


# â”€â”€ Manual associations â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@app.get("/api/manual_associations")
async def manual_associations_list(include_inactive: bool = False):
    return db.list_manual_associations(include_inactive=include_inactive)


@app.post("/api/manual_associations")
async def manual_associations_create(body: dict):
    if not body.get("source_company") or not body.get("known_url"):
        raise HTTPException(400, "source_company and known_url are required.")
    return db.create_manual_association(body)


@app.put("/api/manual_associations/{association_id}")
async def manual_associations_update(association_id: int, body: dict):
    row = db.update_manual_association(association_id, body)
    if not row:
        raise HTTPException(404, "Manual association not found.")
    return row


@app.delete("/api/manual_associations/{association_id}")
async def manual_associations_delete(association_id: int):
    if not db.delete_manual_association(association_id):
        raise HTTPException(404, "Manual association not found.")
    return {"deleted": True, "id": association_id}


# ── Cancel / shutdown ─────────────────────────────────────────────────────────

@app.post("/api/job/{job_id}/cancel")
async def job_cancel(job_id: str):
    if not cancel_job(job_id):
        raise HTTPException(400, "Job not found or not running.")
    return {"status": "cancellation_requested", "job_id": job_id}


@app.post("/api/shutdown")
async def shutdown_server():
    from .job_runner import _jobs
    for job in list(_jobs.values()):
        if job.status == Status.RUNNING:
            cancel_job(job.job_id)

    def _kill():
        time.sleep(1.0)
        os._exit(0)

    threading.Thread(target=_kill, daemon=True).start()
    return {"status": "shutting_down"}


# ── Settings ──────────────────────────────────────────────────────────────────

@app.get("/api/settings")
async def get_settings():
    return settings.to_dict()


@app.put("/api/settings")
async def update_settings(body: dict):
    settings.update(body)
    return settings.to_dict()
