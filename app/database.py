"""
SQLite via SQLAlchemy Core.
Migration path to PostgreSQL: swap DB_URL and install psycopg2. Schema and queries unchanged.
"""

import json
from pathlib import Path
from datetime import datetime

from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, Text, Float, Boolean,
    insert, select, update, text,
)

DB_PATH = Path(__file__).parent.parent / "data" / "magpie.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

engine   = create_engine(
    f"sqlite:///{DB_PATH}",
    connect_args={"check_same_thread": False, "timeout": 30},
)
metadata = MetaData()

# ---------------------------------------------------------------------------
# jobs — one row per job
# ---------------------------------------------------------------------------
jobs = Table("jobs", metadata,
    Column("id",                    Text,    primary_key=True),
    Column("created_at",            Text),
    Column("completed_at",          Text),
    Column("status",                Text,    default="pending"),

    # Experiment metadata
    Column("experiment_name",       Text),
    Column("experiment_notes",      Text),
    Column("parent_job_id",         Text),   # FK to jobs.id for follow-up passes
    Column("source_filter",         Text),   # e.g. "status=low_confidence"
    Column("pipeline_snapshot",     Text),   # JSON of exact settings at run time
    Column("job_settings_snapshot", Text),   # JSON of effective per-job settings
    Column("runtime_snapshot",      Text),   # JSON of process/environment state

    # Input
    Column("file_name",             Text),
    Column("source_file_hash",      Text),
    Column("total_rows_raw",        Integer, default=0),
    Column("source_total_valid_rows", Integer, default=0),
    Column("valid_row_start",       Integer, default=1),
    Column("valid_row_end",         Integer),
    Column("valid_rows",            Integer, default=0),

    # Pipeline config (snapshot keys for quick display)
    Column("haiku_enabled",         Boolean, default=False),
    Column("perplexity_enabled",    Boolean, default=False),
    Column("haiku_validation_enabled", Boolean, default=False),
    Column("manual_associations_enabled", Boolean, default=True),
    Column("salesforce_enrichment_enabled", Boolean, default=False),
    Column("legacy_enrichment_enabled",     Boolean, default=False),
    Column("web_search_fallback_enabled",   Boolean, default=False),
    Column("bing_web_enabled",              Boolean, default=True),
    Column("duckduckgo_web_enabled",        Boolean, default=True),
    Column("bing_maps_enabled",             Boolean, default=False),
    Column("skip_haiku",            Boolean, default=False),   # legacy compat

    # Progress
    Column("completed_rows",        Integer, default=0),
    Column("retry_count",           Integer, default=0),
    Column("error_count",           Integer, default=0),
    Column("run_duration_sec",      Integer),
    Column("last_heartbeat_at",     Text),
    Column("max_heartbeat_gap_sec", Float),
    Column("suspected_sleep_events", Text),
    Column("stale_timeout_sec",     Integer),
    Column("stale_detected",        Boolean, default=False),
    Column("stale_reason",          Text),
    Column("peak_browser_count",    Integer, default=0),
    Column("worker_error_count",    Integer, default=0),
    Column("active_worker_snapshot", Text),

    # GMaps summary
    Column("found_on_maps",         Integer, default=0),
    Column("has_website",           Integer, default=0),

    # Confidence tiers
    Column("tier_high",             Integer, default=0),
    Column("tier_medium",           Integer, default=0),
    Column("tier_low",              Integer, default=0),
    Column("tier_none",             Integer, default=0),

    # Status breakdown
    Column("status_confirmed",      Integer, default=0),
    Column("status_medium_conf",    Integer, default=0),
    Column("status_no_website",     Integer, default=0),
    Column("status_not_found",      Integer, default=0),
    Column("status_max_retries",    Integer, default=0),
    Column("status_manual_verified", Integer, default=0),
    Column("status_historical_found", Integer, default=0),

    # Manual source summary
    Column("manual_rows_with_candidates", Integer, default=0),
    Column("manual_accepted_candidates",  Integer, default=0),
    Column("manual_final_urls",           Integer, default=0),

    # Historical source summary
    Column("historical_rows_with_candidates", Integer, default=0),
    Column("historical_accepted_candidates",  Integer, default=0),
    Column("historical_from_salesforce",      Integer, default=0),
    Column("historical_from_legacy",          Integer, default=0),
    Column("historical_error_rows",           Integer, default=0),

    # Search candidate summary
    Column("search_candidates_evaluated", Integer, default=0),
    Column("gmaps_attempts",              Integer, default=0),
    Column("gmaps_retry_recovered",       Integer, default=0),
    Column("bing_maps_recovered",         Integer, default=0),
    Column("web_search_recovered",        Integer, default=0),
    Column("web_search_attempt_rows",     Integer, default=0),
    Column("web_search_query_count",      Integer, default=0),
    Column("web_search_diagnostic_count", Integer, default=0),
    Column("web_search_error_count",      Integer, default=0),
    Column("web_search_parsed_count",     Integer, default=0),

    Column("avg_confidence_score",  Float),

    # Cost totals
    Column("haiku_calls",           Integer, default=0),
    Column("haiku_input_tokens",    Integer, default=0),
    Column("haiku_output_tokens",   Integer, default=0),
    Column("haiku_cost_usd",        Float,   default=0.0),
    Column("perplexity_calls",      Integer, default=0),
    Column("perplexity_input_tokens", Integer, default=0),
    Column("perplexity_output_tokens", Integer, default=0),
    Column("perplexity_cost_usd",   Float,   default=0.0),
    Column("cost_usd",              Float,   default=0.0),  # total

    Column("output_file",           Text),
    Column("report_json",           Text),
)

# ---------------------------------------------------------------------------
# job_results — one row per processed lead
# ---------------------------------------------------------------------------
job_results = Table("job_results", metadata,
    Column("id",                    Integer, primary_key=True, autoincrement=True),
    Column("job_id",                Text,    index=True),

    # Input
    Column("company",               Text),
    Column("city",                  Text),
    Column("state",                 Text),
    Column("country",               Text),
    Column("source_sheet",          Text),
    Column("source_excel_row",      Integer),
    Column("source_valid_index",    Integer),

    # GMaps output
    Column("gmaps_found",           Boolean),
    Column("gmaps_listing_name",    Text),
    Column("gmaps_website",         Text),
    Column("gmaps_phone",           Text),
    Column("gmaps_address",         Text),
    Column("gmaps_street",          Text),
    Column("gmaps_city_scraped",    Text),
    Column("gmaps_state_scraped",   Text),
    Column("gmaps_zip",             Text),
    Column("gmaps_location_match",  Text),

    # Scores
    Column("name_similarity",       Integer),   # 0–100
    Column("gmaps_confidence_score",Integer),   # Maps-only, no AI

    # Haiku initial
    Column("sig_site_name",         Text),
    Column("sig_site_location",     Text),
    Column("sig_isn_mention",       Text),
    Column("sig_disqualifier",      Text),
    Column("haiku_initial_confidence", Integer),
    Column("haiku_initial_match",   Boolean),
    Column("haiku_initial_stop_reason", Text),
    Column("haiku_initial_latency_ms", Integer),

    # Perplexity
    Column("perplexity_url",        Text),
    Column("perplexity_confidence", Integer),
    Column("perplexity_reason",     Text),
    Column("perplexity_official_name", Text),
    Column("perplexity_evidence_location", Text),
    Column("perplexity_evidence_url", Text),
    Column("perplexity_company_match", Text),
    Column("perplexity_location_match", Text),
    Column("perplexity_is_official", Boolean),
    Column("perplexity_reject_reason", Text),
    Column("perplexity_citations",  Text),   # JSON array
    Column("perplexity_latency_ms", Integer),

    # Historical enrichment
    Column("historical_url",        Text),
    Column("historical_source",     Text),
    Column("historical_record_type",Text),
    Column("historical_record_id",  Text),
    Column("historical_name",       Text),
    Column("historical_city",       Text),
    Column("historical_state",      Text),
    Column("historical_raw_source", Text),
    Column("historical_candidate_count", Integer),
    Column("historical_legacy_raw_rows", Integer),
    Column("historical_legacy_rows_with_email", Integer),
    Column("historical_legacy_usable_domains", Integer),
    Column("historical_legacy_filtered_domains", Integer),
    Column("historical_legacy_query_name_like", Text),
    Column("historical_legacy_query_state", Text),
    Column("historical_legacy_query_state_abbr", Text),
    Column("historical_legacy_query_state_full", Text),
    Column("historical_identity_score", Integer),
    Column("historical_identity_verdict", Text),
    Column("historical_identity_reason", Text),
    Column("historical_errors",     Text),
    Column("historical_latency_ms", Integer),

    # Manual verified associations
    Column("manual_url",            Text),
    Column("manual_association_id", Integer),
    Column("manual_association_type", Text),
    Column("manual_notes",          Text),
    Column("manual_verified_by",    Text),
    Column("manual_verified_at",    Text),
    Column("manual_identity_score", Integer),
    Column("manual_identity_verdict", Text),
    Column("manual_identity_reason", Text),

    # Haiku validation (post-Perplexity)
    Column("haiku_final_confidence", Integer),
    Column("haiku_final_match",     Boolean),
    Column("haiku_final_stop_reason", Text),
    Column("haiku_final_latency_ms", Integer),

    # Final outcome
    Column("final_url",             Text),
    Column("url_source",            Text),   # gmaps | perplexity | manual_verified | salesforce | legacy_db
    Column("url_changed",           Boolean),
    Column("final_confidence_score",Integer),
    Column("confidence_tier",       Text),
    Column("identity_verdict",      Text),
    Column("identity_reason",       Text),
    Column("company_match_score",   Integer),
    Column("domain_match_score",    Integer),
    Column("location_match_level",  Text),
    Column("gmaps_identity_score",  Integer),
    Column("gmaps_identity_verdict", Text),
    Column("perplexity_identity_score", Integer),
    Column("perplexity_identity_verdict", Text),
    Column("manual_best_identity_score", Integer),
    Column("manual_best_identity_verdict", Text),
    Column("historical_best_identity_score", Integer),
    Column("historical_best_identity_verdict", Text),
    Column("haiku_reasoning",       Text),
    Column("stages_run",            Text),   # comma-separated
    Column("total_cost_usd",        Float),
    Column("total_latency_ms",      Integer),
    Column("gmaps_latency_ms",      Integer),
    Column("bing_maps_latency_ms",  Integer),
    Column("web_search_latency_ms", Integer),
    Column("manual_latency_ms",     Integer),
    Column("scoring_latency_ms",    Integer),
    Column("scrape_attempts",       Integer),
    Column("search_candidates_evaluated", Integer),
    Column("gmaps_attempts",        Integer),
    Column("web_search_candidate_count", Integer),
    Column("web_search_attempted",   Boolean),
    Column("web_search_provider_count", Integer),
    Column("web_search_query_count", Integer),
    Column("web_search_diagnostic_count", Integer),
    Column("web_search_error_count", Integer),
    Column("web_search_parsed_count", Integer),
    Column("web_search_error_summary", Text),
    Column("selected_candidate_source", Text),
    Column("selected_candidate_mode",   Text),
    Column("selected_candidate_query",  Text),
    Column("selected_candidate_rank",   Integer),
    Column("status",                Text),
)

# ---------------------------------------------------------------------------
# manual_associations — user-curated known website associations
# ---------------------------------------------------------------------------
manual_associations = Table("manual_associations", metadata,
    Column("id",                Integer, primary_key=True, autoincrement=True),
    Column("source_company",    Text, nullable=False),
    Column("source_city",       Text),
    Column("source_state",      Text),
    Column("source_country",    Text),
    Column("known_url",         Text, nullable=False),
    Column("association_type",  Text, default="current_official_site"),
    Column("notes",             Text),
    Column("verified_by",       Text),
    Column("verified_at",       Text),
    Column("status",            Text, default="active"),
    Column("created_at",        Text),
    Column("updated_at",        Text),
)

# ---------------------------------------------------------------------------
# api_calls — one row per AI API call, any provider
# Extensible: add new providers without schema change
# ---------------------------------------------------------------------------
api_calls = Table("api_calls", metadata,
    Column("id",                Integer, primary_key=True, autoincrement=True),
    Column("job_id",            Text,    index=True),
    Column("result_id",         Integer),   # FK to job_results.id (set after result saved)
    Column("company",           Text),

    # Provider identity — add new providers without schema changes
    Column("provider",          Text),   # anthropic | perplexity | openai | etc.
    Column("stage",             Text),   # haiku_initial | perplexity | haiku_validation | etc.
    Column("model",             Text),

    # Token usage (provider-agnostic names)
    Column("input_tokens",      Integer),
    Column("output_tokens",     Integer),
    Column("cost_usd",          Float),

    # Performance
    Column("latency_ms",        Integer),
    Column("timestamp",         Text),

    # Quality signals
    Column("stop_reason",       Text),   # end_turn | max_tokens | stop | length
    Column("request_id",        Text),   # provider's own ID for debugging

    # Source data (Perplexity citations, or future equivalents)
    Column("citations",         Text),   # JSON array of source URLs
    Column("citation_count",    Integer, default=0),

    # Raw content (truncated) for debugging
    Column("raw_response",      Text),
)

# ---------------------------------------------------------------------------
# job_search_candidates — one row per provider/query candidate
# ---------------------------------------------------------------------------
job_search_candidates = Table("job_search_candidates", metadata,
    Column("id",                Integer, primary_key=True, autoincrement=True),
    Column("job_id",            Text, index=True),
    Column("result_id",         Integer, index=True),
    Column("company",           Text),
    Column("city",              Text),
    Column("state",             Text),
    Column("country",           Text),

    Column("source",            Text),   # gmaps | web_bing | web_duckduckgo
    Column("mode",              Text),
    Column("query",             Text),
    Column("rank",              Integer),
    Column("title",             Text),
    Column("url",               Text),
    Column("address_or_snippet", Text),
    Column("phone",             Text),
    Column("maps_url",          Text),

    Column("identity_score",    Integer),
    Column("identity_verdict",  Text),
    Column("identity_reason",   Text),
    Column("company_match_score", Integer),
    Column("domain_match_score",  Integer),
    Column("location_match_level", Text),
    Column("selected",          Boolean, default=False),
    Column("error",             Text),
    Column("diagnostic",        Boolean, default=False),
    Column("http_status",       Integer),
    Column("response_bytes",    Integer),
    Column("parsed_count",      Integer),
    Column("created_at",        Text),
)

job_stage_events = Table("job_stage_events", metadata,
    Column("id",                 Integer, primary_key=True, autoincrement=True),
    Column("job_id",             Text, index=True),
    Column("event_type",         Text),
    Column("stage",              Text),
    Column("company",            Text),
    Column("city",               Text),
    Column("state",              Text),
    Column("source_sheet",       Text),
    Column("worker",             Text),
    Column("message",            Text),
    Column("details_json",       Text),
    Column("created_at",         Text),
    Column("elapsed_ms",         Integer),
    Column("is_active_snapshot", Boolean, default=False),
)


# ---------------------------------------------------------------------------
# Schema init + migration
# ---------------------------------------------------------------------------

def init_db():
    metadata.create_all(engine)
    _migrate()
    _seed_manual_associations()
    mark_interrupted_jobs_cancelled()


def _migrate():
    """Add columns introduced after initial schema creation."""
    _add_missing_cols("jobs", [
        ("experiment_name",       "TEXT"),
        ("experiment_notes",      "TEXT"),
        ("parent_job_id",         "TEXT"),
        ("source_filter",         "TEXT"),
        ("pipeline_snapshot",     "TEXT"),
        ("job_settings_snapshot", "TEXT"),
        ("runtime_snapshot",      "TEXT"),
        ("source_file_hash",      "TEXT"),
        ("source_total_valid_rows", "INTEGER DEFAULT 0"),
        ("valid_row_start",       "INTEGER DEFAULT 1"),
        ("valid_row_end",         "INTEGER"),
        ("haiku_enabled",         "INTEGER"),
        ("perplexity_enabled",    "INTEGER"),
        ("haiku_validation_enabled", "INTEGER"),
        ("manual_associations_enabled", "INTEGER"),
        ("status_manual_verified",     "INTEGER DEFAULT 0"),
        ("manual_rows_with_candidates", "INTEGER DEFAULT 0"),
        ("manual_accepted_candidates",  "INTEGER DEFAULT 0"),
        ("manual_final_urls",           "INTEGER DEFAULT 0"),
        ("salesforce_enrichment_enabled", "INTEGER"),
        ("legacy_enrichment_enabled",     "INTEGER"),
        ("web_search_fallback_enabled",   "INTEGER"),
        ("bing_web_enabled",              "INTEGER"),
        ("duckduckgo_web_enabled",        "INTEGER"),
        ("bing_maps_enabled",             "INTEGER"),
        ("run_duration_sec",      "INTEGER"),
        ("last_heartbeat_at",     "TEXT"),
        ("max_heartbeat_gap_sec", "REAL"),
        ("suspected_sleep_events","TEXT"),
        ("stale_timeout_sec",     "INTEGER"),
        ("stale_detected",        "INTEGER DEFAULT 0"),
        ("stale_reason",          "TEXT"),
        ("peak_browser_count",    "INTEGER DEFAULT 0"),
        ("worker_error_count",    "INTEGER DEFAULT 0"),
        ("active_worker_snapshot", "TEXT"),
        ("status_historical_found",       "INTEGER DEFAULT 0"),
        ("historical_rows_with_candidates", "INTEGER DEFAULT 0"),
        ("historical_accepted_candidates",  "INTEGER DEFAULT 0"),
        ("historical_from_salesforce",      "INTEGER DEFAULT 0"),
        ("historical_from_legacy",          "INTEGER DEFAULT 0"),
        ("historical_error_rows",           "INTEGER DEFAULT 0"),
        ("search_candidates_evaluated", "INTEGER DEFAULT 0"),
        ("gmaps_attempts",              "INTEGER DEFAULT 0"),
        ("gmaps_retry_recovered",       "INTEGER DEFAULT 0"),
        ("bing_maps_recovered",         "INTEGER DEFAULT 0"),
        ("web_search_recovered",        "INTEGER DEFAULT 0"),
        ("web_search_attempt_rows",     "INTEGER DEFAULT 0"),
        ("web_search_query_count",      "INTEGER DEFAULT 0"),
        ("web_search_diagnostic_count", "INTEGER DEFAULT 0"),
        ("web_search_error_count",      "INTEGER DEFAULT 0"),
        ("web_search_parsed_count",     "INTEGER DEFAULT 0"),
        ("haiku_calls",           "INTEGER DEFAULT 0"),
        ("haiku_input_tokens",    "INTEGER DEFAULT 0"),
        ("haiku_output_tokens",   "INTEGER DEFAULT 0"),
        ("haiku_cost_usd",        "REAL DEFAULT 0"),
        ("perplexity_calls",      "INTEGER DEFAULT 0"),
        ("perplexity_input_tokens","INTEGER DEFAULT 0"),
        ("perplexity_output_tokens","INTEGER DEFAULT 0"),
        ("perplexity_cost_usd",   "REAL DEFAULT 0"),
    ])
    _add_missing_cols("job_results", [
        ("gmaps_confidence_score",    "INTEGER"),
        ("source_excel_row",          "INTEGER"),
        ("source_valid_index",        "INTEGER"),
        ("haiku_initial_confidence",  "INTEGER"),
        ("haiku_initial_match",       "INTEGER"),
        ("haiku_initial_stop_reason", "TEXT"),
        ("haiku_initial_latency_ms",  "INTEGER"),
        ("perplexity_url",            "TEXT"),
        ("perplexity_confidence",     "INTEGER"),
        ("perplexity_reason",         "TEXT"),
        ("perplexity_official_name",  "TEXT"),
        ("perplexity_evidence_location", "TEXT"),
        ("perplexity_evidence_url",   "TEXT"),
        ("perplexity_company_match",  "TEXT"),
        ("perplexity_location_match", "TEXT"),
        ("perplexity_is_official",    "INTEGER"),
        ("perplexity_reject_reason",  "TEXT"),
        ("perplexity_citations",      "TEXT"),
        ("perplexity_latency_ms",     "INTEGER"),
        ("historical_url",            "TEXT"),
        ("historical_source",         "TEXT"),
        ("historical_record_type",    "TEXT"),
        ("historical_record_id",      "TEXT"),
        ("historical_name",           "TEXT"),
        ("historical_city",           "TEXT"),
        ("historical_state",          "TEXT"),
        ("historical_raw_source",     "TEXT"),
        ("historical_candidate_count","INTEGER"),
        ("historical_legacy_raw_rows","INTEGER"),
        ("historical_legacy_rows_with_email","INTEGER"),
        ("historical_legacy_usable_domains","INTEGER"),
        ("historical_legacy_filtered_domains","INTEGER"),
        ("historical_legacy_query_name_like","TEXT"),
        ("historical_legacy_query_state","TEXT"),
        ("historical_legacy_query_state_abbr","TEXT"),
        ("historical_legacy_query_state_full","TEXT"),
        ("historical_identity_score", "INTEGER"),
        ("historical_identity_verdict","TEXT"),
        ("historical_identity_reason","TEXT"),
        ("historical_errors",         "TEXT"),
        ("historical_latency_ms",     "INTEGER"),
        ("manual_url",                "TEXT"),
        ("manual_association_id",     "INTEGER"),
        ("manual_association_type",   "TEXT"),
        ("manual_notes",              "TEXT"),
        ("manual_verified_by",        "TEXT"),
        ("manual_verified_at",        "TEXT"),
        ("manual_identity_score",     "INTEGER"),
        ("manual_identity_verdict",   "TEXT"),
        ("manual_identity_reason",    "TEXT"),
        ("haiku_final_confidence",    "INTEGER"),
        ("haiku_final_match",         "INTEGER"),
        ("haiku_final_stop_reason",   "TEXT"),
        ("haiku_final_latency_ms",    "INTEGER"),
        ("final_url",                 "TEXT"),
        ("url_source",                "TEXT"),
        ("url_changed",               "INTEGER"),
        ("final_confidence_score",    "INTEGER"),
        ("identity_verdict",          "TEXT"),
        ("identity_reason",           "TEXT"),
        ("company_match_score",       "INTEGER"),
        ("domain_match_score",        "INTEGER"),
        ("location_match_level",      "TEXT"),
        ("gmaps_identity_score",      "INTEGER"),
        ("gmaps_identity_verdict",    "TEXT"),
        ("perplexity_identity_score", "INTEGER"),
        ("perplexity_identity_verdict", "TEXT"),
        ("manual_best_identity_score", "INTEGER"),
        ("manual_best_identity_verdict", "TEXT"),
        ("historical_best_identity_score", "INTEGER"),
        ("historical_best_identity_verdict", "TEXT"),
        ("stages_run",                "TEXT"),
        ("total_cost_usd",            "REAL"),
        ("total_latency_ms",          "INTEGER"),
        ("gmaps_latency_ms",          "INTEGER"),
        ("bing_maps_latency_ms",      "INTEGER"),
        ("web_search_latency_ms",     "INTEGER"),
        ("manual_latency_ms",         "INTEGER"),
        ("scoring_latency_ms",        "INTEGER"),
        ("search_candidates_evaluated", "INTEGER"),
        ("gmaps_attempts",            "INTEGER"),
        ("web_search_candidate_count", "INTEGER"),
        ("web_search_attempted",      "INTEGER"),
        ("web_search_provider_count", "INTEGER"),
        ("web_search_query_count",    "INTEGER"),
        ("web_search_diagnostic_count", "INTEGER"),
        ("web_search_error_count",    "INTEGER"),
        ("web_search_parsed_count",   "INTEGER"),
        ("web_search_error_summary",  "TEXT"),
        ("selected_candidate_source", "TEXT"),
        ("selected_candidate_mode",   "TEXT"),
        ("selected_candidate_query",  "TEXT"),
        ("selected_candidate_rank",   "INTEGER"),
    ])
    _add_missing_cols("job_search_candidates", [
        ("diagnostic",        "INTEGER"),
        ("http_status",       "INTEGER"),
        ("response_bytes",    "INTEGER"),
        ("parsed_count",      "INTEGER"),
    ])


def _add_missing_cols(table_name: str, cols: list[tuple]):
    with engine.begin() as conn:
        for col_name, col_def in cols:
            try:
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_def}"))
            except Exception:
                pass  # column already exists


def _seed_manual_associations():
    """Seed known associations discovered during manual review."""
    now = datetime.utcnow().isoformat()
    seed = {
        "source_company":   "ADS, LLC",
        "source_city":      "Mobile",
        "source_state":     "Alabama",
        "source_country":   "United States",
        "known_url":        "https://www.adsconcrete.us/about",
        "association_type": "current_official_site",
        "notes":            "Manual research found likely current website; Salesforce had older ADS Environmental domain.",
        "verified_by":      "Alex",
        "verified_at":      "2026-07-01",
        "status":           "active",
        "created_at":       now,
        "updated_at":       now,
    }
    with engine.begin() as conn:
        existing = conn.execute(
            select(manual_associations.c.id).where(
                manual_associations.c.source_company == seed["source_company"],
                manual_associations.c.source_city == seed["source_city"],
                manual_associations.c.source_state == seed["source_state"],
                manual_associations.c.known_url == seed["known_url"],
            )
        ).fetchone()
        if not existing:
            conn.execute(insert(manual_associations).values(**seed))


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

def save_job_start(job_id: str, file_name: str, valid_rows: int,
                   pipeline_config: dict, experiment: dict,
                   settings_snapshot: dict, created_at: str,
                   job_settings_snapshot: dict | None = None,
                   runtime_snapshot: dict | None = None,
                   source_file_hash: str | None = None,
                   total_rows_raw: int | None = None,
                   source_total_valid_rows: int | None = None,
                   valid_row_start: int | None = None,
                   valid_row_end: int | None = None):
    with engine.begin() as conn:
        conn.execute(insert(jobs).values(
            id                    = job_id,
            created_at            = created_at,
            status                = "running",
            file_name             = file_name,
            source_file_hash      = source_file_hash,
            total_rows_raw        = total_rows_raw,
            source_total_valid_rows = source_total_valid_rows,
            valid_row_start       = valid_row_start,
            valid_row_end         = valid_row_end,
            valid_rows            = valid_rows,
            experiment_name       = experiment.get("name", ""),
            experiment_notes      = experiment.get("notes", ""),
            parent_job_id         = experiment.get("parent_job_id"),
            source_filter         = experiment.get("source_filter"),
            haiku_enabled         = pipeline_config.get("haiku_enabled", False),
            perplexity_enabled    = pipeline_config.get("perplexity_enabled", False),
            haiku_validation_enabled = pipeline_config.get("haiku_validation_enabled", False),
            manual_associations_enabled = pipeline_config.get("manual_associations_enabled", True),
            salesforce_enrichment_enabled = pipeline_config.get("salesforce_enrichment_enabled", False),
            legacy_enrichment_enabled     = pipeline_config.get("legacy_enrichment_enabled", False),
            web_search_fallback_enabled   = pipeline_config.get("web_search_fallback_enabled", False),
            bing_web_enabled              = pipeline_config.get("bing_web_enabled", True),
            duckduckgo_web_enabled        = pipeline_config.get("duckduckgo_web_enabled", True),
            bing_maps_enabled             = pipeline_config.get("bing_maps_enabled", False),
            pipeline_snapshot     = json.dumps(settings_snapshot),
            job_settings_snapshot = json.dumps(job_settings_snapshot or {}),
            runtime_snapshot      = json.dumps(runtime_snapshot or {}),
            stale_timeout_sec     = (job_settings_snapshot or {}).get("job_stall_timeout_seconds"),
            last_heartbeat_at     = created_at,
        ))


def save_job_complete(job_id: str, stats: dict):
    with engine.begin() as conn:
        conn.execute(update(jobs).where(jobs.c.id == job_id).values(**stats))


def update_job_runtime(job_id: str, stats: dict):
    if not stats:
        return
    with engine.begin() as conn:
        conn.execute(update(jobs).where(jobs.c.id == job_id).values(**stats))


def save_result(row: dict) -> int:
    with engine.begin() as conn:
        r = conn.execute(insert(job_results).values(**row))
        return r.inserted_primary_key[0]


def save_api_call(call: dict):
    with engine.begin() as conn:
        conn.execute(insert(api_calls).values(**call))


def save_api_calls_batch(calls: list[dict]):
    if not calls:
        return
    with engine.begin() as conn:
        conn.execute(insert(api_calls), calls)


def save_search_candidates_batch(candidates: list[dict]):
    if not candidates:
        return
    with engine.begin() as conn:
        conn.execute(insert(job_search_candidates), candidates)


def save_result_with_candidates(result_row: dict, candidates: list[dict]) -> int:
    """Persist one final row and its audit candidates in a single transaction."""
    with engine.begin() as conn:
        result = conn.execute(insert(job_results).values(**result_row))
        result_id = result.inserted_primary_key[0]
        if candidates:
            candidate_rows = [dict(row, result_id=result_id) for row in candidates]
            conn.execute(insert(job_search_candidates), candidate_rows)
        return result_id


def save_stage_events_batch(events: list[dict]):
    if not events:
        return
    with engine.begin() as conn:
        conn.execute(insert(job_stage_events), events)


# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------

def get_all_jobs() -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(select(jobs).order_by(jobs.c.created_at.desc()))
        return [dict(r._mapping) for r in rows]


def get_job_row(job_id: str) -> dict | None:
    with engine.connect() as conn:
        row = conn.execute(select(jobs).where(jobs.c.id == job_id)).fetchone()
        return dict(row._mapping) if row else None


def get_job_results(job_id: str) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(select(job_results).where(job_results.c.job_id == job_id))
        return [dict(r._mapping) for r in rows]


def get_job_results_filtered(job_id: str, status_filter: str) -> list[dict]:
    """Return job results matching a status filter string like 'status=low_confidence'."""
    rows = get_job_results(job_id)
    if not status_filter or status_filter == "all":
        return rows
    key, _, val = status_filter.partition("=")
    return [r for r in rows if str(r.get(key.strip(), "")) == val.strip()]


def get_api_calls(job_id: str) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            select(api_calls).where(api_calls.c.job_id == job_id)
                             .order_by(api_calls.c.timestamp)
        )
        return [dict(r._mapping) for r in rows]


def get_search_candidates(job_id: str, result_id: int | None = None) -> list[dict]:
    with engine.connect() as conn:
        stmt = select(job_search_candidates).where(job_search_candidates.c.job_id == job_id)
        if result_id is not None:
            stmt = stmt.where(job_search_candidates.c.result_id == result_id)
        stmt = stmt.order_by(
            job_search_candidates.c.company,
            job_search_candidates.c.source,
            job_search_candidates.c.mode,
            job_search_candidates.c.rank,
        )
        rows = conn.execute(stmt)
        return [dict(r._mapping) for r in rows]


def get_stage_events(job_id: str, limit: int | None = None) -> list[dict]:
    with engine.connect() as conn:
        stmt = select(job_stage_events).where(job_stage_events.c.job_id == job_id)
        stmt = stmt.order_by(job_stage_events.c.id)
        if limit:
            stmt = stmt.limit(limit)
        rows = conn.execute(stmt)
        return [dict(r._mapping) for r in rows]


def list_manual_associations(include_inactive: bool = False) -> list[dict]:
    with engine.connect() as conn:
        stmt = select(manual_associations).order_by(manual_associations.c.updated_at.desc())
        if not include_inactive:
            stmt = stmt.where(manual_associations.c.status == "active")
        rows = conn.execute(stmt)
        return [dict(r._mapping) for r in rows]


def create_manual_association(data: dict) -> dict:
    now = datetime.utcnow().isoformat()
    row = {
        "source_company":   data.get("source_company", "").strip(),
        "source_city":      data.get("source_city", "").strip(),
        "source_state":     data.get("source_state", "").strip(),
        "source_country":   data.get("source_country", "United States").strip() or "United States",
        "known_url":        data.get("known_url", "").strip(),
        "association_type": data.get("association_type", "current_official_site").strip() or "current_official_site",
        "notes":            data.get("notes", "").strip(),
        "verified_by":      data.get("verified_by", "").strip(),
        "verified_at":      data.get("verified_at", "").strip() or now[:10],
        "status":           data.get("status", "active").strip() or "active",
        "created_at":       now,
        "updated_at":       now,
    }
    with engine.begin() as conn:
        result = conn.execute(insert(manual_associations).values(**row))
        row["id"] = result.inserted_primary_key[0]
    return row


def update_manual_association(association_id: int, data: dict) -> dict | None:
    allowed = {
        "source_company", "source_city", "source_state", "source_country",
        "known_url", "association_type", "notes", "verified_by",
        "verified_at", "status",
    }
    values = {k: str(v).strip() for k, v in data.items() if k in allowed and v is not None}
    values["updated_at"] = datetime.utcnow().isoformat()
    with engine.begin() as conn:
        conn.execute(
            update(manual_associations)
            .where(manual_associations.c.id == association_id)
            .values(**values)
        )
        row = conn.execute(
            select(manual_associations).where(manual_associations.c.id == association_id)
        ).fetchone()
        return dict(row._mapping) if row else None


def delete_manual_association(association_id: int) -> bool:
    with engine.begin() as conn:
        result = conn.execute(
            update(manual_associations)
            .where(manual_associations.c.id == association_id)
            .values(status="inactive", updated_at=datetime.utcnow().isoformat())
        )
        return result.rowcount > 0


def mark_interrupted_jobs_cancelled() -> int:
    """Close jobs left running after an app restart or worker interruption."""
    now = datetime.utcnow().isoformat()
    with engine.begin() as conn:
        result = conn.execute(
            update(jobs)
            .where(jobs.c.status == "running")
            .values(
                status="cancelled",
                completed_at=now,
                last_heartbeat_at=now,
                stale_detected=True,
                stale_reason="app_startup_closed_orphaned_running_job",
            )
        )
        return result.rowcount or 0


init_db()
