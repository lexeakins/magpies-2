"""
Perplexity Sonar client.
Uses the OpenAI-compatible endpoint at api.perplexity.ai.
Returns the full response object so nothing is left on the table.
"""

import re
import json
import time
import httpx
from .config import settings

PERPLEXITY_API_URL = "https://api.perplexity.ai/chat/completions"


def call_perplexity(
    company:       str,
    city:          str,
    state:         str,
    gmaps_street:  str  = None,
    gmaps_zip:     str  = None,
    gmaps_phone:   str  = None,
    existing_url:  str  = None,   # URL to verify (validate mode)
    mode:          str  = "find", # "find" | "validate"
) -> dict:
    """
    Call Perplexity Sonar to find or validate a company website.

    Returns a fully populated metadata dict that maps directly to an api_calls row.
    Keys: provider, stage, model, input_tokens, output_tokens, cost_usd,
          latency_ms, stop_reason, request_id, citations, citation_count,
          raw_response, success, parsed (structured result from response text)
    """
    prompt = _build_prompt(company, city, state, gmaps_street, gmaps_zip, gmaps_phone,
                           existing_url, mode)
    start  = time.time()

    try:
        with httpx.Client(timeout=30) as client:
            resp = client.post(
                PERPLEXITY_API_URL,
                headers={
                    "Authorization": f"Bearer {settings.perplexity_api_key}",
                    "Content-Type":  "application/json",
                },
                json={
                    "model":       settings.perplexity_model,
                    "messages":    [{"role": "user", "content": prompt}],
                    "max_tokens":  350,
                    "temperature": 0,
                },
            )

            latency_ms = int((time.time() - start) * 1000)

            if resp.status_code != 200:
                return _error(f"http_{resp.status_code}", latency_ms)

            data = resp.json()

    except httpx.TimeoutException:
        return _error("timeout", int((time.time() - start) * 1000))
    except Exception as exc:
        return _error(str(exc)[:80], int((time.time() - start) * 1000))

    # ── Extract every useful field from the response ─────────────────────

    choice      = (data.get("choices") or [{}])[0]
    message     = choice.get("message") or {}
    content     = message.get("content", "")
    finish      = choice.get("finish_reason", "stop")

    usage       = data.get("usage") or {}
    input_tok   = usage.get("prompt_tokens", 0)
    output_tok  = usage.get("completion_tokens", 0)

    citations   = data.get("citations") or []   # list of URL strings

    cost_usd = (
        input_tok  * settings.perplexity_input_price_per_million  / 1_000_000
        + output_tok * settings.perplexity_output_price_per_million / 1_000_000
        + settings.perplexity_search_fee_per_request
    )

    parsed = _parse_response(content, mode, existing_url)

    return {
        "success":        True,
        "provider":       "perplexity",
        "model":          data.get("model", settings.perplexity_model),
        "request_id":     data.get("id", ""),
        "input_tokens":   input_tok,
        "output_tokens":  output_tok,
        "cost_usd":       cost_usd,
        "latency_ms":     latency_ms,
        "stop_reason":    finish,
        "citations":      citations,
        "citation_count": len(citations),
        "raw_response":   content[:2000],
        "parsed":         parsed,
    }


# ---------------------------------------------------------------------------
# Prompt builders — address data included for precision
# ---------------------------------------------------------------------------

def _build_prompt(company, city, state, gmaps_street, gmaps_zip,
                  gmaps_phone, existing_url, mode) -> str:

    loc_parts = [f"{city}, {state}"]
    if gmaps_zip:      loc_parts[0] += f" {gmaps_zip}"
    if gmaps_street:   loc_parts.insert(0, gmaps_street)
    location = "\n".join(loc_parts)

    phone_line = f"\nPhone: {gmaps_phone}" if gmaps_phone else ""

    if mode == "find":
        return (
            "Find the official owned website for the exact business identity below. "
            "Return JSON only, with no markdown.\n\n"
            f"Company / DBA: {company}\n"
            f"Target location: {location}{phone_line}\n\n"
            "Hard rules:\n"
            "- Return one URL only, or null.\n"
            "- Return null unless the site or a cited source ties the URL to the same company/DBA.\n"
            "- Return null when the URL is for a same-name company at a different city/state.\n"
            "- Return null when you can verify the company name but cannot tie the URL to the target city/state or target address.\n"
            "- Same-name businesses in a different city/state are not matches.\n"
            "- Directories, BBB, SAFER/FMCSA, LinkedIn, Facebook, Yelp, and carrier profiles are evidence only, not final official websites.\n"
            "- If evidence is ambiguous, return null.\n"
            "- Do not infer from industry keywords alone.\n\n"
            "Return exactly this JSON shape:\n"
            '{"url": null, "official_name": "", '
            '"evidence_location": "", "evidence_url": "", '
            '"company_match": "exact|dba|acronym|partial|none", '
            '"location_match": "exact|nearby|state_only|contradiction|unknown", '
            '"is_official": false, "reason": "one sentence", "reject_reason": "one sentence or empty"}'
        )

    # validate mode: we have a URL to check, ask Perplexity to confirm or correct
    return (
        "Verify whether this URL is the official owned website for the exact business identity below. "
        "If the URL is wrong, return a corrected official URL only when strongly evidenced; otherwise return null. "
        "Return JSON only, with no markdown.\n\n"
        f"Company / DBA: {company}\n"
        f"Target location: {location}{phone_line}\n"
        f"URL to verify: {existing_url}\n\n"
        "Hard rules:\n"
        "- Same-name businesses in a different city/state are not matches.\n"
        "- If the URL identifies the company name but the location is different or cannot be tied to the target city/state, return null and is_correct false.\n"
        "- Directories, BBB, SAFER/FMCSA, LinkedIn, Facebook, Yelp, and carrier profiles are evidence only, not final official websites.\n"
        "- If the site does not identify the same company/DBA, return null and is_correct false.\n\n"
        "Return exactly this JSON shape:\n"
        '{"url": null, "is_correct": false, "official_name": "", '
        '"evidence_location": "", "evidence_url": "", '
        '"company_match": "exact|dba|acronym|partial|none", '
        '"location_match": "exact|nearby|state_only|contradiction|unknown", '
        '"is_official": false, "reason": "one sentence", "reject_reason": "one sentence or empty"}'
    )


def _parse_response(content: str, mode: str, existing_url: str | None) -> dict:
    objs = []
    for m in re.finditer(r'\{.*?\}', content, re.DOTALL):
        try:
            objs.append(json.loads(m.group()))
        except json.JSONDecodeError:
            continue
    if not objs:
        return {"url": None, "reason": "parse_error"}

    d = objs[-1]

    url = d.get("url") or None
    reason = str(d.get("reason", ""))
    reject_reason = str(d.get("reject_reason", ""))

    if "not found" in reason.lower() or reject_reason:
        url = None
    is_official = _to_bool_or_none(d.get("is_official"))
    is_correct = _to_bool_or_none(d.get("is_correct"))

    if url and not is_official and not is_correct:
        url = None

    if mode == "validate":
        # If Perplexity says the original URL is correct, use it
        if is_correct and not url:
            url = existing_url
        # If url is same as existing, mark as confirmed
        if url and existing_url and url.rstrip("/") == existing_url.rstrip("/"):
            is_correct = True

    return {
        "url":        url,
        "reason":     reason,
        "is_correct": is_correct,   # validate mode only
        "official_name":     str(d.get("official_name", "")),
        "evidence_location": str(d.get("evidence_location", "")),
        "evidence_url":      str(d.get("evidence_url", "")),
        "company_match":     str(d.get("company_match", "")),
        "location_match":    str(d.get("location_match", "")),
        "is_official":       is_official,
        "reject_reason":     reject_reason,
    }


def _to_bool_or_none(value):
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n", ""}:
            return False
    return None


def _error(error: str, latency_ms: int) -> dict:
    return {
        "success":        False,
        "provider":       "perplexity",
        "model":          settings.perplexity_model,
        "request_id":     "",
        "input_tokens":   0,
        "output_tokens":  0,
        "cost_usd":       0.0,
        "latency_ms":     latency_ms,
        "stop_reason":    "error",
        "citations":      [],
        "citation_count": 0,
        "raw_response":   f"error: {error}",
        "parsed":         {"url": None, "reason": f"error: {error}"},
        "error":          error,
    }
