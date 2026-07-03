"""
Haiku website validator.
Fetches a lightweight page snippet via Jina, then asks Haiku 4 factual questions.
Returns full response metadata — nothing left on the table.
"""

import re
import json
import time
import httpx
from anthropic import Anthropic
from .config import settings

JINA_BASE = "https://r.jina.ai/"
_anthropic_client: Anthropic = None


def _client() -> Anthropic:
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = Anthropic(api_key=settings.anthropic_api_key)
    return _anthropic_client


# ---------------------------------------------------------------------------
# Jina page fetch — title + description + 300 chars only
# ---------------------------------------------------------------------------

def fetch_page_snippet(url: str) -> tuple[str, str]:
    """
    Fetch website via Jina Reader, return only the first 1200 chars.
    Jina's output format puts title and description first — this captures both
    without the nav/footer noise that bloats token usage.

    Returns (snippet, error). error is empty string on success.
    """
    if not url or not url.startswith("http"):
        return "", "invalid_url"

    try:
        with httpx.Client(timeout=settings.jina_timeout_seconds, follow_redirects=True) as client:
            resp = client.get(
                f"{JINA_BASE}{url}",
                headers={"Accept": "text/plain", "X-Return-Format": "markdown"},
            )
            if resp.status_code != 200:
                return "", f"http_{resp.status_code}"
            text = resp.text.strip()
            if len(text) < 50:
                return "", "empty_response"
            # Take only the opening content — avoids token waste on menus/footers
            return text[:1200], ""
    except httpx.TimeoutException:
        return "", "timeout"
    except Exception as e:
        return "", str(e)[:60]


# ---------------------------------------------------------------------------
# Haiku validation call
# ---------------------------------------------------------------------------

def validate_with_haiku(
    company:       str,
    city:          str,
    state:         str,
    page_snippet:  str,
    gmaps_address: str = None,   # full street address from Maps
    gmaps_zip:     str = None,
) -> dict:
    """
    Ask Claude Haiku 4 factual questions about the website snippet.
    Uses street address + zip for precise location matching.

    Returns a fully populated metadata dict mapping to an api_calls row plus
    a 'signals' key containing the structured validation output.

    Signals: name_match, location_found, isn_mention, disqualifier, reasoning
    """
    address_line = ""
    if gmaps_address:
        address_line = f"\nMaps address: {gmaps_address}"
        if gmaps_zip:
            address_line += f" (zip: {gmaps_zip})"

    prompt = f"""You are verifying whether a website belongs to a specific company.

Company: {company}
City/State: {city}, {state}{address_line}

Website content (opening excerpt):
---
{page_snippet}
---

Answer these 4 questions based ONLY on the content above.

1. Does this website mention "{company}" or a very close variation?
   Answer: YES / PARTIAL / NO

2. Does this website mention {city}, {state}, or the address{f" ({gmaps_address})" if gmaps_address else ""}?
   Answer: YES / NO

3. Does this website mention ISNetworld, ISN, or contractor safety compliance?
   Answer: YES / NO

4. Is this clearly a different type of business (dental, legal, restaurant, hotel, retail) — NOT industrial/construction/transportation/energy/field services?
   Answer: YES / NO

Return ONLY this JSON:
{{"name_match": "...", "location_found": "...", "isn_mention": "...", "disqualifier": "...", "reasoning": "one sentence"}}"""

    start = time.time()

    try:
        msg = _client().messages.create(
            model      = settings.haiku_model,
            max_tokens = 200,
            messages   = [{"role": "user", "content": prompt}],
        )

        latency_ms    = int((time.time() - start) * 1000)
        input_tokens  = msg.usage.input_tokens
        output_tokens = msg.usage.output_tokens
        stop_reason   = msg.stop_reason
        request_id    = msg.id
        model_used    = msg.model
        raw_text      = msg.content[0].text.strip()

        cost_usd = (
            input_tokens  * settings.haiku_input_price_per_million  / 1_000_000
            + output_tokens * settings.haiku_output_price_per_million / 1_000_000
        )

        signals = _parse_signals(raw_text)

        return {
            "success":       True,
            "provider":      "anthropic",
            "model":         model_used,
            "request_id":    request_id,
            "input_tokens":  input_tokens,
            "output_tokens": output_tokens,
            "cost_usd":      cost_usd,
            "latency_ms":    latency_ms,
            "stop_reason":   stop_reason,
            "citations":     [],
            "citation_count":0,
            "raw_response":  raw_text[:500],
            "signals":       signals,
        }

    except Exception as exc:
        latency_ms = int((time.time() - start) * 1000)
        return {
            "success":       False,
            "provider":      "anthropic",
            "model":         settings.haiku_model,
            "request_id":    "",
            "input_tokens":  0,
            "output_tokens": 0,
            "cost_usd":      0.0,
            "latency_ms":    latency_ms,
            "stop_reason":   "error",
            "citations":     [],
            "citation_count":0,
            "raw_response":  f"error: {str(exc)[:80]}",
            "signals":       _fallback_signals(str(exc)[:60]),
        }


def _parse_signals(text: str) -> dict:
    m = re.search(r'\{.*\}', text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            pass
    return _fallback_signals("json_parse_error")


def _fallback_signals(error: str) -> dict:
    return {
        "name_match":     "NO",
        "location_found": "NO",
        "isn_mention":    "NO",
        "disqualifier":   "NO",
        "reasoning":      f"validation_error: {error}",
    }


# ---------------------------------------------------------------------------
# Cost helper (also used by other modules)
# ---------------------------------------------------------------------------

def compute_haiku_cost(input_tokens: int, output_tokens: int) -> float:
    return (
        input_tokens  * settings.haiku_input_price_per_million  / 1_000_000
        + output_tokens * settings.haiku_output_price_per_million / 1_000_000
    )
