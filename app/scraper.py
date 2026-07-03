"""
Scrape a single lead via Google Maps.
Imports exclusively from app.gmaps — no dependency on the external magpies.py.
"""

import time
from .gmaps import setup_driver, search_google_maps_candidates, geocode_location
from .scorer import score_candidate_url
from .config import settings


def scrape_lead(lead: dict, driver=None, stage_callback=None) -> dict:
    """
    Scrape one lead. Returns a result dict with all gmaps_ fields.

    lead keys: company, city, state, country, source_sheet
    """
    start  = time.time()
    owns_driver = driver is None
    try:
        if owns_driver:
            driver = setup_driver()

        lat, lng = geocode_location(
            lead.get("city"),
            lead.get("state"),
            lead.get("country", "United States"),
        )

        def strong_match(candidate: dict) -> bool:
            if candidate.get("source") != "gmaps":
                return False
            evaluation = score_candidate_url(
                source_company=lead["company"],
                source_city=lead.get("city"),
                source_state=lead.get("state"),
                source_country=lead.get("country", "United States"),
                candidate_url=candidate.get("url"),
                candidate_source="gmaps",
                candidate_name=candidate.get("title"),
                candidate_address=candidate.get("address_or_snippet"),
                gmaps_location_match=candidate.get("location_match"),
            )
            threshold = int(lead.get("gmaps_strong_stop_score") or settings.gmaps_strong_stop_score)
            return evaluation.get("identity_verdict") == "accepted" and evaluation.get("identity_score", 0) >= threshold

        search = search_google_maps_candidates(
            driver,
            lead["company"],
            lead.get("country", "United States"),
            city=lead.get("city"),
            state=lead.get("state"),
            lat=lat,
            lng=lng,
            max_per_mode=int(lead.get("gmaps_max_candidates_per_mode") or settings.gmaps_max_candidates_per_mode),
            stop_when=strong_match,
            stage_callback=stage_callback,
        )

        candidates = search.get("candidates") or []
        primary = _select_primary_candidate(candidates)
        raw = (primary or {}).get("raw") or _empty_result()
        result = dict(raw)
        result["gmaps_candidates"] = candidates
        result["gmaps_attempts"] = search.get("attempts", 0)

        result["company"]      = lead["company"]
        result["city"]         = lead.get("city", "")
        result["state"]        = lead.get("state", "")
        result["country"]      = lead.get("country", "United States")
        result["source_sheet"] = lead.get("source_sheet", "")
        result["duration"]     = round(time.time() - start, 2)
        return result

    except Exception as exc:
        return _error_result(lead, str(exc)[:100], duration=time.time() - start)

    finally:
        if owns_driver and driver:
            try:
                driver.quit()
            except Exception:
                pass


def _error_result(lead: dict, error: str, duration: float = 0.0) -> dict:
    return {
        "company":            lead.get("company", ""),
        "city":               lead.get("city", ""),
        "state":              lead.get("state", ""),
        "country":            lead.get("country", ""),
        "source_sheet":       lead.get("source_sheet", ""),
        "found":              False,
        "gmaps_listing_name": None,
        "website":            None,
        "phone":              None,
        "address":            None,
        "location_match":     None,
        "gmaps_url":          None,
        "error":              error,
        "duration":           round(duration, 2),
        "gmaps_candidates":   [],
        "gmaps_attempts":     0,
    }


def _empty_result() -> dict:
    return {
        "found":              False,
        "gmaps_listing_name": None,
        "website":            None,
        "phone":              None,
        "address":            None,
        "location_match":     None,
        "gmaps_url":          None,
    }


def _select_primary_candidate(candidates: list[dict]) -> dict | None:
    found = [c for c in candidates if c.get("found")]
    with_url = [c for c in found if c.get("url")]
    if with_url:
        return with_url[0]
    if found:
        return found[0]
    return candidates[0] if candidates else None
