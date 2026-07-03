"""
User-curated known website association lookup.

Manual associations are local, auditable evidence. They can outrank stale CRM
data, but the candidate URL still goes through deterministic identity scoring.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Optional

from . import database as db
from .scorer import name_similarity, score_candidate_url


STATE_ALIASES = {
    "alabama": "al", "alaska": "ak", "arizona": "az", "arkansas": "ar",
    "california": "ca", "colorado": "co", "connecticut": "ct",
    "delaware": "de", "florida": "fl", "georgia": "ga", "hawaii": "hi",
    "idaho": "id", "illinois": "il", "indiana": "in", "iowa": "ia",
    "kansas": "ks", "kentucky": "ky", "louisiana": "la", "maine": "me",
    "maryland": "md", "massachusetts": "ma", "michigan": "mi",
    "minnesota": "mn", "mississippi": "ms", "missouri": "mo",
    "montana": "mt", "nebraska": "ne", "nevada": "nv",
    "new hampshire": "nh", "new jersey": "nj", "new mexico": "nm",
    "new york": "ny", "north carolina": "nc", "north dakota": "nd",
    "ohio": "oh", "oklahoma": "ok", "oregon": "or", "pennsylvania": "pa",
    "rhode island": "ri", "south carolina": "sc", "south dakota": "sd",
    "tennessee": "tn", "texas": "tx", "utah": "ut", "vermont": "vt",
    "virginia": "va", "washington": "wa", "west virginia": "wv",
    "wisconsin": "wi", "wyoming": "wy",
}


@dataclass
class ManualCandidate:
    association_id: int
    source: str
    name: str
    website: str
    city: str = ""
    state: str = ""
    country: str = ""
    association_type: str = ""
    notes: str = ""
    verified_by: str = ""
    verified_at: str = ""


def lookup_manual_associations(
    *,
    company: str,
    city: str,
    state: str,
    country: str,
    associations: list[dict] | None = None,
) -> dict:
    started = time.time()
    candidates: list[ManualCandidate] = []

    for row in associations if associations is not None else db.list_manual_associations(include_inactive=False):
        if not _location_matches(row, city, state, country):
            continue
        if _name_match_level(company, row.get("source_company", "")) == "none":
            continue
        candidates.append(_row_to_candidate(row))

    evaluations = []
    for candidate in candidates:
        evaluation = score_candidate_url(
            source_company=company,
            source_city=city,
            source_state=state,
            source_country=country,
            candidate_url=candidate.website,
            candidate_source="manual_verified",
            candidate_name=candidate.name,
            candidate_address=_join_location(candidate.city, candidate.state, candidate.country),
        )
        evaluations.append({"candidate": candidate, "evaluation": evaluation})

    best = None
    if evaluations:
        best = max(
            evaluations,
            key=lambda item: (
                1 if item["evaluation"].get("identity_verdict") == "accepted" else 0,
                item["evaluation"].get("identity_score", 0),
            ),
        )

    return {
        "candidates_found": len(evaluations),
        "latency_ms": int((time.time() - started) * 1000),
        "best": best,
        "accepted": [
            item for item in evaluations
            if item["evaluation"].get("identity_verdict") == "accepted"
        ],
    }


def _row_to_candidate(row: dict) -> ManualCandidate:
    return ManualCandidate(
        association_id=int(row.get("id") or 0),
        source="manual_verified",
        name=str(row.get("source_company") or ""),
        website=str(row.get("known_url") or ""),
        city=str(row.get("source_city") or ""),
        state=str(row.get("source_state") or ""),
        country=str(row.get("source_country") or ""),
        association_type=str(row.get("association_type") or ""),
        notes=str(row.get("notes") or ""),
        verified_by=str(row.get("verified_by") or ""),
        verified_at=str(row.get("verified_at") or ""),
    )


def _location_matches(row: dict, city: str, state: str, country: str) -> bool:
    row_state = _state_norm(row.get("source_state"))
    row_city = _norm(row.get("source_city"))
    row_country = _norm(row.get("source_country"))
    if row_state and row_state != _state_norm(state):
        return False
    if row_city and row_city != _norm(city):
        return False
    if row_country and _norm(country) and row_country != _norm(country):
        return False
    return True


def _name_match_level(company: str, known_company: Optional[str]) -> str:
    score = name_similarity(company, known_company)
    if score >= 0.92:
        return "exact"
    if score >= 0.82:
        return "near"

    left = set(_tokens(company))
    right = set(_tokens(known_company))
    if left and right and len(left & right) / max(1, len(left)) >= 0.75:
        return "near"
    return "none"


def _tokens(value: Optional[str]) -> list[str]:
    ignored = {
        "llc", "inc", "ltd", "corp", "corporation", "company", "co",
        "dba", "doing", "business", "as",
    }
    return [t for t in _norm(value).split() if t not in ignored]


def _norm(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())).strip()


def _state_norm(value: Optional[str]) -> str:
    n = _norm(value)
    if len(n) == 2:
        return n
    return STATE_ALIASES.get(n, n)


def _join_location(city: str, state: str, country: str) -> str:
    return ", ".join(x for x in [city, state, country] if x)
