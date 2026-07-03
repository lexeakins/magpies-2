import re
from typing import Optional
from urllib.parse import urlparse

from rapidfuzz import fuzz


def name_similarity(input_name: str, listing_name: Optional[str]) -> float:
    """Token-sorted fuzzy ratio between two company names. Returns 0.0–1.0."""
    if not input_name or not listing_name:
        return 0.0
    return fuzz.token_sort_ratio(input_name.lower(), listing_name.lower()) / 100.0


_LEGAL_TERMS = {
    "llc", "l", "ltd", "inc", "incorporated", "corp", "corporation", "co",
    "company", "limited", "pllc", "lp", "llp", "dba", "d", "b", "a",
}

_GENERIC_TERMS = _LEGAL_TERMS | {
    "usa", "united", "states", "home",
}

_BUSINESS_DESCRIPTOR_TERMS = {
    "advanced", "company", "companies", "contractor", "contractors",
    "construction", "consultants", "electrical", "electric", "environmental",
    "industrial", "industries", "installation", "maintenance", "mechanical",
    "metal", "metals", "outdoor", "equipment", "repair", "resources", "roofing",
    "service", "services", "solutions", "technologies",
}

_DIRECTORY_OR_SOCIAL_DOMAINS = {
    "bbb", "facebook", "linkedin", "instagram", "yelp", "dnb", "zoominfo",
    "seamless", "safer", "fmcsa", "loopnet", "locally", "bubba", "loadwrap",
    "dcontrol", "fenderr", "carriersource",
}

_STATE_ALIASES = {
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
_STATE_NAMES_BY_ABBR = {v: k for k, v in _STATE_ALIASES.items()}


def _norm_text(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())).strip()


def _tokens(value: Optional[str]) -> list[str]:
    return [t for t in _norm_text(value).split() if t and t not in _LEGAL_TERMS]


def _distinctive_tokens(value: Optional[str]) -> list[str]:
    return [t for t in _tokens(value) if t not in _GENERIC_TERMS]


def _domain_identity_tokens(value: Optional[str]) -> list[str]:
    # Domain matching needs more identity words than name matching. Words like
    # "industrial" or "maintenance" may be generic by industry, but they still
    # distinguish Agility Industrial from Agility Logistics.
    return [t for t in _tokens(value) if t not in _LEGAL_TERMS and len(t) >= 2]


def _business_identity_tokens(value: Optional[str]) -> list[str]:
    ignored = _GENERIC_TERMS | _BUSINESS_DESCRIPTOR_TERMS
    return [t for t in _tokens(value) if t not in ignored and len(t) >= 2]


def _company_variants(company: str) -> list[str]:
    raw_parts = re.split(r"\b(?:d\.?\s*b\.?\s*a\.?|doing business as)\b", company or "", flags=re.I)
    variants = [p.strip(" -()") for p in raw_parts if p.strip(" -()")]
    variants.append(company or "")

    # Parenthesized acronyms often carry the practical brand identity.
    variants.extend(re.findall(r"\(([A-Za-z0-9&\-\s]{2,})\)", company or ""))

    seen, out = set(), []
    for v in variants:
        n = _norm_text(v)
        if n and n not in seen:
            seen.add(n)
            out.append(v)
    return out


def _acronym(value: Optional[str]) -> str:
    toks = _tokens(value)
    return "".join(t[0] for t in toks if t not in _GENERIC_TERMS and t)


def _domain_root(url: Optional[str]) -> str:
    if not url:
        return ""
    parsed = urlparse(url if "://" in url else f"https://{url}")
    host = (parsed.netloc or parsed.path.split("/")[0]).lower()
    host = host.split("@")[-1].split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    parts = host.split(".")
    if len(parts) >= 2:
        host = parts[-2]
    return re.sub(r"[^a-z0-9]+", "", host)


def _domain_family(url: Optional[str]) -> str:
    parsed = urlparse(url if url and "://" in url else f"https://{url or ''}")
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host.split(".")[0]


def _same_domain(left: Optional[str], right: Optional[str]) -> bool:
    return bool(left and right and _domain_family(left) == _domain_family(right))


def _is_directory_or_social(url: Optional[str]) -> bool:
    root = _domain_family(url)
    return any(marker in root for marker in _DIRECTORY_OR_SOCIAL_DOMAINS)


def _business_token_overlap(source_company: str, candidate_name: Optional[str]) -> Optional[float]:
    if not candidate_name:
        return None
    best = None
    candidate_tokens = set(_business_identity_tokens(candidate_name))
    if not candidate_tokens:
        return None
    for variant in _company_variants(source_company):
        source_tokens = set(_business_identity_tokens(variant))
        if not source_tokens:
            continue
        overlap = len(source_tokens & candidate_tokens) / len(source_tokens)
        best = overlap if best is None else max(best, overlap)
    return best


def business_identity_overlap(source_company: str, candidate_name: Optional[str]) -> Optional[float]:
    """Public wrapper for distinctive business-token overlap diagnostics."""
    return _business_token_overlap(source_company, candidate_name)


def _state_abbr(value: Optional[str]) -> str:
    n = _norm_text(value)
    if len(n) == 2:
        return n
    return _STATE_ALIASES.get(n, "")


def _known_state_abbrevs_in_text(text: Optional[str]) -> set[str]:
    n = _norm_text(text)
    found = set()
    words = set(n.split())
    for name, abbr in _STATE_ALIASES.items():
        if abbr in words or name in n:
            found.add(abbr)
    return found


def _best_company_score(source_company: str, candidate_name: Optional[str], url: Optional[str]) -> tuple[int, int]:
    variants = _company_variants(source_company)

    name_score = 0
    if candidate_name:
        candidate_norm = _norm_text(candidate_name)
        candidate_tokens = set(_domain_identity_tokens(candidate_name))
        for variant in variants:
            variant_tokens = set(_distinctive_tokens(variant))
            candidate_distinctive_tokens = set(_distinctive_tokens(candidate_name))
            token_overlap = (
                len(variant_tokens & candidate_distinctive_tokens) / len(variant_tokens)
                if variant_tokens else 0
            )
            raw_score = max(
                fuzz.token_sort_ratio(_norm_text(variant), _norm_text(candidate_name)),
                fuzz.WRatio(_norm_text(variant), _norm_text(candidate_name)),
            )
            if token_overlap < 0.5 and len(variant_tokens) > 1:
                raw_score = min(raw_score, 60)
            name_score = max(name_score, raw_score)

            acr = _acronym(variant)
            if len(acr) >= 3 and (acr in candidate_tokens or candidate_norm.startswith(acr + " ")):
                name_score = max(name_score, 88)

    root = _domain_root(url)
    domain_score = 0
    if root:
        for variant in variants:
            vtoks = _domain_identity_tokens(variant)
            joined = "".join(vtoks)
            matched_tokens = 0
            acronym_match = False
            if joined:
                domain_score = max(domain_score, fuzz.partial_ratio(joined, root))
            for tok in vtoks:
                if len(tok) >= 3:
                    token_score = fuzz.partial_ratio(tok, root)
                    domain_score = max(domain_score, token_score)
                    if tok in root or token_score >= 92:
                        matched_tokens += 1

            acr = _acronym(variant)
            if acr and len(acr) >= 2 and acr in root:
                domain_score = max(domain_score, 90)
                acronym_match = True

            if len(vtoks) > 1 and matched_tokens < 2 and joined not in root and not acronym_match:
                domain_score = min(domain_score, 72)

            if len(root) <= 3 and len(_domain_identity_tokens(variant)) > 1 and not acronym_match:
                domain_score = min(domain_score, 55)

    return int(round(name_score)), int(round(domain_score))


def _location_level(expected_city: Optional[str], expected_state: Optional[str],
                    address: Optional[str], gmaps_location_match: Optional[str],
                    evidence_location: Optional[str]) -> str:
    expected_state_abbr = _state_abbr(expected_state)
    city_norm = _norm_text(expected_city).replace(" ", "")
    location_text = " ".join(x for x in [address, evidence_location] if x)
    loc_norm = _norm_text(location_text)
    states_found = _known_state_abbrevs_in_text(location_text)

    if expected_state_abbr and states_found and expected_state_abbr not in states_found:
        return "contradiction"

    if city_norm and city_norm in loc_norm.replace(" ", "") and expected_state_abbr in states_found:
        return "exact"

    gm = (gmaps_location_match or "").lower()
    if gm == "exact":
        return "exact"
    if gm == "partial":
        return "nearby"

    if expected_state_abbr and expected_state_abbr in states_found:
        return "state_only"

    return "unknown"


def score_candidate_url(
    *,
    source_company: str,
    source_city: Optional[str],
    source_state: Optional[str],
    source_country: Optional[str],
    candidate_url: Optional[str],
    candidate_source: Optional[str],
    candidate_name: Optional[str] = None,
    candidate_address: Optional[str] = None,
    gmaps_location_match: Optional[str] = None,
    evidence_location: Optional[str] = None,
    evidence_url: Optional[str] = None,
    evidence_company_match: Optional[str] = None,
    evidence_location_match: Optional[str] = None,
    evidence_is_official: Optional[bool] = None,
    provider_confidence: Optional[int] = None,
) -> dict:
    """
    Deterministic identity score for a candidate website.

    Optional provider scores are recorded as context only; they do not directly
    increase this score. The score is meant to answer "is this URL tied to the
    source company identity?", not "did a provider feel confident?"
    """
    if not candidate_url:
        return {
            "candidate_url": None,
            "identity_score": 0,
            "identity_verdict": "no_candidate",
            "identity_reason": "No candidate URL was returned.",
            "company_match_score": 0,
            "domain_match_score": 0,
            "location_match_level": "unknown",
            "provider_confidence": provider_confidence,
        }

    company_score, domain_score = _best_company_score(source_company, candidate_name, candidate_url)
    location_level = _location_level(
        source_city, source_state, candidate_address, gmaps_location_match, evidence_location
    )
    structured_company_match = (evidence_company_match or "").lower()
    structured_location_match = (evidence_location_match or "").lower()
    official_evidence = bool(
        candidate_source == "perplexity"
        and evidence_is_official
        and structured_company_match in {"exact", "dba", "acronym"}
        and _same_domain(candidate_url, evidence_url)
    )

    if official_evidence:
        company_score = max(company_score, 88)
        domain_score = max(domain_score, 75)

    if candidate_source == "perplexity" and structured_location_match == "contradiction":
        location_level = "contradiction"
    elif candidate_source == "perplexity" and structured_location_match == "exact" and location_level != "contradiction":
        location_level = "exact"
    elif candidate_source == "perplexity" and structured_location_match == "state_only" and location_level == "unknown":
        location_level = "state_only"

    loc_points = {
        "exact": 25,
        "nearby": 12,
        "state_only": 8,
        "unknown": 0,
        "contradiction": -35,
    }[location_level]
    source_bonus = 5 if candidate_source in {"gmaps", "bing_maps"} and candidate_address else 0
    source_bonus += 6 if official_evidence else 0
    source_bonus += 5 if candidate_source == "manual_verified" else 0
    identity_bonus = 10 if company_score >= 85 and domain_score >= 75 else 0
    directory_penalty = 20 if _is_directory_or_social(candidate_url) else 0
    historical_source = candidate_source in {"salesforce", "legacy_db"}

    score = int(round(company_score * 0.50 + domain_score * 0.25))
    score += loc_points + source_bonus + identity_bonus - directory_penalty
    score = max(0, min(100, score))

    if max(company_score, domain_score) < 50:
        score = min(score, 25)
    if location_level == "contradiction":
        if max(company_score, domain_score) >= 95:
            score = min(score, 75)
        else:
            score = min(score, 45)
    if candidate_source == "perplexity" and official_evidence and location_level == "unknown":
        score = min(score, 84)
    if _is_directory_or_social(candidate_url) and candidate_source != "manual_verified":
        score = min(score, 45)
    if historical_source:
        # Historical CRM/database websites can be stale or copied from unrelated
        # records. Require stronger identity evidence than live search sources.
        if location_level != "exact":
            score = min(score, 84)
        if company_score < 70:
            score = min(score, 45)
        elif company_score < 85 and domain_score < 85:
            score = min(score, 49)
        elif company_score < 75 and domain_score >= 90:
            score = min(score, 49)
    if candidate_source == "legacy_db":
        business_overlap = _business_token_overlap(source_company, candidate_name)
        if business_overlap == 0 and company_score < 92:
            score = min(score, 49)
        if domain_score < 55 and company_score < 98:
            score = min(score, 49)

    if score >= 85:
        verdict = "accepted"
    elif score >= 50:
        verdict = "review"
    else:
        verdict = "rejected"

    reason_bits = [
        f"company={company_score}",
        f"domain={domain_score}",
        f"location={location_level}",
    ]
    if _is_directory_or_social(candidate_url):
        reason_bits.append("directory_or_social_url")
    if official_evidence:
        reason_bits.append("official_evidence")
    if candidate_source == "manual_verified":
        reason_bits.append("manual_verified")
    if provider_confidence is not None and candidate_source == "gmaps":
        reason_bits.append(f"gmaps_score={provider_confidence}")
    if candidate_source == "legacy_db":
        business_overlap = _business_token_overlap(source_company, candidate_name)
        if business_overlap is not None:
            reason_bits.append(f"business_overlap={round(business_overlap, 2)}")

    return {
        "candidate_url": candidate_url,
        "identity_score": score,
        "identity_verdict": verdict,
        "identity_reason": "; ".join(reason_bits),
        "company_match_score": company_score,
        "domain_match_score": domain_score,
        "location_match_level": location_level,
        "provider_confidence": provider_confidence,
    }


def gmaps_confidence_score(
    gmaps_found:    bool,
    input_name:     str,
    listing_name:   Optional[str],
    location_match: Optional[str],
    has_website:    bool,
    has_phone:      bool,
    has_address:    bool,
) -> int:
    """
    Confidence score derived entirely from Google Maps signals — no AI required.
    Always populated regardless of which AI stages run. Max 85 pts.

    Baseline (found on Maps):  20
    Name similarity:            0 / 6 / 12 / 20
    Location match:             0 / 5 / 10 / 20
    Has website URL:            15
    Has phone:                   5
    Has address:                 5
    """
    if not gmaps_found:
        return 0

    score = 20  # baseline for finding a Maps listing

    sim = name_similarity(input_name, listing_name)
    if sim >= 0.90:
        score += 20
    elif sim >= 0.70:
        score += 12
    elif sim >= 0.50:
        score += 6

    loc_pts = {"exact": 20, "partial": 10, "unknown": 5, "none": 0}
    score += loc_pts.get((location_match or "none").lower(), 0)

    if has_website:  score += 15
    if has_phone:    score += 5
    if has_address:  score += 5

    return min(score, 85)


def haiku_confidence_score(
    gmaps_score:    int,
    haiku_signals:  dict,
) -> int:
    """
    Add Haiku's validation signals on top of the GMaps score. Max 100.

    name_match:      YES +25  PARTIAL +12  NO 0
    location_found:  YES +20
    isn_mention:     YES +15 (bonus)
    disqualifier:    YES -50
    """
    score = gmaps_score

    nm = (haiku_signals.get("name_match") or "NO").upper()
    if nm == "YES":     score += 25
    elif nm == "PARTIAL": score += 12

    if (haiku_signals.get("location_found") or "NO").upper() == "YES":
        score += 20

    if (haiku_signals.get("isn_mention") or "NO").upper() == "YES":
        score += 15

    if (haiku_signals.get("disqualifier") or "NO").upper() == "YES":
        score = max(0, score - 50)

    return max(0, min(100, score))


def confidence_tier(score: int, high: int = 70, low: int = 40) -> str:
    if score is None: return None
    if score >= high: return "High"
    if score >= low:  return "Medium"
    return "Low"


def deprecated_pick_final_score(*scores: Optional[int]) -> Optional[int]:
    """Return the last non-None score — the most authoritative signal."""
    result = None
    for s in scores:
        if s is not None:
            result = s
    return result
