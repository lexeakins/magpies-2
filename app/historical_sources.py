"""
Read-only historical website candidate lookup.

These sources are evidence providers, not authorities. Every returned URL still
flows through the deterministic identity scorer before it can become final_url.
"""

from __future__ import annotations

import os
import re
import time
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from dotenv import load_dotenv

from .config import settings
from .scorer import score_candidate_url


ROOT_DIR = Path(__file__).resolve().parents[3]
MAGPIE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")
load_dotenv(MAGPIE_DIR / ".env")
_THREAD_LOCAL = threading.local()


BLOCKED_SQL_PATTERNS = [
    r"\binsert\b", r"\bupdate\b", r"\bdelete\b", r"\bmerge\b",
    r"\balter\b", r"\bdrop\b", r"\btruncate\b", r"\bcreate\b",
    r"\bexec(?:ute)?\b",
]

GENERIC_NAME_QUERY_TERMS = {
    "advanced", "company", "companies", "contractor", "contractors",
    "construction", "consultants", "electrical", "electric", "environmental",
    "industrial", "industries", "installation", "maintenance", "mechanical",
    "metal", "metals", "outdoor", "equipment", "repair", "resources", "roofing",
    "service", "services", "solutions", "technologies",
}

GENERIC_EMAIL_DOMAINS = {
    "aol.com", "att.net", "bellsouth.net", "comcast.net", "cox.net",
    "earthlink.net", "gmail.com", "googlemail.com", "hotmail.com",
    "icloud.com", "live.com", "mac.com", "me.com", "msn.com",
    "outlook.com", "proton.me", "protonmail.com", "sbcglobal.net",
    "verizon.net", "wildblue.net", "yahoo.com", "ymail.com",
    "charter.net", "frontier.com", "hughes.net", "windstream.net",
}

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
STATE_NAMES = {abbr: name.title() for name, abbr in STATE_ALIASES.items()}


@dataclass
class HistoricalCandidate:
    source: str
    record_type: str
    record_id: str
    name: str
    website: str
    city: str = ""
    state: str = ""
    country: str = ""
    raw_source: str = ""


def lookup_historical_candidates(
    *,
    company: str,
    city: str,
    state: str,
    country: str,
    use_salesforce: bool,
    use_legacy: bool,
    source_objects: Optional[str] = None,
) -> dict:
    started = time.time()
    candidates: list[HistoricalCandidate] = []
    errors: list[str] = []
    source_counts = {"salesforce": 0, "legacy_db": 0}
    diagnostics = {}

    if use_salesforce:
        try:
            sf_candidates = _lookup_salesforce(company, city, state, country, source_objects)
            source_counts["salesforce"] = len(sf_candidates)
            candidates.extend(sf_candidates)
        except Exception as exc:
            errors.append(f"salesforce: {str(exc)[:160]}")

    if use_legacy:
        try:
            legacy_candidates, legacy_diag = _lookup_legacy(company, city, state, country)
            source_counts["legacy_db"] = len(legacy_candidates)
            diagnostics["legacy_db"] = legacy_diag
            candidates.extend(legacy_candidates)
        except Exception as exc:
            errors.append(f"legacy_db: {str(exc)[:160]}")

    evaluations = []
    for candidate in _dedupe_candidates(candidates):
        evaluation = score_candidate_url(
            source_company=company,
            source_city=city,
            source_state=state,
            source_country=country,
            candidate_url=candidate.website,
            candidate_source=candidate.source,
            candidate_name=candidate.name,
            candidate_address=_join_location(candidate.city, candidate.state, candidate.country),
        )
        evaluations.append({"candidate": candidate, "evaluation": evaluation})

    best = None
    if evaluations:
        best = max(
            evaluations,
            key=lambda item: (
                1 if item["evaluation"].get("identity_verdict") in {"accepted", "review"} else 0,
                item["evaluation"].get("identity_score", 0),
                1 if item["candidate"].source == "salesforce" else 0,
            ),
        )

    accepted = [
        item for item in evaluations
        if item["evaluation"].get("identity_verdict") == "accepted"
        and item["evaluation"].get("identity_score", 0) >= settings.historical_enrichment_min_score
    ]

    return {
        "candidates_found": len(evaluations),
        "source_counts": source_counts,
        "diagnostics": diagnostics,
        "errors": errors,
        "latency_ms": int((time.time() - started) * 1000),
        "best": best,
        "accepted": accepted,
    }


def _lookup_salesforce(company: str, city: str, state: str, country: str,
                       source_objects: Optional[str]) -> list[HistoricalCandidate]:
    try:
        from simple_salesforce import Salesforce
    except ImportError as exc:
        raise RuntimeError("simple-salesforce is not installed") from exc

    username = _env("SF_USERNAME")
    password = _env("SF_PASSWORD")
    token = _env("SF_SECURITY_TOKEN")
    domain = _env("SF_DOMAIN", "login")
    if not (username and password and token):
        raise RuntimeError("missing Salesforce credentials")

    sf = _salesforce_client(Salesforce, username, password, token, domain)
    objects = {
        obj.strip().lower()
        for obj in (source_objects or settings.salesforce_enrichment_objects).split(",")
        if obj.strip()
    }
    candidates: list[HistoricalCandidate] = []
    state_q = _soql_escape(state)
    account_name_filter = _soql_name_filter("Name", company)
    lead_name_filter = _soql_name_filter("Company", company)
    contact_account_name_filter = _soql_name_filter("Account.Name", company)
    if not (account_name_filter and lead_name_filter and contact_account_name_filter):
        return []

    if "account" in objects:
        soql = (
            "SELECT Id, Name, Website, BillingCity, BillingState, BillingCountry "
            "FROM Account "
            f"WHERE Website != null AND BillingState = '{state_q}' "
            f"AND ({account_name_filter}) "
            "LIMIT 25"
        )
        candidates.extend(_sf_records_to_candidates(sf.query_all(soql).get("records", []), "Account"))

    if "lead" in objects:
        soql = (
            "SELECT Id, Company, Website, City, State, Country "
            "FROM Lead "
            f"WHERE Website != null AND State = '{state_q}' "
            f"AND ({lead_name_filter}) "
            "LIMIT 25"
        )
        candidates.extend(_sf_records_to_candidates(sf.query_all(soql).get("records", []), "Lead"))

    if "contact" in objects:
        soql = (
            "SELECT Id, AccountId, Account.Name, Account.Website, "
            "Account.BillingCity, Account.BillingState, Account.BillingCountry "
            "FROM Contact "
            f"WHERE Account.Website != null AND Account.BillingState = '{state_q}' "
            f"AND ({contact_account_name_filter}) "
            "LIMIT 25"
        )
        candidates.extend(_sf_records_to_candidates(sf.query_all(soql).get("records", []), "Contact"))

    return candidates


def _sf_records_to_candidates(records: Iterable[dict], record_type: str) -> list[HistoricalCandidate]:
    out = []
    for record in records:
        if record_type == "Account":
            out.append(HistoricalCandidate(
                source="salesforce",
                record_type="Account",
                record_id=str(record.get("Id") or ""),
                name=str(record.get("Name") or ""),
                website=str(record.get("Website") or ""),
                city=str(record.get("BillingCity") or ""),
                state=str(record.get("BillingState") or ""),
                country=str(record.get("BillingCountry") or ""),
                raw_source="Salesforce Account.Website",
            ))
        elif record_type == "Lead":
            out.append(HistoricalCandidate(
                source="salesforce",
                record_type="Lead",
                record_id=str(record.get("Id") or ""),
                name=str(record.get("Company") or ""),
                website=str(record.get("Website") or ""),
                city=str(record.get("City") or ""),
                state=str(record.get("State") or ""),
                country=str(record.get("Country") or ""),
                raw_source="Salesforce Lead.Website",
            ))
        elif record_type == "Contact":
            account = record.get("Account") or {}
            out.append(HistoricalCandidate(
                source="salesforce",
                record_type="Contact.Account",
                record_id=str(record.get("AccountId") or record.get("Id") or ""),
                name=str(account.get("Name") or ""),
                website=str(account.get("Website") or ""),
                city=str(account.get("BillingCity") or ""),
                state=str(account.get("BillingState") or ""),
                country=str(account.get("BillingCountry") or ""),
                raw_source="Salesforce Contact.Account.Website",
            ))
    return [c for c in out if c.website]


def _lookup_legacy(company: str, city: str, state: str, country: str) -> tuple[list[HistoricalCandidate], dict]:
    query = settings.legacy_enrichment_query
    if not query:
        raise RuntimeError("MAGPIE_LEGACY_ENRICHMENT_QUERY is not configured")
    _assert_read_only_sql(query)

    try:
        import pyodbc
    except ImportError as exc:
        raise RuntimeError("pyodbc is not installed") from exc

    state_abbr, state_full = _state_variants(state)
    name_like = f"%{_query_term(company)}%"
    diag = {
        "query_city": city,
        "query_state": state,
        "query_state_abbr": state_abbr,
        "query_state_full": state_full,
        "query_country": country,
        "query_name_like": name_like,
        "raw_rows": 0,
        "rows_with_email": 0,
        "usable_domains": 0,
        "filtered_domains": 0,
    }

    conn = _legacy_connection(pyodbc)
    try:
        params = {
            "company": company,
            "city": city,
            "state": state,
            "state_abbr": state_abbr,
            "state_full": state_full,
            "country": country,
            "name_like": name_like,
        }
        sql, values = _bind_named_params(query, params)
        cur = conn.cursor()
        cur.execute(sql, *values)
        columns = [c[0].lower() for c in cur.description]
        rows = cur.fetchall()
        diag["raw_rows"] = len(rows)
    except Exception:
        _close_legacy_connection()
        raise

    candidates = []
    for row in rows[:50]:
        data = {columns[i]: row[i] for i in range(len(columns))}
        explicit_website = str(_first(data, "website", "web_site", "url", "company_website") or "")
        email_stats = _email_domain_stats(_first(data, "email"), _first(data, "email2"))
        if email_stats["seen"]:
            diag["rows_with_email"] += 1
        diag["usable_domains"] += len(email_stats["usable"])
        diag["filtered_domains"] += len(email_stats["filtered"])
        email_domains = email_stats["usable"]
        websites = [explicit_website] if explicit_website else [f"https://{domain}" for domain in email_domains]
        loc = _legacy_location(data, city, state, country)
        for website in websites:
            if not website:
                continue
            domain = _domain_from_url(website)
            source_detail = "website field" if explicit_website else f"email domain: {domain}"
            candidates.append(HistoricalCandidate(
                source="legacy_db",
                record_type=str(_first(data, "record_type", "source_table", "table_name") or settings.legacy_enrichment_source_label),
                record_id=str(_first(data, "id", "record_id", "company_id", "account_id", "customerid") or domain or ""),
                name=str(_first(data, "name", "company", "company_name", "account_name", "customername", "c_company", "alt_company_name", "parent_company", "ship_name") or ""),
                website=website,
                city=loc["city"],
                state=loc["state"],
                country=loc["country"],
                raw_source=f"{_first(data, 'raw_source') or settings.legacy_enrichment_source_label}; {source_detail}",
            ))
    return candidates, diag


def _salesforce_client(Salesforce, username: str, password: str, token: str, domain: str):
    key = (username, domain)
    cached = getattr(_THREAD_LOCAL, "salesforce_client", None)
    if cached and getattr(_THREAD_LOCAL, "salesforce_client_key", None) == key:
        return cached
    client = Salesforce(username=username, password=password, security_token=token, domain=domain)
    _THREAD_LOCAL.salesforce_client = client
    _THREAD_LOCAL.salesforce_client_key = key
    return client


def _legacy_connection(pyodbc):
    key = _legacy_connection_string()
    cached = getattr(_THREAD_LOCAL, "legacy_connection", None)
    if cached and getattr(_THREAD_LOCAL, "legacy_connection_key", None) == key:
        return cached
    conn = pyodbc.connect(key, autocommit=True)
    _THREAD_LOCAL.legacy_connection = conn
    _THREAD_LOCAL.legacy_connection_key = key
    return conn


def _close_legacy_connection():
    conn = getattr(_THREAD_LOCAL, "legacy_connection", None)
    if conn:
        try:
            conn.close()
        except Exception:
            pass
    _THREAD_LOCAL.legacy_connection = None
    _THREAD_LOCAL.legacy_connection_key = None


def _legacy_connection_string() -> str:
    host = _env("DB_HOST")
    name = _env("DB_NAME")
    if not host or not name:
        raise RuntimeError("missing DB_HOST or DB_NAME")
    port = _env("DB_PORT", "1433")
    driver = _env("DB_DRIVER", "ODBC Driver 18 for SQL Server")
    auth_mode = _env("DB_AUTH_MODE", "sql").lower()
    server = f"{host},{port}" if port else host
    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={server}",
        f"DATABASE={name}",
        f"Encrypt={_env('DB_ENCRYPT', 'yes')}",
        f"TrustServerCertificate={_env('DB_TRUST_SERVER_CERTIFICATE', 'yes')}",
        f"Timeout={_env('DB_TIMEOUT', '30')}",
    ]
    if auth_mode == "windows":
        parts.append("Trusted_Connection=yes")
    else:
        user = _env("DB_USER")
        password = _env("DB_PASSWORD")
        if not user or not password:
            raise RuntimeError("missing DB_USER or DB_PASSWORD")
        parts.extend([f"UID={user}", f"PWD={password}"])
    return ";".join(parts) + ";"


def _assert_read_only_sql(sql: str) -> None:
    stripped = re.sub(r"--.*?$|/\*.*?\*/", "", sql, flags=re.MULTILINE | re.DOTALL).strip()
    if not stripped.lower().startswith(("select", "with")):
        raise ValueError("Legacy enrichment query must be SELECT/CTE only.")
    for pattern in BLOCKED_SQL_PATTERNS:
        if re.search(pattern, stripped, flags=re.IGNORECASE):
            raise ValueError(f"Blocked non-read-only SQL keyword: {pattern}")


def _bind_named_params(sql: str, params: dict) -> tuple[str, list]:
    values = []

    def repl(match):
        key = match.group(1)
        if key not in params:
            raise ValueError(f"Unknown legacy query parameter: {key}")
        values.append(params[key])
        return "?"

    return re.sub(r":([A-Za-z_][A-Za-z0-9_]*)", repl, sql), values


def _dedupe_candidates(candidates: Iterable[HistoricalCandidate]) -> list[HistoricalCandidate]:
    seen = set()
    out = []
    for candidate in candidates:
        key = (_url_key(candidate.website), candidate.source, candidate.record_id)
        if key in seen:
            continue
        seen.add(key)
        out.append(candidate)
    return out


def _query_term(company: str) -> str:
    terms = _query_terms(company)
    if terms:
        return terms[0]
    tokens = [
        t for t in re.sub(r"[^A-Za-z0-9]+", " ", company or "").split()
        if len(t) >= 3 and t.lower() not in {"llc", "inc", "ltd", "corp", "company", "dba"}
    ]
    return max(tokens, key=len) if tokens else (company or "")[:20]


def _query_terms(company: str) -> list[str]:
    raw = re.sub(r"[^A-Za-z0-9]+", " ", company or "").lower().split()
    legal_or_noise = {
        "llc", "inc", "ltd", "corp", "corporation", "company", "co",
        "dba", "doing", "business", "as",
    }
    ignored = legal_or_noise | GENERIC_NAME_QUERY_TERMS
    terms = []
    for token in raw:
        if token in ignored:
            continue
        if len(token) >= 3 or (len(token) >= 2 and any(ch.isdigit() for ch in token)):
            terms.append(token)

    acronym = "".join(t[0] for t in raw if t not in ignored and t)
    if len(acronym) >= 2:
        terms.append(acronym)
    if not terms:
        terms.extend(
            token for token in raw
            if token not in legal_or_noise and (len(token) >= 4 or any(ch.isdigit() for ch in token))
        )

    seen, out = set(), []
    for term in sorted(terms, key=lambda t: (-len(t), t)):
        if term not in seen:
            seen.add(term)
            out.append(term)
    return out[:4]


def _soql_name_filter(field: str, company: str) -> str:
    terms = [_soql_escape(term) for term in _query_terms(company)]
    return " OR ".join(f"{field} LIKE '%{term}%'" for term in terms)


def _soql_escape(value: str) -> str:
    return str(value or "").replace("\\", "\\\\").replace("'", "\\'")


def _join_location(city: str, state: str, country: str) -> str:
    return ", ".join(x for x in [city, state, country] if x)


def _url_key(url: str) -> str:
    return re.sub(r"^https?://", "", str(url or "").lower()).rstrip("/")


def _domain_from_url(url: str) -> str:
    return re.sub(r"^www\.", "", re.sub(r"^https?://", "", str(url or "").lower()).split("/")[0])


def _email_domains(*values) -> list[str]:
    return _email_domain_stats(*values)["usable"]


def _email_domain_stats(*values) -> dict:
    domains = []
    filtered = []
    seen_any = False
    for value in values:
        if not value:
            continue
        for match in re.finditer(r"[A-Z0-9._%+-]+@([A-Z0-9.-]+\.[A-Z]{2,})", str(value), flags=re.IGNORECASE):
            seen_any = True
            domain = match.group(1).lower().strip(".")
            if _usable_email_domain(domain):
                domains.append(domain)
            else:
                filtered.append(domain)
    seen, out = set(), []
    for domain in domains:
        if domain not in seen:
            seen.add(domain)
            out.append(domain)
    filtered_seen, filtered_out = set(), []
    for domain in filtered:
        if domain not in filtered_seen:
            filtered_seen.add(domain)
            filtered_out.append(domain)
    return {"usable": out, "filtered": filtered_out, "seen": seen_any}


def _usable_email_domain(domain: str) -> bool:
    if not domain or domain in GENERIC_EMAIL_DOMAINS:
        return False
    if domain.endswith((".local", ".internal", ".invalid", ".test")):
        return False
    labels = domain.split(".")
    if len(labels) < 2 or any(not label for label in labels):
        return False
    return any(ch.isalpha() for ch in labels[-2])


def _legacy_location(data: dict, source_city: str, source_state: str, source_country: str) -> dict:
    bill = {
        "city": str(_first(data, "city", "billcity", "billing_city") or ""),
        "state": str(_first(data, "state", "billstate", "billing_state") or ""),
        "country": str(_first(data, "country", "billcountry", "billing_country") or ""),
    }
    ship = {
        "city": str(_first(data, "ship_city", "shipcity") or ""),
        "state": str(_first(data, "ship_state", "shipstate") or ""),
        "country": str(_first(data, "ship_country", "shipcountry") or ""),
    }
    if _same_location(ship, source_city, source_state):
        return _fill_country(ship, source_country)
    if _same_location(bill, source_city, source_state):
        return _fill_country(bill, source_country)
    if _norm(ship["state"]) == _norm(source_state):
        return _fill_country(ship, source_country)
    if _norm(bill["state"]) == _norm(source_state):
        return _fill_country(bill, source_country)
    return _fill_country(bill if bill["city"] or bill["state"] else ship, source_country)


def _same_location(candidate: dict, city: str, state: str) -> bool:
    return bool(
        candidate.get("city") and candidate.get("state")
        and _norm(candidate["city"]) == _norm(city)
        and _norm(candidate["state"]) == _norm(state)
    )


def _fill_country(location: dict, fallback: str) -> dict:
    return {
        "city": location.get("city", ""),
        "state": location.get("state", ""),
        "country": location.get("country") or fallback or "",
    }


def _norm(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _state_variants(value: str) -> tuple[str, str]:
    raw = str(value or "").strip()
    normed = re.sub(r"\s+", " ", raw.lower())
    if len(normed) == 2:
        return normed.upper(), STATE_NAMES.get(normed, raw)
    abbr = STATE_ALIASES.get(normed, raw[:2].upper() if len(raw) == 2 else raw)
    return str(abbr).upper(), STATE_NAMES.get(str(abbr).lower(), raw)


def _first(data: dict, *keys):
    for key in keys:
        if data.get(key) not in (None, ""):
            return data.get(key)
    return None


def _env(key: str, default: str = "") -> str:
    prefix = os.getenv("SF_ENV_PREFIX", "SF").strip()
    if key.startswith("SF_") and prefix and prefix != "SF":
        alt = f"{prefix}_{key[3:]}"
        return os.getenv(alt) or os.getenv(key, default)
    return os.getenv(key, default)
