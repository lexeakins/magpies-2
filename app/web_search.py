"""
Best-effort browser/query-style web search fallbacks.

These providers intentionally avoid paid search APIs. They parse public search
result HTML and should be treated as optional, fragile candidate sources.
"""

from html import unescape
from html.parser import HTMLParser
import re
from urllib.parse import quote_plus, urlparse, parse_qs, unquote
import base64

import httpx


SEARCH_TIMEOUT = 12
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def build_web_search_queries(company: str, city: str | None, state: str | None) -> list[dict]:
    company = re.sub(r"\s+", " ", str(company or "")).strip()
    city = re.sub(r"\s+", " ", str(city or "")).strip()
    state = re.sub(r"\s+", " ", str(state or "")).strip()
    if not company:
        return []

    queries = [
        {
            "mode": "quoted_company_city_state_official",
            "query": " ".join(p for p in [f'"{company}"', f'"{city}"' if city else "", f'"{state}"' if state else "", "official website"] if p),
        },
        {
            "mode": "literal_company_city_state",
            "query": " ".join(p for p in [company, city, state] if p),
        },
    ]
    seen = set()
    out = []
    for item in queries:
        key = item["query"].lower()
        if item["query"] and key not in seen:
            seen.add(key)
            out.append(item)
    return out


def lookup_web_candidates(company: str, city: str | None, state: str | None,
                          *, use_bing: bool, use_duckduckgo: bool,
                          max_results: int = 10) -> list[dict]:
    candidates = []
    queries = build_web_search_queries(company, city, state)
    if not queries:
        return candidates

    with httpx.Client(
        timeout=SEARCH_TIMEOUT,
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"},
    ) as client:
        for query_def in queries:
            if use_bing:
                candidates.extend(_search_bing(client, query_def, max_results))
            if use_duckduckgo:
                candidates.extend(_search_duckduckgo(client, query_def, max_results))
    deduped = _dedupe(candidates)
    diagnostics = [c for c in deduped if c.get("diagnostic")]
    result_candidates = [c for c in deduped if not c.get("diagnostic")]
    return diagnostics + result_candidates[:max_results]


def _search_bing(client: httpx.Client, query_def: dict, max_results: int) -> list[dict]:
    url = f"https://www.bing.com/search?q={quote_plus(query_def['query'])}"
    try:
        response = client.get(url)
        response.raise_for_status()
        parsed = _parse_bing(response.text, query_def, max_results)
        return [
            _diagnostic_candidate("web_bing", query_def, response.status_code, len(response.content), len(parsed))
        ] + parsed
    except Exception as exc:
        return [_diagnostic_candidate(
            "web_bing",
            query_def,
            _error_status(exc),
            0,
            0,
            error=str(exc)[:200],
        )]


def _search_duckduckgo(client: httpx.Client, query_def: dict, max_results: int) -> list[dict]:
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query_def['query'])}"
    try:
        response = client.get(url)
        response.raise_for_status()
        parsed = _parse_duckduckgo(response.text, query_def, max_results)
        return [
            _diagnostic_candidate("web_duckduckgo", query_def, response.status_code, len(response.content), len(parsed))
        ] + parsed
    except Exception as exc:
        return [_diagnostic_candidate(
            "web_duckduckgo",
            query_def,
            _error_status(exc),
            0,
            0,
            error=str(exc)[:200],
        )]


def _parse_bing(html: str, query_def: dict, max_results: int) -> list[dict]:
    parser = _BingParser()
    parser.feed(html or "")
    items = []
    for row in parser.results:
        href = _clean_url(_bing_url(row.get("href") or ""))
        if href:
            items.append(_candidate(
                "web_bing", query_def, len(items) + 1,
                _clean_result_title(row.get("title") or "", href),
                href,
                row.get("snippet") or "",
            ))
        if len(items) >= max_results:
            return items

    # Regex fallback for Bing layout changes the HTMLParser did not cover.
    for match in re.finditer(
        r"<li\\b[^>]*class=['\"][^'\"]*\\bb_algo\\b[^'\"]*['\"][^>]*>.*?</li>",
        html or "",
        flags=re.IGNORECASE | re.DOTALL,
    ):
        block = match.group(0)
        link = re.search(
            r"<a\\b[^>]+href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>",
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not link:
            continue
        href = _clean_url(_bing_url(unescape(link.group(1))))
        title = _strip_tags(link.group(2))
        snippet_match = re.search(r"<p[^>]*>(.*?)</p>", block, flags=re.IGNORECASE | re.DOTALL)
        snippet = _strip_tags(snippet_match.group(1)) if snippet_match else ""
        if href:
            items.append(_candidate(
                "web_bing",
                query_def,
                len(items) + 1,
                _clean_result_title(title, href),
                href,
                snippet,
            ))
        if len(items) >= max_results:
            break
    return items


def _parse_duckduckgo(html: str, query_def: dict, max_results: int) -> list[dict]:
    parser = _DuckDuckGoParser()
    parser.feed(html)
    items = []
    for row in parser.results:
        href = _clean_url(_duck_url(row.get("href") or ""))
        if href:
            items.append(_candidate(
                "web_duckduckgo", query_def, len(items) + 1,
                _clean_result_title(row.get("title") or "", href),
                href,
                row.get("snippet") or "",
            ))
        if len(items) >= max_results:
            break
    return items


class _DuckDuckGoParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.results = []
        self._current = None
        self._capture = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        klass = attrs.get("class", "")
        if tag == "a" and ("result__a" in klass or "result-link" in klass or "result-link" in attrs.get("rel", "")):
            self._current = {"href": attrs.get("href", ""), "title": "", "snippet": ""}
            self._capture = "title"
        elif self._current is not None and tag in {"a", "div"} and "result__snippet" in klass:
            self._capture = "snippet"

    def handle_data(self, data):
        if self._current is not None and self._capture:
            self._current[self._capture] += data

    def handle_endtag(self, tag):
        if tag == "a" and self._current is not None and self._capture == "title":
            self._capture = None
        elif tag == "div" and self._current is not None:
            self.results.append({
                "href": self._current.get("href", ""),
                "title": re.sub(r"\s+", " ", self._current.get("title", "")).strip(),
                "snippet": re.sub(r"\s+", " ", self._current.get("snippet", "")).strip(),
            })
            self._current = None
            self._capture = None


class _BingParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.results = []
        self._in_algo = False
        self._algo_depth = 0
        self._current = None
        self._capture = None
        self._capture_depth = 0

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        klass = attrs.get("class", "")
        if tag == "li" and "b_algo" in klass.split():
            self._in_algo = True
            self._algo_depth = 1
            self._current = {"href": "", "title": "", "snippet": ""}
            return

        if self._in_algo:
            self._algo_depth += 1
            if tag == "a" and not self._current.get("href"):
                self._current["href"] = attrs.get("href", "")
                self._capture = "title"
                self._capture_depth = self._algo_depth
            elif tag == "p":
                self._capture = "snippet"
                self._capture_depth = self._algo_depth

    def handle_data(self, data):
        if self._in_algo and self._capture and self._current is not None:
            self._current[self._capture] += data

    def handle_endtag(self, tag):
        if not self._in_algo:
            return
        if self._capture and self._algo_depth <= self._capture_depth:
            self._capture = None
        if tag == "li" and self._algo_depth == 1:
            href = self._current.get("href") if self._current else ""
            if href:
                self.results.append({
                    "href": href,
                    "title": re.sub(r"\s+", " ", self._current.get("title", "")).strip(),
                    "snippet": re.sub(r"\s+", " ", self._current.get("snippet", "")).strip(),
                })
            self._in_algo = False
            self._algo_depth = 0
            self._current = None
            self._capture = None
            self._capture_depth = 0
            return
        self._algo_depth = max(0, self._algo_depth - 1)


def _candidate(source: str, query_def: dict, rank: int,
               title: str, url: str, snippet: str) -> dict:
    return {
        "source": source,
        "mode": query_def["mode"],
        "query": query_def["query"],
        "rank": rank,
        "title": title,
        "url": url,
        "address_or_snippet": snippet,
        "phone": None,
        "maps_url": None,
        "found": True,
        "location_match": None,
        "error": None,
        "diagnostic": False,
        "http_status": None,
        "response_bytes": None,
        "parsed_count": None,
        "raw": {},
    }


def _diagnostic_candidate(source: str, query_def: dict, http_status: int | None,
                          response_bytes: int, parsed_count: int,
                          error: str | None = None) -> dict:
    diagnostic_error = error
    if diagnostic_error is None and http_status not in (None, 200):
        diagnostic_error = f"unexpected_status_{http_status}"
    if diagnostic_error is None and parsed_count == 0:
        diagnostic_error = "no_results_parsed"
    summary = (
        f"http_status={http_status or ''}; "
        f"response_bytes={response_bytes}; "
        f"parsed_count={parsed_count}"
    )
    if diagnostic_error:
        summary = f"{summary}; error={diagnostic_error}"
    return {
        "source": source,
        "mode": query_def["mode"],
        "query": query_def["query"],
        "rank": 0,
        "title": f"[diagnostic] {source}",
        "url": None,
        "address_or_snippet": summary,
        "phone": None,
        "maps_url": None,
        "found": False,
        "location_match": None,
        "error": diagnostic_error,
        "diagnostic": True,
        "http_status": http_status,
        "response_bytes": response_bytes,
        "parsed_count": parsed_count,
        "raw": {},
    }


def _error_status(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", None)


def _strip_tags(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value or "")
    return re.sub(r"\s+", " ", unescape(text)).strip()


def _clean_result_title(title: str, url: str) -> str:
    title = _strip_tags(title)
    if not title:
        return ""
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    display_host = re.escape(host)
    title = re.sub(r"https?://\S+", " ", title, flags=re.IGNORECASE)
    title = re.sub(display_host + r"\S*", " ", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*›\s*.*$", " ", title)
    return re.sub(r"\s+", " ", title).strip()


def _duck_url(value: str) -> str:
    parsed = urlparse(unescape(value))
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path.startswith("/l/"):
        return unquote(parse_qs(parsed.query).get("uddg", [""])[0])
    return value


def _bing_url(value: str) -> str:
    value = unescape(value or "")
    parsed = urlparse(value)
    if "bing.com" not in parsed.netloc.lower():
        return value
    encoded = parse_qs(parsed.query).get("u", [""])[0]
    if not encoded:
        return value
    encoded = unquote(encoded)
    for candidate in (encoded, encoded[2:] if encoded.startswith("a1") else ""):
        if not candidate:
            continue
        padded = candidate + "=" * (-len(candidate) % 4)
        try:
            decoded = base64.urlsafe_b64decode(padded).decode("utf-8", errors="ignore")
        except Exception:
            continue
        if decoded.startswith(("http://", "https://")):
            return decoded
    return value


def _clean_url(value: str) -> str | None:
    value = (value or "").strip()
    if not value.startswith(("http://", "https://")):
        return None
    host = urlparse(value).netloc.lower()
    if not host or "bing.com" in host or "duckduckgo.com" in host:
        return None
    return value


def _dedupe(candidates: list[dict]) -> list[dict]:
    out = []
    seen = set()
    for candidate in candidates:
        key = (
            candidate.get("source"),
            candidate.get("mode"),
            candidate.get("query"),
            candidate.get("rank"),
            candidate.get("url") or "",
            candidate.get("error") or "",
            bool(candidate.get("diagnostic")),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(candidate)
    return out
