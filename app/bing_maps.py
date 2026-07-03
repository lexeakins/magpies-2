"""
Optional Bing Maps scraping fallback.

This mirrors the Google Maps candidate model: multiple query modes, up to N
map listing candidates per mode, and one normalized candidate row per listing.
Bing Maps is UI-scraped and should remain an experimental, auditable fallback.
"""

import re
import time
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from .gmaps import (
    PAGE_LOAD_TIMEOUT,
    build_gmaps_search_modes,
    compute_location_match,
    geocode_location,
    load_url,
    parse_address,
    setup_driver,
)


BING_MAPS_SOURCE = "bing_maps"
BLOCKED_WEBSITE_HOST_PARTS = (
    "bing.com",
    "bingplaces.com",
    "microsoft.com",
    "virtualearth.net",
    "msn.com",
    "google.com",
    "maplibre.org",
    "cookieyes.com",
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "twitter.com",
    "x.com",
    "yelp.com",
    "clarity.ms",
    "doubleclick.net",
    "googletagmanager.com",
)
WEBSITE_LABEL_RE = re.compile(
    r"\b("
    r"website|web\s*site|official\s+site|company\s+site|visit\s+site|"
    r"visit\s+website|open\s+website|open\s+site"
    r")\b",
    re.IGNORECASE,
)
BAD_LISTING_TITLE_RE = re.compile(
    r"\b("
    r"call\s+today|add\s+a\s+missing\s+business|share\s+feedback|"
    r"directions|nearby|save|send\s+to\s+phone|claim\s+this\s+business"
    r")\b",
    re.IGNORECASE,
)
MAX_LINK_DIAGNOSTICS_PER_LISTING = 8


def lookup_bing_maps_candidates(company_name: str, country: str,
                                city=None, state=None, full_address=None,
                                max_per_mode: int = 5,
                                stop_when=None, driver=None,
                                stage_callback=None) -> dict:
    """Return Bing Maps candidates, reusing a caller-owned browser when given."""
    start = time.time()
    owns_driver = driver is None
    try:
        if owns_driver:
            driver = setup_driver()
        lat, lng = geocode_location(city, state, country or "United States")
        result = search_bing_maps_candidates(
            driver,
            company_name,
            country or "United States",
            city=city,
            state=state,
            lat=lat,
            lng=lng,
            full_address=full_address,
            max_per_mode=max_per_mode,
            stop_when=stop_when,
            stage_callback=stage_callback,
        )
        result["duration"] = round(time.time() - start, 2)
        return result
    except Exception as exc:
        return {
            "candidates": [_candidate_from_result(
                _empty_result(), "startup_error", company_name or "", 0, error=str(exc)[:200]
            )],
            "attempts": 0,
            "duration": round(time.time() - start, 2),
        }
    finally:
        if owns_driver and driver:
            try:
                driver.quit()
            except Exception:
                pass


def build_bing_maps_search_modes(company_name: str, country: str,
                                 city=None, state=None, lat=None, lng=None) -> list[dict]:
    """Reuse the established Maps query modes, but target Bing Maps URLs."""
    modes = []
    for item in build_gmaps_search_modes(company_name, country, city, state, lat, lng):
        query = item.get("query", "")
        if not query:
            continue
        params = f"q={quote_plus(query)}"
        if lat is not None and lng is not None:
            params += f"&cp={lat}~{lng}&lvl=16"
        modes.append({
            "mode": item.get("mode"),
            "query": query,
            "url": f"https://www.bing.com/maps?{params}",
        })
    return modes


def search_bing_maps_candidates(driver, company_name: str, country: str,
                                city=None, state=None, lat=None, lng=None,
                                full_address=None, max_per_mode: int = 5,
                                stop_when=None, stage_callback=None) -> dict:
    candidates = []
    attempts = 0
    modes = build_bing_maps_search_modes(company_name, country, city, state, lat, lng)

    for mode_def in modes:
        attempts += 1
        mode = mode_def["mode"]
        query = mode_def["query"]
        search_url = mode_def["url"]
        if stage_callback:
            stage_callback("bing_maps_search_mode_start", mode, {
                "query": query,
                "attempt": attempts,
                "url": search_url,
            })
        try:
            load_url(driver, search_url)
            _wait_for_bing_maps(driver)
            listing_refs = _collect_listing_refs(driver, max_per_mode)

            if not listing_refs:
                if stage_callback:
                    stage_callback("bing_maps_listing_start", mode, {
                        "query": query,
                        "rank": 1,
                        "href": driver.current_url,
                        "current_place": True,
                    })
                result = _extract_current_place(driver, country, city, state, full_address)
                candidate = _candidate_from_result(result, mode, query, 1)
                candidates.append(candidate)
                candidates.extend(_link_diagnostic_candidates(result, mode, query, 1))
                if stop_when and stop_when(candidate):
                    return {"candidates": candidates, "attempts": attempts}
                continue

            for rank, ref in enumerate(listing_refs[:max_per_mode], 1):
                try:
                    if stage_callback:
                        stage_callback("bing_maps_listing_start", mode, {
                            "query": query,
                            "rank": rank,
                            "href": ref.get("href"),
                            "text": ref.get("text"),
                        })
                    _open_listing_ref(driver, ref)
                    _wait_for_bing_maps(driver)
                    result = _extract_current_place(driver, country, city, state, full_address)
                    if ref.get("text") and not result.get("bing_maps_listing_name"):
                        result["bing_maps_listing_name"] = ref["text"]
                        result["found"] = True
                    candidate = _candidate_from_result(result, mode, query, rank)
                    candidates.append(candidate)
                    candidates.extend(_link_diagnostic_candidates(result, mode, query, rank))
                    if stop_when and stop_when(candidate):
                        return {"candidates": candidates, "attempts": attempts}
                except Exception as exc:
                    if stage_callback:
                        stage_callback("bing_maps_listing_error", mode, {
                            "query": query,
                            "rank": rank,
                            "error": str(exc)[:200],
                        })
                    candidates.append(_candidate_from_result(
                        _empty_result(), mode, query, rank, error=str(exc)[:200]
                    ))
        except Exception as exc:
            if stage_callback:
                stage_callback("bing_maps_search_mode_error", mode, {
                    "query": query,
                    "attempt": attempts,
                    "error": str(exc)[:200],
                })
            candidates.append(_candidate_from_result(
                _empty_result(), mode, query, 0, error=str(exc)[:200]
            ))

    return {"candidates": candidates, "attempts": attempts}


def _wait_for_bing_maps(driver):
    wait = WebDriverWait(driver, PAGE_LOAD_TIMEOUT)
    try:
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except TimeoutException:
        raise
    time.sleep(1.0)


def _collect_listing_refs(driver, max_per_mode: int) -> list[dict]:
    """
    Return candidate clickable result refs. Bing's Maps DOM changes often, so
    this intentionally favors business-looking result refs over page controls.
    """
    script = """
    const selectors = [
      "[role='listitem'] a",
      "[role='link'][aria-label]",
      "[class*='entity'] a",
      "[class*='Entity'] a",
      "a[href*='/maps?']"
    ];
    const badTextRe = /^(x|×|✕|close|feedback|settings and quick links|settings|more actions|directions|traffic|route|routes|nearby|save|share|send to phone|sign in|all images)$/i;
    const badContainsRe = /(privacy|terms|microsoft|collapse|expand|zoom in|zoom out|keyboard shortcuts|map type|aerial view|road view)/i;
    const badHrefRe = /(\\/images\\/search|\\/aclick\\?|\\/ck\\/a\\?|bingplaces\\.com|doubleclick\\.net|thumbtack\\.|angi\\.com|top-hvac-pros\\.|tel:|mode=d|direction_button|mepi=)/i;
    const businessTextRe = /[a-z0-9].*[a-z0-9]/i;
    const isUsefulHref = (href) => {
      if (!href) return false;
      const lower = href.toLowerCase();
      if (badHrefRe.test(lower)) return false;
      if (lower.startsWith('javascript:')) return false;
      if (lower === '#') return false;
      if (lower.endsWith('#') && lower.includes('/maps/search?')) return false;
      return lower.includes('/maps?where') ||
             lower.includes('/maps/place') ||
             lower.includes('mkt=') ||
             lower.includes('&ss=') ||
             lower.includes('&ty=');
    };
    const isUsefulText = (text) => {
      const cleaned = (text || '').replace(/\\s+/g, ' ').trim();
      if (cleaned.length < 3 || cleaned.length > 180) return false;
      if (!businessTextRe.test(cleaned)) return false;
      if (badTextRe.test(cleaned)) return false;
      if (badContainsRe.test(cleaned)) return false;
      return true;
    };
    const rows = [];
    const seen = new Set();
    for (const selector of selectors) {
      for (const el of document.querySelectorAll(selector)) {
        const href = el.href || el.getAttribute('href') || '';
        const text = (el.innerText || el.getAttribute('aria-label') || el.title || '').trim();
        if (!isUsefulText(text) && !isUsefulHref(href)) continue;
        const lower = (text + ' ' + href).toLowerCase();
        if (badContainsRe.test(lower)) continue;
        if (badHrefRe.test(lower)) continue;
        if (
          lower.includes('about our ads') ||
          lower.includes('manage business') ||
          lower.includes('suggest an edit') ||
          lower.includes('call now') ||
          lower.includes('search by category') ||
          lower.includes('browse our pro directory')
        ) continue;
        const key = (href || '') + '|' + text.slice(0, 120);
        if (seen.has(key)) continue;
        seen.add(key);
        rows.push({href, text});
        if (rows.length >= arguments[0]) return rows;
      }
    }
    return rows;
    """
    refs = driver.execute_script(script, max_per_mode) or []
    return [
        {"href": (r.get("href") or "").strip(), "text": _clean_text(r.get("text"))}
        for r in refs
        if (r.get("href") or r.get("text"))
    ]


def _open_listing_ref(driver, ref: dict):
    href = ref.get("href")
    if href and href.startswith("http"):
        load_url(driver, href)
        return
    if href and href.startswith("/"):
        load_url(driver, f"https://www.bing.com{href}")
        return
    text = ref.get("text")
    if not text:
        return
    xpath_text = _xpath_literal(text[:80])
    try:
        el = driver.find_element(By.XPATH, f"//*[contains(normalize-space(.), {xpath_text})]")
        el.click()
    except NoSuchElementException:
        pass


def _extract_current_place(driver, country=None, city=None, state=None,
                           full_address=None) -> dict:
    result = _empty_result()
    result["bing_maps_url"] = driver.current_url

    text = _page_text(driver)
    result["bing_maps_listing_name"] = _extract_title(driver, text)
    if result["bing_maps_listing_name"]:
        result["found"] = True

    website = _extract_website(driver)
    result["website_link_rows"] = website.get("rows", []) if website else []
    if website and website.get("href"):
        result["website"] = website["href"]
        result["website_label"] = website.get("label")
        result["website_provenance"] = website.get("provenance")
        result["found"] = True

    phone = _extract_phone(text)
    if phone:
        result["phone"] = phone
        result["found"] = True

    address = _extract_address(driver, text)
    if address:
        result["address"] = address
        result["found"] = True
        parse_address(result, country)
        result["location_match"] = compute_location_match(result, city, state, full_address)

    return result


def _extract_title(driver, page_text: str) -> str | None:
    selectors = [
        "h1",
        "[aria-level='1']",
        "[class*='entityTitle']",
        "[class*='EntityTitle']",
        "[class*='title']",
    ]
    for selector in selectors:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, selector):
                text = _clean_text(el.text or el.get_attribute("aria-label"))
                if _looks_like_business_title(text):
                    return text
        except Exception:
            pass
    for line in page_text.splitlines()[:12]:
        line = _clean_text(line)
        if _looks_like_business_title(line):
            return line
    return None


def _extract_website(driver) -> dict | None:
    links = _website_link_rows(driver)
    if not links:
        return None

    visible_subtitle = [
        row for row in links
        if row.get("is_subtitle")
        and row.get("visible_url")
        and _usable_website(row.get("href"), row.get("label"), require_label=False)
    ]
    if visible_subtitle:
        out = dict(visible_subtitle[0])
        out["provenance"] = "visible_subtitle_url"
        out["rows"] = links
        return out

    explicit = [row for row in links if _usable_website(row.get("href"), row.get("label"), require_label=True)]
    if explicit:
        out = dict(explicit[0])
        out["provenance"] = "explicit_website_label"
        out["rows"] = links
        return out

    # Some Bing Maps layouts use icon-only website buttons. Accept a direct
    # outbound link only when it is inside a small action/control surface, not
    # from arbitrary page scripts, attribution, consent, or listing-management UI.
    icon_actions = [
        row for row in links
        if row.get("is_action") and _usable_website(row.get("href"), row.get("label"), require_label=False)
    ]
    if icon_actions:
        out = dict(icon_actions[0])
        out["provenance"] = "icon_action_link"
        out["rows"] = links
        return out
    return {"href": None, "rows": links}


def _extract_phone(text: str) -> str | None:
    m = re.search(r"(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}", text or "")
    return m.group(0).strip() if m else None


def _extract_address(driver, text: str) -> str | None:
    try:
        for el in driver.find_elements(By.CSS_SELECTOR, "[aria-label*='Address'], [title*='Address']"):
            label = _clean_text(el.get_attribute("aria-label") or el.get_attribute("title") or el.text)
            label = re.sub(r"^Address[:\s]*", "", label, flags=re.IGNORECASE).strip()
            if _looks_like_address(label):
                return label
    except Exception:
        pass
    for line in text.splitlines():
        line = _clean_text(line)
        if _looks_like_address(line):
            return line
    return None


def _page_text(driver) -> str:
    try:
        return driver.find_element(By.TAG_NAME, "body").text or ""
    except Exception:
        return ""


def _website_link_rows(driver) -> list[dict]:
    script = """
      const rows = [];
      const seen = new Set();
      const visibleUrlRe = /(https?:\\/\\/[^\\s<>"']+|www\\.[^\\s<>"']+\\.[a-z]{2,}[^\\s<>"']*)/i;
      const normalizeVisibleUrl = (value) => {
        if (!value) return '';
        const match = String(value).match(visibleUrlRe);
        if (!match) return '';
        let url = match[1].replace(/[),.;]+$/, '');
        if (url.startsWith('www.')) url = 'https://' + url;
        return url;
      };
      const decodeBingOutboundUrl = (href) => {
        try {
          const parsed = new URL(href);
          const host = parsed.hostname.toLowerCase();
          if (!host.endsWith('bing.com')) return '';
          const direct = parsed.searchParams.get('url');
          if (direct) return direct;
          const encoded = parsed.searchParams.get('u');
          if (encoded) {
            try {
              const cleaned = encoded.startsWith('a1') ? encoded.slice(2) : encoded;
              return atob(cleaned.replace(/-/g, '+').replace(/_/g, '/'));
            } catch (_) {
              return '';
            }
          }
        } catch (_) {
          return '';
        }
        return '';
      };
      const selectors = [
        ".eh_subtitle a[href]",
        "[class*='eh_subtitle'] a[href]",
        "a[aria-label*='Website'], a[aria-label*='website']",
        "a[title*='Website'], a[title*='website']",
        "a[class*='website'], a[class*='Website']",
        "a[href*='/alink/link?url=']"
      ];
      const collect = (el) => {
        const rawHref = el.href || el.getAttribute('href') || '';
        const visibleText = (el.innerText || '').replace(/\\s+/g, ' ').trim();
        const visibleUrl = normalizeVisibleUrl(visibleText);
        const decodedHref = decodeBingOutboundUrl(rawHref);
        const href = visibleUrl || decodedHref || rawHref;
        const subtitleParent = el.closest(".eh_subtitle, [class*='eh_subtitle']");
        const labelParts = [
          visibleText,
          el.getAttribute('aria-label'),
          el.getAttribute('title'),
          el.getAttribute('data-tooltip'),
          el.getAttribute('data-title'),
        ];
        let parent = el.parentElement;
        for (let i = 0; parent && i < 2; i++, parent = parent.parentElement) {
          labelParts.push(parent.getAttribute && parent.getAttribute('aria-label'));
          labelParts.push(parent.getAttribute && parent.getAttribute('title'));
        }
        const label = labelParts.filter(Boolean).join(' ').replace(/\\s+/g, ' ').trim();
        const key = href + '|' + rawHref + '|' + label.slice(0, 120);
        if (seen.has(key)) return;
        seen.add(key);
        const role = (el.getAttribute('role') || '').toLowerCase();
        const classes = (el.className || '').toString().toLowerCase();
        const lowerLabel = label.toLowerCase();
        const isAction =
          role === 'button' ||
          classes.includes('button') ||
          classes.includes('action') ||
          classes.includes('website') ||
          lowerLabel.includes('website') ||
          lowerLabel.includes('site');
        rows.push({
          href,
          raw_href: rawHref,
          label,
          visible_text: visibleText,
          visible_url: visibleUrl,
          decoded_href: decodedHref,
          is_action: isAction,
          is_subtitle: Boolean(subtitleParent)
        });
      };
      for (const selector of selectors) {
        for (const el of document.querySelectorAll(selector)) {
          collect(el);
        }
      }
      return rows;
    """
    try:
        return driver.execute_script(script) or []
    except Exception:
        try:
            links = driver.find_elements(By.CSS_SELECTOR, "a[href^='http']")
        except Exception:
            return []
        return [
            {
                "href": _normalize_bing_website_href(
                    link.get_attribute("href"),
                    link.text or link.get_attribute("aria-label") or link.get_attribute("title"),
                ),
                "raw_href": (link.get_attribute("href") or "").strip(),
                "label": _clean_text(
                    link.text or link.get_attribute("aria-label") or link.get_attribute("title")
                ),
                "visible_text": _clean_text(link.text),
                "visible_url": _visible_url_from_text(link.text),
                "decoded_href": _decode_bing_outbound_url(link.get_attribute("href")),
                "is_action": False,
                "is_subtitle": False,
            }
            for link in links
        ]


def _usable_website(href: str, label: str, *, require_label: bool) -> bool:
    href = _normalize_bing_website_href(href, label)
    if not href.startswith(("http://", "https://")):
        return False
    if _is_blocked_website_url(href):
        return False
    if require_label:
        return bool(WEBSITE_LABEL_RE.search(label or ""))
    return True


def _link_reject_reason(row: dict) -> str:
    href = row.get("href")
    label = row.get("label") or ""
    if not href:
        return "missing_href"
    if _is_blocked_website_url(href):
        return "blocked_host"
    if row.get("is_subtitle") and row.get("visible_url"):
        return "accepted_visible_subtitle_url"
    if WEBSITE_LABEL_RE.search(label):
        return "accepted_explicit_website_label"
    if row.get("is_action"):
        return "accepted_icon_action_link"
    return "missing_website_label_or_action"


def _is_blocked_website_url(href: str | None) -> bool:
    if not href:
        return True
    host = urlparse(href).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return not host or any(part in host for part in BLOCKED_WEBSITE_HOST_PARTS)


def _normalize_bing_website_href(href: str | None, label: str | None = None) -> str:
    visible_url = _visible_url_from_text(label)
    if visible_url:
        return visible_url
    decoded = _decode_bing_outbound_url(href)
    if decoded:
        return decoded
    return (href or "").strip()


def _visible_url_from_text(text: str | None) -> str:
    match = re.search(r"(https?://[^\s<>'\"]+|www\.[^\s<>'\"]+\.[a-z]{2,}[^\s<>'\"]*)", text or "", re.IGNORECASE)
    if not match:
        return ""
    url = match.group(1).rstrip("),.;")
    if url.lower().startswith("www."):
        url = f"https://{url}"
    return url


def _decode_bing_outbound_url(href: str | None) -> str:
    parsed = urlparse(href or "")
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    if not host.endswith("bing.com"):
        return ""
    params = parse_qs(parsed.query)
    for key in ("url", "u"):
        value = (params.get(key) or [""])[0]
        if not value:
            continue
        decoded = unquote(value)
        if decoded.startswith(("http://", "https://")):
            return decoded
    return ""


def _looks_like_business_title(text: str | None) -> bool:
    if not text or len(text) < 2 or len(text) > 140:
        return False
    lowered = text.lower()
    blocked = ("bing maps", "directions", "traffic", "search", "sign in", "terms", "privacy")
    if BAD_LISTING_TITLE_RE.search(text):
        return False
    return not any(b in lowered for b in blocked)


def _looks_like_address(text: str | None) -> bool:
    if not text or len(text) < 10 or len(text) > 180:
        return False
    has_number = bool(re.search(r"\b\d{1,6}\b", text))
    has_state_zip = bool(re.search(r"\b[A-Z]{2}\s+\d{5}(?:-\d{4})?\b", text))
    has_country = "united states" in text.lower() or "usa" in text.lower()
    return has_number and (has_state_zip or has_country)


def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _xpath_literal(value: str) -> str:
    if "'" not in value:
        return f"'{value}'"
    if '"' not in value:
        return f'"{value}"'
    parts = value.split("'")
    return "concat(" + ', "\'", '.join(f"'{p}'" for p in parts) + ")"


def _empty_result() -> dict:
    return {
        "website": None,
        "phone": None,
        "address": None,
        "city": None,
        "state": None,
        "bing_maps_url": None,
        "bing_maps_listing_name": None,
        "website_label": None,
        "website_provenance": None,
        "website_link_rows": [],
        "found": False,
        "location_match": None,
    }


def _candidate_from_result(result: dict, mode: str, query: str,
                           rank: int, error: str = None) -> dict:
    return {
        "source": BING_MAPS_SOURCE,
        "mode": mode,
        "query": query,
        "rank": rank,
        "title": result.get("bing_maps_listing_name"),
        "url": result.get("website"),
        "address_or_snippet": result.get("address"),
        "phone": result.get("phone"),
        "maps_url": result.get("bing_maps_url"),
        "found": result.get("found", False),
        "location_match": result.get("location_match"),
        "error": error,
        "diagnostic": False,
        "http_status": None,
        "response_bytes": None,
        "parsed_count": None,
        "raw": result,
    }


def _link_diagnostic_candidates(result: dict, mode: str, query: str, rank: int) -> list[dict]:
    rows = result.get("website_link_rows") or []
    diagnostics = []
    for idx, row in enumerate(rows[:MAX_LINK_DIAGNOSTICS_PER_LISTING], 1):
        href = row.get("href")
        host = urlparse(href or "").netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        reason = _link_reject_reason(row)
        label = _clean_text(row.get("label"))[:220]
        diagnostics.append({
            "source": BING_MAPS_SOURCE,
            "mode": f"{mode}_outbound_link",
            "query": query,
            "rank": (rank * 100) + idx,
            "title": f"[link] {host or 'unknown'}",
            "url": href,
            "address_or_snippet": f"label={label}; is_action={bool(row.get('is_action'))}; reason={reason}",
            "phone": None,
            "maps_url": result.get("bing_maps_url"),
            "found": False,
            "location_match": None,
            "error": None if reason.startswith("accepted_") else reason,
            "diagnostic": True,
            "http_status": None,
            "response_bytes": None,
            "parsed_count": None,
            "raw": {"link_row": row, "link_reject_reason": reason},
        })
    return diagnostics
