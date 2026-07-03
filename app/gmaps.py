"""
Google Maps scraping core — extracted from Magpies and extended with
gmaps_listing_name extraction.

This module is self-contained: no dependency on the external magpies.py.
"""

import re
import time
import threading
from urllib.parse import quote_plus

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

PAGE_LOAD_TIMEOUT = 10

# ---------------------------------------------------------------------------
# Geocoding (city/state/country → lat/lng via Nominatim, cached in-process)
# ---------------------------------------------------------------------------

_geocode_cache: dict = {}
_geocoder = Nominatim(user_agent="magpie-app/1.0")
_geocode_lock = threading.Lock()
_chrome_driver_path = None
_chrome_driver_lock = threading.Lock()


def geocode_location(city, state, country):
    """
    Geocode a city/state/country to (lat, lng).
    Cached in-process. Returns (None, None) on failure.
    """
    key = (
        (city    or "").strip().lower(),
        (state   or "").strip().lower(),
        (country or "").strip().lower(),
    )

    with _geocode_lock:
        if key in _geocode_cache:
            return _geocode_cache[key]

    parts = [p for p in [city, state, country] if p and str(p).strip()]
    query = ", ".join(parts)
    if not query:
        return (None, None)

    try:
        loc = _geocoder.geocode(query, timeout=10)
        result = (loc.latitude, loc.longitude) if loc else (None, None)
    except (GeocoderTimedOut, GeocoderServiceError):
        result = (None, None)

    with _geocode_lock:
        _geocode_cache[key] = result
    return result


# ---------------------------------------------------------------------------
# Address scoring helpers
# ---------------------------------------------------------------------------

def score_address_match(input_addr, candidate_text):
    """Score 0.0–1.0 how closely candidate_text matches input_addr."""
    if not input_addr or not candidate_text:
        return 0.0

    def norm(s):
        return re.sub(r'[^\w\s]', ' ', str(s).lower())

    inp  = norm(input_addr)
    cand = norm(candidate_text)
    score = 0.0

    num_m = re.search(r'\b(\d+)\b', inp)
    if num_m and num_m.group(1) in cand:
        score += 0.5

    tokens = [t for t in inp.split() if not t.isdigit() and len(t) > 2][:4]
    if tokens:
        cand_words = set(cand.split())
        overlap = sum(1 for t in tokens if t in cand_words)
        score += 0.3 * (overlap / len(tokens))

    zip_m = re.search(r'\b(\d{5})\b', inp)
    if zip_m and zip_m.group(1) in cand:
        score += 0.2

    return min(score, 1.0)


def select_best_result_link(result_links, full_address=None, city=None, state=None):
    """Pick the best listing link from a Google Maps results list."""
    if not result_links:
        return None

    if full_address and len(result_links) > 1:
        best_link, best_score = result_links[0], -1.0
        for link in result_links[:5]:
            try:
                s = score_address_match(full_address, link.text)
                if s > best_score:
                    best_score, best_link = s, link
            except Exception:
                pass
        return best_link

    if (city or state) and len(result_links) > 1:
        tokens = [t.lower() for t in ((city or "") + " " + (state or "")).split() if len(t) > 2]
        for link in result_links[:5]:
            try:
                if any(t in link.text.lower() for t in tokens):
                    return link
            except Exception:
                pass

    return result_links[0]


def _clean_location_tokens(city, state):
    """Sanitize city/state values before including in a fallback search query."""
    city_clean = re.sub(r'^\s*\d+\s+', '', str(city)).strip() if city else ''
    if not city_clean or re.match(r'^\d+$', city_clean) or len(city_clean) < 3:
        city_clean = ''

    state_str = str(state).strip() if state else ''
    state_clean = '' if (not state_str or re.match(r'^\d+$', state_str) or len(state_str) < 3) else state_str

    return city_clean, state_clean


def _company_search_variants(company_name: str) -> list[str]:
    """Return cleaned company variants for fallback search modes."""
    name = re.sub(r"\s+", " ", str(company_name or "")).strip()
    if not name:
        return []

    variants = [name]
    dba_re = re.compile(r"\b(?:d\.?\s*b\.?\s*a\.?|aka|a/k/a)\b", re.IGNORECASE)
    parts = [p.strip(" -,:;()") for p in dba_re.split(name) if p.strip(" -,:;()")]
    variants.extend(parts)

    cleaned = []
    seen = set()
    for value in variants:
        value = re.sub(r"\s+", " ", value).strip()
        key = value.lower()
        if value and key not in seen:
            seen.add(key)
            cleaned.append(value)
    return cleaned


def build_gmaps_search_modes(company_name: str, country: str,
                             city=None, state=None, lat=None, lng=None) -> list[dict]:
    """Build ordered Google Maps query modes for one company row."""
    modes = []
    c, s = _clean_location_tokens(city, state)
    country = country or "United States"

    if lat is not None and lng is not None:
        query = str(company_name or "").strip()
        modes.append({
            "mode": "coordinate_company",
            "query": query,
            "url": f"https://www.google.com/maps/search/{quote_plus(query)}/@{lat},{lng},13z",
        })

    literal_query = " ".join(str(p).strip() for p in [company_name, c, s] if p and str(p).strip())
    if literal_query:
        modes.append({
            "mode": "literal_company_city_state",
            "query": literal_query,
            "url": f"https://www.google.com/maps/search/{quote_plus(literal_query)}",
        })

    quoted_query = " ".join(str(p).strip() for p in [f'"{company_name}"' if company_name else "", c, s] if p and str(p).strip())
    if quoted_query:
        modes.append({
            "mode": "quoted_company_city_state",
            "query": quoted_query,
            "url": f"https://www.google.com/maps/search/{quote_plus(quoted_query)}",
        })

    base_key = (company_name or "").strip().lower()
    for variant in _company_search_variants(company_name):
        if variant.lower() == base_key:
            continue
        query = " ".join(p for p in [variant, c, s] if p)
        if query:
            modes.append({
                "mode": "normalized_company_city_state",
                "query": query,
                "url": f"https://www.google.com/maps/search/{quote_plus(query)}",
            })

    broader_query = " ".join(str(p).strip() for p in [company_name, s, country] if p and str(p).strip())
    if broader_query:
        modes.append({
            "mode": "company_state_country",
            "query": broader_query,
            "url": f"https://www.google.com/maps/search/{quote_plus(broader_query)}",
        })

    deduped = []
    seen = set()
    for mode in modes:
        key = (mode["mode"], mode["query"].lower())
        if mode["query"] and key not in seen:
            seen.add(key)
            deduped.append(mode)
    return deduped


# ---------------------------------------------------------------------------
# Address parsing (city/state from scraped Google Maps address)
# ---------------------------------------------------------------------------

_US_STATES = {
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut",
    "Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
    "Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan",
    "Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada",
    "New Hampshire","New Jersey","New Mexico","New York","North Carolina",
    "North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island",
    "South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont",
    "Virginia","Washington","West Virginia","Wisconsin","Wyoming",
    "District of Columbia",
}

_CA_PROVINCES = {
    "Ontario","Quebec","British Columbia","Alberta","Manitoba","Saskatchewan",
    "Nova Scotia","New Brunswick","Newfoundland and Labrador","Prince Edward Island",
    "Northwest Territories","Yukon","Nunavut",
}

_ALL_REGIONS = _US_STATES | _CA_PROVINCES

_POSTAL_RE = re.compile(
    r'\b\d{5}(-\d{4})?\b'
    r'|\b[A-Z]\d[A-Z]\s*\d[A-Z]\d\b'
)


def parse_address(result: dict, country: str = None):
    """Extract city and state from a scraped Google Maps address string."""
    if not result.get("address"):
        return

    address = result["address"]
    parts   = address.split(",")
    tail    = ",".join(parts[max(0, len(parts) - 3):])

    # Find state/region in the last 3 segments
    for region in _ALL_REGIONS:
        if region.lower() in tail.lower():
            result["state"] = region
            break

    neighborhood_terms = ["Col.", "Colonia", "Barrio", "Sector", "Zona", "Urb."]

    if len(parts) >= 2:
        for part in reversed(parts[:-1]):
            part = part.strip()
            if re.match(r"^\d{4,6}", part):
                m = re.search(r"\d{4,6}\s+(.+)", part)
                if m:
                    result["city"] = m.group(1).strip()
                    break
            elif not re.match(r"^\d+\s", part) and len(part) > 3:
                if _POSTAL_RE.search(part):
                    continue
                if part not in _ALL_REGIONS and not any(t in part for t in neighborhood_terms):
                    result["city"] = part
                    break


def compute_location_match(result: dict, expected_city=None, expected_state=None,
                           expected_full_address=None) -> str:
    """Return 'exact' | 'partial' | 'none' | 'unknown'."""
    if expected_full_address:
        addr_score = score_address_match(expected_full_address, result.get("address", ""))
        if addr_score >= 0.7:
            return "exact"
        elif addr_score >= 0.3:
            return "partial"
        elif not expected_city and not expected_state:
            return "none"

    if not expected_city and not expected_state:
        return "unknown"

    def normalize(s):
        return re.sub(r'\W+', ' ', str(s).lower()).strip() if s else ""

    extracted_city  = normalize(result.get("city"))
    extracted_state = normalize(result.get("state"))
    scraped_addr    = normalize(result.get("address", ""))
    exp_city        = normalize(expected_city)
    exp_state       = normalize(expected_state)

    def matches(expected, extracted):
        if not expected:
            return None
        return expected in extracted or extracted in expected or expected in scraped_addr

    city_match  = matches(exp_city, extracted_city)
    state_match = matches(exp_state, extracted_state)

    if expected_city and expected_state:
        if city_match and state_match: return "exact"
        if city_match or state_match:  return "partial"
        return "none"
    elif expected_city:
        return "exact" if city_match else "none"
    else:
        return "exact" if state_match else "none"


# ---------------------------------------------------------------------------
# Chrome WebDriver setup
# ---------------------------------------------------------------------------

def setup_driver():
    """Create a headless Chrome instance."""
    options = Options()
    options.page_load_strategy = "eager"
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-notifications")
    options.add_argument("--no-first-run")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=en-US")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})

    global _chrome_driver_path
    with _chrome_driver_lock:
        if not _chrome_driver_path:
            _chrome_driver_path = ChromeDriverManager().install()
    service = Service(_chrome_driver_path)
    driver  = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    driver.set_script_timeout(PAGE_LOAD_TIMEOUT)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def load_url(driver, url: str) -> bool:
    """Navigate without allowing Chrome to block a worker indefinitely."""
    try:
        driver.get(url)
        return True
    except TimeoutException:
        try:
            driver.execute_script("window.stop();")
        except Exception:
            pass
        return False


def _empty_result() -> dict:
    return {
        "website":           None,
        "phone":             None,
        "address":           None,
        "city":              None,
        "state":             None,
        "gmaps_url":         None,
        "gmaps_listing_name": None,
        "found":             False,
        "location_match":    None,
    }


def _extract_current_place(driver, country=None, city=None, state=None,
                           full_address=None) -> dict:
    """Extract the visible Google Maps place panel into the legacy result shape."""
    result = _empty_result()
    result["gmaps_url"] = driver.current_url

    try:
        h1 = driver.find_element(By.TAG_NAME, "h1")
        name = h1.text.strip()
        if name:
            result["gmaps_listing_name"] = name
            result["found"] = True
    except NoSuchElementException:
        pass

    try:
        el = driver.find_element(By.CSS_SELECTOR, "a[data-item-id='authority']")
        result["website"] = el.get_attribute("href")
        result["found"] = True
    except NoSuchElementException:
        try:
            el = driver.find_element(By.CSS_SELECTOR, "[data-tooltip='Open website']")
            result["website"] = el.get_attribute("href")
            result["found"] = True
        except NoSuchElementException:
            pass

    try:
        el = driver.find_element(By.CSS_SELECTOR, "button[data-item-id^='phone:']")
        data = el.get_attribute("data-item-id")
        if data:
            result["phone"] = data.replace("phone:tel:", "").replace("phone:", "")
            result["found"] = True
    except NoSuchElementException:
        try:
            for btn in driver.find_elements(By.CSS_SELECTOR, "button[aria-label*='Phone']"):
                label = btn.get_attribute("aria-label")
                if label:
                    m = re.search(r"Phone:\s*(.+)", label)
                    if m:
                        result["phone"] = m.group(1).strip()
                        result["found"] = True
                        break
        except Exception:
            pass

    try:
        el = driver.find_element(By.CSS_SELECTOR, "button[data-item-id='address']")
        text = el.get_attribute("aria-label")
        if text:
            result["address"] = text.replace("Address: ", "").strip()
            result["found"] = True
            parse_address(result, country)
            result["location_match"] = compute_location_match(result, city, state, full_address)
    except NoSuchElementException:
        try:
            el = driver.find_element(By.CSS_SELECTOR, "[data-tooltip='Copy address']")
            parent = el.find_element(By.XPATH, "./..")
            result["address"] = parent.text.strip()
            result["found"] = True
            parse_address(result, country)
            result["location_match"] = compute_location_match(result, city, state, full_address)
        except NoSuchElementException:
            pass

    return result


def _candidate_from_result(result: dict, source: str, mode: str,
                           query: str, rank: int, error: str = None) -> dict:
    return {
        "source": source,
        "mode": mode,
        "query": query,
        "rank": rank,
        "title": result.get("gmaps_listing_name"),
        "url": result.get("website"),
        "address_or_snippet": result.get("address"),
        "phone": result.get("phone"),
        "maps_url": result.get("gmaps_url"),
        "found": result.get("found", False),
        "location_match": result.get("location_match"),
        "error": error,
        "raw": result,
    }


def search_google_maps_candidates(driver, company_name: str, country: str,
                                  city=None, state=None, lat=None, lng=None,
                                  full_address=None, max_per_mode: int = 5,
                                  stop_when=None, stage_callback=None) -> dict:
    """
    Search Google Maps using multiple query modes and return auditable candidates.
    stop_when(candidate) may return True to stop early after a strong match.
    """
    candidates = []
    attempts = 0
    modes = build_gmaps_search_modes(company_name, country, city, state, lat, lng)

    for mode_def in modes:
        attempts += 1
        mode = mode_def["mode"]
        query = mode_def["query"]
        search_url = mode_def["url"]
        if stage_callback:
            stage_callback("gmaps_search_mode_start", mode, {
                "query": query,
                "attempt": attempts,
                "url": search_url,
            })

        if mode == "coordinate_company" and lat is not None and lng is not None:
            try:
                driver.execute_cdp_cmd("Emulation.setGeolocationOverride", {
                    "latitude": lat, "longitude": lng, "accuracy": 50,
                })
            except Exception:
                pass

        try:
            loaded = load_url(driver, search_url)
            if not loaded:
                if stage_callback:
                    stage_callback("gmaps_search_mode_timeout", mode, {
                        "query": query,
                        "attempt": attempts,
                    })
                candidates.append(_candidate_from_result(
                    _empty_result(), "gmaps", mode, query, 0, error="page_load_timeout"
                ))
            wait = WebDriverWait(driver, PAGE_LOAD_TIMEOUT)
            try:
                wait.until(EC.any_of(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/maps/place/']")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-item-id]")),
                    EC.presence_of_element_located((By.TAG_NAME, "h1")),
                ))
            except TimeoutException:
                if stage_callback:
                    stage_callback("gmaps_search_mode_timeout", mode, {
                        "query": query,
                        "attempt": attempts,
                    })
                candidates.append(_candidate_from_result(
                    _empty_result(), "gmaps", mode, query, 0, error="timeout"
                ))
                continue

            links = []
            seen_hrefs = set()
            for link in driver.find_elements(By.CSS_SELECTOR, "a[href*='/maps/place/']"):
                href = link.get_attribute("href")
                if href and href not in seen_hrefs:
                    seen_hrefs.add(href)
                    links.append(href)
                if len(links) >= max_per_mode:
                    break

            if not links:
                if stage_callback:
                    stage_callback("gmaps_listing_start", mode, {
                        "query": query,
                        "rank": 1,
                        "href": driver.current_url,
                        "current_place": True,
                    })
                result = _extract_current_place(driver, country, city, state, full_address)
                candidate = _candidate_from_result(result, "gmaps", mode, query, 1)
                candidates.append(candidate)
                if stop_when and stop_when(candidate):
                    break
                continue

            for rank, href in enumerate(links[:max_per_mode], 1):
                try:
                    if stage_callback:
                        stage_callback("gmaps_listing_start", mode, {
                            "query": query,
                            "rank": rank,
                            "href": href,
                        })
                    load_url(driver, href)
                    try:
                        wait.until(EC.any_of(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "button[data-item-id='address']")),
                            EC.presence_of_element_located((By.CSS_SELECTOR, "a[data-item-id='authority']")),
                            EC.presence_of_element_located((By.CSS_SELECTOR, "button[data-item-id^='phone:']")),
                            EC.presence_of_element_located((By.TAG_NAME, "h1")),
                        ))
                    except TimeoutException:
                        pass
                    result = _extract_current_place(driver, country, city, state, full_address)
                    candidate = _candidate_from_result(result, "gmaps", mode, query, rank)
                    candidates.append(candidate)
                    if stop_when and stop_when(candidate):
                        return {"candidates": candidates, "attempts": attempts}
                except Exception as exc:
                    candidates.append(_candidate_from_result(
                        _empty_result(), "gmaps", mode, query, rank, error=str(exc)[:100]
                    ))
        except Exception as exc:
            if stage_callback:
                stage_callback("gmaps_search_mode_error", mode, {
                    "query": query,
                    "attempt": attempts,
                    "error": str(exc)[:200],
                })
            candidates.append(_candidate_from_result(
                _empty_result(), "gmaps", mode, query, 0, error=str(exc)[:100]
            ))

    return {"candidates": candidates, "attempts": attempts}


# ---------------------------------------------------------------------------
# Main scrape function
# ---------------------------------------------------------------------------

def search_google_maps(driver, company_name: str, country: str,
                       city=None, state=None, lat=None, lng=None,
                       full_address=None) -> dict:
    """
    Search Google Maps for a company (geo mode: city/state geocoded coords).
    Returns a result dict including gmaps_listing_name.
    """
    result = {
        "website":           None,
        "phone":             None,
        "address":           None,
        "city":              None,
        "state":             None,
        "gmaps_url":         None,
        "gmaps_listing_name": None,
        "found":             False,
        "location_match":    None,
    }

    have_coords = lat is not None and lng is not None

    if have_coords:
        try:
            driver.execute_cdp_cmd("Emulation.setGeolocationOverride", {
                "latitude": lat, "longitude": lng, "accuracy": 50,
            })
        except Exception:
            pass
        query      = company_name
        search_url = f"https://www.google.com/maps/search/{quote_plus(query)}/@{lat},{lng},13z"
    else:
        parts = [company_name]
        c, s  = _clean_location_tokens(city, state)
        if c: parts.append(c)
        if s: parts.append(s)
        parts.append(country)
        query      = " ".join(p for p in parts if p)
        search_url = f"https://www.google.com/maps/search/{quote_plus(query)}"

    try:
        load_url(driver, search_url)
        wait = WebDriverWait(driver, PAGE_LOAD_TIMEOUT)

        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-item-id]")))
        except TimeoutException:
            return result

        # Click the best result from a list page
        try:
            links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/maps/place/']")
            if links:
                best = select_best_result_link(links, full_address, city, state)
                best.click()
                try:
                    wait.until(EC.any_of(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "button[data-item-id='address']")),
                        EC.presence_of_element_located((By.CSS_SELECTOR, "a[data-item-id='authority']")),
                        EC.presence_of_element_located((By.CSS_SELECTOR, "button[data-item-id^='phone:']")),
                    ))
                except TimeoutException:
                    pass
        except NoSuchElementException:
            pass

        result["gmaps_url"] = driver.current_url

        # Listing name (h1) — added over original Magpies
        try:
            h1 = driver.find_element(By.TAG_NAME, "h1")
            name = h1.text.strip()
            if name:
                result["gmaps_listing_name"] = name
                result["found"] = True
        except NoSuchElementException:
            pass

        # Website
        try:
            el = driver.find_element(By.CSS_SELECTOR, "a[data-item-id='authority']")
            result["website"] = el.get_attribute("href")
            result["found"]   = True
        except NoSuchElementException:
            try:
                el = driver.find_element(By.CSS_SELECTOR, "[data-tooltip='Open website']")
                result["website"] = el.get_attribute("href")
                result["found"]   = True
            except NoSuchElementException:
                pass

        # Phone
        try:
            el   = driver.find_element(By.CSS_SELECTOR, "button[data-item-id^='phone:']")
            data = el.get_attribute("data-item-id")
            if data:
                result["phone"] = data.replace("phone:tel:", "").replace("phone:", "")
                result["found"] = True
        except NoSuchElementException:
            try:
                for btn in driver.find_elements(By.CSS_SELECTOR, "button[aria-label*='Phone']"):
                    label = btn.get_attribute("aria-label")
                    if label:
                        m = re.search(r"Phone:\s*(.+)", label)
                        if m:
                            result["phone"] = m.group(1).strip()
                            result["found"] = True
                            break
            except Exception:
                pass

        # Address
        try:
            el   = driver.find_element(By.CSS_SELECTOR, "button[data-item-id='address']")
            text = el.get_attribute("aria-label")
            if text:
                result["address"] = text.replace("Address: ", "").strip()
                result["found"]   = True
                parse_address(result, country)
                result["location_match"] = compute_location_match(result, city, state, full_address)
        except NoSuchElementException:
            try:
                el = driver.find_element(By.CSS_SELECTOR, "[data-tooltip='Copy address']")
                parent = el.find_element(By.XPATH, "./..")
                result["address"] = parent.text.strip()
                result["found"]   = True
                parse_address(result, country)
                result["location_match"] = compute_location_match(result, city, state, full_address)
            except NoSuchElementException:
                pass

    except Exception as e:
        result["error"] = str(e)[:100]

    return result
