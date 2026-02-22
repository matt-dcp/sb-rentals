#!/usr/bin/env python3
"""
SB + Ojai Craigslist Rental Scraper
Writes to Supabase (Postgres) via REST API.
"""

import requests
from bs4 import BeautifulSoup
import time
import logging
import re
import os
import json
import urllib.request
from datetime import datetime, timezone

MARKETS = [
    {
        "market":     "santa_barbara",
        "base_url":   "https://santabarbara.craigslist.org",
        "apartments": "https://santabarbara.craigslist.org/search/apa",
        "houses":     "https://santabarbara.craigslist.org/search/hhh",
    },
    {
        "market":     "ojai",
        "base_url":   "https://ventura.craigslist.org",
        "apartments": "https://ventura.craigslist.org/search/ojai-ca/apa",
        "houses":     "https://ventura.craigslist.org/search/ojai-ca/hhh",
    },
]

# Neighborhoods that get promoted to their own market silo.
# Checked against the lowercase neighborhood string from Craigslist.
# Order matters — more specific patterns first.
NEIGHBORHOOD_SILOS = [
    ("isla_vista",  ["isla vista", "isla vista iv", "ucsb iv", "ucsb area", "iv "]),
    ("goleta",      ["goleta", "noleta", "ellwood", "storke", "glen annie", "glenn annie"]),
    ("carpinteria", ["carpinteria", "carpintería", "summerland"]),
    ("solvang",     ["solvang", "ballard", "santa ynez", "los olivos"]),
    ("buellton",    ["buellton"]),
    ("lompoc",      ["lompoc", "vandenberg"]),
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}
REQUEST_DELAY = 1.5
MAX_PAGES     = 10

# ── Craigslist URL category codes ─────────────────────────────────────────
# The listing URL contains a 2-3 letter code: craigslist.org/CODE/d/...
# This is more reliable than title pattern matching for primary classification.
URL_CODE_JUNK      = {'off', 'prk', 'reb', 'reo', 'sbw', 'vac'}
URL_CODE_ROOM      = {'roo', 'sha'}
URL_CODE_HOUSES    = {'hou'}
URL_CODE_APARTMENT = {'apa', 'sub'}

# Title words that indicate a house even when posted in the apa category
HOUSE_IN_APT_RE = re.compile(
    r'\b(house|home|bungalow|cottage|duplex|townhome|townhouse|ranch|cabin|villa|estate)\b',
    re.I
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

WORD_TO_NUM = {'one':1,'two':2,'three':3,'four':4,'five':5,'six':6}

# ── Supabase config ───────────────────────────────────────────────────────
SUPABASE_URL = "https://wzlccltlthlaguazgten.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind6bGNjbHRsdGhsYWd1YXpndGVuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE3Nzc4ODMsImV4cCI6MjA4NzM1Mzg4M30.5KuGMYwAYGiK0UYDcHxgIjXAHd-s_v6gutigVIH_zZM")

# ── Junk detection ────────────────────────────────────────────────────────
# Listings that match any of these patterns are skipped entirely.
JUNK_PATTERNS = [
    # Housing wanted / seeking
    r'\bwanted\b', r'\biso\b', r'looking for', r'housing needed', r'need(ing)? (a |)room',
    r'seeking (a |)room', r'in search of', r'house sitter', r'need(s?) housing',
    # Commercial / non-residential
    r'office space', r'retail space', r'commercial (space|yard|zoned)',
    r'warehouse', r'\bindustrial\b', r'lab(oratory)? (space|office)',
    r'co-?working', r'storage (unit|space|facility|lot)',
    r'self storage', r'parking space', r'carport', r'garage (space|for rent)',
    # For sale / land
    r'for sale', r'\bacres?\b', r'\blot\b.*\$[5-9]\d{4}', r'commercial zoned land',
    # Misc junk
    r'private money loan', r'scam alert', r'^free\b',
    r'house sitter', r'vacation maint',
    # Vehicles
    r'\bcamper\b', r'\btrailer for rent\b', r'\brv (space|lot|storage)\b',
]
JUNK_RE = re.compile('|'.join(JUNK_PATTERNS), re.I)

# ── Room rental detection ─────────────────────────────────────────────────
# Listings that match are kept but categorized as 'room_rental'.
ROOM_PATTERNS = [
    r'\broom for rent\b', r'\broom(s)? (available|to rent|4 rent)\b',
    r'\bprivate room\b', r'\bfurnished room\b', r'\broom in (a |)house\b',
    r'\broom in (a |)(apt|apartment|condo)\b',
    r'\bbedroom (for rent|available|to rent)\b',
    r'\bprivate bedroom\b', r'\bmaster bedroom\b',
    r'\bhousemate\b', r'\broommate wanted\b',
    r'\bbed.?space\b', r'\bbunk (bed|room)\b',
    r'\bsingle (room|bedroom)\b',
    r'\bcorner room\b', r'\blarge room\b', r'\bspacious room\b',
    r'\bmedium.sized (bedroom|room)\b',
    r'\bfemale (to share|only|preferred)\b', r'\bmale (to share|only|preferred)\b',
    r'\broom to (live|rent)\b',
    r'individual bed.?space',
    r'\bavail(able)? for (immediate|female|male|1 )',
    r'\b(room|bedroom) (w/|with) (private|shared) bath\b',
]
ROOM_RE = re.compile('|'.join(ROOM_PATTERNS), re.I)


def resolve_market(base_market, neighborhood):
    """
    For santa_barbara listings, check if the neighborhood string
    maps to one of our silos. Returns the resolved market string.
    """
    if base_market != 'santa_barbara' or not neighborhood:
        return base_market
    nl = neighborhood.lower()
    # Only reclassify if the neighborhood string unambiguously matches one silo
    # (skip it if it mentions multiple cities)
    multi_city = sum(
        1 for silo, patterns in NEIGHBORHOOD_SILOS
        if any(p in nl for p in patterns)
    )
    if multi_city > 1:
        return 'santa_barbara'
    for silo, patterns in NEIGHBORHOOD_SILOS:
        if any(p in nl for p in patterns):
            return silo
    return base_market


def is_junk(title):
    return bool(JUNK_RE.search(title or ''))


def is_room_rental(title):
    return bool(ROOM_RE.search(title or ''))


def parse_price(text):
    m = re.search(r"\$?([\d,]+)", text or "")
    return int(m.group(1).replace(",", "")) if m else None


def parse_beds_baths(s):
    beds = baths = None
    if not s:
        return beds, baths
    sl = s.lower()
    if re.search(r'\bstudio\b', sl):
        beds = 0.0
    if beds is None:
        bed_patterns = [
            r'(\d+)\s*(?:br|bed(?:room)?s?|bd|bdrm)\b',
            r'(\d+)-bed(?:room)?s?\b',
            r'\b(one|two|three|four|five|six)\s*(?:-\s*)?bed(?:room)?s?\b',
            r'\b(\d)[Bb]\s*[+/]\s*\d[Bb]\b',
            r'\b(\d)[Bb](\d)[Bb]\b',
            r'\b([1-5])\s*[xX]\s*[1-5]\b',
            r'\b(\d)\s*/\s*\d\b(?=.{0,60}(?:furnished|duplex|condo|upgraded|utilities|rent|house|home|apt|unit))',
        ]
        for pat in bed_patterns:
            m = re.search(pat, sl)
            if m:
                val = m.group(1)
                beds = float(WORD_TO_NUM[val]) if val in WORD_TO_NUM else float(val)
                break
    bath_patterns = [
        r'(\d+(?:\.\d+)?)\s*(?:ba|bath(?:room)?s?)\b',
        r'(\d+(?:\.\d+)?)-bath(?:room)?s?\b',
        r'\b(one|two|three|four)\s*(?:-\s*)?bath(?:room)?s?\b',
    ]
    for pat in bath_patterns:
        m = re.search(pat, sl)
        if m:
            val = m.group(1)
            baths = float(WORD_TO_NUM[val]) if val in WORD_TO_NUM else float(val)
            break
    return beds, baths


def parse_sqft(s):
    if s:
        m = re.search(r'([\d,]+)\s*(?:sq\.?\s*ft|sqft|ft)', s, re.I)
        if m:
            return int(m.group(1).replace(",", ""))
    return None


def scrape_page(session, url, market, category):
    try:
        resp = session.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.warning(f"Failed: {url}: {e}")
        return []

    soup    = BeautifulSoup(resp.text, "html.parser")
    results = []

    for item in soup.select("li.cl-static-search-result"):
        try:
            title    = (item.get("title") or "").strip() or None
            link_el  = item.select_one("a")
            post_url = link_el["href"] if link_el else None

            post_id  = None
            url_code = None
            if post_url:
                m = re.search(r"/(\d+)\.html", post_url)
                if m:
                    post_id = m.group(1)
                c = re.search(r'\.org/([a-z]+)/', post_url)
                if c:
                    url_code = c.group(1)
            if not post_id:
                continue

            # URL code is the primary classification gate — more reliable than title matching
            if url_code in URL_CODE_JUNK:
                log.debug(f"URL-code junk skipped [{url_code}]: {title}")
                continue

            # Title-based junk filter catches anything the URL code misses
            if is_junk(title):
                log.debug(f"Title junk skipped: {title}")
                continue

            price_el = item.select_one(".price")
            price    = parse_price(price_el.get_text(strip=True)) if price_el else None

            # Skip listings with no price — universally junk
            if not price:
                log.debug(f"No-price skipped: {title}")
                continue

            hood_el      = item.select_one(".location")
            neighborhood = hood_el.get_text(strip=True).title() if hood_el else None

            # Resolve market silo from neighborhood
            resolved_market = resolve_market(market, neighborhood)

            # Determine category — URL code takes precedence, then title inference
            if url_code in URL_CODE_ROOM or is_room_rental(title):
                resolved_category = 'room_rental'
            elif url_code in URL_CODE_HOUSES:
                resolved_category = 'houses'
            elif url_code in URL_CODE_APARTMENT:
                # Some landlords post houses in the apa category — detect by title
                if HOUSE_IN_APT_RE.search(title or ''):
                    resolved_category = 'houses'
                else:
                    resolved_category = 'apartments'
            else:
                resolved_category = category  # fallback to search category passed in

            beds, baths = parse_beds_baths(title)
            sqft        = parse_sqft(title)

            results.append({
                "id":           post_id,
                "market":       resolved_market,
                "category":     resolved_category,
                "title":        title,
                "price":        price,
                "bedrooms":     beds,
                "bathrooms":    baths,
                "sqft":         sqft,
                "neighborhood": neighborhood,
                "url":          post_url,
                "posted_date":  datetime.now(timezone.utc).date().isoformat(),
                "scraped_at":   datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            log.debug(f"Parse error: {e}")


    return results


def scrape_category(session, market, category, search_url):
    all_listings = []
    for page in range(MAX_PAGES):
        url      = f"{search_url}?start={page * 120}"
        log.info(f"  [{market}/{category}] page {page + 1}")
        listings = scrape_page(session, url, market, category)
        if not listings:
            log.info(f"  [{market}/{category}] no more results")
            break
        all_listings.extend(listings)
        time.sleep(REQUEST_DELAY)
    return all_listings


def supabase_upsert(records):
    url  = f"{SUPABASE_URL}/rest/v1/listings"
    data = json.dumps(records).encode('utf-8')
    req  = urllib.request.Request(
        url, data=data, method='POST',
        headers={
            'apikey':        SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}',
            'Content-Type':  'application/json',
            'Prefer':        'resolution=ignore-duplicates,return=minimal',
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, None
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8', errors='replace')


def main():
    log.info("═" * 60)
    log.info("SB + Ojai Rental Scraper — starting")

    session = requests.Session()
    all_listings = []

    for mkt in MARKETS:
        market = mkt["market"]
        log.info(f"Market: {market}")
        for category in ("apartments", "houses"):
            listings = scrape_category(session, market, category, mkt[category])
            log.info(f"  Fetched {len(listings)} listings")
            all_listings.extend(listings)

    # Summarize what we're about to insert
    from collections import Counter
    market_counts = Counter(l['market'] for l in all_listings)
    cat_counts    = Counter(l['category'] for l in all_listings)
    log.info(f"Total scraped: {len(all_listings)}")
    log.info(f"By market: {dict(market_counts)}")
    log.info(f"By category: {dict(cat_counts)}")

    # Upload in batches of 50
    BATCH = 50
    inserted = errors = 0
    for i in range(0, len(all_listings), BATCH):
        batch = all_listings[i:i+BATCH]
        status, err = supabase_upsert(batch)
        if err:
            errors += len(batch)
            log.warning(f"Batch {i//BATCH+1} error ({status}): {err[:200]}")
        else:
            inserted += len(batch)

    log.info(f"Done. Uploaded: {inserted}  Errors: {errors}")
    log.info("═" * 60)


if __name__ == "__main__":
    main()
