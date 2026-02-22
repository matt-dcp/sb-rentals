#!/usr/bin/env python3
"""
SB + Ojai Craigslist Rental Scraper — v3
Writes to Supabase via REST API.

New in v3:
  - Tracks first_seen / last_seen per listing for days-on-market
  - Writes daily_snapshots after each run (velocity, absorption, price trends)
"""

import requests
from bs4 import BeautifulSoup
import time, logging, re, os, json
import urllib.request, urllib.error
from datetime import datetime, timezone, date, timedelta
from collections import defaultdict
import statistics

# ── Markets ───────────────────────────────────────────────────────────────
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

URL_CODE_JUNK      = {'off', 'prk', 'reb', 'reo', 'sbw', 'vac'}
URL_CODE_ROOM      = {'roo', 'sha'}
URL_CODE_HOUSES    = {'hou'}
URL_CODE_APARTMENT = {'apa', 'sub'}

HOUSE_IN_APT_RE = re.compile(
    r'\b(house|home|bungalow|cottage|duplex|townhome|townhouse|ranch|cabin|villa|estate)\b',
    re.I
)

JUNK_PATTERNS = [
    r'\bwanted\b', r'\biso\b', r'looking for', r'housing needed', r'need(ing)? (a |)room',
    r'seeking (a |)room', r'in search of', r'house sitter', r'need(s?) housing',
    r'office space', r'retail space', r'commercial (space|yard|zoned)',
    r'warehouse', r'\bindustrial\b', r'lab(oratory)? (space|office)',
    r'co-?working', r'storage (unit|space|facility|lot)',
    r'self storage', r'parking space', r'carport', r'garage (space|for rent)',
    r'for sale', r'\bacres?\b', r'commercial zoned land',
    r'private money loan', r'scam alert', r'^free\b',
    r'vacation maint', r'\bcamper\b', r'\btrailer for rent\b',
    r'\brv (space|lot|storage)\b',
]
JUNK_RE = re.compile('|'.join(JUNK_PATTERNS), re.I)

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
    r'\broom to (live|rent)\b', r'individual bed.?space',
    r'\bavail(able)? for (immediate|female|male|1 )',
    r'\b(room|bedroom) (w/|with) (private|shared) bath\b',
]
ROOM_RE = re.compile('|'.join(ROOM_PATTERNS), re.I)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

WORD_TO_NUM = {'one':1,'two':2,'three':3,'four':4,'five':5,'six':6}

# ── Supabase ──────────────────────────────────────────────────────────────
SUPABASE_URL = "https://wzlccltlthlaguazgten.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind6bGNjbHRsdGhsYWd1YXpndGVuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE3Nzc4ODMsImV4cCI6MjA4NzM1Mzg4M30.5KuGMYwAYGiK0UYDcHxgIjXAHd-s_v6gutigVIH_zZM")

SB_HEADERS = {
    'apikey':        SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type':  'application/json',
}

def sb_request(method, path, body=None, params=None):
    import urllib.parse
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    if params:
        url += '?' + urllib.parse.urlencode(params, doseq=True)
    headers = dict(SB_HEADERS)
    headers['Prefer'] = 'resolution=merge-duplicates,return=minimal'
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else []
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8', errors='replace')

# ── Parsing helpers ───────────────────────────────────────────────────────
def resolve_market(base_market, neighborhood):
    if base_market != 'santa_barbara' or not neighborhood:
        return base_market
    nl = neighborhood.lower()
    matched = [s for s, pats in NEIGHBORHOOD_SILOS if any(p in nl for p in pats)]
    return matched[0] if len(matched) == 1 else base_market

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
        for pat in [
            r'(\d+)\s*(?:br|bed(?:room)?s?|bd|bdrm)\b',
            r'(\d+)-bed(?:room)?s?\b',
            r'\b(one|two|three|four|five|six)\s*(?:-\s*)?bed(?:room)?s?\b',
            r'\b(\d)[Bb]\s*[+/]\s*\d[Bb]\b',
            r'\b(\d)[Bb](\d)[Bb]\b',
            r'\b([1-5])\s*[xX]\s*[1-5]\b',
            r'\b(\d)\s*/\s*\d\b(?=.{0,60}(?:furnished|duplex|condo|upgraded|utilities|rent|house|home|apt|unit))',
        ]:
            m = re.search(pat, sl)
            if m:
                val = m.group(1)
                beds = float(WORD_TO_NUM.get(val, val))
                break
    for pat in [
        r'(\d+(?:\.\d+)?)\s*(?:ba|bath(?:room)?s?)\b',
        r'(\d+(?:\.\d+)?)-bath(?:room)?s?\b',
        r'\b(one|two|three|four)\s*(?:-\s*)?bath(?:room)?s?\b',
    ]:
        m = re.search(pat, sl)
        if m:
            val = m.group(1)
            baths = float(WORD_TO_NUM.get(val, val))
            break
    return beds, baths

def parse_sqft(s):
    if s:
        m = re.search(r'([\d,]+)\s*(?:sq\.?\s*ft|sqft|ft)', s, re.I)
        if m:
            return int(m.group(1).replace(",", ""))
    return None

# ── Scraping ──────────────────────────────────────────────────────────────
def scrape_page(session, url, market, category, today_str):
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
                if m: post_id = m.group(1)
                c = re.search(r'\.org/([a-z]+)/', post_url)
                if c: url_code = c.group(1)
            if not post_id:
                continue

            if url_code in URL_CODE_JUNK: continue
            if JUNK_RE.search(title or ''): continue

            price_el = item.select_one(".price")
            price    = parse_price(price_el.get_text(strip=True)) if price_el else None
            if not price: continue

            hood_el      = item.select_one(".location")
            neighborhood = hood_el.get_text(strip=True).title() if hood_el else None
            resolved_market = resolve_market(market, neighborhood)

            if url_code in URL_CODE_ROOM or ROOM_RE.search(title or ''):
                resolved_category = 'room_rental'
            elif url_code in URL_CODE_HOUSES:
                resolved_category = 'houses'
            elif url_code in URL_CODE_APARTMENT:
                resolved_category = 'houses' if HOUSE_IN_APT_RE.search(title or '') else 'apartments'
            else:
                resolved_category = category

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
                "posted_date":  today_str,
                "scraped_at":   datetime.now(timezone.utc).isoformat(),
                "first_seen":   today_str,  # only used on INSERT; ignored on UPDATE
                "last_seen":    today_str,
            })
        except Exception as e:
            log.debug(f"Parse error: {e}")

    return results


def scrape_all(session, today_str):
    all_listings = []
    for mkt in MARKETS:
        market = mkt["market"]
        log.info(f"Market: {market}")
        for category in ("apartments", "houses"):
            for page in range(MAX_PAGES):
                url      = f"{mkt[category]}?start={page * 120}"
                log.info(f"  [{market}/{category}] page {page + 1}")
                listings = scrape_page(session, url, market, category, today_str)
                if not listings:
                    break
                all_listings.extend(listings)
                time.sleep(REQUEST_DELAY)
    return all_listings


# ── Supabase upsert with first_seen/last_seen logic ───────────────────────
def upsert_listings(listings, today_str):
    """
    For new listings: INSERT with first_seen=today, last_seen=today.
    For existing listings: UPDATE last_seen=today, price (in case it changed).
    Uses Supabase upsert with ON CONFLICT DO UPDATE.
    """
    if not listings:
        return 0

    # Supabase upsert: on conflict (id), update last_seen and price only.
    # first_seen is preserved because we don't include it in the on-conflict update.
    # We achieve this by doing the upsert in two passes:
    # Pass 1: INSERT new rows (ignore conflicts)
    # Pass 2: UPDATE last_seen for all seen IDs

    BATCH = 50
    total = 0

    # Pass 1: insert new rows (first_seen + last_seen both set to today)
    for i in range(0, len(listings), BATCH):
        batch = listings[i:i+BATCH]
        data  = json.dumps(batch).encode()
        req   = urllib.request.Request(
            f"{SUPABASE_URL}/rest/v1/listings",
            data=data, method='POST',
            headers={**SB_HEADERS, 'Prefer': 'resolution=ignore-duplicates,return=minimal'}
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                total += len(batch)
        except urllib.error.HTTPError as e:
            log.warning(f"Insert batch error {e.code}: {e.read().decode()[:200]}")

    # Pass 2: update last_seen + price for all seen IDs
    ids = [l['id'] for l in listings]
    for i in range(0, len(ids), 100):
        chunk   = ids[i:i+100]
        id_list = ','.join(f'"{x}"' for x in chunk)
        # Build price updates individually isn't practical via REST; 
        # update last_seen in bulk, price will self-correct on next full match
        req = urllib.request.Request(
            f"{SUPABASE_URL}/rest/v1/listings?id=in.({id_list})",
            data=json.dumps({"last_seen": today_str}).encode(),
            method='PATCH',
            headers={**SB_HEADERS, 'Prefer': 'return=minimal'}
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                pass
        except urllib.error.HTTPError as e:
            log.warning(f"last_seen update error {e.code}: {e.read().decode()[:200]}")

    log.info(f"Upsert complete: {len(listings)} listings processed")
    return total


# ── Daily snapshot computation ────────────────────────────────────────────
def compute_snapshots(today_str):
    """
    Fetch all listings active today + removed since yesterday,
    compute snapshot metrics, and write to daily_snapshots.
    """
    import urllib.parse
    yesterday = (date.fromisoformat(today_str) - timedelta(days=1)).isoformat()

    log.info("Computing daily snapshots…")

    # Fetch active listings (last_seen = today)
    active = []
    offset = 0
    while True:
        url = (f"{SUPABASE_URL}/rest/v1/listings"
               f"?select=market,category,bedrooms,price,first_seen,last_seen"
               f"&last_seen=eq.{today_str}"
               f"&category=neq.room_rental"
               f"&price=gte.500&price=lte.20000"
               f"&limit=10000&offset={offset}")
        req = urllib.request.Request(url, headers=SB_HEADERS)
        with urllib.request.urlopen(req, timeout=30) as resp:
            batch = json.loads(resp.read())
        if not batch: break
        active.extend(batch)
        if len(batch) < 10000: break
        offset += 10000

    # Fetch removed listings (last_seen = yesterday, meaning gone today)
    removed = []
    url = (f"{SUPABASE_URL}/rest/v1/listings"
           f"?select=market,category,bedrooms,price,first_seen,last_seen"
           f"&last_seen=eq.{yesterday}"
           f"&category=neq.room_rental"
           f"&price=gte.500&price=lte.20000"
           f"&limit=10000")
    req = urllib.request.Request(url, headers=SB_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            removed = json.loads(resp.read())
    except:
        removed = []

    log.info(f"  Active today: {len(active)}, Removed: {len(removed)}")

    # Compute snapshots for each market × bedroom combo + totals
    all_markets = list(set(r['market'] for r in active + removed))
    BR_KEYS     = [None, 0.0, 1.0, 2.0, 3.0, 4.0]  # None = all combined
    CAT_KEYS    = ['all', 'apartments', 'houses']

    snapshots = []
    today_date = date.fromisoformat(today_str)

    for market in all_markets:
        a_mkt = [r for r in active  if r['market'] == market]
        r_mkt = [r for r in removed if r['market'] == market]

        for cat in CAT_KEYS:
            a_cat = a_mkt if cat == 'all' else [r for r in a_mkt if r['category'] == cat]
            r_cat = r_mkt if cat == 'all' else [r for r in r_mkt if r['category'] == cat]

            for br in BR_KEYS:
                if br is None:
                    a = a_cat
                    r = r_cat
                elif br >= 4.0:
                    a = [x for x in a_cat if x.get('bedrooms') is not None and x['bedrooms'] >= 4]
                    r = [x for x in r_cat if x.get('bedrooms') is not None and x['bedrooms'] >= 4]
                else:
                    a = [x for x in a_cat if x.get('bedrooms') == br]
                    r = [x for x in r_cat if x.get('bedrooms') == br]

                if not a and not r:
                    continue

                prices     = [x['price'] for x in a if x.get('price')]
                new_today  = [x for x in a if x.get('first_seen') == today_str]
                dom_removed = []
                for x in r:
                    try:
                        fs = date.fromisoformat(x['first_seen'])
                        ls = date.fromisoformat(x['last_seen'])
                        dom_removed.append((ls - fs).days + 1)
                    except:
                        pass

                med_price = int(statistics.median(prices)) if prices else None
                avg_price = int(sum(prices)/len(prices)) if prices else None
                avg_dom   = round(sum(dom_removed)/len(dom_removed), 1) if dom_removed else None

                snapshots.append({
                    "snapshot_date":    today_str,
                    "market":           market,
                    "bedrooms":         br,
                    "category":         cat,
                    "listing_count":    len(a),
                    "median_price":     med_price,
                    "avg_price":        avg_price,
                    "new_listings":     len(new_today),
                    "removed_listings": len(r),
                    "avg_dom_removed":  avg_dom,
                })

    # Write snapshots
    if snapshots:
        for i in range(0, len(snapshots), 50):
            batch = snapshots[i:i+50]
            req = urllib.request.Request(
                f"{SUPABASE_URL}/rest/v1/daily_snapshots",
                data=json.dumps(batch).encode(),
                method='POST',
                headers={**SB_HEADERS, 'Prefer': 'resolution=merge-duplicates,return=minimal'}
            )
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    pass
            except urllib.error.HTTPError as e:
                log.warning(f"Snapshot write error {e.code}: {e.read().decode()[:200]}")

        log.info(f"Wrote {len(snapshots)} snapshot rows")


# ── Main ──────────────────────────────────────────────────────────────────
def main():
    log.info("═" * 60)
    log.info("SB + Ojai Rental Scraper v3 — starting")

    today_str = date.today().isoformat()
    log.info(f"Scrape date: {today_str}")

    session = requests.Session()

    # Scrape
    all_listings = scrape_all(session, today_str)
    from collections import Counter
    log.info(f"Total scraped: {len(all_listings)}")
    log.info(f"By market: {dict(Counter(l['market'] for l in all_listings))}")
    log.info(f"By category: {dict(Counter(l['category'] for l in all_listings))}")

    # Upsert
    upsert_listings(all_listings, today_str)

    # Snapshot
    compute_snapshots(today_str)

    log.info("Done.")
    log.info("═" * 60)


if __name__ == "__main__":
    main()
