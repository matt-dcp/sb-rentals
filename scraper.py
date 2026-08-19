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
import time, logging, re, os, sys, json
import urllib.request, urllib.error
from datetime import datetime, timezone, date, timedelta
from collections import defaultdict
import statistics

# ── Markets ───────────────────────────────────────────────────────────────
MARKETS = [
    # AreaIDs from https://reference.craigslist.org/Areas
    {"market": "santa_barbara", "area": 62,  "host": "santabarbara"},
    # Ojai has no Craigslist subarea ID, so it is scoped by radius from the town.
    {"market": "ojai",          "area": 208, "host": "ventura",
     "lat": 34.4480, "lon": -119.2429, "radius": 10},
    {"market": "santa_maria",   "area": 710, "host": "santamaria"},
]

# Craigslist stopped serving listings in the search HTML to datacenter IPs: the
# page returns 200 with zero result nodes, which is indistinguishable from "no
# more pages". We read the JSON API the site's own front end uses instead.
CL_API      = "https://sapi.craigslist.org/web/v8/postings/search/full"
API_CAP     = 360          # hard ceiling on items the API returns per query
SEARCH_PATH = {"apartments": "apa", "houses": "hhh"}

# The API caps every response at API_CAP and exposes no usable offset or cursor
# (the site's own "load more" uses a cacheId that the JSON never returns), so
# coverage comes from partitioning the result set by price instead. Any band
# that still comes back full gets bisected -- see fetch_band().
#
# Craigslist also pads a thin result set with listings from NEARBY areas, which
# ignore both the price band and the search radius. Those padded rows always sit
# after the first totalResultCount entries, so every response is truncated to
# that count before decoding -- otherwise Ojai fills up with Ventura county and
# the same postings repeat across every band.
PRICE_BANDS = [(0, 1500), (1501, 2200), (2201, 2800), (2801, 3500),
               (3501, 4500), (4501, 6000), (6001, 10000), (10001, None)]

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
MAX_RESULTS   = 3000   # safety cap per market/category

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
# Must be the service_role key: RLS (migration 003) makes anon read-only,
# so writes with the anon key silently violate policy. No fallback on purpose.
try:
    SUPABASE_KEY = os.environ["SUPABASE_KEY"]
except KeyError:
    raise SystemExit("SUPABASE_KEY env var is required (service_role key; anon is read-only since migration 003)")

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
def api_fetch(session, mkt, category, lo, hi):
    """One API query, scoped to a price band. Raises on any transport error.

    Returns (items, total) with nearby-area padding already stripped.
    """
    params = {
        "batch":      f"{mkt['area']}-0-{API_CAP}-0-0",
        "searchPath": SEARCH_PATH[category],
        "cc":         "US",
        "lang":       "en",
        "min_price":  lo,
    }
    if hi is not None:
        params["max_price"] = hi
    if mkt.get("lat") is not None:
        params.update({"lat": mkt["lat"], "lon": mkt["lon"],
                       "search_distance": mkt["radius"]})
    resp = session.get(CL_API, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data  = resp.json()["data"]
    total = data.get("totalResultCount") or 0
    items = data.get("items") or []
    if len(items) > total:
        data["items"] = items[:total]
    return data, total


def decode_items(data, market, category, today_str):
    """Decode the API's delta-encoded item arrays into listing dicts.

    Each item is a mixed array: item[0] is a delta on decode.minPostingId,
    item[3] the price, and the rest are tagged sub-arrays -- 5:[beds, sqft],
    6:[slug], 13:[canonical token] -- plus a "1:{locIdx}~lat~lon" geo string
    and the title as the last untagged string.
    """
    dec   = data.get("decode") or {}
    minid = dec.get("minPostingId") or 0
    locs  = dec.get("locationDescriptions") or []
    out   = []

    for it in data.get("items") or []:
        try:
            post_id = str(minid + it[0])
            price   = it[3] if len(it) > 3 and isinstance(it[3], int) else None
            if not price:
                continue

            slug = token = neighborhood = None
            beds = sqft = None
            plain = []

            for x in it:
                if isinstance(x, list) and x:
                    if   x[0] == 6  and len(x) > 1: slug  = x[1]
                    elif x[0] == 13 and len(x) > 1: token = x[1]
                    elif x[0] == 5:
                        if len(x) > 1: beds = x[1]
                        if len(x) > 2 and x[2]: sqft = x[2]
                elif isinstance(x, str):
                    if x.startswith("1:"):
                        idx = int(x[2:].split("~")[0]) + 1
                        if 0 <= idx < len(locs) and isinstance(locs[idx], str):
                            neighborhood = locs[idx]
                    else:
                        plain.append(x)

            title = plain[-1].strip() if plain else None
            if not title or JUNK_RE.search(title):
                continue

            if ROOM_RE.search(title):
                resolved_category = 'room_rental'
            elif category == 'houses':
                # Reached only for ids absent from the apartments pass.
                resolved_category = 'houses'
            else:
                resolved_category = 'houses' if HOUSE_IN_APT_RE.search(title) else 'apartments'

            _, baths = parse_beds_baths(title)
            if beds is None:
                beds, _ = parse_beds_baths(title)
            if not sqft:
                sqft = parse_sqft(title)

            url = (f"https://www.craigslist.org/view/d/{slug}/{token}" if slug and token
                   else f"https://{mkt_host(market)}.craigslist.org/apa/d/{slug or 'x'}/{post_id}.html")

            out.append({
                "id":           post_id,
                "market":       resolve_market(market, neighborhood),
                "category":     resolved_category,
                "title":        title,
                "price":        price,
                "bedrooms":     beds,
                "bathrooms":    baths,
                "sqft":         sqft,
                "neighborhood": neighborhood.title() if neighborhood else None,
                "url":          url,
                "posted_date":  today_str,
                "scraped_at":   datetime.now(timezone.utc).isoformat(),
                "first_seen":   today_str,  # only used on INSERT; ignored on UPDATE
                "last_seen":    today_str,
            })
        except Exception as e:
            log.debug(f"Decode error: {e}")

    return out


_HOSTS = {m["market"]: m["host"] for m in MARKETS}
def mkt_host(market):
    return _HOSTS.get(market, "santabarbara")


def fetch_band(session, mkt, category, lo, hi, today_str, seen, depth=0):
    """Fetch one price band, bisecting it if the API truncates at the cap."""
    data, total = api_fetch(session, mkt, category, lo, hi)

    if total >= API_CAP and depth < 5 and (hi is None or hi - lo > 100):
        mid = (lo + hi) // 2 if hi is not None else lo * 2
        log.info(f"      band {lo}-{hi} returned {total} (capped), splitting at {mid}")
        time.sleep(REQUEST_DELAY)
        out = fetch_band(session, mkt, category, lo, mid, today_str, seen, depth + 1)
        time.sleep(REQUEST_DELAY)
        out += fetch_band(session, mkt, category, mid + 1, hi, today_str, seen, depth + 1)
        return out

    items = [l for l in decode_items(data, mkt["market"], category, today_str)
             if l["id"] not in seen]
    for l in items:
        seen.add(l["id"])
    return items


def scrape_all(session, today_str):
    """Scrape every market/category. Returns (listings, failures).

    'hhh' is the parent housing category and is a superset of 'apa', so
    apartments are scraped first and their ids suppressed from the houses pass;
    otherwise every apartment would be re-classified as a house on the way in.

    A market/category that yields nothing is recorded as a failure rather than
    treated as "no results" -- that is the exact signal that went unnoticed when
    Craigslist began soft-blocking the runner in June.
    """
    all_listings, failures = [], []
    seen = set()

    for mkt in MARKETS:
        market = mkt["market"]
        log.info(f"Market: {market}")
        for category in ("apartments", "houses"):
            got = 0
            for lo, hi in PRICE_BANDS:
                label = f"{market}/{category} {lo}-{hi if hi else 'up'}"
                try:
                    items = fetch_band(session, mkt, category, lo, hi, today_str, seen)
                except Exception as e:
                    failures.append(f"{label}: {e}")
                    log.error(f"  [{label}] request failed: {e}")
                    continue
                got += len(items)
                all_listings.extend(items)
                log.info(f"  [{label}] {len(items)} new")
                time.sleep(REQUEST_DELAY)

            log.info(f"  {market}/{category}: {got} kept")
            if got == 0:
                failures.append(f"{market}/{category}: zero listings across every price band")
                log.error(f"  [{market}/{category}] returned nothing at all")

    return all_listings, failures


# ── Title similarity for repost detection ─────────────────────────────────
def normalize_title(title):
    """Normalize title for comparison: lowercase, strip prices/numbers, common words."""
    if not title:
        return ''
    t = title.lower()
    t = re.sub(r'\$[\d,]+', '', t)           # strip prices
    t = re.sub(r'\b\d{4,}\b', '', t)         # strip long numbers (IDs, zip codes)
    t = re.sub(r'[^\w\s]', ' ', t)           # punctuation → spaces
    t = re.sub(r'\s+', ' ', t).strip()
    return t

def title_similarity(a, b):
    """Word overlap similarity between two titles (Jaccard index)."""
    if not a or not b:
        return 0.0
    wa = set(normalize_title(a).split())
    wb = set(normalize_title(b).split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / len(wa | wb)


# ── Supabase upsert with price tracking ───────────────────────────────────
def upsert_listings(listings, today_str):
    """
    For new listings: INSERT with first_seen=today, last_seen=today, original_price=price.
    For existing listings: detect price changes, update last_seen + price.
    For new listings: detect probable reposts of recently-removed listings.
    """
    if not listings:
        return 0

    BATCH = 50
    by_id = {l['id']: l for l in listings}
    all_ids = list(by_id.keys())

    # ── Fetch existing listings to detect price changes ──────────────────
    existing = {}  # id → {price, market, category, bedrooms, title}
    for i in range(0, len(all_ids), 100):
        chunk = all_ids[i:i+100]
        id_list = ','.join(f'"{x}"' for x in chunk)
        url = f"{SUPABASE_URL}/rest/v1/listings?id=in.({id_list})&select=id,price,market,category,bedrooms,title"
        req = urllib.request.Request(url, headers=SB_HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                for row in json.loads(resp.read()):
                    existing[row['id']] = row
        except urllib.error.HTTPError as e:
            log.warning(f"Fetch existing error {e.code}: {e.read().decode()[:200]}")

    new_ids = [lid for lid in all_ids if lid not in existing]
    existing_ids = [lid for lid in all_ids if lid in existing]
    log.info(f"  New: {len(new_ids)}, Existing: {len(existing_ids)}")

    # ── Detect price changes on existing listings ────────────────────────
    price_changes = []
    for lid in existing_ids:
        old_price = existing[lid].get('price')
        new_price = by_id[lid]['price']
        if old_price and new_price and old_price != new_price:
            pct = round(((new_price - old_price) / old_price) * 100, 2)
            price_changes.append({
                'listing_id': lid,
                'market': by_id[lid]['market'],
                'category': by_id[lid].get('category'),
                'bedrooms': by_id[lid].get('bedrooms'),
                'old_price': old_price,
                'new_price': new_price,
                'change_pct': pct,
                'change_date': today_str,
                'title': by_id[lid].get('title'),
            })

    if price_changes:
        log.info(f"  Price changes detected: {len(price_changes)}")
        for pc in price_changes[:5]:
            log.info(f"    {pc['listing_id']}: ${pc['old_price']} → ${pc['new_price']} ({pc['change_pct']:+.1f}%)")
        for i in range(0, len(price_changes), BATCH):
            batch = price_changes[i:i+BATCH]
            req = urllib.request.Request(
                f"{SUPABASE_URL}/rest/v1/price_changes",
                data=json.dumps(batch).encode(), method='POST',
                headers={**SB_HEADERS, 'Prefer': 'resolution=ignore-duplicates,return=minimal'}
            )
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    pass
            except urllib.error.HTTPError as e:
                log.warning(f"Price change insert error {e.code}: {e.read().decode()[:200]}")

    # ── Pass 1: INSERT new rows ──────────────────────────────────────────
    for i in range(0, len(listings), BATCH):
        batch = listings[i:i+BATCH]
        # Set original_price on all rows; only matters for new inserts
        for row in batch:
            row['original_price'] = row['price']
        data = json.dumps(batch).encode()
        req = urllib.request.Request(
            f"{SUPABASE_URL}/rest/v1/listings",
            data=data, method='POST',
            headers={**SB_HEADERS, 'Prefer': 'resolution=ignore-duplicates,return=minimal'}
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                pass
        except urllib.error.HTTPError as e:
            log.warning(f"Insert batch error {e.code}: {e.read().decode()[:200]}")

    # ── Pass 2: UPDATE existing listings (last_seen + price) ─────────────
    # Update price individually only for listings whose price changed
    changed_ids = {pc['listing_id'] for pc in price_changes}
    for lid in changed_ids:
        listing = by_id[lid]
        patch = {"last_seen": today_str, "price": listing['price']}
        req = urllib.request.Request(
            f"{SUPABASE_URL}/rest/v1/listings?id=eq.{lid}",
            data=json.dumps(patch).encode(), method='PATCH',
            headers={**SB_HEADERS, 'Prefer': 'return=minimal'}
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                pass
        except urllib.error.HTTPError as e:
            log.warning(f"Price update error {lid}: {e.code}")

    # Bulk-update last_seen for all seen IDs
    for i in range(0, len(all_ids), 100):
        chunk = all_ids[i:i+100]
        id_list = ','.join(f'"{x}"' for x in chunk)
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
            log.warning(f"Bulk last_seen update error {e.code}: {e.read().decode()[:200]}")

    # ── Repost detection for new listings ────────────────────────────────
    detect_reposts(new_ids, by_id, today_str)

    log.info(f"Upsert complete: {len(listings)} listings ({len(new_ids)} new, {len(existing_ids)} updated, {len(price_changes)} price changes)")
    return len(listings)


def detect_reposts(new_ids, by_id, today_str):
    """
    For each new listing, look for recently-removed listings with matching
    market + bedrooms and similar title. If found, log as a probable repost.
    """
    if not new_ids:
        return

    # Group new listings by market for efficient querying
    by_market = defaultdict(list)
    for lid in new_ids:
        by_market[by_id[lid]['market']].append(lid)

    # Look back 7 days for removed listings
    cutoff = (date.fromisoformat(today_str) - timedelta(days=7)).isoformat()

    reposts = []
    for market, lids in by_market.items():
        # Fetch recently-removed listings in this market
        url = (f"{SUPABASE_URL}/rest/v1/listings"
               f"?select=id,title,price,bedrooms,market,category"
               f"&market=eq.{market}"
               f"&last_seen=gte.{cutoff}"
               f"&last_seen=lt.{today_str}"
               f"&limit=1000")
        req = urllib.request.Request(url, headers=SB_HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                removed = json.loads(resp.read())
        except Exception as e:
            log.warning(f"Repost fetch error for {market}: {e}")
            continue

        if not removed:
            continue

        # Index removed by bedrooms for faster matching
        removed_by_br = defaultdict(list)
        for r in removed:
            removed_by_br[r.get('bedrooms')].append(r)

        for lid in lids:
            new_listing = by_id[lid]
            br = new_listing.get('bedrooms')
            candidates = removed_by_br.get(br, [])

            for old in candidates:
                sim = title_similarity(new_listing.get('title'), old.get('title'))
                if sim < 0.5:
                    continue

                old_price = old.get('price')
                new_price = new_listing.get('price')
                price_chg = (new_price - old_price) if old_price and new_price else None
                price_pct = round((price_chg / old_price) * 100, 2) if price_chg and old_price else None

                reposts.append({
                    'original_id': old['id'],
                    'repost_id': lid,
                    'market': market,
                    'bedrooms': br,
                    'original_price': old_price,
                    'repost_price': new_price,
                    'price_change': price_chg,
                    'price_change_pct': price_pct,
                    'title_similarity': round(sim, 3),
                    'original_title': old.get('title'),
                    'repost_title': new_listing.get('title'),
                    'detected_date': today_str,
                })
                break  # best match only

    if reposts:
        log.info(f"  Probable reposts detected: {len(reposts)}")
        for rp in reposts[:5]:
            direction = f"${rp['original_price']}→${rp['repost_price']}" if rp['price_change'] else "same price"
            log.info(f"    {rp['original_id']}→{rp['repost_id']}: {direction} (sim={rp['title_similarity']:.2f})")

        BATCH = 50
        for i in range(0, len(reposts), BATCH):
            batch = reposts[i:i+BATCH]
            req = urllib.request.Request(
                f"{SUPABASE_URL}/rest/v1/reposts",
                data=json.dumps(batch).encode(), method='POST',
                headers={**SB_HEADERS, 'Prefer': 'resolution=ignore-duplicates,return=minimal'}
            )
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    pass
            except urllib.error.HTTPError as e:
                log.warning(f"Repost insert error {e.code}: {e.read().decode()[:200]}")


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
    dry_run = "--dry-run" in sys.argv
    log.info("═" * 60)
    log.info("SB + Ojai + Santa Maria Rental Scraper v3 — starting"
             + ("  [DRY RUN — no database writes]" if dry_run else ""))

    today_str = date.today().isoformat()
    log.info(f"Scrape date: {today_str}")

    session = requests.Session()

    # Scrape
    all_listings, failures = scrape_all(session, today_str)
    from collections import Counter
    log.info(f"Total scraped: {len(all_listings)}")
    log.info(f"By market: {dict(Counter(l['market'] for l in all_listings))}")
    log.info(f"By category: {dict(Counter(l['category'] for l in all_listings))}")

    # A scrape that collected nothing must never look like a clean run. This is
    # what let the job report success daily from 2026-06-24 while writing no data.
    if not all_listings:
        for f in failures:
            log.error(f"  {f}")
        raise SystemExit("Scraped 0 listings across all markets - aborting before "
                         "any write, so snapshots are not poisoned with empty data.")

    if dry_run:
        log.info("Dry run: skipping upsert and snapshots.")
    else:
        # Upsert
        upsert_listings(all_listings, today_str)

        # Snapshot
        compute_snapshots(today_str)

    if failures:
        for f in failures:
            log.error(f"  {f}")
        raise SystemExit(f"Completed with {len(failures)} market/category failure(s) - "
                         "data written, but coverage is incomplete.")

    log.info("Done.")
    log.info("═" * 60)


if __name__ == "__main__":
    main()
