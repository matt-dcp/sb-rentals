#!/usr/bin/env python3
"""
One-time re-classification of existing Supabase data.

Applies current scraper logic to every row already in the database:
  1. DELETE  rows with no price or price = 0
  2. DELETE  rows with junk URL codes (off, prk, reb, reo, sbw, vac)
  3. DELETE  rows matching junk title patterns
  4. UPDATE  category = 'room_rental' for roo/sha URL codes or matching titles
  5. UPDATE  category = 'houses' for hou URL codes or house-title-in-apa
  6. UPDATE  market to correct neighborhood silo

Safe to re-run — all operations are idempotent.
"""

import urllib.request, urllib.error, urllib.parse
import json, re, sys
from collections import Counter

SUPABASE_URL = "https://wzlccltlthlaguazgten.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind6bGNjbHRsdGhsYWd1YXpndGVuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE3Nzc4ODMsImV4cCI6MjA4NzM1Mzg4M30.5KuGMYwAYGiK0UYDcHxgIjXAHd-s_v6gutigVIH_zZM"

HEADERS = {
    'apikey':        SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type':  'application/json',
    'Prefer':        'return=representation',
}

# ── Classification logic (mirrors scraper.py) ─────────────────────────────
URL_CODE_JUNK      = {'off', 'prk', 'reb', 'reo', 'sbw', 'vac'}
URL_CODE_ROOM      = {'roo', 'sha'}
URL_CODE_HOUSES    = {'hou'}
URL_CODE_APARTMENT = {'apa', 'sub'}

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

HOUSE_IN_APT_RE = re.compile(
    r'\b(house|home|bungalow|cottage|duplex|townhome|townhouse|ranch|cabin|villa|estate)\b',
    re.I
)

NEIGHBORHOOD_SILOS = [
    ("isla_vista",  ["isla vista", "isla vista iv", "ucsb iv", "ucsb area", "iv "]),
    ("goleta",      ["goleta", "noleta", "ellwood", "storke", "glen annie", "glenn annie"]),
    ("carpinteria", ["carpinteria", "carpintería", "summerland"]),
    ("solvang",     ["solvang", "ballard", "santa ynez", "los olivos"]),
    ("buellton",    ["buellton"]),
    ("lompoc",      ["lompoc", "vandenberg"]),
]

def url_code(url):
    m = re.search(r'\.org/([a-z]+)/', url or '')
    return m.group(1) if m else None

def resolve_silo(neighborhood):
    if not neighborhood:
        return None
    nl = neighborhood.lower()
    matched = [s for s, patterns in NEIGHBORHOOD_SILOS if any(p in nl for p in patterns)]
    return matched[0] if len(matched) == 1 else None

# ── Supabase helpers ──────────────────────────────────────────────────────
def sb_request(method, path, body=None, params=None):
    url  = f"{SUPABASE_URL}/rest/v1/{path}"
    if params:
        url += '?' + urllib.parse.urlencode(params, doseq=True)
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(url, data=data, method=method, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else []
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8', errors='replace')

def fetch_all():
    rows, offset = [], 0
    while True:
        status, data = sb_request('GET', 'listings', params={
            'select': 'id,market,category,title,price,neighborhood,url',
            'order': 'id', 'limit': '1000', 'offset': str(offset),
        })
        if not isinstance(data, list) or not data:
            break
        rows.extend(data)
        if len(data) < 1000:
            break
        offset += 1000
    return rows

def delete_ids(ids, reason):
    if not ids:
        print(f"  —    Deleted      0 rows — {reason}")
        return
    id_list = ','.join(f'"{i}"' for i in ids)
    status, _ = sb_request('DELETE', f'listings?id=in.({id_list})')
    mark = '✓' if status in (200, 204) else f'ERROR {status}'
    print(f"  {mark}  Deleted   {len(ids):>4} rows — {reason}")

def patch_field(updates, field, reason):
    if not updates:
        print(f"  —    Updated      0 rows — {reason}")
        return
    by_val = {}
    for rid, val in updates:
        by_val.setdefault(val, []).append(rid)
    total = 0
    for val, ids in by_val.items():
        id_list = ','.join(f'"{i}"' for i in ids)
        status, resp = sb_request('PATCH', f'listings?id=in.({id_list})', body={field: val})
        if status in (200, 204):
            total += len(ids)
        else:
            print(f"  ⚠️   PATCH error ({status}): {str(resp)[:200]}")
    print(f"  ✓   Updated   {total:>4} rows — {reason}")

# ── Main ──────────────────────────────────────────────────────────────────
def main():
    print("\n── Re-classification: Supabase Existing Data ────────────────")
    print("Fetching all rows…", end=" ", flush=True)
    rows = fetch_all()
    print(f"✓  ({len(rows):,} rows)")

    to_delete_no_price  = []
    to_delete_url_junk  = []
    to_delete_title_junk = []
    to_room_rental      = []
    to_houses           = []
    to_resilo           = []

    for r in rows:
        rid   = r['id']
        title = r.get('title') or ''
        price = r.get('price')
        hood  = r.get('neighborhood') or ''
        mkt   = r.get('market') or ''
        cat   = r.get('category') or ''
        code  = url_code(r.get('url') or '')

        if not price:
            to_delete_no_price.append(rid)
            continue
        if code in URL_CODE_JUNK:
            to_delete_url_junk.append(rid)
            continue
        if JUNK_RE.search(title):
            to_delete_title_junk.append(rid)
            continue

        # Room rental
        if cat != 'room_rental' and (code in URL_CODE_ROOM or ROOM_RE.search(title)):
            to_room_rental.append((rid, 'room_rental'))
            continue  # don't also try to resilo room rentals

        # House correction
        if cat == 'apartments':
            if code in URL_CODE_HOUSES or HOUSE_IN_APT_RE.search(title):
                to_houses.append((rid, 'houses'))

        # Neighborhood silo (santa_barbara only)
        if mkt == 'santa_barbara':
            silo = resolve_silo(hood)
            if silo:
                to_resilo.append((rid, silo))

    # ── Preview ──
    print(f"\nPlan:")
    print(f"  Delete — no price:         {len(to_delete_no_price):>4}")
    print(f"  Delete — junk URL code:    {len(to_delete_url_junk):>4}")
    print(f"  Delete — junk title:       {len(to_delete_title_junk):>4}")
    print(f"  Update — → room_rental:    {len(to_room_rental):>4}")
    print(f"  Update — apt → houses:     {len(to_houses):>4}")
    print(f"  Update — resilo market:    {len(to_resilo):>4}")

    if to_room_rental:
        print(f"\n  Room rental samples:")
        rm_ids = {rid for rid,_ in to_room_rental}
        for r in rows:
            if r['id'] in rm_ids:
                print(f"    ${r.get('price','?'):>5}  {r.get('title','')[:65]}")

    if to_houses:
        print(f"\n  Apt → houses samples:")
        h_ids = {rid for rid,_ in to_houses}
        for r in rows:
            if r['id'] in h_ids:
                print(f"           {r.get('title','')[:65]}")

    if to_resilo:
        print(f"\n  Resilo samples (first 15):")
        silo_map = {rid: silo for rid, silo in to_resilo}
        shown = 0
        for r in rows:
            if r['id'] in silo_map and shown < 15:
                print(f"    → {silo_map[r['id']]:<14}  {r.get('neighborhood','')[:40]}")
                shown += 1

    print()
    confirm = input("Apply these changes? [y/N] ").strip().lower()
    if confirm != 'y':
        print("Aborted — no changes made.\n")
        sys.exit(0)

    print("\nApplying…")
    delete_ids(to_delete_no_price,   "no price")
    delete_ids(to_delete_url_junk,   "junk URL code (off/prk/reb/reo/sbw/vac)")
    delete_ids(to_delete_title_junk, "junk title pattern")
    patch_field(to_room_rental, 'category', "room_rental")
    patch_field(to_houses,      'category', "apartments → houses")
    patch_field(to_resilo,      'market',   "neighborhood silo")

    # ── Verify ──
    print("\nFinal counts…")
    _, data = sb_request('GET', 'listings', params={'select': 'market,category', 'limit': '10000'})
    if isinstance(data, list):
        mkt_counts = Counter(r['market'] for r in data)
        cat_counts = Counter(r['category'] for r in data)
        print(f"\n  Total rows: {len(data):,}\n")
        print("  By market:")
        for k, v in sorted(mkt_counts.items()):
            print(f"    {k:<20} {v:>4}")
        print("\n  By category:")
        for k, v in sorted(cat_counts.items()):
            print(f"    {k:<20} {v:>4}")

    print("\n✅  Re-classification complete.")
    print("────────────────────────────────────────────────────────────\n")

if __name__ == "__main__":
    main()
