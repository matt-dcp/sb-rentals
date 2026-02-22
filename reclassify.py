#!/usr/bin/env python3
"""
One-time re-classification of existing Supabase data.

Applies current scraper logic to every row already in the database:
  1. DELETE  rows with no price or price = 0 (junk)
  2. DELETE  rows matching junk title patterns
  3. UPDATE  category = 'room_rental' where title matches room patterns
  4. UPDATE  market to correct neighborhood silo (goleta, isla_vista, etc.)

Safe to re-run — all operations are idempotent.
"""

import urllib.request
import urllib.error
import urllib.parse
import json
import re
import sys

# ── Credentials (hardcoded — same as scraper) ─────────────────────────────
SUPABASE_URL = "https://wzlccltlthlaguazgten.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind6bGNjbHRsdGhsYWd1YXpndGVuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE3Nzc4ODMsImV4cCI6MjA4NzM1Mzg4M30.5KuGMYwAYGiK0UYDcHxgIjXAHd-s_v6gutigVIH_zZM"

HEADERS = {
    'apikey':        SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type':  'application/json',
    'Prefer':        'return=representation',
}

# ── Logic (mirrors scraper.py exactly) ───────────────────────────────────
JUNK_PATTERNS = [
    r'\bwanted\b', r'\biso\b', r'looking for', r'housing needed', r'need(ing)? (a |)room',
    r'seeking (a |)room', r'in search of', r'house sitter', r'need(s?) housing',
    r'office space', r'retail space', r'commercial (space|yard|zoned)',
    r'warehouse', r'\bindustrial\b', r'lab(oratory)? (space|office)',
    r'co-?working', r'storage (unit|space|facility|lot)',
    r'self storage', r'parking space', r'carport', r'garage (space|for rent)',
    r'for sale', r'\bacres?\b', r'\blot\b.*\$[5-9]\d{4}', r'commercial zoned land',
    r'private money loan', r'scam alert', r'^free\b',
    r'house sitter', r'vacation maint',
    r'\bcamper\b', r'\btrailer for rent\b', r'\brv (space|lot|storage)\b',
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
    r'\broom to (live|rent)\b',
    r'individual bed.?space',
    r'\bavail(able)? for (immediate|female|male|1 )',
    r'\b(room|bedroom) (w/|with) (private|shared) bath\b',
]
ROOM_RE = re.compile('|'.join(ROOM_PATTERNS), re.I)

NEIGHBORHOOD_SILOS = [
    ("isla_vista",  ["isla vista", "isla vista iv", "ucsb iv", "ucsb area", "iv "]),
    ("goleta",      ["goleta", "noleta", "ellwood", "storke", "glen annie", "glenn annie"]),
    ("carpinteria", ["carpinteria", "carpintería", "summerland"]),
    ("solvang",     ["solvang", "ballard", "santa ynez", "los olivos"]),
    ("buellton",    ["buellton"]),
    ("lompoc",      ["lompoc", "vandenberg"]),
]

def resolve_market(neighborhood):
    """Returns silo name or None if it stays in santa_barbara."""
    if not neighborhood:
        return None
    nl = neighborhood.lower()
    matched = [silo for silo, patterns in NEIGHBORHOOD_SILOS if any(p in nl for p in patterns)]
    if len(matched) == 1:
        return matched[0]
    return None  # multi-city or no match → stay in santa_barbara


# ── Supabase helpers ──────────────────────────────────────────────────────
def sb_request(method, path, body=None, params=None):
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    if params:
        url += '?' + urllib.parse.urlencode(params, doseq=True)
    data = json.dumps(body).encode('utf-8') if body else None
    req  = urllib.request.Request(url, data=data, method=method, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else []
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8', errors='replace')

def fetch_all():
    """Fetch all listings in pages of 1000."""
    all_rows = []
    offset   = 0
    PAGE     = 1000
    while True:
        status, data = sb_request('GET', 'listings', params={
            'select': 'id,market,category,title,price,neighborhood',
            'order':  'id',
            'limit':  str(PAGE),
            'offset': str(offset),
        })
        if not isinstance(data, list) or not data:
            break
        all_rows.extend(data)
        if len(data) < PAGE:
            break
        offset += PAGE
    return all_rows

def delete_ids(ids, reason):
    if not ids:
        return
    # Supabase REST: DELETE with id=in.(a,b,c)
    id_list = ','.join(f'"{i}"' for i in ids)
    status, resp = sb_request('DELETE', f'listings?id=in.({id_list})')
    mark = '✓' if status in (200, 204) else f'ERROR {status}'
    print(f"  {mark}  Deleted {len(ids):>4} rows — {reason}")

def update_rows(updates, field, reason):
    """
    updates: list of (id, new_value)
    Groups by new_value and issues one PATCH per distinct value.
    """
    if not updates:
        return
    by_value = {}
    for rid, val in updates:
        by_value.setdefault(val, []).append(rid)
    total = 0
    for val, ids in by_value.items():
        id_list = ','.join(f'"{i}"' for i in ids)
        status, resp = sb_request(
            'PATCH',
            f'listings?id=in.({id_list})',
            body={field: val}
        )
        if status in (200, 204):
            total += len(ids)
        else:
            print(f"  ⚠️  PATCH error ({status}): {str(resp)[:200]}")
    mark = '✓' if total else '⚠️'
    print(f"  {mark}  Updated {total:>4} rows — {reason}")


# ── Main ──────────────────────────────────────────────────────────────────
def main():
    print("\n── Re-classification: Supabase Existing Data ────────────────")

    print("Fetching all rows from Supabase…", end=" ", flush=True)
    rows = fetch_all()
    print(f"✓  ({len(rows):,} rows)")

    # Categorize every row
    to_delete_no_price  = []
    to_delete_junk      = []
    to_room_rental      = []
    to_resifo           = []   # (id, new_market)

    for r in rows:
        rid   = r['id']
        title = r.get('title') or ''
        price = r.get('price')
        hood  = r.get('neighborhood') or ''
        mkt   = r.get('market') or ''
        cat   = r.get('category') or ''

        # 1. No price → delete
        if not price:
            to_delete_no_price.append(rid)
            continue

        # 2. Junk title → delete
        if JUNK_RE.search(title):
            to_delete_junk.append(rid)
            continue

        # 3. Room rental detection (only if not already set)
        if cat != 'room_rental' and ROOM_RE.search(title):
            to_room_rental.append((rid, 'room_rental'))

        # 4. Neighborhood silo (only for santa_barbara listings)
        if mkt == 'santa_barbara':
            silo = resolve_market(hood)
            if silo:
                to_resifo.append((rid, silo))

    # Print preview before making any changes
    print(f"\nPlan:")
    print(f"  Delete — no price:    {len(to_delete_no_price):>4}")
    print(f"  Delete — junk title:  {len(to_delete_junk):>4}")
    print(f"  Update — room rental: {len(to_room_rental):>4}")
    print(f"  Update — resilo mkt:  {len(to_resifo):>4}")

    if to_room_rental:
        print(f"\n  Room rental titles (sample):")
        ids_set = {rid for rid,_ in to_room_rental}
        for r in rows:
            if r['id'] in ids_set:
                print(f"    ${r.get('price','?'):>5}  {r.get('title','')[:70]}")

    if to_resifo:
        print(f"\n  Resilo assignments (sample):")
        ids_set = {rid for rid,_ in to_resifo}
        silo_map = {rid: silo for rid, silo in to_resifo}
        shown = 0
        for r in rows:
            if r['id'] in ids_set and shown < 20:
                print(f"    {silo_map[r['id']]:<15}  {r.get('neighborhood','')[:40]}")
                shown += 1

    print()
    confirm = input("Apply these changes? [y/N] ").strip().lower()
    if confirm != 'y':
        print("Aborted — no changes made.\n")
        sys.exit(0)

    print("\nApplying…")
    delete_ids(to_delete_no_price, "no price")
    delete_ids(to_delete_junk,     "junk title")
    update_rows(to_room_rental, 'category', "room_rental category")
    update_rows(to_resifo,      'market',   "neighborhood silo")

    # Final count
    print("\nVerifying…", end=" ", flush=True)
    status, data = sb_request('GET', 'listings', params={
        'select': 'market',
        'limit':  '10000',
    })
    if isinstance(data, list):
        from collections import Counter
        counts = Counter(r['market'] for r in data)
        print(f"✓  ({len(data):,} rows remain)\n")
        print("  Rows by market:")
        for mkt, cnt in sorted(counts.items()):
            print(f"    {mkt:<20} {cnt:>4}")
    else:
        print("could not verify")

    print("\n✅  Re-classification complete.")
    print("────────────────────────────────────────────────────────────\n")

if __name__ == "__main__":
    main()
