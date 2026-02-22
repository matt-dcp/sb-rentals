#!/usr/bin/env python3
"""
One-time migration: SQLite (rentals.db) → Supabase via REST API

Usage:
    python3 migrate.py

Reads SUPABASE_URL and SUPABASE_KEY from the top of this file.
No environment variables needed.
"""

import sqlite3
import urllib.request
import urllib.error
import json
import re
import sys
from pathlib import Path

# ── Set your credentials here ─────────────────────────────────────────────
SUPABASE_URL = "https://wzlccltlthlaguazgten.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind6bGNjbHRsdGhsYWd1YXpndGVuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE3Nzc4ODMsImV4cCI6MjA4NzM1Mzg4M30.5KuGMYwAYGiK0UYDcHxgIjXAHd-s_v6gutigVIH_zZM"
# ──────────────────────────────────────────────────────────────────────────

DB_PATH    = Path(__file__).parent / "rentals.db"
BATCH_SIZE = 50   # REST API works best with smaller batches

WORD_TO_NUM = {'one':1,'two':2,'three':3,'four':4,'five':5,'six':6}

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

def supabase_upsert(records):
    """POST a batch of records to Supabase REST API with upsert."""
    url = f"{SUPABASE_URL}/rest/v1/listings"
    data = json.dumps(records).encode('utf-8')
    req = urllib.request.Request(
        url,
        data=data,
        method='POST',
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
        body = e.read().decode('utf-8', errors='replace')
        return e.code, body

def supabase_count():
    """Get current row count from Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/listings?select=id"
    req = urllib.request.Request(
        url,
        headers={
            'apikey':        SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}',
            'Prefer':        'count=exact',
            'Range':         '0-0',
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            content_range = resp.headers.get('Content-Range', '')
            # Format: 0-0/TOTAL
            if '/' in content_range:
                return int(content_range.split('/')[1])
    except Exception:
        pass
    return None

def main():
    print("\n── SB Rentals: SQLite → Supabase Migration ──────────────────")

    # ── Read SQLite ──
    if not DB_PATH.exists():
        print(f"❌  rentals.db not found at {DB_PATH}")
        sys.exit(1)

    print("Reading rows from rentals.db…", end=" ", flush=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT id, market, category, title, price, bedrooms, bathrooms,
               sqft, neighborhood, url, posted_date, scraped_at
        FROM listings ORDER BY scraped_at
    """).fetchall()
    conn.close()
    sqlite_total = len(rows)
    print(f"✓  ({sqlite_total:,} rows)")

    # ── Re-parse bedrooms ──
    print("Re-parsing bedroom/bath data from titles…", end=" ", flush=True)
    records = []
    reparsed = 0
    for r in rows:
        beds  = r['bedrooms']
        baths = r['bathrooms']
        if beds is None and r['title']:
            new_beds, new_baths = parse_beds_baths(r['title'])
            if new_beds is not None or new_baths is not None:
                beds  = new_beds
                baths = new_baths if new_baths is not None else baths
                reparsed += 1
        records.append({
            'id':           r['id'],
            'market':       r['market'],
            'category':     r['category'],
            'title':        r['title'],
            'price':        r['price'],
            'bedrooms':     beds,
            'bathrooms':    baths,
            'sqft':         r['sqft'],
            'neighborhood': r['neighborhood'],
            'url':          r['url'],
            'posted_date':  r['posted_date'],
            'scraped_at':   r['scraped_at'],
        })
    print(f"✓  (improved {reparsed:,} rows)")

    # ── Upload in batches ──
    print(f"Uploading {sqlite_total:,} rows to Supabase in batches of {BATCH_SIZE}…")
    errors = 0
    total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(records), BATCH_SIZE):
        batch     = records[i:i+BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        status, err = supabase_upsert(batch)
        if err:
            errors += len(batch)
            print(f"\n  ⚠️  Batch {batch_num}/{total_batches} failed ({status}): {err[:200]}")
        else:
            done = min(i + BATCH_SIZE, len(records))
            print(f"  {done:>4}/{len(records)} rows  [{batch_num}/{total_batches}]", end="\r")

    print()

    # ── Verify ──
    print("Verifying…", end=" ", flush=True)
    pg_total = supabase_count()
    if pg_total is not None:
        print(f"✓")
        print(f"\n  SQLite rows:   {sqlite_total:>6,}")
        print(f"  Supabase rows: {pg_total:>6,}")
        if pg_total >= sqlite_total:
            print(f"\n✅  Migration complete. All {sqlite_total:,} rows are in Supabase.")
        else:
            print(f"\n⚠️  {sqlite_total - pg_total} rows missing — re-run the script (safe to re-run).")
    else:
        print("could not verify count — check Supabase table editor to confirm rows arrived.")

    if errors:
        print(f"⚠️  {errors} rows had upload errors — re-run to retry.")

    print("─────────────────────────────────────────────────────────────\n")

if __name__ == "__main__":
    main()
