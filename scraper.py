#!/usr/bin/env python3
"""
SB + Ojai Craigslist Rental Scraper
Scrapes apartments/condos and houses daily from Santa Barbara and Ojai.
Run manually or via cron: 0 7 * * * /usr/bin/python3 /path/to/scraper.py
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

DB_PATH  = Path(__file__).parent / "rentals.db"
LOG_PATH = Path(__file__).parent / "scraper.log"

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

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}
REQUEST_DELAY = 1.5
MAX_PAGES     = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

WORD_TO_NUM = {'one':1,'two':2,'three':3,'four':4,'five':5,'six':6}


def init_db(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS listings (
            id            TEXT PRIMARY KEY,
            market        TEXT,
            category      TEXT,
            title         TEXT,
            price         INTEGER,
            bedrooms      REAL,
            bathrooms     REAL,
            sqft          INTEGER,
            neighborhood  TEXT,
            url           TEXT,
            posted_date   TEXT,
            scraped_at    TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_scraped ON listings(scraped_at);
        CREATE INDEX IF NOT EXISTS idx_price   ON listings(price);
        CREATE INDEX IF NOT EXISTS idx_market  ON listings(market);
        CREATE INDEX IF NOT EXISTS idx_hood    ON listings(neighborhood);
    """)
    cols = [row[1] for row in conn.execute("PRAGMA table_info(listings)").fetchall()]
    if "market" not in cols:
        conn.execute("ALTER TABLE listings ADD COLUMN market TEXT DEFAULT 'santa_barbara'")
        log.info("Migrated DB: added market column")
    conn.commit()


def parse_price(text):
    m = re.search(r"\$?([\d,]+)", text or "")
    return int(m.group(1).replace(",", "")) if m else None


def parse_beds_baths(s):
    """
    Comprehensive title parser. Catches all known Craigslist SB/Ojai formats:
      2br/1ba  2BD/2BATH  2bed/1bath  2 bed 1 bath  2 bedroom 1 bathroom
      4 BR  Studio  2.5 bath  2B+1B  2B/1B  2B2B  2x2  2 x1
      1-Bed  1-Bedroom  One Bedroom  Three Bedroom  Two Bath
      2/1  2/2  Duplex 2/1  3/2.5 Condo
    """
    beds = baths = None
    if not s:
        return beds, baths
    sl = s.lower()

    # ── Studio ──
    if re.search(r'\bstudio\b', sl):
        beds = 0.0

    # ── Bedrooms ──
    if beds is None:
        bed_patterns = [
            # Standard: 2br, 2bd, 2bdrm, 2bed, 2bedroom, 2bedrooms
            r'(\d+)\s*(?:br|bed(?:room)?s?|bd|bdrm)\b',
            # Hyphenated: 1-bed, 2-bedroom, 1-Bed
            r'(\d+)-bed(?:room)?s?\b',
            # Written out: One Bedroom, Three Bedrooms
            r'\b(one|two|three|four|five|six)\s*(?:-\s*)?bed(?:room)?s?\b',
            # Shorthand combos: 2B+1B, 2B/1B (beds = first number before B)
            r'\b(\d)[Bb]\s*[+/]\s*\d[Bb]\b',
            # Compact: 2B2B (beds = first digit)
            r'\b(\d)[Bb](\d)[Bb]\b',
            # NxN format: 2x2, 2x1, 2 x1 (beds/baths)
            r'\b([1-5])\s*[xX]\s*[1-5]\b',
            # Slash fraction with context: 2/1, 2/2 near housing words
            r'\b(\d)\s*/\s*\d\b(?=.{0,60}(?:furnished|duplex|condo|upgraded|utilities|rent|house|home|apt|unit))',
        ]
        for pat in bed_patterns:
            m = re.search(pat, sl)
            if m:
                val = m.group(1)
                beds = float(WORD_TO_NUM[val]) if val in WORD_TO_NUM else float(val)
                break

    # ── Bathrooms ──
    bath_patterns = [
        # Standard: 1ba, 1bath, 1bathroom, 2.5 bath
        r'(\d+(?:\.\d+)?)\s*(?:ba|bath(?:room)?s?)\b',
        # Hyphenated: 1-bath, 2-bathroom
        r'(\d+(?:\.\d+)?)-bath(?:room)?s?\b',
        # Written: Two Bath
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


def scrape_page(session, url, market, category, base_url):
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

            post_id = None
            if post_url:
                m = re.search(r"/(\d+)\.html", post_url)
                if m:
                    post_id = m.group(1)
            if not post_id:
                continue

            price_el     = item.select_one(".price")
            price        = parse_price(price_el.get_text(strip=True)) if price_el else None
            hood_el      = item.select_one(".location")
            neighborhood = hood_el.get_text(strip=True).title() if hood_el else None
            beds, baths  = parse_beds_baths(title)
            sqft         = parse_sqft(title)

            results.append({
                "id":           post_id,
                "market":       market,
                "category":     category,
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


def scrape_category(session, market, category, search_url, base_url):
    all_listings = []
    for page in range(MAX_PAGES):
        url      = f"{search_url}?start={page * 120}"
        log.info(f"  [{market}/{category}] page {page + 1}")
        listings = scrape_page(session, url, market, category, base_url)
        if not listings:
            log.info(f"  [{market}/{category}] no more results")
            break
        all_listings.extend(listings)
        time.sleep(REQUEST_DELAY)
    return all_listings


def upsert_listings(conn, listings):
    inserted = skipped = 0
    for l in listings:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO listings
                    (id, market, category, title, price, bedrooms, bathrooms,
                     sqft, neighborhood, url, posted_date, scraped_at)
                VALUES
                    (:id, :market, :category, :title, :price, :bedrooms, :bathrooms,
                     :sqft, :neighborhood, :url, :posted_date, :scraped_at)
            """, l)
            if conn.execute("SELECT changes()").fetchone()[0]:
                inserted += 1
            else:
                skipped += 1
        except sqlite3.Error as e:
            log.warning(f"DB error on {l.get('id')}: {e}")
    conn.commit()
    return inserted, skipped


def backfill_bedrooms(conn):
    """Re-parse bedroom/bath counts for existing listings that have NULL bedrooms."""
    c = conn.cursor()
    c.execute("SELECT id, title FROM listings WHERE bedrooms IS NULL AND title IS NOT NULL")
    rows = c.fetchall()
    updated = 0
    for (lid, title) in rows:
        beds, baths = parse_beds_baths(title)
        if beds is not None or baths is not None:
            c.execute(
                "UPDATE listings SET bedrooms=?, bathrooms=? WHERE id=?",
                (beds, baths, lid)
            )
            updated += 1
    conn.commit()
    log.info(f"Backfill: updated {updated} of {len(rows)} NULL-bedroom records")
    return updated


def main():
    log.info("═" * 60)
    log.info("SB + Ojai Rental Scraper — starting")

    conn    = sqlite3.connect(DB_PATH)
    session = requests.Session()
    init_db(conn)

    # Backfill existing records first
    log.info("Running bedroom backfill on existing data…")
    backfill_bedrooms(conn)

    total_inserted = total_skipped = 0
    for mkt in MARKETS:
        market   = mkt["market"]
        base_url = mkt["base_url"]
        log.info(f"Market: {market}")
        for category in ("apartments", "houses"):
            listings = scrape_category(session, market, category, mkt[category], base_url)
            log.info(f"  Fetched {len(listings)} listings")
            ins, skp = upsert_listings(conn, listings)
            log.info(f"  Inserted: {ins}  |  Skipped: {skp}")
            total_inserted += ins
            total_skipped  += skp

    conn.close()
    log.info(f"Done. Inserted: {total_inserted}  |  Skipped: {total_skipped}")
    log.info("═" * 60)


if __name__ == "__main__":
    main()
