#!/usr/bin/env python3
"""
SB + Ojai Rental Dashboard Server
Run: python3 server.py
Then open: http://localhost:5050
"""

from flask import Flask, jsonify, send_from_directory, request
import sqlite3
from pathlib import Path

DB_PATH     = Path(__file__).parent / "rentals.db"
STATIC_PATH = Path(__file__).parent

app = Flask(__name__)

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def market_filter(market):
    if market and market != "all":
        return "AND market = ?", [market]
    return "", []

@app.route("/")
def index():
    return send_from_directory(STATIC_PATH, "dashboard.html")

# ── Summary stats ─────────────────────────────────────────────────────────────
@app.route("/api/summary")
def summary():
    if not DB_PATH.exists():
        return jsonify({"error": "No database found. Run scraper.py first."}), 404

    mkt  = request.args.get("market", "all")
    mf, mp = market_filter(mkt)
    conn = get_db()
    c    = conn.cursor()

    c.execute(f"SELECT COUNT(*) as total, COUNT(DISTINCT DATE(scraped_at)) as days_tracked FROM listings WHERE 1=1 {mf}", mp)
    overview = dict(c.fetchone())
    c.execute("SELECT MAX(scraped_at) as last_scrape FROM listings")
    overview["last_scrape"] = (c.fetchone()["last_scrape"] or "Never")[:19]

    # By category
    c.execute(f"""
        SELECT category,
               COUNT(*) as count,
               ROUND(AVG(price),0) as avg_price,
               MIN(price) as min_price,
               MAX(price) as max_price,
               ROUND(AVG(bedrooms),1) as avg_beds
        FROM listings
        WHERE price BETWEEN 500 AND 20000 {mf}
        GROUP BY category
    """, mp)
    by_category = [dict(r) for r in c.fetchall()]

    # Price distribution
    c.execute(f"""
        SELECT
            CASE
                WHEN price < 1500 THEN 'Under $1.5k'
                WHEN price < 2000 THEN '$1.5–2k'
                WHEN price < 2500 THEN '$2–2.5k'
                WHEN price < 3000 THEN '$2.5–3k'
                WHEN price < 4000 THEN '$3–4k'
                WHEN price < 5000 THEN '$4–5k'
                ELSE '$5k+'
            END as bucket,
            COUNT(*) as count
        FROM listings
        WHERE price BETWEEN 500 AND 20000 {mf}
        GROUP BY bucket
        ORDER BY MIN(price)
    """, mp)
    price_dist = [dict(r) for r in c.fetchall()]

    # Price by bedrooms
    c.execute(f"""
        SELECT
            CASE
                WHEN bedrooms IS NULL THEN 'Unknown'
                WHEN bedrooms = 0    THEN 'Studio'
                WHEN bedrooms = 1    THEN '1 BR'
                WHEN bedrooms = 2    THEN '2 BR'
                WHEN bedrooms = 3    THEN '3 BR'
                ELSE '4+ BR'
            END as br_label,
            ROUND(AVG(price),0) as avg_price,
            COUNT(*) as count
        FROM listings
        WHERE price BETWEEN 500 AND 20000 {mf}
        GROUP BY br_label
        ORDER BY MIN(COALESCE(bedrooms, -1))
    """, mp)
    price_by_br = [dict(r) for r in c.fetchall()]

    # Daily new listings
    c.execute(f"""
        SELECT DATE(scraped_at) as date, COUNT(*) as new_listings
        FROM listings
        WHERE 1=1 {mf}
        GROUP BY DATE(scraped_at)
        ORDER BY date DESC LIMIT 30
    """, mp)
    over_time = list(reversed([dict(r) for r in c.fetchall()]))

    # Market comparison (always unfiltered)
    c.execute("""
        SELECT market,
               COUNT(*) as count,
               ROUND(AVG(price),0) as avg_price
        FROM listings
        WHERE price BETWEEN 500 AND 20000
        GROUP BY market
    """)
    by_market = [dict(r) for r in c.fetchall()]

    conn.close()
    return jsonify({
        "overview":    overview,
        "by_category": by_category,
        "price_dist":  price_dist,
        "price_by_br": price_by_br,
        "over_time":   over_time,
        "by_market":   by_market,
    })

# ── Neighborhood data for map ─────────────────────────────────────────────────
@app.route("/api/neighborhoods")
def neighborhoods():
    if not DB_PATH.exists():
        return jsonify([])

    mkt    = request.args.get("market", "all")
    mf, mp = market_filter(mkt)
    conn   = get_db()
    c      = conn.cursor()

    c.execute(f"""
        SELECT neighborhood,
               market,
               COUNT(*) as count,
               ROUND(AVG(price),0) as avg_price,
               ROUND(AVG(bedrooms),1) as avg_beds,
               MIN(price) as min_price,
               MAX(price) as max_price
        FROM listings
        WHERE neighborhood IS NOT NULL
          AND price BETWEEN 500 AND 20000
          {mf}
        GROUP BY neighborhood, market
        ORDER BY count DESC
        LIMIT 40
    """, mp)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify(rows)

# ── Listings table ────────────────────────────────────────────────────────────
@app.route("/api/listings")
def listings():
    if not DB_PATH.exists():
        return jsonify([])

    mkt       = request.args.get("market",    "all")
    category  = request.args.get("category",  "all")
    min_price = int(request.args.get("min_price", 0))
    max_price = int(request.args.get("max_price", 99999))
    bedrooms  = request.args.get("bedrooms",  "all")
    limit     = int(request.args.get("limit", 200))

    conn   = get_db()
    c      = conn.cursor()
    params = [min_price, max_price]
    where  = ["price BETWEEN ? AND ?"]

    if mkt != "all":
        where.append("market = ?"); params.append(mkt)
    if category != "all":
        where.append("category = ?"); params.append(category)
    if bedrooms == "studio":
        where.append("bedrooms = 0")
    elif bedrooms == "4+":
        where.append("bedrooms >= 4")
    elif bedrooms != "all":
        where.append("bedrooms = ?"); params.append(float(bedrooms))

    query = f"""
        SELECT id, market, category, title, price, bedrooms, bathrooms,
               sqft, neighborhood, url, posted_date, scraped_at
        FROM listings
        WHERE {' AND '.join(where)}
        ORDER BY scraped_at DESC
        LIMIT ?
    """
    params.append(limit)
    c.execute(query, params)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify(rows)

if __name__ == "__main__":
    print("\n  SB + Ojai Rental Dashboard")
    print("  Open: http://localhost:5050\n")
    app.run(port=5050, debug=False)
