# Santa Barbara Rental Market Tracker

Scrapes Craigslist SB daily for apartments/condos and houses.
Stores data in a local SQLite database. Serves a web dashboard.

## Setup

```bash
pip3 install requests beautifulsoup4 flask
```

## Usage

### 1. Run the scraper (first time + daily)
```bash
python3 scraper.py
```
This hits Craigslist SB apartments and houses categories, up to 1,200 listings
per category. New listings are inserted; duplicates are skipped. Runs in ~2-3 min.

### 2. Start the dashboard server
```bash
python3 server.py
```
Then open **http://localhost:5050** in your browser.

## Automate with cron (macOS/Linux)

Run `crontab -e` and add:
```
0 7 * * * /usr/bin/python3 /path/to/sb_rentals/scraper.py >> /path/to/sb_rentals/scraper.log 2>&1
```
This runs the scraper every morning at 7am.

## Files

| File             | Purpose                                      |
|------------------|----------------------------------------------|
| `scraper.py`     | Craigslist scraper — run daily               |
| `server.py`      | Flask API + dashboard server                 |
| `dashboard.html` | Web dashboard (served by Flask)              |
| `rentals.db`     | SQLite database (auto-created on first run)  |
| `scraper.log`    | Scrape history and error log                 |

## Dashboard features

- **KPIs**: total listings, avg rent overall/by type, days tracked
- **Price distribution**: histogram by price bucket
- **Avg rent by bedrooms**: Studio → 4+BR
- **New listings over time**: 30-day trend
- **Top neighborhoods**: ranked by count with avg price
- **Browse/filter table**: filter by type, bedrooms, price range, links to original posts

## Notes

- Price filters exclude outliers (<$500, >$20k) from averages
- Craigslist listings expire ~30 days after posting
- `INSERT OR IGNORE` means the same post ID is never double-counted
- Neighborhood parsing depends on Craigslist's HTML structure; may need 
  adjustment if they change their markup
