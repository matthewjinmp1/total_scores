# Short Interest Scraper

This folder contains scripts for scraping short interest data from Finviz.

## Files

- **`scrape_short_interest.py`** - Main scraper that scrapes short interest for all tickers in `../top_scores.db`
- **`check_short_interest.py`** - Interactive tool to check short interest for a single ticker
- **`scrape_finviz_tickers.py`** - Scraper that gets tickers directly from Finviz screener (non-Selenium version)
- **`scrape_finviz_selenium.py`** - Selenium-based scraper for Finviz screener (handles JavaScript-rendered content)
- **`test_finviz_scraper.py`** - Test script for the non-Selenium Finviz scraper
- **`test_selenium_scraper.py`** - Test script for the Selenium-based scraper
- **`short_interest.db`** - SQLite database storing scraped short interest data

## Database Schema

The `short_interest.db` database has the following schema:

```sql
CREATE TABLE short_interest (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT UNIQUE NOT NULL,
    short_interest_percent REAL,
    scraped_at TEXT,
    error TEXT
)
```

## Usage

### Scrape short interest for all tickers in top_scores.db:
```bash
python3 scrape_short_interest.py
```

### Check short interest for a single ticker:
```bash
python3 check_short_interest.py
```

### Scrape from Finviz screener (Selenium - recommended):
```bash
# Test first
python3 test_selenium_scraper.py

# Then run full scraper
python3 scrape_finviz_selenium.py
```

## Requirements

- `selenium` - For JavaScript-enabled scraping
- `webdriver-manager` - For automatic ChromeDriver management
- `beautifulsoup4` - For HTML parsing
- `requests` - For HTTP requests

Install with:
```bash
pip install selenium webdriver-manager beautifulsoup4 requests
```

