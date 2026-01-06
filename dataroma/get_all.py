#!/usr/bin/env python3
"""
Scrape Dataroma data for all tickers in finviz/top_tickers.db
and store the metrics in dataroma/metrics.db
"""

import sys
import os
import sqlite3
import time
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataroma.get_one import scrape_dataroma_stock

# Database paths
TOP_TICKERS_DB = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'finviz', 'top_tickers.db')
METRICS_DB = os.path.join(os.path.dirname(__file__), 'metrics.db')


def init_metrics_db():
    """Initialize the Dataroma metrics database."""
    conn = sqlite3.connect(METRICS_DB)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dataroma_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            company_name TEXT,
            ownership_count REAL,
            portfolio_percent REAL,
            price_move_percent REAL,
            net_buys REAL,
            net_dollars_percent_of_market_cap REAL,
            error TEXT,
            scraped_at TEXT,
            UNIQUE(ticker, scraped_at)
        )
    ''')
    
    # Create index for faster lookups
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_ticker_scraped 
        ON dataroma_metrics(ticker, scraped_at)
    ''')
    
    conn.commit()
    conn.close()
    print(f"✓ Initialized metrics database: {METRICS_DB}")


def get_tickers_from_db():
    """Get all unique tickers from top_tickers.db, ordered by rank."""
    if not os.path.exists(TOP_TICKERS_DB):
        raise FileNotFoundError(f"top_tickers.db not found at {TOP_TICKERS_DB}")
    
    conn = sqlite3.connect(TOP_TICKERS_DB)
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='top_tickers'")
    if not cursor.fetchone():
        conn.close()
        raise FileNotFoundError(f"'top_tickers' table not found in {TOP_TICKERS_DB}")
    
    # Get the most recent fetch (latest fetched_at)
    cursor.execute('''
        SELECT DISTINCT ticker
        FROM top_tickers
        WHERE fetched_at = (SELECT MAX(fetched_at) FROM top_tickers)
        ORDER BY rank
    ''')
    
    tickers = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    return tickers


def get_existing_tickers():
    """Get tickers that already have data in the metrics database (latest record per ticker)."""
    if not os.path.exists(METRICS_DB):
        return set()
    
    conn = sqlite3.connect(METRICS_DB)
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dataroma_metrics'")
    if not cursor.fetchone():
        conn.close()
        return set()
    
    # Get tickers that have the most recent data (latest scraped_at)
    cursor.execute('''
        SELECT DISTINCT ticker
        FROM dataroma_metrics
        WHERE scraped_at = (SELECT MAX(scraped_at) FROM dataroma_metrics d2 WHERE d2.ticker = dataroma_metrics.ticker)
    ''')
    existing = {row[0] for row in cursor.fetchall()}
    conn.close()
    
    return existing


def save_metrics(data):
    """Save Dataroma metrics to the database."""
    conn = sqlite3.connect(METRICS_DB)
    cursor = conn.cursor()
    
    scraped_at = datetime.now().isoformat()
    
    # Use INSERT OR IGNORE to avoid duplicates, or get the latest record per ticker
    # We'll store multiple records with timestamps, but can query for latest
    cursor.execute('''
        INSERT INTO dataroma_metrics (
            ticker, company_name, ownership_count, portfolio_percent, price_move_percent,
            net_buys, net_dollars_percent_of_market_cap, error, scraped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data.get('ticker'),
        data.get('company_name'),
        data.get('ownership_count'),
        data.get('portfolio_percent'),
        data.get('price_move_percent'),
        data.get('net_buys'),
        data.get('net_dollars_percent_of_market_cap'),
        data.get('error'),
        scraped_at
    ))
    
    conn.commit()
    conn.close()
    return True


def main():
    """Main function to scrape Dataroma data for all tickers."""
    print("=" * 80)
    print("DATAROMA METRICS SCRAPER")
    print("=" * 80)
    print()
    
    # Initialize database
    init_metrics_db()
    
    # Get all tickers
    print("Loading tickers from finviz/top_tickers.db...")
    all_tickers = get_tickers_from_db()
    
    if not all_tickers:
        print("✗ No tickers found in database")
        return
    
    print(f"✓ Found {len(all_tickers)} tickers")
    
    # Get existing tickers
    existing_tickers = get_existing_tickers()
    print(f"Found {len(existing_tickers)} tickers already in dataroma database")
    
    # Filter out existing tickers (or comment this out to re-scrape all)
    tickers_to_scrape = [t for t in all_tickers if t not in existing_tickers]
    
    if not tickers_to_scrape:
        print("✓ All tickers already scraped!")
        return
    
    print(f"Scraping {len(tickers_to_scrape)} new tickers...")
    print()
    
    # Scrape with rate limiting
    success_count = 0
    error_count = 0
    skip_count = 0
    
    for i, ticker in enumerate(tickers_to_scrape, 1):
        print(f"[{i}/{len(tickers_to_scrape)}] Processing {ticker}...", end=' ', flush=True)
        
        # Scrape data
        data = scrape_dataroma_stock(ticker)
        
        if data.get('error'):
            print(f"✗ {data['error']}")
            error_count += 1
            # Still save the error
            save_metrics(data)
            continue
        
        # Check if we got any useful data (at least one metric)
        if (data.get('ownership_count') is None and 
            data.get('portfolio_percent') is None and
            data.get('price_move_percent') is None and
            data.get('net_buys') is None and
            data.get('net_dollars_percent_of_market_cap') is None):
            print("✗ No data found")
            skip_count += 1
            save_metrics(data)
            continue
        
        # Save metrics
        if save_metrics(data):
            metrics_found = sum([
                1 for key in ['ownership_count', 'portfolio_percent', 'price_move_percent',
                             'net_buys', 'net_dollars_percent_of_market_cap']
                if data.get(key) is not None
            ])
            print(f"✓ Saved ({metrics_found} metrics)")
            success_count += 1
        else:
            print("✗ Save failed")
            error_count += 1
        
        # Rate limiting - small delay between requests
        time.sleep(0.5)
    
    print()
    print("=" * 80)
    print("SCRAPING SUMMARY")
    print("=" * 80)
    print(f"Successfully scraped: {success_count}")
    print(f"Errors: {error_count}")
    print(f"Skipped (no data): {skip_count}")
    print(f"Metrics saved to: {METRICS_DB}")
    print()


if __name__ == "__main__":
    main()

