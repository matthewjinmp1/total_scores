#!/usr/bin/env python3
"""
Scrape short interest data from Finviz for tickers in top_tickers.db
and store the data in a new database.
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import os
import time
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Database paths
TOP_TICKERS_DB = os.path.join(os.path.dirname(__file__), "top_tickers.db")
FINVIZ_DB = os.path.join(os.path.dirname(__file__), "finviz.db")

# Finviz base URL
FINVIZ_BASE_URL = "https://finviz.com/quote.ashx?t="

# Headers to mimic a browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

def init_short_interest_db():
    """Initialize the finviz metrics database with all fields."""
    conn = sqlite3.connect(FINVIZ_DB)
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='short_interest'")
    table_exists = cursor.fetchone() is not None
    
    if not table_exists:
        # Create new table with all columns
    cursor.execute('''
            CREATE TABLE short_interest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT UNIQUE NOT NULL,
            short_interest_percent REAL,
                forward_pe REAL,
                eps_growth_next_5y REAL,
                insider_ownership REAL,
                roa REAL,
                roic REAL,
                gross_margin REAL,
                operating_margin REAL,
                perf_10y REAL,
                recommendation TEXT,
                price_move_percent REAL,
            scraped_at TEXT,
            error TEXT
        )
    ''')
    else:
        # Table exists - add missing columns if needed
        new_columns = {
            'forward_pe': 'REAL',
            'eps_growth_next_5y': 'REAL',
            'insider_ownership': 'REAL',
            'roa': 'REAL',
            'roic': 'REAL',
            'gross_margin': 'REAL',
            'operating_margin': 'REAL',
            'perf_10y': 'REAL',
            'recommendation': 'TEXT',
            'price_move_percent': 'REAL'
        }
        
        # Get existing columns
        cursor.execute("PRAGMA table_info(short_interest)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        
        # Add missing columns
        for col_name, col_type in new_columns.items():
            if col_name not in existing_columns:
                try:
                    cursor.execute(f'ALTER TABLE short_interest ADD COLUMN {col_name} {col_type}')
                except sqlite3.OperationalError:
                    pass  # Column might already exist
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker ON short_interest(ticker)')
    conn.commit()
    conn.close()

def parse_short_interest_value(value_str):
    """
    Parse a string value that might be a number, percentage, or 'N/A'.
    Handles formats like "10.5%", "1.2%", etc.
    """
    if not value_str or value_str.strip().upper() in ['N/A', 'NAN', '']:
        return None
    
    cleaned = value_str.replace(',', '').replace('%', '').strip().upper()
    
    try:
        return float(cleaned)
    except ValueError:
        return None

def parse_pe_value(value_str):
    """Parse a PE ratio value (Forward P/E)."""
    if not value_str or value_str.strip().upper() in ['N/A', 'NAN', '']:
        return None
    
    cleaned = value_str.replace(',', '').strip()
    
    try:
        return float(cleaned)
    except ValueError:
        return None

def parse_growth_percent(value_str):
    """Parse a growth percentage value."""
    if not value_str or value_str.strip().upper() in ['N/A', 'NAN', '']:
        return None
    
    cleaned = value_str.replace(',', '').replace('%', '').strip()
    
    try:
        return float(cleaned)
    except ValueError:
        return None

def parse_recommendation(value_str):
    """Parse analyst recommendation (text value)."""
    if not value_str or value_str.strip().upper() in ['N/A', 'NAN', '']:
        return None
    
    return value_str.strip()

def parse_price(value_str):
    """Parse a price value."""
    if not value_str or value_str.strip().upper() in ['N/A', 'NAN', '']:
        return None
    
    cleaned = value_str.replace('$', '').replace(',', '').strip()
    
    try:
        return float(cleaned)
    except ValueError:
        return None

def scrape_finviz_short_interest(ticker, max_retries=3, retry_delay=5):
    """
    Scrape multiple financial metrics from Finviz for a single ticker in one request.
    
    Args:
        ticker: Ticker symbol to scrape
        max_retries: Maximum number of retry attempts
        retry_delay: Base delay in seconds between retries
    
    Returns:
        Dictionary with all metrics, ticker, scraped_at, and optional error
    """
    url = f"{FINVIZ_BASE_URL}{ticker.upper()}"
    response = None
    
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            
            if response.status_code == 429:
                if attempt < max_retries:
                    wait_time = retry_delay * (2 ** attempt)
                    time.sleep(wait_time)
                    continue
                else:
                    return {
                        'ticker': ticker.upper(),
                        'scraped_at': datetime.now().isoformat(),
                        'error': f"Rate limit error (429) after {max_retries} retries"
                    }
            
            if response.status_code == 404:
                return {
                    'ticker': ticker.upper(),
                    'scraped_at': datetime.now().isoformat(),
                    'error': f"Ticker not found (404)"
                }
            
            response.raise_for_status()
            break  # Success, exit retry loop
            
        except requests.exceptions.HTTPError as e:
            if hasattr(e, 'response') and e.response is not None and e.response.status_code == 429:
                if attempt < max_retries:
                    wait_time = retry_delay * (2 ** attempt)
                    time.sleep(wait_time)
                    continue
                else:
                    return {
                        'ticker': ticker.upper(),
                        'scraped_at': datetime.now().isoformat(),
                        'error': f"Rate limit error (429) after {max_retries} retries"
                    }
            # For other HTTP errors, if it's the last attempt, return error
            if attempt >= max_retries:
                return {
                    'ticker': ticker.upper(),
                    'scraped_at': datetime.now().isoformat(),
                    'error': f"HTTP error: {str(e)}"
                }
            time.sleep(retry_delay * (2 ** attempt))
            
        except requests.exceptions.RequestException as e:
            # For general request errors, if it's the last attempt, return error
            if attempt >= max_retries:
                return {
                    'ticker': ticker.upper(),
                    'scraped_at': datetime.now().isoformat(),
                    'error': f"Request error: {str(e)}"
                }
            time.sleep(retry_delay * (2 ** attempt))
    
    if response is None:
        return {
            'ticker': ticker.upper(),
            'scraped_at': datetime.now().isoformat(),
            'error': "Failed to get response after retries"
        }
    
    soup = BeautifulSoup(response.text, 'html.parser')
    snapshot_table = soup.find('table', class_='snapshot-table2')
    
    if not snapshot_table:
        return {
            'ticker': ticker.upper(),
            'scraped_at': datetime.now().isoformat(),
            'error': "Could not find snapshot table"
        }
    
    data = {'ticker': ticker.upper(), 'scraped_at': datetime.now().isoformat()}
    all_cells = snapshot_table.find_all('td')
    found_metrics = set()
    
    # Parse all metrics in one pass through the table
    for i in range(0, len(all_cells) - 1, 2):
        if i + 1 >= len(all_cells):
            break
            
        label = all_cells[i].get_text(strip=True)
        value = all_cells[i + 1].get_text(strip=True)
        label_upper = label.upper()
        
        # Look for Short Float
        if 'SHORT FLOAT' in label_upper and 'short_interest_percent' not in found_metrics:
            data['short_interest_percent'] = parse_short_interest_value(value)
            found_metrics.add('short_interest_percent')
        
        # Look for Forward P/E
        if ('FORWARD P/E' in label_upper or 'FORWARD PE' in label_upper) and 'forward_pe' not in found_metrics:
            data['forward_pe'] = parse_pe_value(value)
            found_metrics.add('forward_pe')
        
        # Look for EPS growth next 5 years
        if ('EPS NEXT 5Y' in label_upper or 'EPS NEXT 5 YEARS' in label_upper) and 'eps_growth_next_5y' not in found_metrics:
            data['eps_growth_next_5y'] = parse_growth_percent(value)
            found_metrics.add('eps_growth_next_5y')
        
        # Look for Insider Ownership
        if 'INSIDER OWN' in label_upper and 'insider_ownership' not in found_metrics:
            data['insider_ownership'] = parse_growth_percent(value)
            found_metrics.add('insider_ownership')
        
        # Look for ROA
        if label_upper == 'ROA' and 'roa' not in found_metrics:
            data['roa'] = parse_growth_percent(value)
            found_metrics.add('roa')
        
        # Look for ROIC
        if label_upper == 'ROIC' and 'roic' not in found_metrics:
            data['roic'] = parse_growth_percent(value)
            found_metrics.add('roic')
        
        # Look for Gross Margin
        if 'GROSS M' in label_upper and 'gross_margin' not in found_metrics:
            data['gross_margin'] = parse_growth_percent(value)
            found_metrics.add('gross_margin')
        
        # Look for Operating Margin
        if 'operating_margin' not in found_metrics:
            if (('OPERAT' in label_upper and 'MARGIN' in label_upper) or
                (label_upper.startswith('OPER') and ' M' in label_upper) or
                label_upper == 'OPER M' or label_upper == 'OPER. M' or
                'OP MARGIN' in label_upper or label_upper == 'OP M'):
                data['operating_margin'] = parse_growth_percent(value)
                found_metrics.add('operating_margin')
        
        # Look for Performance 10Y
        if ('PERF 10Y' in label_upper or 'PERFORMANCE 10Y' in label_upper or 
            ('10Y' in label_upper and 'PERF' in label_upper)) and 'perf_10y' not in found_metrics:
            data['perf_10y'] = parse_growth_percent(value)
            found_metrics.add('perf_10y')
        
        # Look for Analyst Recommendation
        if 'recommendation' not in found_metrics:
            if ('RECOMMENDATION' in label_upper or 
                ('ANALYST' in label_upper and 'REC' in label_upper) or
                label_upper == 'REC' or 'RECOMMEND' in label_upper or
                'RECOM' in label_upper):
                data['recommendation'] = parse_recommendation(value)
                found_metrics.add('recommendation')
        
        # Look for Target Price
        if ('TARGET' in label_upper and 'PRICE' in label_upper) and 'target_price' not in found_metrics:
            data['target_price'] = parse_price(value)
            found_metrics.add('target_price')
        
        # Look for Current Price (needed to calculate move, but won't be stored)
        if label_upper == 'PRICE' and 'current_price' not in found_metrics:
            data['current_price'] = parse_price(value)
            found_metrics.add('current_price')
    
    # Calculate price move percent if both prices are available
    if data.get('current_price') is not None and data.get('target_price') is not None:
        price_move = data['target_price'] - data['current_price']
        data['price_move_percent'] = (price_move / data['current_price']) * 100
        # Remove individual prices since we only need the percentage
        del data['current_price']
        del data['target_price']
    elif 'current_price' in data:
        del data['current_price']
    elif 'target_price' in data:
        del data['target_price']
    
    return data

# Thread-safe database connection lock
db_lock = threading.Lock()

def save_short_interest_data(data):
    """Save all finviz metrics data to the database (thread-safe)."""
    with db_lock:
        conn = sqlite3.connect(FINVIZ_DB)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT OR REPLACE INTO short_interest (
                ticker, short_interest_percent, forward_pe, eps_growth_next_5y,
                insider_ownership, roa, roic, gross_margin, operating_margin,
                perf_10y, recommendation, price_move_percent,
                scraped_at, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data.get('ticker'),
        data.get('short_interest_percent'),
            data.get('forward_pe'),
            data.get('eps_growth_next_5y'),
            data.get('insider_ownership'),
            data.get('roa'),
            data.get('roic'),
            data.get('gross_margin'),
            data.get('operating_margin'),
            data.get('perf_10y'),
            data.get('recommendation'),
            data.get('price_move_percent'),
        data.get('scraped_at'),
        data.get('error')
    ))
    
    conn.commit()
    conn.close()

def get_tickers_from_db():
    """Get all unique tickers from top_tickers.db, ordered by rank."""
    if not os.path.exists(TOP_TICKERS_DB):
        raise FileNotFoundError(f"top_tickers.db not found at {TOP_TICKERS_DB}")
    
    conn = sqlite3.connect(TOP_TICKERS_DB)
    cursor = conn.cursor()
    
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
    """Get tickers that are already in the finviz database."""
    if not os.path.exists(FINVIZ_DB):
        return set()
    
    conn = sqlite3.connect(FINVIZ_DB)
    cursor = conn.cursor()
    
    # Get tickers that don't have errors
    cursor.execute('''
        SELECT ticker FROM short_interest 
        WHERE error IS NULL
    ''')
    
    existing = set(row[0] for row in cursor.fetchall())
    conn.close()
    
    return existing

def scrape_ticker_with_delay(ticker, delay):
    """Scrape a single ticker with a delay before the request (for rate limiting)."""
    time.sleep(delay)
    return scrape_finviz_short_interest(ticker)

def main():
    """Main function with multithreading and rate limit handling."""
    print("=" * 80)
    print("SCRAPING FINVIZ METRICS FOR TOP TICKERS (MULTITHREADED)")
    print("=" * 80)
    print()
    
    # Initialize database
    init_short_interest_db()
    
    # Get tickers from top_tickers.db
    print("Loading tickers from top_tickers.db...")
    try:
        all_tickers = get_tickers_from_db()
        print(f"✓ Found {len(all_tickers)} tickers")
    except FileNotFoundError as e:
        print(f"✗ Error: {e}")
        return
    
    if not all_tickers:
        print("✗ No tickers found in database")
        return
    
    # Get existing tickers
    existing_tickers = get_existing_tickers()
    print(f"Found {len(existing_tickers)} tickers already in finviz database")
    
    # Filter out existing tickers
    tickers_to_scrape = [t for t in all_tickers if t not in existing_tickers]
    
    if not tickers_to_scrape:
        print("✓ All tickers already scraped!")
        return
    
    print(f"Scraping {len(tickers_to_scrape)} new tickers...")
    print(f"Using multithreading with rate limit protection...")
    print()
    
    # Configuration
    MAX_WORKERS = 5  # Number of concurrent threads
    INITIAL_DELAY = 0.5  # Initial delay between requests per thread (seconds)
    RATE_LIMIT_BACKOFF = 5  # Extra delay after rate limit (seconds)
    
    successful = 0
    errors = 0
    rate_limited_tickers = []
    completed = 0
    
    # Track delays per thread to stagger requests
    delays = [i * INITIAL_DELAY for i in range(MAX_WORKERS)]
    
    # First pass: scrape all tickers with rate limiting
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks with staggered delays
        futures = {}
        for idx, ticker in enumerate(tickers_to_scrape):
            thread_delay = delays[idx % MAX_WORKERS]
            future = executor.submit(scrape_ticker_with_delay, ticker, thread_delay)
            futures[future] = ticker
        
        # Process results as they complete
        for future in as_completed(futures):
            ticker = futures[future]
            completed += 1
            
            try:
                data = future.result()
        save_short_interest_data(data)
        
        if data.get('error'):
            if '429' in str(data['error']) or 'Rate limit' in str(data['error']):
                        rate_limited_tickers.append(ticker)
                        print(f"[{completed}/{len(tickers_to_scrape)}] {ticker}: ✗ Rate limited")
                        errors += 1
            else:
                        print(f"[{completed}/{len(tickers_to_scrape)}] {ticker}: ✗ {data['error']}")
            errors += 1
        else:
                    metrics_found = sum([
                        1 for key in ['short_interest_percent', 'forward_pe', 'eps_growth_next_5y',
                                     'insider_ownership', 'roa', 'roic', 'gross_margin',
                                     'operating_margin', 'perf_10y', 'recommendation',
                                     'price_move_percent']
                        if data.get(key) is not None
                    ])
                    print(f"[{completed}/{len(tickers_to_scrape)}] {ticker}: ✓ {metrics_found} metrics")
                    successful += 1
            except Exception as e:
                print(f"[{completed}/{len(tickers_to_scrape)}] {ticker}: ✗ Exception: {str(e)}")
                errors += 1
    
    # Second pass: retry rate-limited tickers with slower rate
    if rate_limited_tickers:
        print()
        print("=" * 80)
        print(f"RETRYING {len(rate_limited_tickers)} RATE-LIMITED TICKERS (SLOWER RATE)")
        print("=" * 80)
        print()
        
        # Use fewer workers and longer delays for retries
        RETRY_DELAY = 2.0  # Longer delay for retries
        RETRY_WORKERS = 2  # Fewer concurrent requests
        
        retry_delays = [i * RETRY_DELAY for i in range(RETRY_WORKERS)]
        retry_successful = 0
        retry_errors = 0
        
        with ThreadPoolExecutor(max_workers=RETRY_WORKERS) as executor:
            retry_futures = {}
            for idx, ticker in enumerate(rate_limited_tickers):
                thread_delay = retry_delays[idx % RETRY_WORKERS]
                future = executor.submit(scrape_ticker_with_delay, ticker, thread_delay)
                retry_futures[future] = ticker
            
            for future in as_completed(retry_futures):
                ticker = retry_futures[future]
                
                try:
                    data = future.result()
                    save_short_interest_data(data)
                    
                    if data.get('error'):
                        if '429' in str(data['error']) or 'Rate limit' in str(data['error']):
                            print(f"  {ticker}: ✗ Still rate limited (will skip)")
                            retry_errors += 1
                        else:
                            print(f"  {ticker}: ✗ {data['error']}")
                            retry_errors += 1
            else:
                        metrics_found = sum([
                            1 for key in ['short_interest_percent', 'forward_pe', 'eps_growth_next_5y',
                                         'insider_ownership', 'roa', 'roic', 'gross_margin',
                                         'operating_margin', 'perf_10y', 'recommendation',
                                         'price_move_percent']
                            if data.get(key) is not None
                        ])
                        print(f"  {ticker}: ✓ {metrics_found} metrics")
                        retry_successful += 1
            successful += 1
                        errors -= 1  # Correct the count since we're retrying
                except Exception as e:
                    print(f"  {ticker}: ✗ Exception: {str(e)}")
                    retry_errors += 1
        
        print()
        print(f"Retry results: {retry_successful} successful, {retry_errors} failed")
    
    print()
    print("=" * 80)
    print("SCRAPING COMPLETE")
    print("=" * 80)
    print(f"Total tickers: {len(tickers_to_scrape)}")
    print(f"Successful: {successful}")
    print(f"Errors: {errors}")
    if rate_limited_tickers:
        print(f"Rate limited (retried): {len(rate_limited_tickers)}")
    print()
    print(f"Database: {os.path.basename(FINVIZ_DB)}")

if __name__ == "__main__":
    main()

