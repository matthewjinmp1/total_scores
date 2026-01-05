#!/usr/bin/env python3
"""
Scrape short interest data from Finviz for all tickers available on Finviz.
This scraper gets tickers directly from Finviz (via screener) and scrapes them
in batches, which may be faster than scraping one-by-one.
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import os
import time
import re
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Database paths
SHORT_INTEREST_DB = os.path.join(os.path.dirname(__file__), "short_interest.db")

# Finviz URLs
FINVIZ_SCREENER_URL = "https://finviz.com/screener.ashx"
FINVIZ_QUOTE_URL = "https://finviz.com/quote.ashx?t="

# Headers to mimic a browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

def parse_short_interest_value(value_str):
    """
    Parse a string value that might be a number, percentage, or 'N/A'.
    Handles formats like "10.5M", "1.2B", "50.5%", etc.
    """
    if not value_str or value_str.strip().upper() in ['N/A', 'NAN', '']:
        return None
    
    # Remove commas and percentage signs
    cleaned = value_str.replace(',', '').replace('%', '').strip().upper()
    
    # Handle multipliers (M = million, B = billion, K = thousand)
    multiplier = 1
    if cleaned.endswith('M'):
        multiplier = 1e6
        cleaned = cleaned[:-1]
    elif cleaned.endswith('B'):
        multiplier = 1e9
        cleaned = cleaned[:-1]
    elif cleaned.endswith('K'):
        multiplier = 1e3
        cleaned = cleaned[:-1]
    
    try:
        return float(cleaned) * multiplier
    except ValueError:
        return None

def scrape_screener_page_for_short_interest(page=1, max_retries=3):
    """
    Scrape a screener page to get tickers and their short interest data.
    This is much faster as we get multiple tickers per request.
    
    Args:
        page: Page number to scrape
        max_retries: Maximum retries for rate limit errors
    
    Returns:
        List of dictionaries with ticker and short_interest_percent
    """
    url = f"{FINVIZ_SCREENER_URL}?v=151&o=-marketcap&r={(page-1)*20+1}"
    
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            
            if response.status_code == 429:
                if attempt < max_retries:
                    wait_time = 5 * (2 ** attempt)
                    time.sleep(wait_time)
                    continue
                else:
                    return {'error': f"Rate limit error (429) after {max_retries} retries"}
            
            if response.status_code == 404:
                return {'error': "Page not found (404)"}
            
            response.raise_for_status()
            break
            
        except requests.exceptions.RequestException as e:
            if attempt >= max_retries:
                return {'error': f"Request error: {str(e)}"}
            time.sleep(5 * (2 ** attempt))
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the screener table
    table = soup.find('table', class_='screener_table')
    if not table:
        table = soup.find('table', {'id': 'screener_table'})
    
    if not table:
        return {'error': "Could not find screener table"}
    
    results = []
    rows = table.find_all('tr')
    
    if not rows or len(rows) < 2:
        return {'error': "No data rows found in table"}
    
    # Find header row - it's usually the first row or a row with class 'table-header'
    header_row = None
    header_row_idx = 0
    
    # Look for header row
    for idx, row in enumerate(rows):
        # Check if this looks like a header row
        cells = row.find_all(['td', 'th'])
        if cells:
            first_cell_text = cells[0].get_text(strip=True).upper()
            # Header rows often have "NO", "TICKER", or column names
            if 'TICKER' in first_cell_text or 'NO' in first_cell_text or first_cell_text == '':
                header_row = row
                header_row_idx = idx
                break
    
    if not header_row:
        # Try first row as header
        header_row = rows[0]
        header_row_idx = 0
    
    header_cells = header_row.find_all(['td', 'th'])
    ticker_col = None
    short_float_col = None
    
    # Find column indices
    for idx, cell in enumerate(header_cells):
        text = cell.get_text(strip=True).upper()
        # Ticker column might be labeled "Ticker" or be the second column (after "No.")
        if 'TICKER' in text:
            ticker_col = idx
        elif 'SHORT' in text and 'FLOAT' in text:
            short_float_col = idx
    
    # If ticker column not found by name, try common positions
    if ticker_col is None:
        # Usually ticker is in column 1 (after "No." in column 0)
        if len(header_cells) > 1:
            # Check if column 1 has links (tickers usually have links)
            test_row = rows[header_row_idx + 1] if len(rows) > header_row_idx + 1 else None
            if test_row:
                test_cells = test_row.find_all('td')
                if len(test_cells) > 1 and test_cells[1].find('a'):
                    ticker_col = 1
    
    if ticker_col is None:
        return {'error': f"Could not find ticker column. Found {len(header_cells)} columns. Header: {[c.get_text(strip=True) for c in header_cells[:5]]}"}
    
    # Extract data from each row (skip header row)
    for row_idx, row in enumerate(rows):
        if row_idx <= header_row_idx:
            continue  # Skip header row(s)
        
        cells = row.find_all('td')
        if len(cells) <= ticker_col:
            continue
        
        # Get ticker
        ticker_cell = cells[ticker_col]
        ticker_link = ticker_cell.find('a')
        if not ticker_link:
            # Sometimes ticker is just text, not a link
            ticker = ticker_cell.get_text(strip=True).upper()
        else:
            ticker = ticker_link.get_text(strip=True).upper()
        
        if not ticker or len(ticker) > 10:  # Sanity check
            continue
        
        # Get short float if column found
        short_interest_percent = None
        if short_float_col is not None and short_float_col < len(cells):
            short_float_text = cells[short_float_col].get_text(strip=True)
            short_interest_percent = parse_short_interest_value(short_float_text)
        else:
            # Try to find short float by searching all cells
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                # Short float is usually a percentage like "1.13%"
                if '%' in cell_text and cell_text.replace('%', '').replace('.', '').isdigit():
                    short_interest_percent = parse_short_interest_value(cell_text)
                    break
        
        results.append({
            'ticker': ticker,
            'short_interest_percent': short_interest_percent,
            'scraped_at': datetime.now().isoformat()
        })
    
    return {'data': results}

def get_tickers_and_short_interest_from_screener(max_tickers=None, max_pages=None, save_to_db=True):
    """
    Scrape Finviz screener to get both tickers and short interest data.
    This is much faster as we get multiple tickers per page request.
    Saves to database as it runs.
    
    Args:
        max_tickers: Maximum number of tickers to get (None for all)
        max_pages: Maximum number of pages to scrape (None for all)
        save_to_db: Whether to save to database as we go (default: True)
    
    Returns:
        Dictionary with stats: {'total': count, 'saved': count, 'skipped': count, 'no_data': count}
    """
    print("Scraping tickers and short interest from Finviz screener...")
    print("This is faster as we get multiple tickers per page request")
    print("Saving to database as we go...")
    print()
    
    # Check which are already in database
    conn = sqlite3.connect(SHORT_INTEREST_DB)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT ticker FROM short_interest 
        WHERE error IS NULL OR error NOT LIKE '%429%' AND error NOT LIKE '%Rate limit%'
    ''')
    existing_tickers = set(row[0] for row in cursor.fetchall())
    conn.close()
    
    total_scraped = 0
    total_saved = 0
    total_skipped = 0
    total_no_data = 0
    page = 1
    max_pages = max_pages or 1000  # Safety limit
    
    while page <= max_pages:
        print(f"  Scraping page {page}...", end=' ', flush=True)
        
        result = scrape_screener_page_for_short_interest(page)
        
        if 'error' in result:
            print(f"Error: {result['error']}")
            if '429' in result['error']:
                print("  Rate limited, waiting 30 seconds...")
                time.sleep(30)
                continue  # Retry same page
            break  # Other errors, stop
        
        if 'data' not in result or not result['data']:
            print("No more data found")
            break
        
        page_results = result['data']
        total_scraped += len(page_results)
        
        # Save to database as we go
        if save_to_db:
            page_saved = 0
            page_skipped = 0
            page_no_data = 0
            
            for item in page_results:
                ticker = item['ticker']
                
                # Skip if already in database
                if ticker in existing_tickers:
                    page_skipped += 1
                    continue
                
                short_interest = item.get('short_interest_percent')
                
                data = {
                    'ticker': ticker,
                    'short_interest_percent': short_interest,
                    'scraped_at': item.get('scraped_at', datetime.now().isoformat()),
                    'error': None
                }
                
                save_short_interest_data(data)
                existing_tickers.add(ticker)  # Mark as saved
                
                if short_interest is not None:
                    page_saved += 1
                else:
                    page_no_data += 1
            
            total_saved += page_saved
            total_skipped += page_skipped
            total_no_data += page_no_data
            
            print(f"Found {len(page_results)} tickers | Saved: {page_saved}, Skipped: {page_skipped}, No data: {page_no_data} | Total: {total_saved}")
        else:
            print(f"Found {len(page_results)} tickers (total: {total_scraped})")
        
        if max_tickers and total_scraped >= max_tickers:
            break
        
        page += 1
        time.sleep(1)  # Rate limiting between pages
    
    print(f"\nScraping complete!")
    print(f"  Total scraped: {total_scraped:,}")
    if save_to_db:
        print(f"  Saved to database: {total_saved:,}")
        print(f"  Skipped (already in DB): {total_skipped:,}")
        print(f"  No short float data: {total_no_data:,}")
    
    return {
        'total': total_scraped,
        'saved': total_saved,
        'skipped': total_skipped,
        'no_data': total_no_data
    }

def scrape_finviz_short_interest(ticker, max_retries=3, retry_delay=5):
    """
    Scrape short interest data from Finviz for a given ticker.
    Same function as in scrape_short_interest.py
    """
    url = f"{FINVIZ_QUOTE_URL}{ticker.upper()}"
    response = None
    
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            
            # Handle rate limit errors (429) with retry
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
            break
            
        except requests.exceptions.HTTPError as e:
            if hasattr(e, 'response') and e.response is not None:
                if e.response.status_code == 429:
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
            
            if attempt >= max_retries:
                return {
                    'ticker': ticker.upper(),
                    'scraped_at': datetime.now().isoformat(),
                    'error': f"HTTP error: {str(e)}"
                }
            time.sleep(retry_delay * (2 ** attempt))
            
        except requests.exceptions.RequestException as e:
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
    
    for i in range(0, len(all_cells) - 1, 2):
        if i + 1 >= len(all_cells):
            break
        
        label = all_cells[i].get_text(strip=True)
        value = all_cells[i + 1].get_text(strip=True)
        
        if 'Short Float' in label:
            data['short_interest_percent'] = parse_short_interest_value(value)
            break
    
    return data

def save_short_interest_data(data, skip_rate_limit_errors=False):
    """Save short interest data to the database."""
    if skip_rate_limit_errors:
        error = data.get('error', '')
        if '429' in str(error) or 'Rate limit' in str(error):
            return
    
    conn = sqlite3.connect(SHORT_INTEREST_DB)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT OR REPLACE INTO short_interest (
            ticker, short_interest_percent, scraped_at, error
        ) VALUES (?, ?, ?, ?)
    ''', (
        data.get('ticker'),
        data.get('short_interest_percent'),
        data.get('scraped_at'),
        data.get('error')
    ))
    
    conn.commit()
    conn.close()

def scrape_single_ticker(ticker, delay=1.0, rate_limit_errors=None):
    """Scrape short interest for a single ticker."""
    if delay > 0:
        time.sleep(delay)
    
    data = scrape_finviz_short_interest(ticker)
    
    if rate_limit_errors is not None and '429' in str(data.get('error', '')):
        rate_limit_errors.append(ticker)
    
    skip_save = '429' in str(data.get('error', '')) or 'Rate limit' in str(data.get('error', ''))
    save_short_interest_data(data, skip_rate_limit_errors=skip_save)
    
    return data

def calculate_optimal_threads(delay, max_requests_per_second=10):
    """Calculate optimal number of threads."""
    if delay <= 0:
        return 1
    optimal = int(max_requests_per_second * delay)
    return max(1, min(optimal, 30))

def init_short_interest_db():
    """Initialize the short interest database."""
    conn = sqlite3.connect(SHORT_INTEREST_DB)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS short_interest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT UNIQUE NOT NULL,
            short_interest_percent REAL,
            scraped_at TEXT,
            error TEXT
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker ON short_interest(ticker)')
    conn.commit()
    conn.close()

def main():
    """Main function to scrape all Finviz tickers."""
    import sys
    
    print("=" * 80)
    print("FINVIZ TICKER SHORT INTEREST SCRAPER")
    print("=" * 80)
    print("This scraper gets tickers directly from Finviz and scrapes them")
    print()
    
    # Initialize database
    init_short_interest_db()
    
    # Parse arguments
    max_tickers = None
    delay = 1.0
    max_workers = None
    
    if len(sys.argv) > 1:
        try:
            max_tickers = int(sys.argv[1])
        except ValueError:
            print(f"Invalid max_tickers: {sys.argv[1]}")
    
    if len(sys.argv) > 2:
        try:
            delay = float(sys.argv[2])
        except ValueError:
            print(f"Invalid delay: {sys.argv[2]}")
    
    if len(sys.argv) > 3:
        if sys.argv[3].lower() != 'auto':
            try:
                max_workers = int(sys.argv[3])
            except ValueError:
                max_workers = None
        else:
            max_workers = None
    
    # Calculate optimal threads
    if max_workers is None:
        max_workers = calculate_optimal_threads(delay)
        print(f"Auto-calculated optimal threads: {max_workers}")
    else:
        print(f"Using specified thread count: {max_workers}")
    
    requests_per_second = max_workers / delay if delay > 0 else max_workers
    print(f"Effective rate: ~{requests_per_second:.1f} requests/second")
    print()
    
    # Get tickers and short interest from Finviz screener (much faster!)
    # This function saves to database as it runs
    stats = get_tickers_and_short_interest_from_screener(max_tickers=max_tickers, save_to_db=True)
    
    if stats['total'] == 0:
        print("No data found!")
        return
    
    print(f"\n{'=' * 80}")
    print("SCRAPING COMPLETE")
    print(f"{'=' * 80}")
    print(f"Total tickers scraped: {stats['total']:,}")
    print(f"Tickers saved with short interest data: {stats['saved']:,}")
    print(f"Tickers skipped (already in DB): {stats['skipped']:,}")
    print(f"Tickers with no short float data: {stats['no_data']:,}")

if __name__ == "__main__":
    main()

