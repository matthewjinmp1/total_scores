#!/usr/bin/env python3
"""
Fetch the top 100 tickers from Finviz screener (sorted by market cap).
"""

import requests
from bs4 import BeautifulSoup
import time
import sqlite3
import os
from datetime import datetime

# Database path
TOP_TICKERS_DB = os.path.join(os.path.dirname(__file__), "top_tickers.db")

# Finviz screener URL
FINVIZ_SCREENER_URL = "https://finviz.com/screener.ashx?v=111&o=-marketcap&r="

# Headers to mimic a browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

def scrape_screener_page(page=1, max_retries=3):
    """
    Scrape a single page of the Finviz screener.
    
    Args:
        page: Page number (1-indexed)
        max_retries: Maximum number of retry attempts
    
    Returns:
        List of ticker symbols, or None on error
    """
    url = f"{FINVIZ_SCREENER_URL}{(page-1)*20+1}"
    
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table', class_='screener_table')
            if not table:
                table = soup.find('table', {'id': 'screener_table'})
            
            if not table:
                if attempt < max_retries:
                    time.sleep(2)
                    continue
                return None
            
            rows = table.find_all('tr')
            if len(rows) < 2:
                return []
            
            # Find ticker column in header
            header_row = rows[0]
            header_cells = header_row.find_all(['td', 'th'])
            ticker_col = None
            
            for idx, cell in enumerate(header_cells):
                text = cell.get_text(strip=True).upper()
                if 'TICKER' in text:
                    ticker_col = idx
                    break
            
            # Fallback: assume ticker is in column 1
            if ticker_col is None:
                ticker_col = 1
            
            # Extract tickers from data rows
            tickers = []
            for row in rows[1:]:  # Skip header
                cells = row.find_all('td')
                if len(cells) <= ticker_col:
                    continue
                
                ticker_cell = cells[ticker_col]
                ticker_link = ticker_cell.find('a')
                if ticker_link:
                    ticker = ticker_link.get_text(strip=True).upper()
                else:
                    ticker = ticker_cell.get_text(strip=True).upper()
                
                if ticker and len(ticker) <= 10:  # Valid ticker length
                    tickers.append(ticker)
            
            return tickers
            
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                wait_time = 2 * (attempt + 1)
                print(f"  Error on page {page}, attempt {attempt + 1}/{max_retries + 1}: {e}")
                print(f"  Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"  Failed to fetch page {page} after {max_retries + 1} attempts: {e}")
                return None
        except Exception as e:
            print(f"  Unexpected error on page {page}: {e}")
            return None
    
    return None

def fetch_top_tickers(count=100):
    """
    Fetch the top N tickers from Finviz screener (sorted by market cap).
    
    Args:
        count: Number of tickers to fetch (default: 100)
    
    Returns:
        List of ticker symbols
    """
    print("=" * 80)
    print(f"FETCHING TOP {count} TICKERS FROM FINVIZ")
    print("=" * 80)
    print()
    
    all_tickers = []
    pages_needed = (count + 19) // 20  # 20 tickers per page
    
    for page in range(1, pages_needed + 1):
        print(f"Scraping page {page}...", end=' ', flush=True)
        
        tickers = scrape_screener_page(page)
        
        if tickers is None:
            print("✗ Failed")
            break
        
        if not tickers:
            print("No more tickers found")
            break
        
        all_tickers.extend(tickers)
        print(f"✓ Found {len(tickers)} tickers (Total: {len(all_tickers)})")
        
        if len(all_tickers) >= count:
            break
        
        # Rate limiting between pages
        if page < pages_needed:
            time.sleep(1)
    
    # Trim to requested count
    all_tickers = all_tickers[:count]
    
    print()
    print("=" * 80)
    print(f"SUCCESSFULLY FETCHED {len(all_tickers)} TICKERS")
    print("=" * 80)
    print()
    
    return all_tickers

def init_top_tickers_db():
    """Initialize the top tickers database."""
    conn = sqlite3.connect(TOP_TICKERS_DB)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS top_tickers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rank INTEGER NOT NULL,
            ticker TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            UNIQUE(rank, ticker, fetched_at)
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker ON top_tickers(ticker)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_rank ON top_tickers(rank)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fetched_at ON top_tickers(fetched_at)')
    
    conn.commit()
    conn.close()

def save_tickers_to_db(tickers):
    """Save tickers to the database with their rank."""
    init_top_tickers_db()
    
    conn = sqlite3.connect(TOP_TICKERS_DB)
    cursor = conn.cursor()
    
    fetched_at = datetime.now().isoformat()
    
    saved_count = 0
    for rank, ticker in enumerate(tickers, start=1):
        try:
            cursor.execute('''
                INSERT INTO top_tickers (rank, ticker, fetched_at)
                VALUES (?, ?, ?)
            ''', (rank, ticker, fetched_at))
            saved_count += 1
        except sqlite3.IntegrityError:
            # Skip duplicates
            pass
    
    conn.commit()
    conn.close()
    
    print(f"✓ Saved {saved_count} tickers to {TOP_TICKERS_DB}")
    print(f"  Database: {os.path.basename(TOP_TICKERS_DB)}")
    return saved_count

def display_tickers(tickers):
    """Display tickers in a formatted way."""
    print("Top Tickers:")
    print("-" * 80)
    
    # Display in columns
    cols = 5
    for i in range(0, len(tickers), cols):
        row_tickers = tickers[i:i+cols]
        print("  ".join(f"{t:10s}" for t in row_tickers))
    
    print()
    print(f"Total: {len(tickers)} tickers")

def main():
    """Main function."""
    import sys
    
    count = 100
    if len(sys.argv) > 1:
        try:
            count = int(sys.argv[1])
        except ValueError:
            print(f"Invalid count: {sys.argv[1]}. Using default: 100")
    
    tickers = fetch_top_tickers(count)
    
    if not tickers:
        print("No tickers were fetched.")
        return
    
    display_tickers(tickers)
    
    # Save to database
    print()
    save_tickers_to_db(tickers)

if __name__ == "__main__":
    main()

