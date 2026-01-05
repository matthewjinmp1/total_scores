#!/usr/bin/env python3
"""
Scrape short interest data from Finviz screener using Selenium to handle JavaScript.
This gets both ticker and short float from the screener table after JavaScript renders it.
"""

import sqlite3
import os
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    print("ERROR: Selenium or webdriver-manager is not installed.")
    print("Please install with: pip install selenium webdriver-manager")
    exit(1)

# Database paths
SHORT_INTEREST_DB = os.path.join(os.path.dirname(__file__), "short_interest.db")

# Finviz URLs
FINVIZ_SCREENER_URL = "https://finviz.com/screener.ashx?v=151&o=-marketcap&r="

def parse_short_interest_value(value_str):
    """Parse a string value that might be a number, percentage, or 'N/A'."""
    if not value_str or value_str.strip().upper() in ['N/A', 'NAN', '']:
        return None
    
    cleaned = value_str.replace(',', '').replace('%', '').strip().upper()
    
    try:
        return float(cleaned)
    except ValueError:
        return None

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

def get_chrome_driver(headless=True):
    """Create and return a Chrome WebDriver instance."""
    chrome_options = Options()
    if headless:
        chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    
    try:
        # Use webdriver-manager to automatically download ChromeDriver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver
    except Exception as e:
        print(f"Error creating Chrome driver: {e}")
        print("Make sure Chrome is installed on your system")
        raise

def scrape_screener_page_selenium(page=1, driver=None, close_driver=False):
    """
    Scrape a screener page using Selenium to get tickers and short float.
    
    Args:
        page: Page number to scrape
        driver: Existing WebDriver instance (or None to create new)
        close_driver: Whether to close the driver when done
    
    Returns:
        Dictionary with 'data' (list of results) or 'error'
    """
    use_existing_driver = driver is not None
    if not use_existing_driver:
        driver = get_chrome_driver(headless=True)
    
    try:
        url = f"{FINVIZ_SCREENER_URL}{(page-1)*20+1}"
        driver.get(url)
        
        # Wait for table to load (JavaScript renders it)
        wait = WebDriverWait(driver, 10)
        try:
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "screener_table")))
        except TimeoutException:
            return {'error': "Timeout waiting for table to load"}
        
        # Wait a bit more for JavaScript to fully render
        time.sleep(2)
        
        # Find the table
        table = driver.find_element(By.CLASS_NAME, "screener_table")
        rows = table.find_elements(By.TAG_NAME, "tr")
        
        if len(rows) < 2:
            return {'error': "No data rows found"}
        
        # Find header row to locate columns
        header_row = rows[0]
        header_cells = header_row.find_elements(By.TAG_NAME, "td")
        
        ticker_col = None
        short_float_col = None
        
        for idx, cell in enumerate(header_cells):
            text = cell.text.strip().upper()
            if 'TICKER' in text:
                ticker_col = idx
            elif 'SHORT' in text and 'FLOAT' in text:
                short_float_col = idx
        
        if ticker_col is None:
            # Try column 1 as fallback
            ticker_col = 1
        
        if short_float_col is None:
            return {'error': "Short Float column not found in header"}
        
        results = []
        
        # Extract data from data rows
        for row in rows[1:]:  # Skip header
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) <= max(ticker_col, short_float_col):
                continue
            
            # Get ticker
            ticker_cell = cells[ticker_col]
            ticker_link = ticker_cell.find_elements(By.TAG_NAME, "a")
            if ticker_link:
                ticker = ticker_link[0].text.strip().upper()
            else:
                ticker = ticker_cell.text.strip().upper()
            
            if not ticker or len(ticker) > 10:
                continue
            
            # Get short float
            short_interest_percent = None
            if short_float_col < len(cells):
                short_float_text = cells[short_float_col].text.strip()
                short_interest_percent = parse_short_interest_value(short_float_text)
            
            results.append({
                'ticker': ticker,
                'short_interest_percent': short_interest_percent,
                'scraped_at': datetime.now().isoformat()
            })
        
        return {'data': results}
        
    except Exception as e:
        return {'error': f"Error scraping page: {str(e)}"}
    finally:
        if close_driver or not use_existing_driver:
            try:
                driver.quit()
            except:
                pass

def scrape_all_pages_selenium(max_pages=None, max_tickers=None):
    """
    Scrape all screener pages using Selenium.
    
    Args:
        max_pages: Maximum number of pages to scrape (None for all)
        max_tickers: Maximum number of tickers to get (None for all)
    
    Returns:
        Dictionary with stats
    """
    print("=" * 80)
    print("FINVIZ SCREENER SCRAPER (Selenium)")
    print("=" * 80)
    print("Using Selenium to execute JavaScript and get Short Float data")
    print()
    
    # Initialize database
    init_short_interest_db()
    
    # Check existing tickers
    conn = sqlite3.connect(SHORT_INTEREST_DB)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT ticker FROM short_interest 
        WHERE error IS NULL OR error NOT LIKE '%429%' AND error NOT LIKE '%Rate limit%'
    ''')
    existing_tickers = set(row[0] for row in cursor.fetchall())
    conn.close()
    
    print(f"Found {len(existing_tickers):,} tickers already in database")
    print()
    
    # Create driver (reuse for multiple pages)
    print("Initializing Chrome driver...")
    driver = get_chrome_driver(headless=True)
    
    total_scraped = 0
    total_saved = 0
    total_skipped = 0
    total_no_data = 0
    page = 1
    max_pages = max_pages or 1000
    
    try:
        while page <= max_pages:
            print(f"Scraping page {page}...", end=' ', flush=True)
            
            result = scrape_screener_page_selenium(page, driver=driver, close_driver=False)
            
            if 'error' in result:
                print(f"Error: {result['error']}")
                break
            
            if 'data' not in result or not result['data']:
                print("No more data found")
                break
            
            page_results = result['data']
            total_scraped += len(page_results)
            
            # Save to database
            page_saved = 0
            page_skipped = 0
            page_no_data = 0
            
            for item in page_results:
                ticker = item['ticker']
                
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
                existing_tickers.add(ticker)
                
                if short_interest is not None:
                    page_saved += 1
                else:
                    page_no_data += 1
            
            total_saved += page_saved
            total_skipped += page_skipped
            total_no_data += page_no_data
            
            print(f"Found {len(page_results)} tickers | Saved: {page_saved}, Skipped: {page_skipped}, No data: {page_no_data} | Total saved: {total_saved}")
            
            if max_tickers and total_scraped >= max_tickers:
                break
            
            page += 1
            time.sleep(1)  # Rate limiting between pages
    
    finally:
        driver.quit()
    
    print()
    print("=" * 80)
    print("SCRAPING COMPLETE")
    print("=" * 80)
    print(f"Total scraped: {total_scraped:,}")
    print(f"Saved to database: {total_saved:,}")
    print(f"Skipped (already in DB): {total_skipped:,}")
    print(f"No short float data: {total_no_data:,}")
    
    return {
        'total': total_scraped,
        'saved': total_saved,
        'skipped': total_skipped,
        'no_data': total_no_data
    }

def main():
    """Main function."""
    import sys
    
    max_pages = None
    max_tickers = None
    
    if len(sys.argv) > 1:
        try:
            max_pages = int(sys.argv[1])
        except ValueError:
            print(f"Invalid max_pages: {sys.argv[1]}")
    
    if len(sys.argv) > 2:
        try:
            max_tickers = int(sys.argv[2])
        except ValueError:
            print(f"Invalid max_tickers: {sys.argv[2]}")
    
    scrape_all_pages_selenium(max_pages=max_pages, max_tickers=max_tickers)

if __name__ == "__main__":
    main()

