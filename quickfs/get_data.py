#!/usr/bin/env python3
"""
Fetch all financial data from QuickFS API for stocks in top_tickers database.
"""

import sqlite3
import os
import time
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

try:
    from quickfs import QuickFS
    HAS_QUICKFS_SDK = True
except ImportError:
    HAS_QUICKFS_SDK = False
    print("Warning: QuickFS SDK not installed. Installing it...")
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "quickfs"])
    from quickfs import QuickFS
    HAS_QUICKFS_SDK = True

# Database paths
TOP_TICKERS_DB = os.path.join(os.path.dirname(__file__), "..", "finviz", "top_tickers.db")
QUICKFS_DB = os.path.join(os.path.dirname(__file__), "data.db")
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "config.json")

# Load configuration from config file
def load_config():
    """Load configuration from config.json file."""
    default_config = {
        'api_key': '',
        'api_base': 'https://public-api.quickfs.net/v1',
        'request_delay': 0.5,
        'retry_delay': 60,
        'max_workers': 5
    }
    
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
                # Merge with defaults
                default_config.update(config)
        except Exception as e:
            print(f"Warning: Could not load config file: {e}")
            print("Using default configuration")
    
    # Allow environment variable to override API key
    api_key = os.environ.get('QUICKFS_API_KEY', default_config.get('api_key', ''))
    if api_key:
        default_config['api_key'] = api_key
    
    return default_config

# Load config
config = load_config()
QUICKFS_API_KEY = config['api_key']
QUICKFS_API_BASE = config['api_base']
REQUEST_DELAY = config.get('request_delay', 0.5)
RETRY_DELAY = config.get('retry_delay', 60)
MAX_WORKERS = config.get('max_workers', 5)  # Number of concurrent threads

def init_quickfs_db():
    """Initialize the QuickFS database with a table to store all financial data."""
    conn = sqlite3.connect(QUICKFS_DB)
    cursor = conn.cursor()
    
    # Create table to store QuickFS data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS quickfs_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            data_type TEXT NOT NULL,
            data_json TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            UNIQUE(ticker, data_type, fetched_at)
        )
    ''')
    
    # Create index for faster lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker ON quickfs_data(ticker)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_data_type ON quickfs_data(data_type)')
    
    conn.commit()
    conn.close()
    print(f"Initialized QuickFS database: {QUICKFS_DB}")

def get_all_tickers():
    """Get all unique tickers from top_tickers database."""
    if not os.path.exists(TOP_TICKERS_DB):
        print(f"Error: Top tickers database not found at {TOP_TICKERS_DB}")
        return []
    
    conn = sqlite3.connect(TOP_TICKERS_DB)
    cursor = conn.cursor()
    
    # Get distinct tickers
    cursor.execute("SELECT DISTINCT ticker FROM top_tickers ORDER BY ticker")
    tickers = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return tickers

def format_ticker(ticker):
    """Format ticker for QuickFS API (add :US suffix for US stocks)."""
    # Assume all tickers are US stocks for now
    # TODO: Add logic to detect other exchanges if needed
    if ':' not in ticker:
        return f"{ticker}:US"
    return ticker

def fetch_all_data_for_ticker_sdk(ticker, client):
    """
    Fetch all data for a ticker using QuickFS SDK's get_data_full method.
    
    Args:
        ticker: Stock ticker symbol
        client: QuickFS client instance
    
    Returns:
        Dictionary with all financial data, or None if error
    """
    formatted_ticker = format_ticker(ticker)
    
    try:
        full_data = client.get_data_full(symbol=formatted_ticker)
        return full_data
    except Exception as e:
        error_msg = str(e).lower()
        if 'not found' in error_msg or '404' in error_msg:
            # Ticker not found - this is okay
            return None
        elif 'rate limit' in error_msg or '429' in error_msg:
            print(f"  Rate limit hit for {ticker}. Waiting {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
            # Retry once
            try:
                full_data = client.get_data_full(symbol=formatted_ticker)
                return full_data
            except Exception as retry_e:
                print(f"  Retry failed for {ticker}: {str(retry_e)}")
                return None
        elif '401' in error_msg or 'unauthorized' in error_msg:
            print(f"  Authentication error - check API key")
            return None
        else:
            print(f"  Error fetching data for {ticker}: {str(e)}")
            return None

def save_quickfs_data(ticker, data):
    """Save QuickFS full data to the database (thread-safe)."""
    conn = sqlite3.connect(QUICKFS_DB, timeout=30.0)  # Increase timeout for concurrent access
    cursor = conn.cursor()
    
    try:
        data_json = json.dumps(data)
        fetched_at = datetime.now().isoformat()
        
        # Store all data under 'full' data_type
        cursor.execute('''
            INSERT OR REPLACE INTO quickfs_data (ticker, data_type, data_json, fetched_at)
            VALUES (?, ?, ?, ?)
        ''', (ticker, 'full', data_json, fetched_at))
        
        conn.commit()
        
    except Exception as e:
        print(f"  Error saving data for {ticker}: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

def process_ticker(ticker, client, thread_id, delay):
    """
    Process a single ticker (worker function for threading).
    
    Args:
        ticker: Stock ticker symbol
        client: QuickFS client instance (should be thread-safe)
        thread_id: Thread identifier for staggering delays
        delay: Delay before making the request (for rate limiting)
    
    Returns:
        Tuple of (ticker, success, error_message)
    """
    # Stagger requests across threads
    if delay > 0:
        time.sleep(delay)
    
    try:
        full_data = fetch_all_data_for_ticker_sdk(ticker, client)
        
        if full_data:
            save_quickfs_data(ticker, full_data)
            return (ticker, True, None)
        else:
            return (ticker, False, "No data found")
    except Exception as e:
        return (ticker, False, str(e))

def main():
    """Main function to fetch QuickFS data for all tickers."""
    print("=" * 80)
    print("QuickFS Data Fetcher")
    print("=" * 80)
    print()
    
    # Check API key
    if not QUICKFS_API_KEY:
        print("ERROR: QUICKFS_API_KEY not set in config.json!")
        print("Please add your API key to config.json in the root directory")
        return
    
    # Initialize database
    init_quickfs_db()
    
    # Get all tickers
    tickers = get_all_tickers()
    
    if not tickers:
        print("No tickers found in top_tickers database")
        return
    
    print(f"Found {len(tickers)} unique tickers")
    print(f"Tickers: {', '.join(tickers[:10])}{'...' if len(tickers) > 10 else ''}")
    print()
    
    # Initialize QuickFS client
    print("Initializing QuickFS client...")
    client = QuickFS(QUICKFS_API_KEY)
    
    # Ask for confirmation
    estimated_time = (len(tickers) * REQUEST_DELAY) / (MAX_WORKERS * 60)  # minutes with multithreading
    print(f"\nConfiguration:")
    print(f"  - Total tickers: {len(tickers)}")
    print(f"  - Max workers (threads): {MAX_WORKERS}")
    print(f"  - Delay per request: {REQUEST_DELAY}s")
    print(f"  - Estimated time: ~{estimated_time:.1f} minutes (with {MAX_WORKERS} threads)")
    response = input(f"\nFetch all data for {len(tickers)} tickers? (y/n): ")
    if response.lower() != 'y':
        print("Cancelled.")
        return
    
    print()
    print("Starting data fetch with multithreading...")
    print("-" * 80)
    
    # Thread-safe counters
    success_count = 0
    error_count = 0
    completed_count = 0
    lock = threading.Lock()
    
    # Stagger delays per thread to avoid simultaneous requests
    thread_delays = [i * REQUEST_DELAY / MAX_WORKERS for i in range(MAX_WORKERS)]
    
    # Process tickers using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks with staggered delays
        futures = {}
        for idx, ticker in enumerate(tickers):
            thread_id = idx % MAX_WORKERS
            thread_delay = thread_delays[thread_id]
            future = executor.submit(process_ticker, ticker, client, thread_id, thread_delay)
            futures[future] = ticker
        
        # Process results as they complete
        for future in as_completed(futures):
            ticker = futures[future]
            
            try:
                result_ticker, success, error_msg = future.result()
                
                with lock:
                    completed_count += 1
                    if success:
                        success_count += 1
                        print(f"[{completed_count}/{len(tickers)}] {ticker}: ✓ Success")
                    else:
                        error_count += 1
                        print(f"[{completed_count}/{len(tickers)}] {ticker}: ✗ {error_msg}")
                
                # Progress update every 10 tickers
                if completed_count % 10 == 0:
                    print(f"  Progress: {completed_count}/{len(tickers)} ({success_count} successful, {error_count} errors)")
                    
            except Exception as e:
                with lock:
                    completed_count += 1
                    error_count += 1
                    print(f"[{completed_count}/{len(tickers)}] {ticker}: ✗ Exception: {str(e)}")
    
    print()
    print("=" * 80)
    print("Data fetch complete!")
    print(f"Successfully fetched: {success_count} tickers")
    print(f"Errors: {error_count} tickers")
    print(f"Database: {QUICKFS_DB}")
    print("=" * 80)

if __name__ == '__main__':
    main()

