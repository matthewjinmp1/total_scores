#!/usr/bin/env python3
"""
Test script to verify Selenium can extract Short Float from Finviz screener.
"""

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.common.exceptions import TimeoutException
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    print("ERROR: Selenium or webdriver-manager is not installed.")
    print("Please install with: pip install selenium webdriver-manager")
    exit(1)

def parse_short_interest_value(value_str):
    """Parse a string value that might be a number, percentage, or 'N/A'."""
    if not value_str or value_str.strip().upper() in ['N/A', 'NAN', '']:
        return None
    
    cleaned = value_str.replace(',', '').replace('%', '').strip().upper()
    
    try:
        return float(cleaned)
    except ValueError:
        return None

def get_chrome_driver(headless=True):
    """Create and return a Chrome WebDriver instance."""
    chrome_options = Options()
    if headless:
        chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    
    try:
        # Use webdriver-manager to automatically download ChromeDriver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver
    except Exception as e:
        print(f"Error creating Chrome driver: {e}")
        print("Make sure Chrome is installed on your system")
        raise

def test_selenium_scraper():
    """Test scraping first page with Selenium."""
    print("=" * 80)
    print("TESTING SELENIUM SCRAPER")
    print("=" * 80)
    print()
    
    url = "https://finviz.com/screener.ashx?v=151&o=-marketcap&r=1"
    print(f"Loading: {url}")
    print("Waiting for JavaScript to render...")
    print()
    
    driver = None
    try:
        driver = get_chrome_driver(headless=True)
        driver.get(url)
        
        # Wait for table to load
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "screener_table")))
        
        # Wait a bit more for JavaScript to render Short Float
        time.sleep(3)
        
        print("✓ Page loaded")
        
        # Find table
        table = driver.find_element(By.CLASS_NAME, "screener_table")
        rows = table.find_elements(By.TAG_NAME, "tr")
        
        print(f"Found {len(rows)} rows")
        print()
        
        if len(rows) < 2:
            print("✗ Not enough rows")
            return
        
        # Find header
        header_row = rows[0]
        header_cells = header_row.find_elements(By.TAG_NAME, "td")
        
        print("Header columns:")
        ticker_col = None
        short_float_col = None
        
        for idx, cell in enumerate(header_cells):
            text = cell.text.strip()
            print(f"  Column {idx}: '{text}'")
            
            text_upper = text.upper()
            if 'TICKER' in text_upper:
                ticker_col = idx
            elif 'SHORT' in text_upper and 'FLOAT' in text_upper:
                short_float_col = idx
        
        print()
        
        if ticker_col is None:
            ticker_col = 1  # Fallback
            print("⚠ Ticker column not found by name, using column 1")
        
        if short_float_col is None:
            print("✗ Short Float column not found in header!")
            print("This means Short Float is still not being rendered.")
            return
        else:
            print(f"✓ Found Short Float column at index {short_float_col}")
        
        print()
        print("=" * 80)
        print("EXTRACTING DATA FROM FIRST 20 ROWS")
        print("=" * 80)
        print()
        
        results = []
        for row_idx, row in enumerate(rows[1:21], 1):  # First 20 data rows
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) <= max(ticker_col, short_float_col):
                continue
            
            # Get ticker
            ticker_cell = cells[ticker_col]
            ticker_links = ticker_cell.find_elements(By.TAG_NAME, "a")
            if ticker_links:
                ticker = ticker_links[0].text.strip().upper()
            else:
                ticker = ticker_cell.text.strip().upper()
            
            if not ticker:
                continue
            
            # Get short float
            short_interest_percent = None
            if short_float_col < len(cells):
                short_float_text = cells[short_float_col].text.strip()
                short_interest_percent = parse_short_interest_value(short_float_text)
            
            results.append({
                'ticker': ticker,
                'short_interest_percent': short_interest_percent
            })
            
            if short_interest_percent is not None:
                print(f"{row_idx:2d}. {ticker:10s} - Short Interest: {short_interest_percent:.2f}%")
            else:
                print(f"{row_idx:2d}. {ticker:10s} - Short Interest: N/A")
        
        print()
        print("=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total rows extracted: {len(results)}")
        print(f"Rows with short interest data: {sum(1 for r in results if r['short_interest_percent'] is not None)}")
        print(f"Rows without short interest data: {sum(1 for r in results if r['short_interest_percent'] is None)}")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    import time
    test_selenium_scraper()

