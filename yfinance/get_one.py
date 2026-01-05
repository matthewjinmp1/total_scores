#!/usr/bin/env python3
"""
Fetch and display current year and next year projected growth from Yahoo Finance.
Uses web scraping as a fallback when the API is rate-limited.
"""

import yfinance as yf
import sys
import time
import pandas as pd
from requests.exceptions import HTTPError
import requests
from bs4 import BeautifulSoup
import re
import json

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    from webdriver_manager.chrome import ChromeDriverManager
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("WARNING: Selenium not available. Install with: pip install selenium webdriver-manager", file=sys.stderr)

# Headers to mimic a browser (similar to Finviz scraper)
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0"
}


def get_chrome_driver(headless=True):
    """Create and return a Chrome WebDriver instance."""
    if not SELENIUM_AVAILABLE:
        raise ImportError("Selenium is not available. Install with: pip install selenium webdriver-manager")
    
    chrome_options = Options()
    if headless:
        chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    try:
        print("DEBUG: Downloading/checking ChromeDriver (this may take 30-60 seconds on first run)...", file=sys.stderr)
        sys.stderr.flush()
        service = Service(ChromeDriverManager().install())
        print("DEBUG: ChromeDriver ready, creating WebDriver instance...", file=sys.stderr)
        sys.stderr.flush()
        driver = webdriver.Chrome(service=service, options=chrome_options)
        print("DEBUG: WebDriver created successfully", file=sys.stderr)
        sys.stderr.flush()
        return driver
    except Exception as e:
        print(f"ERROR: Failed to create Chrome driver: {str(e)}", file=sys.stderr)
        print("Make sure Chrome is installed on your system", file=sys.stderr)
        raise


def get_growth_from_web_scraping(ticker_symbol):
    """
    Try to get growth projections by scraping Yahoo Finance web page using Selenium.
    This renders JavaScript and avoids API rate limits.
    
    Args:
        ticker_symbol: Stock ticker symbol (e.g., 'AAPL')
    
    Returns:
        Tuple of (current_year_growth, next_year_growth, method_used) or (None, None, None) if error
    """
    if not SELENIUM_AVAILABLE:
        print("DEBUG: Selenium not available, cannot scrape JavaScript-rendered pages", file=sys.stderr)
        return (None, None, None)
    
    driver = None
    try:
        print(f"DEBUG: Initializing Chrome driver...", file=sys.stderr)
        sys.stderr.flush()  # Ensure message is displayed
        
        try:
            driver = get_chrome_driver(headless=True)
            print(f"DEBUG: Chrome driver initialized successfully", file=sys.stderr)
            sys.stderr.flush()
        except Exception as e:
            print(f"DEBUG: Failed to initialize Chrome driver: {str(e)}", file=sys.stderr)
            return (None, None, None)
        
        # Navigate to analysis page
        analysis_url = f"https://finance.yahoo.com/quote/{ticker_symbol.upper()}/analysis/"
        print(f"DEBUG: Loading {analysis_url} (this may take 10-20 seconds)...", file=sys.stderr)
        sys.stderr.flush()
        
        try:
            driver.set_page_load_timeout(30)  # 30 second timeout for page load
            driver.get(analysis_url)
            print(f"DEBUG: Page loaded, waiting for content...", file=sys.stderr)
            sys.stderr.flush()
        except TimeoutException:
            print(f"DEBUG: Page load timeout (30s)", file=sys.stderr)
            return (None, None, None)
        except Exception as e:
            print(f"DEBUG: Error loading page: {str(e)}", file=sys.stderr)
            return (None, None, None)
        
        # Wait for page to load and tables to appear
        wait = WebDriverWait(driver, 20)  # Increased to 20 seconds
        try:
            print(f"DEBUG: Waiting for tables to appear...", file=sys.stderr)
            sys.stderr.flush()
            # Wait for any table to appear (Revenue Estimate table)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            print(f"DEBUG: Table found, waiting for JavaScript to fully render...", file=sys.stderr)
            sys.stderr.flush()
            time.sleep(3)  # Give JavaScript time to fully render
        except TimeoutException:
            print(f"DEBUG: Timeout waiting for table to load (20s)", file=sys.stderr)
            # Try to get page source anyway - maybe tables are there but selector is wrong
            html_content = driver.page_source
            if 'revenue estimate' in html_content.lower() or 'sales growth' in html_content.lower():
                print(f"DEBUG: Found revenue/sales growth text, continuing anyway...", file=sys.stderr)
            else:
                return (None, None, None)
        except Exception as e:
            print(f"DEBUG: Error waiting for table: {str(e)}", file=sys.stderr)
            return (None, None, None)
        
        # Get the rendered HTML
        html_content = driver.page_source
        print(f"DEBUG: Got rendered HTML, length: {len(html_content)} characters", file=sys.stderr)
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Debug: Count tables found
        tables = soup.find_all('table')
        print(f"DEBUG: Found {len(tables)} table(s) in rendered HTML", file=sys.stderr)
        
        # Debug: Check for revenue estimate text
        page_text = soup.get_text().lower()
        if 'revenue estimate' in page_text:
            print("DEBUG: Found 'revenue estimate' text in rendered page", file=sys.stderr)
        if 'sales growth' in page_text:
            print("DEBUG: Found 'sales growth' text in rendered page", file=sys.stderr)
        
        # Try to find growth estimates in the page
        current_year_growth = None
        next_year_growth = None
        
        # Method 1: Parse HTML tables - specifically look for Revenue Estimate table
        # Yahoo Finance has "Revenue Estimate" table with "Sales Growth (year/est)" row
        for table in tables:
            # Check if this is the Revenue Estimate table
            table_text = table.get_text()
            if 'revenue estimate' in table_text.lower() or 'sales growth' in table_text.lower():
                rows = table.find_all('tr')
                
                # Find the header row to identify column positions
                current_year_col = None
                next_year_col = None
                
                for row in rows:
                    cells = row.find_all(['th', 'td'])
                    row_text = ' '.join([cell.get_text(strip=True) for cell in cells])
                    row_lower = row_text.lower()
                    
                    # Look for header row with "Current Year" and "Next Year"
                    if 'current year' in row_lower or 'next year' in row_lower:
                        for idx, cell in enumerate(cells):
                            cell_text = cell.get_text(strip=True).lower()
                            if 'current year' in cell_text and 'next year' not in cell_text:
                                current_year_col = idx
                            elif 'next year' in cell_text:
                                next_year_col = idx
                        break
                
                # Now find the "Sales Growth (year/est)" row
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 3:
                        continue
                    
                    first_cell = cells[0].get_text(strip=True).lower()
                    
                    # Look for the sales growth row
                    if 'sales growth' in first_cell or ('growth' in first_cell and 'sales' in first_cell):
                        # Extract values from Current Year and Next Year columns
                        if current_year_col is not None and current_year_col < len(cells):
                            current_val_text = cells[current_year_col].get_text(strip=True)
                            current_val_clean = current_val_text.replace('%', '').replace(',', '').strip()
                            try:
                                current_val = float(current_val_clean)
                                if current_year_growth is None:
                                    current_year_growth = current_val / 100.0 if abs(current_val) > 1 else current_val
                            except:
                                pass
                        
                        if next_year_col is not None and next_year_col < len(cells):
                            next_val_text = cells[next_year_col].get_text(strip=True)
                            next_val_clean = next_val_text.replace('%', '').replace(',', '').strip()
                            try:
                                next_val = float(next_val_clean)
                                if next_year_growth is None:
                                    next_year_growth = next_val / 100.0 if abs(next_val) > 1 else next_val
                            except:
                                pass
                        
                        if current_year_growth is not None or next_year_growth is not None:
                            break
        
        if current_year_growth is not None or next_year_growth is not None:
            print(f"DEBUG: Successfully extracted growth data: current={current_year_growth}, next={next_year_growth}", file=sys.stderr)
            return (current_year_growth, next_year_growth, 'selenium_scraping')
        
        print("DEBUG: Could not extract growth data from rendered page", file=sys.stderr)
        return (None, None, None)
        
    except Exception as e:
        print(f"DEBUG: Exception during Selenium scraping: {str(e)}", file=sys.stderr)
        import traceback
        print(f"DEBUG: Traceback: {traceback.format_exc()}", file=sys.stderr)
        return (None, None, None)
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass


def get_growth_from_marketwatch(ticker_symbol):
    """
    Try to get growth projections from MarketWatch as an alternative to Yahoo Finance.
    
    Args:
        ticker_symbol: Stock ticker symbol (e.g., 'AAPL')
    
    Returns:
        Tuple of (current_year_growth, next_year_growth, method_used) or (None, None, None) if error
    """
    try:
        print(f"DEBUG: Trying MarketWatch for {ticker_symbol}...", file=sys.stderr)
        sys.stderr.flush()
        
        # MarketWatch analyst estimates URL
        url = f"https://www.marketwatch.com/investing/stock/{ticker_symbol.upper()}/analystestimates"
        
        session = requests.Session()
        session.headers.update(BROWSER_HEADERS)
        
        time.sleep(1.5)
        response = session.get(url, timeout=15, allow_redirects=True)
        
        if response.status_code != 200:
            print(f"DEBUG: MarketWatch returned status {response.status_code}", file=sys.stderr)
            return (None, None, None)
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        current_year_growth = None
        next_year_growth = None
        
        # MarketWatch typically has tables with revenue estimates
        tables = soup.find_all('table')
        
        for table in tables:
            table_text = table.get_text().lower()
            if 'revenue' in table_text and ('growth' in table_text or 'estimate' in table_text):
                rows = table.find_all('tr')
                
                # Look for revenue growth or estimates
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 3:
                        continue
                    
                    row_text = ' '.join([cell.get_text(strip=True) for cell in cells]).lower()
                    
                    # Look for rows with revenue growth or revenue estimates
                    if 'revenue' in row_text and ('growth' in row_text or 'next year' in row_text or 'current year' in row_text):
                        # Try to extract percentage values
                        for cell in cells[1:]:
                            cell_text = cell.get_text(strip=True)
                            if '%' in cell_text:
                                clean = cell_text.replace('%', '').replace(',', '').strip()
                                try:
                                    pct = float(clean)
                                    pct_decimal = pct / 100.0 if abs(pct) > 1 else pct
                                    if next_year_growth is None and ('next' in row_text or '+1' in row_text):
                                        next_year_growth = pct_decimal
                                    elif current_year_growth is None and ('current' in row_text or '0' in row_text):
                                        current_year_growth = pct_decimal
                                except:
                                    pass
        
        # Also check for any text with revenue growth percentages
        page_text = soup.get_text()
        growth_matches = re.findall(r'revenue.*?growth.*?([+-]?\d+\.?\d*)%', page_text, re.IGNORECASE)
        if growth_matches:
            try:
                # Take the first match as next year growth typically
                if next_year_growth is None:
                    val = float(growth_matches[0])
                    next_year_growth = val / 100.0 if abs(val) > 1 else val
            except:
                pass
        
        if current_year_growth is not None or next_year_growth is not None:
            print(f"DEBUG: Found growth data on MarketWatch", file=sys.stderr)
            return (current_year_growth, next_year_growth, 'marketwatch')
        
        return (None, None, None)
        
    except Exception as e:
        print(f"DEBUG: Error with MarketWatch: {str(e)}", file=sys.stderr)
        return (None, None, None)


def get_growth_projection(ticker_symbol, retry_count=3, delay=2.0, use_scraping=True):
    """
    Get current year and next year projected growth for a ticker.
    
    Args:
        ticker_symbol: Stock ticker symbol (e.g., 'AAPL')
        retry_count: Number of retries on rate limit errors (default: 3)
        delay: Initial delay between requests in seconds (default: 2.0)
    
    Returns:
        Tuple of (current_year_growth, next_year_growth, method_used) or (None, None, None) if error
    """
    try:
        # Add delay to avoid rate limiting (increased from 0.5s)
        time.sleep(delay)
        
        # Create Ticker object
        ticker = yf.Ticker(ticker_symbol)
        
        current_year_growth = None
        next_year_growth = None
        method_used = None
        
        # Method 1: Try get_growth_estimates() which should directly provide growth estimates
        try:
            growth_estimates = ticker.get_growth_estimates()
            time.sleep(0.5)  # Small delay between API calls
            if growth_estimates is not None and len(growth_estimates) > 0:
                # growth_estimates should be a DataFrame with periods in index
                # Common periods: '0y' (current year), '+1y' (next year), '+5y' (5-year)
                for period in growth_estimates.index:
                    period_str = str(period).lower()
                    # Get the growth value (usually in 'Company' column or first column)
                    if len(growth_estimates.columns) > 0:
                        growth_col = growth_estimates.columns[0]  # Usually 'Company' or first column
                        growth_value = growth_estimates.loc[period, growth_col]
                        
                        if pd.notna(growth_value) and growth_value is not None:
                            # Current year: '0y', 'currentyear', 'cy'
                            if period_str in ['0y', 'currentyear', 'cy'] or (period_str.startswith('0') and 'y' in period_str and '5' not in period_str):
                                current_year_growth = growth_value
                                method_used = 'get_growth_estimates'
                            # Next year: '+1y', 'nextyear', 'ny', '1y'
                            elif period_str in ['+1y', 'nextyear', 'ny', '1y'] or (period_str.startswith('+1') and '5' not in period_str) or (period_str.startswith('1y') and '5' not in period_str):
                                next_year_growth = growth_value
                                method_used = 'get_growth_estimates'
        except Exception:
            pass
        
        # Method 2: Try earnings_estimate which has a 'growth' column
        try:
            earnings_estimate = ticker.earnings_estimate
            time.sleep(0.5)  # Small delay between API calls
            if earnings_estimate is not None and len(earnings_estimate) > 0 and 'growth' in earnings_estimate.columns:
                # The DataFrame should have periods in the index
                # Check common period names: '0y' (current year), '+1y' (next year)
                for period in earnings_estimate.index:
                    if pd.notna(earnings_estimate.loc[period, 'growth']):
                        growth_value = earnings_estimate.loc[period, 'growth']
                        period_str = str(period).lower()
                        
                        # Current year: '0y', 'currentyear', or starts with '0'
                        if period_str in ['0y', 'currentyear', 'cy'] or (period_str.startswith('0') and 'y' in period_str):
                            current_year_growth = growth_value
                            method_used = 'earnings_estimate'
                        
                        # Next year: '+1y', 'nextyear', 'ny', or starts with '+1'
                        elif period_str in ['+1y', 'nextyear', 'ny', '1y'] or (period_str.startswith('+1') or period_str.startswith('1y')):
                            next_year_growth = growth_value
                            method_used = 'earnings_estimate'
        except Exception:
            pass
        
        # Method 3: Try earnings_forecasts and calculate growth from EPS estimates
        if current_year_growth is None or next_year_growth is None:
            try:
                earnings_forecasts = ticker.earnings_forecasts
                time.sleep(0.5)  # Small delay between API calls
                if earnings_forecasts is not None and len(earnings_forecasts) > 0:
                    # earnings_forecasts DataFrame should have periods and average EPS estimates
                    # Calculate growth: (next_year_eps - current_year_eps) / current_year_eps
                    
                    # Look for current year and next year estimates
                    current_year_eps = None
                    next_year_eps = None
                    
                    for period in earnings_forecasts.index:
                        period_str = str(period).lower()
                        # Try to get average EPS estimate
                        if 'avg' in earnings_forecasts.columns:
                            eps_value = earnings_forecasts.loc[period, 'avg']
                        elif 'mean' in earnings_forecasts.columns:
                            eps_value = earnings_forecasts.loc[period, 'mean']
                        elif 'estimate' in earnings_forecasts.columns:
                            eps_value = earnings_forecasts.loc[period, 'estimate']
                        else:
                            # Try first numeric column
                            numeric_cols = earnings_forecasts.select_dtypes(include=[float, int]).columns
                            if len(numeric_cols) > 0:
                                eps_value = earnings_forecasts.loc[period, numeric_cols[0]]
                            else:
                                continue
                        
                        if pd.notna(eps_value) and eps_value is not None and eps_value > 0:
                            if period_str in ['0y', 'currentyear', 'cy'] or (period_str.startswith('0') and 'y' in period_str):
                                current_year_eps = float(eps_value)
                            elif period_str in ['+1y', 'nextyear', 'ny', '1y'] or (period_str.startswith('+1') or period_str.startswith('1y')):
                                next_year_eps = float(eps_value)
                    
                    # Calculate growth if we have both values
                    if current_year_eps is not None and next_year_eps is not None and current_year_eps > 0:
                        if current_year_growth is None:
                            # Calculate growth from current to next
                            next_year_growth = (next_year_eps - current_year_eps) / current_year_eps
                            method_used = 'earnings_forecasts_calculated'
                        
                        # Also check if we can calculate current year growth
                        # We'd need previous year EPS for that
                        # For now, just calculate next year growth
            except Exception:
                pass
        
        # Method 4: Try revenue_estimate which might have growth
        if current_year_growth is None or next_year_growth is None:
            try:
                revenue_estimate = ticker.get_revenue_estimate()
                time.sleep(0.5)  # Small delay between API calls
                if revenue_estimate is not None and len(revenue_estimate) > 0:
                    # Similar structure to earnings_estimate
                    if 'growth' in revenue_estimate.columns:
                        for period in revenue_estimate.index:
                            period_str = str(period).lower()
                            growth_value = revenue_estimate.loc[period, 'growth']
                            if pd.notna(growth_value) and growth_value is not None:
                                if period_str in ['0y', 'currentyear', 'cy'] or (period_str.startswith('0') and 'y' in period_str):
                                    if current_year_growth is None:
                                        current_year_growth = growth_value
                                        method_used = 'revenue_estimate'
                                elif period_str in ['+1y', 'nextyear', 'ny', '1y']:
                                    if next_year_growth is None:
                                        next_year_growth = growth_value
                                        method_used = 'revenue_estimate'
            except Exception:
                pass
        
        # Method 5: Check info dictionary for any growth estimates
        if current_year_growth is None or next_year_growth is None:
            try:
                info = ticker.info
                time.sleep(0.5)  # Small delay between API calls
                
                # These might be estimates rather than historical
                if 'earningsEstimateCurrentYear' in info and 'earningsEstimateNextYear' in info:
                    current_est = info.get('earningsEstimateCurrentYear')
                    next_est = info.get('earningsEstimateNextYear')
                    if current_est and next_est and current_est > 0:
                        if next_year_growth is None:
                            next_year_growth = (next_est - current_est) / current_est
                            method_used = 'info_earnings_estimates'
            except Exception:
                pass
        
        return (current_year_growth, next_year_growth, method_used)
    
    except HTTPError as e:
        # Handle rate limiting (429) and other HTTP errors
        if e.response is not None and e.response.status_code == 429:
            # If rate limited and scraping is enabled, try web scraping as fallback
            if use_scraping:
                print("API rate limited. Trying web scraping as alternative...", file=sys.stderr)
                time.sleep(2.0)
                scraping_result = get_growth_from_web_scraping(ticker_symbol)
                if scraping_result[0] is not None or scraping_result[1] is not None:
                    return scraping_result
            
            if retry_count > 0:
                # Exponential backoff: wait longer on each retry
                wait_time = delay * (4 - retry_count)  # 2s, 4s, 6s for retries
                print(f"Rate limited. Waiting {wait_time:.1f} seconds before retry...", file=sys.stderr)
                time.sleep(wait_time)
                return get_growth_projection(ticker_symbol, retry_count - 1, delay, use_scraping)
            else:
                print(f"Rate limited after {retry_count} retries. Please wait a few minutes and try again.", file=sys.stderr)
        else:
            print(f"HTTP error fetching data for {ticker_symbol}: {str(e)}", file=sys.stderr)
        return (None, None, None)
    except Exception as e:
        error_msg = str(e)
        # Check if it's a rate limit error in the message
        if "429" in error_msg or "Too Many Requests" in error_msg or "rate limit" in error_msg.lower():
            # If rate limited and scraping is enabled, try web scraping as fallback
            if use_scraping:
                print("API rate limited. Trying web scraping as alternative...", file=sys.stderr)
                time.sleep(2.0)
                scraping_result = get_growth_from_web_scraping(ticker_symbol)
                if scraping_result[0] is not None or scraping_result[1] is not None:
                    return scraping_result
            
            if retry_count > 0:
                wait_time = delay * (4 - retry_count)
                print(f"Rate limited. Waiting {wait_time:.1f} seconds before retry...", file=sys.stderr)
                time.sleep(wait_time)
                return get_growth_projection(ticker_symbol, retry_count - 1, delay, use_scraping)
            else:
                print(f"Rate limited after {retry_count} retries. Please wait a few minutes and try again.", file=sys.stderr)
        else:
            print(f"Error fetching data for {ticker_symbol}: {error_msg}", file=sys.stderr)
            # If API fails for any reason and we haven't tried scraping yet, try it
            if use_scraping:
                print("Trying web scraping as fallback...", file=sys.stderr)
                time.sleep(1.0)
                scraping_result = get_growth_from_web_scraping(ticker_symbol)
                if scraping_result[0] is not None or scraping_result[1] is not None:
                    return scraping_result
        
        return (None, None, None)


def format_growth_percentage(growth_value):
    """Format growth value as percentage string."""
    if growth_value is None:
        return "N/A"
    
    try:
        # If it's already a percentage (e.g., 0.15 for 15%), convert to percentage
        if isinstance(growth_value, (int, float)):
            # Assume it's a decimal (0.15 = 15%)
            return f"{growth_value * 100:.2f}%"
        else:
            return str(growth_value)
    except:
        return "N/A"


def main():
    """Main interactive loop."""
    print("=" * 80)
    print("YAHOO FINANCE GROWTH PROJECTION LOOKUP")
    print("=" * 80)
    print()
    
    while True:
        # Get ticker from user
        ticker_input = input("Enter ticker symbol (or 'quit' to exit): ").strip().upper()
        
        if ticker_input.lower() in ['quit', 'exit', 'q']:
            print("Goodbye!")
            break
        
        if not ticker_input:
            print("Please enter a valid ticker symbol.")
            continue
        
        print(f"\nFetching growth projections for {ticker_input}...")
        print("(Trying Yahoo Finance first, then MarketWatch as fallback)")
        print("-" * 80)
        
        # Try Yahoo Finance first with Selenium
        current_year_growth, next_year_growth, method = get_growth_from_web_scraping(ticker_input)
        
        # If Yahoo Finance fails, try MarketWatch
        if current_year_growth is None and next_year_growth is None:
            print("\nYahoo Finance failed, trying MarketWatch...")
            current_year_growth, next_year_growth, method = get_growth_from_marketwatch(ticker_input)
        
        # If Yahoo Finance fails, try MarketWatch
        if current_year_growth is None and next_year_growth is None:
            print("\nYahoo Finance failed, trying MarketWatch...")
            current_year_growth, next_year_growth, method = get_growth_from_marketwatch(ticker_input)
        
        # Display results
        print(f"\nGrowth Projections for {ticker_input}:")
        print(f"  Current Year Projected Growth: {format_growth_percentage(current_year_growth)}")
        print(f"  Next Year Projected Growth:    {format_growth_percentage(next_year_growth)}")
        
        if method:
            print(f"\n  Data source: {method}")
        
        if current_year_growth is None and next_year_growth is None:
            print("\nNote: Growth projections could not be found for this ticker.")
            print("      This could be due to:")
            print("      - Insufficient analyst coverage")
            print("      - Ticker not found")
            print("      - Yahoo Finance page structure has changed")
            print("      - Data temporarily unavailable")
            print(f"\n      Tip: Try checking https://finance.yahoo.com/quote/{ticker_input}/analysis manually")
        
        print()
        print("-" * 80)
        print()


if __name__ == "__main__":
    main()
