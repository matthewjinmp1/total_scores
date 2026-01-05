#!/usr/bin/env python3
"""
Test script to fetch first page of Finviz screener and print tickers and short interest.
"""

import requests
from bs4 import BeautifulSoup
import re

# Finviz URLs
FINVIZ_SCREENER_URL = "https://finviz.com/screener.ashx"

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
    """Parse a string value that might be a number, percentage, or 'N/A'."""
    if not value_str or value_str.strip().upper() in ['N/A', 'NAN', '']:
        return None
    
    cleaned = value_str.replace(',', '').replace('%', '').strip().upper()
    
    try:
        return float(cleaned)
    except ValueError:
        return None

def test_scrape_first_page():
    """Test scraping the first page of Finviz screener."""
    print("=" * 80)
    print("TESTING FINVIZ SCREENER SCRAPER")
    print("=" * 80)
    print()
    
    # Try different view parameters to find one with Short Float
    # v=151 is overview, but Short Float might be in a different view
    # Let's try the URL that shows Short Float column
    url = f"{FINVIZ_SCREENER_URL}?v=111&o=-marketcap&r=1"  # v=111 might have more columns
    print(f"Fetching: {url}")
    print()
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        print(f"✓ Successfully fetched page (status: {response.status_code})")
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching page: {e}")
        return
    
    # Save HTML for inspection
    with open('/tmp/finviz_page.html', 'w', encoding='utf-8') as f:
        f.write(response.text)
    print("  (Saved HTML to /tmp/finviz_page.html for inspection)")
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Search for "Short Float" in the HTML to see where it appears
    if 'Short Float' in response.text or 'short float' in response.text.lower():
        print("  ✓ Found 'Short Float' text in HTML")
    else:
        print("  ⚠ 'Short Float' text not found in HTML")
    
    # Find the screener table
    print("\nLooking for screener table...")
    table = soup.find('table', class_='screener_table')
    if not table:
        table = soup.find('table', {'id': 'screener_table'})
    
    if not table:
        print("✗ Could not find screener table")
        print("\nLooking for any tables...")
        all_tables = soup.find_all('table')
        print(f"Found {len(all_tables)} tables")
        for i, t in enumerate(all_tables[:5]):
            print(f"  Table {i+1}: class={t.get('class')}, id={t.get('id')}")
        return
    
    print("✓ Found screener table")
    
    rows = table.find_all('tr')
    print(f"Found {len(rows)} rows in table")
    print()
    
    if len(rows) < 2:
        print("✗ Not enough rows found")
        return
    
    # Find header row
    print("Analyzing header row...")
    header_row = rows[0]
    header_cells = header_row.find_all(['td', 'th'])
    
    print(f"Header has {len(header_cells)} cells:")
    for idx, cell in enumerate(header_cells):
        text = cell.get_text(strip=True)
        print(f"  Column {idx}: '{text}'")
    
    print()
    
    # Find column indices
    ticker_col = None
    short_float_col = None
    
    for idx, cell in enumerate(header_cells):
        text = cell.get_text(strip=True).upper()
        if 'TICKER' in text:
            ticker_col = idx
            print(f"✓ Found TICKER column at index {idx}")
        elif 'SHORT' in text and 'FLOAT' in text:
            short_float_col = idx
            print(f"✓ Found SHORT FLOAT column at index {idx}")
    
    if ticker_col is None:
        print("⚠ TICKER column not found by name, trying column 1...")
        # Check if column 1 has links (tickers usually have links)
        if len(rows) > 1:
            test_row = rows[1]
            test_cells = test_row.find_all('td')
            if len(test_cells) > 1:
                if test_cells[1].find('a'):
                    ticker_col = 1
                    print(f"✓ Using column 1 as ticker (has links)")
    
    if ticker_col is None:
        print("✗ Could not determine ticker column")
        return
    
    print()
    print("=" * 80)
    print("EXTRACTING DATA FROM FIRST 20 ROWS")
    print("=" * 80)
    print()
    
    results = []
    
    # Extract data from first 20 data rows
    for row_idx, row in enumerate(rows[1:21], 1):  # Skip header, get first 20
        cells = row.find_all('td')
        if len(cells) <= ticker_col:
            continue
        
        # Get ticker
        ticker_cell = cells[ticker_col]
        ticker_link = ticker_cell.find('a')
        if ticker_link:
            ticker = ticker_link.get_text(strip=True).upper()
        else:
            ticker = ticker_cell.get_text(strip=True).upper()
        
        if not ticker or len(ticker) > 10:
            continue
        
        # Get short float
        # NOTE: Short Float column is NOT in the HTML we receive - it's loaded via JavaScript
        # We would need Selenium or similar to get it from the screener table
        # For now, we'll try to find it, but it likely won't be there
        short_interest_percent = None
        if short_float_col is not None and short_float_col < len(cells):
            short_float_text = cells[short_float_col].get_text(strip=True)
            short_interest_percent = parse_short_interest_value(short_float_text)
        else:
            # Search for percentage pattern in all cells
            # Short float is usually a positive percentage between 0-100%
            # Change column is usually negative, so skip those
            for cell_idx, cell in enumerate(cells):
                cell_text = cell.get_text(strip=True)
                if '%' in cell_text:
                    # Check if it looks like a percentage (has % and is numeric)
                    cleaned = cell_text.replace('%', '').replace(',', '').strip()
                    try:
                        value = float(cleaned)
                        # Short float is usually positive and reasonable (0-100%)
                        # Change is often negative, so skip negative values
                        if value >= 0 and value <= 100:
                            short_interest_percent = value
                            print(f"    (Found potential short float in column {cell_idx}: {cell_text})")
                            break
                    except ValueError:
                        continue
        
        results.append({
            'ticker': ticker,
            'short_interest_percent': short_interest_percent
        })
        
        # Print each result
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
    print()
    print("=" * 80)
    print("FINDINGS")
    print("=" * 80)
    print("✓ Successfully extracted tickers from screener table")
    print("✗ Short Float column is NOT in the HTML - it's loaded via JavaScript")
    print()
    print("The HTML we receive only has these columns:")
    print("  No., Ticker, Company, Sector, Industry, Country, Market Cap, P/E, Price, Change, Volume")
    print()
    print("To get Short Float from the screener, we need to:")
    print("  1. Use Selenium/Playwright to execute JavaScript")
    print("  2. OR scrape individual quote pages (works but slower)")
    print()
    print("Since Short Float is visible in the browser but not in HTML,")
    print("it must be loaded dynamically after page load via JavaScript.")
    
    # Show sample of all cell values from first data row for debugging
    if len(rows) > 1:
        print()
        print("=" * 80)
        print("DEBUG: First data row cell values (all columns)")
        print("=" * 80)
        first_data_row = rows[1]
        cells = first_data_row.find_all('td')
        print(f"Total cells in first data row: {len(cells)}")
        print()
        for idx, cell in enumerate(cells):
            text = cell.get_text(strip=True)
            # Check if this cell has a link (tickers usually do)
            has_link = "✓" if cell.find('a') else " "
            # Check if it's a percentage
            is_percent = " [%]" if '%' in text else ""
            print(f"  Cell {idx:2d}{has_link}: '{text}'{is_percent}")
        
        print()
        print("Looking for Short Float column...")
        print("Short Float should be a positive percentage (0-100%)")
        print("Let's check which column has percentages that look like short float:")
        for idx, cell in enumerate(cells):
            text = cell.get_text(strip=True)
            if '%' in text:
                cleaned = text.replace('%', '').replace(',', '').strip()
                try:
                    value = float(cleaned)
                    if 0 <= value <= 100:
                        print(f"  Column {idx} has value {value}% - could be Short Float")
                except ValueError:
                    pass

if __name__ == "__main__":
    test_scrape_first_page()

