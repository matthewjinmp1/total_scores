#!/usr/bin/env python3
"""
Interactive tool to check short interest for a single ticker from Finviz.
"""

import requests
from bs4 import BeautifulSoup
import re

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

def parse_pe_value(value_str):
    """
    Parse a PE ratio value (Forward P/E).
    Handles formats like "25.5", "N/A", etc.
    """
    if not value_str or value_str.strip().upper() in ['N/A', 'NAN', '']:
        return None
    
    # Remove commas
    cleaned = value_str.replace(',', '').strip()
    
    try:
        return float(cleaned)
    except ValueError:
        return None

def parse_growth_percent(value_str):
    """
    Parse a growth percentage value (e.g., EPS growth next 5 years).
    Handles formats like "25.5%", "N/A", etc.
    """
    if not value_str or value_str.strip().upper() in ['N/A', 'NAN', '']:
        return None
    
    # Remove commas and percentage signs
    cleaned = value_str.replace(',', '').replace('%', '').strip()
    
    try:
        return float(cleaned)
    except ValueError:
        return None

def parse_recommendation(value_str):
    """
    Parse analyst recommendation (text value like "Strong Buy", "Buy", "Hold", etc.).
    Returns the string as-is if valid, None otherwise.
    """
    if not value_str or value_str.strip().upper() in ['N/A', 'NAN', '']:
        return None
    
    return value_str.strip()

def parse_price(value_str):
    """
    Parse a price value (target price).
    Handles formats like "$180.50", "180.50", etc.
    """
    if not value_str or value_str.strip().upper() in ['N/A', 'NAN', '']:
        return None
    
    # Remove dollar signs and commas
    cleaned = value_str.replace('$', '').replace(',', '').strip()
    
    try:
        return float(cleaned)
    except ValueError:
        return None

def scrape_finviz_short_interest(ticker):
    """
    Scrape multiple financial metrics from Finviz for a given ticker in a single request.
    
    Returns a dictionary with: short interest, forward PE, EPS growth, insider ownership, 
    ROA, ROIC, gross margin, operating margin, 10-year performance, analyst recommendation,
    target price, and current price data or error message.
    """
    url = f"{FINVIZ_BASE_URL}{ticker.upper()}"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        return {
            'ticker': ticker.upper(),
            'error': f"Request error: {str(e)}"
        }
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the snapshot table (contains most financial metrics)
    snapshot_table = soup.find('table', class_='snapshot-table2')
    if not snapshot_table:
        return {
            'ticker': ticker.upper(),
            'error': "Could not find snapshot table on Finviz page"
        }
    
    # Parse the table - Finviz uses a grid layout with alternating label/value cells
    data = {'ticker': ticker.upper()}
    all_cells = snapshot_table.find_all('td')
    found_metrics = set()
    
    # Debug: collect all labels to help identify the exact label for operating margin
    all_labels = []
    
    # Finviz table structure: label, value, label, value, etc.
    # Continue through all cells to find all metrics in one pass
    for i in range(0, len(all_cells) - 1, 2):
        if i + 1 >= len(all_cells):
            break
            
        label = all_cells[i].get_text(strip=True)
        value = all_cells[i + 1].get_text(strip=True)
        label_upper = label.upper()  # For case-insensitive matching
        all_labels.append(label)  # Store for debugging
        
        # Look for Short Float (percentage of float)
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
        if ('INSIDER OWN' in label_upper) and 'insider_ownership' not in found_metrics:
            data['insider_ownership'] = parse_growth_percent(value)
            found_metrics.add('insider_ownership')
        
        # Look for Return on Assets (ROA)
        if label_upper == 'ROA' and 'roa' not in found_metrics:
            data['roa'] = parse_growth_percent(value)
            found_metrics.add('roa')
        
        # Look for Return on Invested Capital (ROIC)
        if label_upper == 'ROIC' and 'roic' not in found_metrics:
            data['roic'] = parse_growth_percent(value)
            found_metrics.add('roic')
        
        # Look for Gross Margin
        if 'GROSS M' in label_upper and 'gross_margin' not in found_metrics:
            data['gross_margin'] = parse_growth_percent(value)
            found_metrics.add('gross_margin')
        
        # Look for Operating Margin - try multiple patterns
        # Pattern 1: contains both "operat" and "margin"
        # Pattern 2: starts with "oper" and contains " m" (for abbreviated forms like "Oper M")
        # Pattern 3: exact matches for common abbreviations
        if 'operating_margin' not in found_metrics:
            if (('OPERAT' in label_upper and 'MARGIN' in label_upper) or
                (label_upper.startswith('OPER') and ' M' in label_upper) or
                label_upper == 'OPER M' or
                label_upper == 'OPER. M' or
                'OP MARGIN' in label_upper or
                label_upper == 'OP M'):
                data['operating_margin'] = parse_growth_percent(value)
                found_metrics.add('operating_margin')
        
        # Look for Performance 10Y (10-year performance)
        if ('PERF 10Y' in label_upper or 'PERFORMANCE 10Y' in label_upper or 
            '10Y' in label_upper and 'PERF' in label_upper) and 'perf_10y' not in found_metrics:
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
        
        # Look for Current Price (should be just "Price" without "Target")
        if label_upper == 'PRICE' and 'current_price' not in found_metrics:
            data['current_price'] = parse_price(value)
            found_metrics.add('current_price')
        
        # Stop early if we found all twelve metrics
        if len(found_metrics) >= 12:
            break
    
    # Debug: if operating margin not found, print labels containing "margin" or "operat"
    if 'operating_margin' not in found_metrics:
        margin_labels = [l for l in all_labels if 'margin' in l.lower() or 'operat' in l.lower()]
        if margin_labels:
            data['_debug_margin_labels'] = margin_labels
    
    return data

def main():
    """Main interactive loop."""
    print("=" * 80)
    print("FINANCIAL METRICS CHECKER")
    print("=" * 80)
    print("Enter a ticker symbol to check its financial metrics:")
    print("  - Short Interest, Forward PE, EPS Growth Next 5Y")
    print("  - Insider Ownership, ROA, ROIC")
    print("  - Gross Margin, Operating Margin, Performance 10Y")
    print("  - Analyst Recommendation, Price Move (Current → Target)")
    print("Type 'quit' or 'exit' to stop")
    print("-" * 80)
    print()
    
    while True:
        ticker = input("Enter ticker: ").strip().upper()
        
        if not ticker:
            continue
        
        if ticker in ['QUIT', 'EXIT', 'Q']:
            print("Goodbye!")
            break
        
        print(f"\nFetching data for {ticker}...")
        
        data = scrape_finviz_short_interest(ticker)
        
        if data.get('error'):
            print(f"✗ Error: {data['error']}")
        else:
            # Display short interest
            if data.get('short_interest_percent') is not None:
                print(f"✓ Short Interest: {data['short_interest_percent']:.2f}% of float")
            else:
                print(f"✗ Short Interest: Not available")
            
            # Display forward PE
            if data.get('forward_pe') is not None:
                print(f"✓ Forward P/E: {data['forward_pe']:.2f}")
            else:
                print(f"✗ Forward P/E: Not available")
            
            # Display EPS growth next 5 years
            if data.get('eps_growth_next_5y') is not None:
                print(f"✓ EPS Growth Next 5Y: {data['eps_growth_next_5y']:.2f}%")
            else:
                print(f"✗ EPS Growth Next 5Y: Not available")
            
            # Display insider ownership
            if data.get('insider_ownership') is not None:
                print(f"✓ Insider Ownership: {data['insider_ownership']:.2f}%")
            else:
                print(f"✗ Insider Ownership: Not available")
            
            # Display ROA
            if data.get('roa') is not None:
                print(f"✓ Return on Assets (ROA): {data['roa']:.2f}%")
            else:
                print(f"✗ Return on Assets (ROA): Not available")
            
            # Display ROIC
            if data.get('roic') is not None:
                print(f"✓ Return on Invested Capital (ROIC): {data['roic']:.2f}%")
            else:
                print(f"✗ Return on Invested Capital (ROIC): Not available")
            
            # Display Gross Margin
            if data.get('gross_margin') is not None:
                print(f"✓ Gross Margin: {data['gross_margin']:.2f}%")
            else:
                print(f"✗ Gross Margin: Not available")
            
            # Display Operating Margin
            if data.get('operating_margin') is not None:
                print(f"✓ Operating Margin: {data['operating_margin']:.2f}%")
            else:
                print(f"✗ Operating Margin: Not available")
                # Debug output
                if data.get('_debug_margin_labels'):
                    print(f"  Debug: Found margin/operat-related labels: {data['_debug_margin_labels']}")
            
            # Display Performance 10Y
            if data.get('perf_10y') is not None:
                print(f"✓ Performance 10Y: {data['perf_10y']:.2f}%")
            else:
                print(f"✗ Performance 10Y: Not available")
            
            # Display Analyst Recommendation
            if data.get('recommendation') is not None:
                print(f"✓ Analyst Recommendation: {data['recommendation']}")
            else:
                print(f"✗ Analyst Recommendation: Not available")
            
            # Display move from current price to target price (percentage only)
            if data.get('current_price') is not None and data.get('target_price') is not None:
                price_move = data['target_price'] - data['current_price']
                price_move_percent = (price_move / data['current_price']) * 100
                print(f"✓ Price Move (Current → Target): {price_move_percent:+.2f}%")
            elif data.get('current_price') is not None:
                print(f"✗ Price Move: Current price available, but target price not available")
            elif data.get('target_price') is not None:
                print(f"✗ Price Move: Target price available, but current price not available")
            else:
                print(f"✗ Price Move: Current and/or target price not available")
            
            # If none were found, provide helpful message
            found_any = any([
                data.get('short_interest_percent') is not None,
                data.get('forward_pe') is not None,
                data.get('eps_growth_next_5y') is not None,
                data.get('insider_ownership') is not None,
                data.get('roa') is not None,
                data.get('roic') is not None,
                data.get('gross_margin') is not None,
                data.get('operating_margin') is not None,
                data.get('perf_10y') is not None,
                data.get('recommendation') is not None,
                data.get('target_price') is not None,
                data.get('current_price') is not None
            ])
            if not found_any:
                print("\n  This may mean:")
                print("  - The ticker doesn't exist on Finviz")
                print("  - Data is not available for this stock")
                print("  - The page structure may have changed")
        
        print()
        print("-" * 80)
        print()

if __name__ == "__main__":
    main()

