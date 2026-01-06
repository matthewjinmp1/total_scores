#!/usr/bin/env python3
"""
Scrape stock data from Dataroma for a single ticker.

Extracts and displays:
- Ownership count
- Percent of all portfolios
- Current price to holder price percent move
- Net buys (buys - sells transactions)
- Net dollars bought as a percent of market cap
"""

import requests
from bs4 import BeautifulSoup
import re
import sys

# Dataroma base URL
DATAROMA_BASE_URL = "https://www.dataroma.com/m/stock.php?sym="

# Headers to mimic a browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}


def parse_number(value_str):
    """
    Parse a string value that might be a number, percentage, or contain commas.
    
    Args:
        value_str: String to parse (e.g., "1.446%", "19", "$254.63")
    
    Returns:
        Float value or None if cannot parse
    """
    if not value_str:
        return None
    
    # Remove commas, dollar signs, and percentage signs
    cleaned = value_str.replace(',', '').replace('$', '').replace('%', '').strip()
    
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_currency(value_str):
    """
    Parse a currency string (e.g., "$87,262,746" or "$0").
    
    Args:
        value_str: Currency string to parse
    
    Returns:
        Float value or None if cannot parse
    """
    if not value_str:
        return None
    
    # Remove dollar sign and commas
    cleaned = value_str.replace('$', '').replace(',', '').strip()
    
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_market_cap(value_str):
    """
    Parse market cap string (e.g., "3.12T", "150.5B", "50.2M").
    
    Args:
        value_str: Market cap string to parse
    
    Returns:
        Float value in dollars or None if cannot parse
    """
    if not value_str:
        return None
    
    cleaned = value_str.strip().upper().replace(',', '').replace(' ', '')
    
    # Handle multipliers
    multiplier = 1
    if cleaned.endswith('T'):
        multiplier = 1e12
        cleaned = cleaned[:-1]
    elif cleaned.endswith('B'):
        multiplier = 1e9
        cleaned = cleaned[:-1]
    elif cleaned.endswith('M'):
        multiplier = 1e6
        cleaned = cleaned[:-1]
    elif cleaned.endswith('K'):
        multiplier = 1e3
        cleaned = cleaned[:-1]
    
    try:
        return float(cleaned) * multiplier
    except ValueError:
        return None


def get_stock_price_and_market_cap_from_finviz(ticker):
    """
    Get stock price and market cap from Finviz.
    
    Args:
        ticker: Ticker symbol
    
    Returns:
        Tuple of (current_price, market_cap) or (None, None) if error
    """
    try:
        import time
        time.sleep(0.3)  # Small delay to avoid rate limiting
        
        finviz_url = f"https://finviz.com/quote.ashx?t={ticker.upper()}"
        finviz_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        }
        
        response = requests.get(finviz_url, headers=finviz_headers, timeout=10)
        
        if response.status_code != 200:
            return None, None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        current_price = None
        market_cap = None
        
        # Finviz stores data in a table with class "snapshot-table2"
        # Find the table
        table = soup.find('table', class_='snapshot-table2')
        if not table:
            return None, None
        
        # Finviz table structure: all cells are in td tags, pairs of (label, value)
        # Parse all cells in pairs
        all_cells = table.find_all('td')
        for i in range(0, len(all_cells) - 1, 2):
            if i + 1 >= len(all_cells):
                break
            
            label = all_cells[i].get_text(strip=True)
            value = all_cells[i + 1].get_text(strip=True)
            label_upper = label.upper()
            
            # Look for Price (case-insensitive)
            if label_upper == 'PRICE':
                try:
                    # Remove $ and commas
                    price_str = value.replace('$', '').replace(',', '').strip()
                    price_val = float(price_str)
                    if 0.01 <= price_val <= 100000:
                        current_price = price_val
                except (ValueError, AttributeError):
                    pass
            
            # Look for Market Cap
            elif label_upper == 'MARKET CAP':
                # Market cap is usually in format like "1.23B" or "456.78M"
                market_cap = parse_market_cap(value)
        
        return current_price, market_cap
        
    except Exception:
        return None, None


def get_stock_price_and_market_cap(ticker):
    """
    Get current stock price and market cap from Finviz.
    
    Args:
        ticker: Ticker symbol
    
    Returns:
        Tuple of (current_price, market_cap) or (None, None) if error
    """
    return get_stock_price_and_market_cap_from_finviz(ticker)


def scrape_dataroma_stock(ticker, max_retries=3):
    """
    Scrape stock data from Dataroma for a single ticker.
    
    Args:
        ticker: Ticker symbol to scrape
        max_retries: Maximum number of retry attempts
    
    Returns:
        Dictionary with stock data, or dictionary with error if failed
    """
    url = f"{DATAROMA_BASE_URL}{ticker.upper()}"
    
    result = {
        'ticker': ticker.upper(),
        'company_name': None,
        'ownership_count': None,
        'portfolio_percent': None,
        'hold_price': None,
        'current_price': None,
        'price_move_percent': None,
        'net_buys': None,
        'insider_buys_total': None,
        'insider_sells_total': None,
        'net_dollars': None,
        'market_cap': None,
        'net_dollars_percent_of_market_cap': None,
        'error': None
    }
    
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            
            if response.status_code == 404:
                result['error'] = f"Ticker not found (404)"
                return result
            
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract company name - Dataroma format is usually "Company Name (TICKER)" on a single line
            page_text = soup.get_text()
            ticker_upper = ticker.upper()
            
            # Look for the line that contains the pattern: "Company Name (TICKER)"
            # Split by newlines and find the line with the ticker in parentheses
            lines = page_text.split('\n')
            for line in lines:
                line = line.strip()
                # Look for line that has the ticker in parentheses and looks like a company name
                if f'({ticker_upper})' in line and len(line) < 200:  # Reasonable length for company name
                    # Extract company name before (TICKER)
                    parts = line.split(f'({ticker_upper})', 1)
                    company_name = parts[0].strip()
                    # Only use if it looks like a company name (has letters, not just symbols)
                    if company_name and any(c.isalpha() for c in company_name):
                        # Limit to reasonable length
                        company_name = ' '.join(company_name.split()[:15])
                        result['company_name'] = company_name
                        break
            
            # Extract Super Investor Stats (in table with id="t1")
            # Ownership count
            ownership_count_label = soup.find('td', string=re.compile(r'Ownership count:', re.I))
            if ownership_count_label:
                ownership_count_td = ownership_count_label.find_next_sibling('td')
                if ownership_count_td:
                    ownership_count_b = ownership_count_td.find('b')
                    if ownership_count_b:
                        result['ownership_count'] = parse_number(ownership_count_b.get_text(strip=True))
            
            # % of all portfolios
            portfolio_percent_label = soup.find('td', string=re.compile(r'% of all portfolios:', re.I))
            if portfolio_percent_label:
                portfolio_percent_td = portfolio_percent_label.find_next_sibling('td')
                if portfolio_percent_td:
                    portfolio_percent_b = portfolio_percent_td.find('b')
                    if portfolio_percent_b:
                        result['portfolio_percent'] = parse_number(portfolio_percent_b.get_text(strip=True))
            
            # Hold Price (in tr with id="hold_price", td with id="price")
            hold_price_td = soup.find('td', id='price')
            if hold_price_td:
                price_text = hold_price_td.get_text(strip=True)
                result['hold_price'] = parse_currency(price_text)
            
            # Extract insider transactions (in table with id="ins_sum")
            insider_buys_transactions = None
            insider_buys_total = None
            insider_sells_transactions = None
            insider_sells_total = None
            
            ins_table = soup.find('table', id='ins_sum')
            if ins_table:
                # Find buys row (class="buys")
                buys_row = ins_table.find('tr', class_='buys')
                if buys_row:
                    buys_cells = buys_row.find_all('td', class_='num')
                    if len(buys_cells) >= 2:
                        insider_buys_transactions = parse_number(buys_cells[0].get_text(strip=True))
                        insider_buys_total = parse_currency(buys_cells[1].get_text(strip=True))
                
                # Find sells row (class="sells")
                sells_row = ins_table.find('tr', class_='sells')
                if sells_row:
                    sells_cells = sells_row.find_all('td', class_='num')
                    if len(sells_cells) >= 2:
                        insider_sells_transactions = parse_number(sells_cells[0].get_text(strip=True))
                        insider_sells_total = parse_currency(sells_cells[1].get_text(strip=True))
            
            result['insider_buys_total'] = insider_buys_total
            result['insider_sells_total'] = insider_sells_total
            result['insider_buys_transactions'] = insider_buys_transactions
            result['insider_sells_transactions'] = insider_sells_transactions
            
            # Calculate net buys (transactions)
            if insider_buys_transactions is not None and insider_sells_transactions is not None:
                result['net_buys'] = insider_buys_transactions - insider_sells_transactions
            
            # Get current price and market cap from Finviz
            current_price, market_cap = get_stock_price_and_market_cap(ticker)
            result['current_price'] = current_price
            result['market_cap'] = market_cap
            
            # Calculate current price to holder price percent move
            # This shows how much the current price needs to move TO reach the holding price
            # Formula: ((Holding Price - Current Price) / Current Price) * 100
            if current_price is not None and result['hold_price'] is not None:
                result['price_move_percent'] = ((result['hold_price'] - current_price) / current_price) * 100
            
            # Calculate net dollars (buys - sells)
            if insider_buys_total is not None and insider_sells_total is not None:
                result['net_dollars'] = insider_buys_total - insider_sells_total
                
                # Calculate net dollars as percent of market cap
                if market_cap is not None and market_cap > 0:
                    result['net_dollars_percent_of_market_cap'] = (result['net_dollars'] / market_cap) * 100
            
            return result
            
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                continue
            result['error'] = f"Request error: {str(e)}"
            return result
        except Exception as e:
            if attempt < max_retries:
                continue
            result['error'] = f"Error parsing page: {str(e)}"
            return result
    
    result['error'] = f"Failed after {max_retries} retries"
    return result


def main():
    """Main function for command-line usage."""
    # If ticker provided as command-line argument, use it once and exit
    if len(sys.argv) >= 2:
        ticker = sys.argv[1].upper()
        print(f"Scraping Dataroma data for {ticker}...")
        
        data = scrape_dataroma_stock(ticker)
        
        if data.get('error'):
            print(f"Error: {data['error']}")
            sys.exit(1)
        
        print("\n" + "=" * 80)
        company_name = data.get('company_name')
        if company_name:
            print(f"DATAROMA DATA FOR {ticker} ({company_name})")
        else:
            print(f"DATAROMA DATA FOR {ticker}")
        print("=" * 80)
        
        # Display metrics (same as below)
        display_metrics(data, ticker)
        return
    
    # Otherwise, continuously ask for tickers
    while True:
        try:
            ticker = input("\nEnter ticker symbol (or 'q' to quit): ").strip().upper()
            if not ticker or ticker == 'Q':
                print("Goodbye!")
                break
            
            print(f"Scraping Dataroma data for {ticker}...")
            
            data = scrape_dataroma_stock(ticker)
            
            if data.get('error'):
                print(f"Error: {data['error']}")
                continue
            
            print("\n" + "=" * 80)
            company_name = data.get('company_name')
            if company_name:
                print(f"DATAROMA DATA FOR {ticker} ({company_name})")
            else:
                print(f"DATAROMA DATA FOR {ticker}")
            print("=" * 80)
            
            # Display metrics
            display_metrics(data, ticker)
            
        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except EOFError:
            print("\n\nGoodbye!")
            break


def display_metrics(data, ticker):
    """Display all metrics for a ticker."""
    
    # Ownership count
    if data.get('ownership_count') is not None:
        print(f"\n1. Ownership count: {data['ownership_count']:.0f}")
        print(f"   Calculation: Directly from Dataroma (number of super investors holding this stock)")
    else:
        print(f"\n1. Ownership count: Not available")
        print(f"   Reason: Could not find ownership count data on Dataroma")
    
    # Percent of all portfolios
    if data.get('portfolio_percent') is not None:
        print(f"\n2. Percent of all portfolios: {data['portfolio_percent']:.3f}%")
        print(f"   Calculation: Directly from Dataroma (percentage of all super investor portfolios)")
    else:
        print(f"\n2. Percent of all portfolios: Not available")
        print(f"   Reason: Could not find portfolio percentage data on Dataroma")
    
    # Current price to holder price percent move
    if data.get('price_move_percent') is not None:
        sign = "+" if data['price_move_percent'] >= 0 else ""
        current_price = data.get('current_price')
        hold_price = data.get('hold_price')
        print(f"\n3. Current price to holder price percent move: {sign}{data['price_move_percent']:.2f}%")
        if current_price is not None and hold_price is not None:
            price_diff = hold_price - current_price
            print(f"   Calculation: ((Holding Price - Current Price) / Current Price) * 100")
            print(f"                = ((${hold_price:.2f} - ${current_price:.2f}) / ${current_price:.2f}) * 100")
            print(f"                = (${price_diff:.2f} / ${current_price:.2f}) * 100")
            print(f"                = {sign}{data['price_move_percent']:.2f}%")
            print(f"   Current Price: ${current_price:.2f} (from Finviz)")
            print(f"   Holding Price: ${hold_price:.2f} (from Dataroma)")
            if data['price_move_percent'] > 0:
                print(f"   Interpretation: Price needs to increase by {abs(data['price_move_percent']):.2f}% to reach holding price")
            elif data['price_move_percent'] < 0:
                print(f"   Interpretation: Price is {abs(data['price_move_percent']):.2f}% above the holding price")
    else:
        print(f"\n3. Current price to holder price percent move: Not available")
        current_price = data.get('current_price')
        hold_price = data.get('hold_price')
        if current_price is None and hold_price is None:
            print(f"   Reason: Could not get current price (from Finviz) and hold price (from Dataroma)")
        elif current_price is None:
            print(f"   Reason: Could not get current price from Finviz")
        elif hold_price is None:
            print(f"   Reason: Could not get hold price from Dataroma (stock may not be tracked by super investors)")
    
    # Net buys
    if data.get('net_buys') is not None:
        buys_transactions = data.get('insider_buys_transactions')
        sells_transactions = data.get('insider_sells_transactions')
        print(f"\n4. Net buys: {data['net_buys']:.0f}")
        print(f"   Calculation: Insider Buys Transactions - Insider Sells Transactions")
        if buys_transactions is not None and sells_transactions is not None:
            print(f"                = {buys_transactions:.0f} - {sells_transactions:.0f}")
            print(f"                = {data['net_buys']:.0f} transactions")
            print(f"   Insider Buys Transactions: {buys_transactions:.0f}")
            print(f"   Insider Sells Transactions: {sells_transactions:.0f}")
        else:
            print(f"                = {data['net_buys']:.0f} transactions")
        print(f"   Note: Positive = net buying, Negative = net selling")
    
    # Net dollars bought as a percent of market cap
    if data.get('net_dollars_percent_of_market_cap') is not None:
        net_dollars = data.get('net_dollars')
        market_cap = data.get('market_cap')
        insider_buys_total = data.get('insider_buys_total')
        insider_sells_total = data.get('insider_sells_total')
        
        print(f"\n5. Net dollars bought as a percent of market cap: {data['net_dollars_percent_of_market_cap']:.4f}%")
        if net_dollars is not None and market_cap is not None:
            print(f"   Calculation: (Net Dollars / Market Cap) * 100")
            print(f"                = ((Insider Buys Total - Insider Sells Total) / Market Cap) * 100")
            if insider_buys_total is not None and insider_sells_total is not None:
                print(f"                = ((${insider_buys_total:,.0f} - ${insider_sells_total:,.0f}) / ${market_cap:,.0f}) * 100")
                print(f"                = (${net_dollars:,.0f} / ${market_cap:,.0f}) * 100")
            else:
                print(f"                = (${net_dollars:,.0f} / ${market_cap:,.0f}) * 100")
            print(f"                = {data['net_dollars_percent_of_market_cap']:.4f}%")
            if insider_buys_total is not None and insider_sells_total is not None:
                print(f"   Insider Buys Total: ${insider_buys_total:,.0f}")
                print(f"   Insider Sells Total: ${insider_sells_total:,.0f}")
                print(f"   Net Dollars: ${net_dollars:,.0f}")
            print(f"   Market Cap: ${market_cap:,.0f} (from Finviz)")
    else:
        print(f"\n5. Net dollars bought as a percent of market cap: Not available")
        insider_buys_total = data.get('insider_buys_total')
        insider_sells_total = data.get('insider_sells_total')
        market_cap = data.get('market_cap')
        reasons = []
        if insider_buys_total is None or insider_sells_total is None:
            reasons.append("Could not get insider transaction totals from Dataroma")
        if market_cap is None:
            reasons.append("Could not get market cap from Finviz")
        print(f"   Reason: {' and '.join(reasons) if reasons else 'Unknown error'}")
    
    print()


if __name__ == "__main__":
    main()
