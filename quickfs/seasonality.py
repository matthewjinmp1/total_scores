#!/usr/bin/env python3
"""
Show revenue seasonality by calculating average revenue percentage for each quarter (Q1-Q4)
across all years to identify seasonal patterns.
"""

import sqlite3
import json
import os
import statistics

# Database path
QUICKFS_DB = os.path.join(os.path.dirname(__file__), "data.db")

def get_ticker_data(ticker):
    """Get QuickFS data for a ticker from the database."""
    if not os.path.exists(QUICKFS_DB):
        print(f"Error: Database not found at {QUICKFS_DB}")
        return None
    
    conn = sqlite3.connect(QUICKFS_DB)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT data_json FROM quickfs_data 
            WHERE ticker = ? AND data_type = 'full'
            ORDER BY fetched_at DESC
            LIMIT 1
        ''', (ticker.upper(),))
        
        row = cursor.fetchone()
        if row:
            data_json = row[0]
            return json.loads(data_json)
        else:
            return None
    finally:
        conn.close()

def parse_quarter_from_date(date_str):
    """
    Parse date string to determine which quarter it belongs to.
    
    Date format is typically YYYY-MM (e.g., "2024-03" for March 2024)
    Quarter determination:
    - Q1: Months 01-03 (Jan, Feb, Mar)
    - Q2: Months 04-06 (Apr, May, Jun)
    - Q3: Months 07-09 (Jul, Aug, Sep)
    - Q4: Months 10-12 (Oct, Nov, Dec)
    
    Returns: (year, quarter) tuple, or None if invalid
    """
    try:
        if '-' in date_str:
            parts = date_str.split('-')
            year = int(parts[0])
            month = int(parts[1])
            quarter = (month - 1) // 3 + 1
            return (year, quarter)
        else:
            # Try to extract year from beginning
            year_str = date_str[:4]
            if year_str.isdigit():
                year = int(year_str)
                # Try to find month
                if len(date_str) >= 6:
                    month_str = date_str[4:6]
                    if month_str.isdigit():
                        month = int(month_str)
                        quarter = (month - 1) // 3 + 1
                        return (year, quarter)
    except (ValueError, IndexError):
        pass
    return None

def calculate_seasonality(ticker_data):
    """
    Calculate revenue and operating profit seasonality by quarter.
    
    Groups revenue and operating profit by quarter (Q1-Q4) across all years, but only includes
    complete years (all 4 quarters). Calculates:
    - Average revenue and operating profit for each quarter
    - Percentage each quarter represents of total (sums to 100% for each)
    
    Args:
        ticker_data: Dictionary containing QuickFS financial data
    
    Returns:
        Dictionary with seasonality data for both revenue and operating profit, or None if insufficient data
    """
    if not ticker_data or 'financials' not in ticker_data:
        return None
    
    financials = ticker_data['financials']
    
    # Use quarterly data
    if 'quarterly' not in financials:
        return None
    
    quarterly_data = financials['quarterly']
    
    if 'revenue' not in quarterly_data or 'period_end_date' not in quarterly_data:
        return None
    
    revenues = quarterly_data['revenue']
    dates = quarterly_data['period_end_date']
    operating_incomes = quarterly_data.get('operating_income', [None] * len(revenues))
    
    # Filter out None values and get valid data (both revenue and operating income)
    valid_data = []
    for date, rev, op_inc in zip(dates, revenues, operating_incomes):
        if rev is not None and rev > 0:
            valid_data.append((date, rev, op_inc if op_inc is not None else None))
    
    if len(valid_data) < 4:  # Need at least 4 quarters (1 year)
        return None
    
    # Group by year and quarter
    years_data = {}  # {year: {1: (date, revenue, op_income), 2: ..., 3: ..., 4: ...}}
    
    for date, rev, op_inc in valid_data:
        parsed = parse_quarter_from_date(date)
        if parsed:
            year, quarter = parsed
            if year not in years_data:
                years_data[year] = {}
            years_data[year][quarter] = (date, rev, op_inc)
    
    # Only keep years that have all 4 quarters
    complete_years = {}
    for year, quarters in years_data.items():
        if len(quarters) == 4 and all(q in quarters for q in [1, 2, 3, 4]):
            complete_years[year] = quarters
    
    if len(complete_years) == 0:
        return None
    
    # Now group by quarter across all complete years
    quarters_data = {1: [], 2: [], 3: [], 4: []}  # Q1, Q2, Q3, Q4
    
    for year, quarters in complete_years.items():
        for quarter in [1, 2, 3, 4]:
            date, rev, op_inc = quarters[quarter]
            quarters_data[quarter].append((year, date, rev, op_inc))
    
    # Calculate totals for each quarter (sum across all complete years)
    revenue_quarter_totals = {}
    op_income_quarter_totals = {}
    seasonality = {}
    
    for quarter in [1, 2, 3, 4]:
        if len(quarters_data[quarter]) > 0:
            revenues_for_quarter = [rev for _, _, rev, _ in quarters_data[quarter]]
            op_incomes_for_quarter = [op_inc for _, _, _, op_inc in quarters_data[quarter] if op_inc is not None]
            
            total_revenue = sum(revenues_for_quarter)
            avg_revenue = statistics.mean(revenues_for_quarter)
            min_revenue = min(revenues_for_quarter)
            max_revenue = max(revenues_for_quarter)
            count = len(revenues_for_quarter)
            
            revenue_quarter_totals[quarter] = total_revenue
            
            # Operating income calculations (only if we have data)
            total_op_income = sum(op_incomes_for_quarter) if op_incomes_for_quarter else None
            avg_op_income = statistics.mean(op_incomes_for_quarter) if op_incomes_for_quarter else None
            min_op_income = min(op_incomes_for_quarter) if op_incomes_for_quarter else None
            max_op_income = max(op_incomes_for_quarter) if op_incomes_for_quarter else None
            
            if total_op_income is not None:
                op_income_quarter_totals[quarter] = total_op_income
            
            seasonality[quarter] = {
                'total_revenue': total_revenue,
                'average_revenue': avg_revenue,
                'min_revenue': min_revenue,
                'max_revenue': max_revenue,
                'count': count,
                'data': quarters_data[quarter],
                'total_op_income': total_op_income,
                'average_op_income': avg_op_income,
                'min_op_income': min_op_income,
                'max_op_income': max_op_income,
            }
    
    # Calculate grand totals
    revenue_grand_total = sum(revenue_quarter_totals.values())
    op_income_grand_total = sum(op_income_quarter_totals.values()) if op_income_quarter_totals else None
    
    if revenue_grand_total <= 0:
        return None
    
    # Calculate percentages (each quarter's total as % of grand total - sums to 100%)
    for quarter in seasonality:
        seasonality[quarter]['revenue_percentage'] = (seasonality[quarter]['total_revenue'] / revenue_grand_total) * 100
        if seasonality[quarter]['total_op_income'] is not None and op_income_grand_total and op_income_grand_total != 0:
            seasonality[quarter]['op_income_percentage'] = (seasonality[quarter]['total_op_income'] / op_income_grand_total) * 100
        else:
            seasonality[quarter]['op_income_percentage'] = None
    
    # Store the complete years info
    seasonality['_complete_years'] = sorted(complete_years.keys())
    seasonality['_num_years'] = len(complete_years)
    seasonality['_has_op_income'] = op_income_grand_total is not None and op_income_grand_total != 0
    
    return seasonality

def format_revenue(revenue):
    """Format revenue as billions with appropriate suffix."""
    if revenue >= 1e9:
        return f"${revenue / 1e9:.2f}B"
    elif revenue >= 1e6:
        return f"${revenue / 1e6:.2f}M"
    else:
        return f"${revenue:,.0f}"

def main():
    """Main function to show revenue seasonality."""
    print("=" * 80)
    print("Revenue Seasonality Analyzer")
    print("=" * 80)
    print()
    
    # Check if database exists
    if not os.path.exists(QUICKFS_DB):
        print(f"Error: Database not found at {QUICKFS_DB}")
        print("Please run get_all_data.py first to fetch QuickFS data.")
        return
    
    while True:
        print("-" * 80)
        ticker = input("Enter ticker symbol (or 'quit' to exit): ").strip().upper()
        
        if ticker.lower() in ['quit', 'exit', 'q']:
            print("Goodbye!")
            break
        
        if not ticker:
            print("Please enter a valid ticker symbol.")
            continue
        
        print(f"\nAnalyzing seasonality for {ticker}...")
        
        # Get ticker data
        ticker_data = get_ticker_data(ticker)
        
        if not ticker_data:
            print(f"✗ No data found for {ticker}")
            print("  Make sure the ticker exists in the database.")
            continue
        
        # Calculate seasonality
        seasonality = calculate_seasonality(ticker_data)
        
        if not seasonality or len(seasonality) < 4:
            print(f"✗ Insufficient data to calculate seasonality for {ticker}")
            print("  Need at least 4 quarters (1 year) of data.")
            continue
        
        # Display results
        print()
        print("=" * 80)
        print(f"REVENUE SEASONALITY: {ticker}")
        print("=" * 80)
        print()
        
        # Get complete years info
        complete_years = seasonality.get('_complete_years', [])
        num_years = seasonality.get('_num_years', 0)
        
        has_op_income = seasonality.get('_has_op_income', False)
        
        print(f"Revenue and Operating Profit by quarter (using {num_years} complete years with all 4 quarters):")
        print()
        
        if has_op_income:
            # Header row with proper spacing
            print(f"{'Quarter':<12} {'Avg Revenue':>18} {'Rev %':>9} {'Avg Op Profit':>18} {'Op %':>9} {'Years':>8}")
            print("-" * 86)
            
            quarter_names = {1: "Q1 (Jan-Mar)", 2: "Q2 (Apr-Jun)", 3: "Q3 (Jul-Sep)", 4: "Q4 (Oct-Dec)"}
            
            total_revenue_percentage = 0
            total_op_percentage = 0
            for quarter in [1, 2, 3, 4]:
                if quarter in seasonality:
                    data = seasonality[quarter]
                    total_revenue_percentage += data['revenue_percentage']
                    if has_op_income and data.get('op_income_percentage') is not None:
                        total_op_percentage += data['op_income_percentage']
                        
                        print(f"{quarter_names[quarter]:<12} {format_revenue(data['average_revenue']):>18} "
                              f"{data['revenue_percentage']:>8.1f}% {format_revenue(data['average_op_income']):>18} "
                              f"{data['op_income_percentage']:>8.1f}% {data['count']:>8}")
                    else:
                        print(f"{quarter_names[quarter]:<12} {format_revenue(data['average_revenue']):>18} "
                              f"{data['revenue_percentage']:>8.1f}% {'N/A':>18} {'N/A':>9} {data['count']:>8}")
            
            print("-" * 86)
            print(f"{'TOTAL':<12} {'':>18} {total_revenue_percentage:>8.1f}% {'':>18} {total_op_percentage:>8.1f}%")
        else:
            print(f"{'Quarter':<12} {'Avg Revenue':>18} {'Rev %':>9} {'Years':>8}")
            print("-" * 47)
            
            quarter_names = {1: "Q1 (Jan-Mar)", 2: "Q2 (Apr-Jun)", 3: "Q3 (Jul-Sep)", 4: "Q4 (Oct-Dec)"}
            
            total_revenue_percentage = 0
            for quarter in [1, 2, 3, 4]:
                if quarter in seasonality:
                    data = seasonality[quarter]
                    total_revenue_percentage += data['revenue_percentage']
                    print(f"{quarter_names[quarter]:<12} {format_revenue(data['average_revenue']):>18} "
                          f"{data['revenue_percentage']:>8.1f}% {data['count']:>8}")
            
            print("-" * 47)
            print(f"{'TOTAL':<12} {'':>18} {total_revenue_percentage:>8.1f}%")
        
        print()
        
        # Show year range
        if complete_years:
            min_year = min(complete_years)
            max_year = max(complete_years)
            print(f"Complete years included: {min_year} to {max_year} ({num_years} years)")
            print()
        
        # Identify strongest/weakest quarters (filter out metadata keys)
        quarter_data = {k: v for k, v in seasonality.items() if isinstance(k, int) and isinstance(v, dict) and 'average_revenue' in v}
        sorted_quarters = sorted(quarter_data.items(), key=lambda x: x[1]['average_revenue'], reverse=True)
        
        if len(sorted_quarters) >= 2:
            strongest_quarter = sorted_quarters[0]
            weakest_quarter = sorted_quarters[-1]
            
            print(f"Revenue - Strongest quarter: {quarter_names[strongest_quarter[0]]} "
                  f"({format_revenue(strongest_quarter[1]['average_revenue'])}, "
                  f"{strongest_quarter[1]['revenue_percentage']:.1f}% of total)")
            print(f"Revenue - Weakest quarter: {quarter_names[weakest_quarter[0]]} "
                  f"({format_revenue(weakest_quarter[1]['average_revenue'])}, "
                  f"{weakest_quarter[1]['revenue_percentage']:.1f}% of total)")
            
            # Calculate seasonality spread
            spread = strongest_quarter[1]['revenue_percentage'] - weakest_quarter[1]['revenue_percentage']
            print(f"Revenue seasonality spread: {spread:.1f} percentage points")
            
            # Operating profit strongest/weakest if available
            if has_op_income:
                sorted_quarters_op = sorted(quarter_data.items(), 
                                           key=lambda x: x[1].get('average_op_income', 0) or 0, 
                                           reverse=True)
                if len(sorted_quarters_op) >= 2:
                    strongest_op = sorted_quarters_op[0]
                    weakest_op = sorted_quarters_op[-1]
                    
                    if strongest_op[1].get('average_op_income') is not None:
                        print()
                        print(f"Operating Profit - Strongest quarter: {quarter_names[strongest_op[0]]} "
                              f"({format_revenue(strongest_op[1]['average_op_income'])}, "
                              f"{strongest_op[1]['op_income_percentage']:.1f}% of total)")
                        print(f"Operating Profit - Weakest quarter: {quarter_names[weakest_op[0]]} "
                              f"({format_revenue(weakest_op[1]['average_op_income'])}, "
                              f"{weakest_op[1]['op_income_percentage']:.1f}% of total)")
                        
                        op_spread = strongest_op[1]['op_income_percentage'] - weakest_op[1]['op_income_percentage']
                        print(f"Operating Profit seasonality spread: {op_spread:.1f} percentage points")
        
        print()
        print("=" * 80)
        print()

if __name__ == '__main__':
    main()

