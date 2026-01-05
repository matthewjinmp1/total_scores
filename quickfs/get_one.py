#!/usr/bin/env python3
"""
Calculate 5-year revenue growth rate for a ticker from QuickFS data.
"""

import sqlite3
import json
import os
from datetime import datetime
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

def get_previous_quarter(year, month):
    """Get the previous quarter's year and month.
    
    Quarters are 3 months apart. For any given month, the previous quarter
    is 3 months earlier. If we go before January, we go to the previous year.
    """
    # Calculate previous month (3 months earlier)
    if month <= 3:
        # If month is Jan, Feb, or Mar, previous quarter is in previous year
        return year - 1, month + 9  # Jan->Oct, Feb->Nov, Mar->Dec
    else:
        # Otherwise, just subtract 3 months
        return year, month - 3

def get_consecutive_quarters(quarterly_data, field_name, min_quarters_required):
    """
    Get consecutive quarters of data for a given field, starting from the most recent.
    
    Args:
        quarterly_data: Dictionary containing quarterly financial data
        field_name: Name of the field to extract (e.g., 'revenue', 'operating_income')
        min_quarters_required: Minimum number of consecutive quarters needed
    
    Returns:
        List of tuples (date, value) in reverse chronological order (most recent first),
        or None if insufficient consecutive data.
    """
    if field_name not in quarterly_data or 'period_end_date' not in quarterly_data:
        return None
    
    values = quarterly_data[field_name]
    dates = quarterly_data['period_end_date']
    
    # Filter out None values and get valid data
    valid_data = [(date, val) for date, val in zip(dates, values) 
                  if date is not None and val is not None]
    
    if len(valid_data) == 0:
        return None
    
    # Sort by date (most recent first)
    valid_data.sort(key=lambda x: x[0], reverse=True)
    
    # Find consecutive quarters starting from the most recent
    consecutive_quarters = []
    
    for i, (date, val) in enumerate(valid_data):
        if i == 0:
            consecutive_quarters.append((date, val))
            continue
        
        # Parse dates
        year1, month1 = map(int, date.split('-'))
        prev_date = consecutive_quarters[-1][0]
        year2, month2 = map(int, prev_date.split('-'))
        
        # Calculate expected previous quarter
        expected_year, expected_month = get_previous_quarter(year2, month2)
        
        # Check if this quarter is exactly one quarter before the previous one
        if year1 == expected_year and month1 == expected_month:
            consecutive_quarters.append((date, val))
        else:
            # Gap detected - stop here
            break
    
    # Check if we have enough consecutive quarters
    if len(consecutive_quarters) < min_quarters_required:
        return None
    
    return consecutive_quarters

def calculate_5y_revenue_growth(ticker_data):
    """
    Calculate 5-year compound annual growth rate (CAGR) for revenue using quarterly data.
    
    Uses:
    - Numerator: Sum of most recent 4 quarters
    - Denominator: Sum of 4 quarters from approximately 5 years ago
    
    This ensures we use the most recent data available without being siloed by year boundaries.
    
    Args:
        ticker_data: Dictionary containing QuickFS financial data
    
    Returns:
        Tuple of (growth_rate, current_revenue_sum, old_revenue_sum, current_periods, old_periods, years_diff)
        Returns None if insufficient data
    """
    if not ticker_data or 'financials' not in ticker_data:
        return None
    
    financials = ticker_data['financials']
    
    # Use quarterly data for 5-year calculation
    if 'quarterly' not in financials:
        return None
    
    quarterly_data = financials['quarterly']
    
    # Get consecutive quarters (requires 20 to find 4 quarters from 5 years ago)
    consecutive_quarters = get_consecutive_quarters(quarterly_data, 'revenue', 20)
    if consecutive_quarters is None:
        return None
    
    # Filter to only positive revenue values
    consecutive_quarters = [(date, rev) for date, rev in consecutive_quarters if rev > 0]
    if len(consecutive_quarters) < 20:
        return None
    
    # Get most recent 4 quarters (numerator)
    recent_4 = consecutive_quarters[:4]
    current_revenue_sum = sum(rev for _, rev in recent_4)
    current_periods = [date for date, _ in recent_4]
    
    if current_revenue_sum <= 0:
        return None
    
    # Find 4 quarters from approximately 5 years ago (quarters 17-20 from the 20 consecutive quarters)
    old_4 = consecutive_quarters[16:20]  # Quarters 17-20 (5 years back, 4 quarters)
    old_revenue_sum = sum(rev for _, rev in old_4)
    old_periods = [date for date, _ in old_4]
    
    if old_revenue_sum <= 0:
        return None
    
    # Calculate the actual time difference in years
    try:
        # Get dates from the periods
        newest_date = recent_4[0][0]  # Most recent quarter
        oldest_date = old_4[-1][0]    # Oldest of the 4 quarters from 5 years ago
        
        # Parse years
        if '-' in newest_date:
            newest_year = float(newest_date.split('-')[0])
            newest_month = float(newest_date.split('-')[1]) if len(newest_date.split('-')) > 1 else 6.0
        else:
            newest_year = float(newest_date[:4]) if len(newest_date) >= 4 else None
            newest_month = 6.0
        
        if '-' in oldest_date:
            oldest_year = float(oldest_date.split('-')[0])
            oldest_month = float(oldest_date.split('-')[1]) if len(oldest_date.split('-')) > 1 else 6.0
        else:
            oldest_year = float(oldest_date[:4]) if len(oldest_date) >= 4 else None
            oldest_month = 6.0
        
        if newest_year and oldest_year:
            # Calculate years difference more accurately
            years_diff = (newest_year + newest_month/12.0) - (oldest_year + oldest_month/12.0)
        else:
            years_diff = 5.0  # Default to 5 years
    except (ValueError, IndexError, TypeError):
        years_diff = 5.0  # Default to 5 years if parsing fails
    
    if years_diff <= 0:
        return None
    
    # CAGR formula: ((Ending Value / Beginning Value) ^ (1/Number of Years)) - 1
    growth_rate = ((current_revenue_sum / old_revenue_sum) ** (1.0 / years_diff)) - 1.0
    
    return (growth_rate, current_revenue_sum, old_revenue_sum, current_periods, old_periods, years_diff)

def calculate_5y_halfway_revenue_growth(ticker_data):
    """
    Calculate 5-year halfway revenue growth using quarterly data.
    
    Formula: Sum of most recent 10 quarters / Sum of oldest 10 quarters (from most recent 20 quarters)
    
    For 5 years = 20 quarters total:
    - Most recent 10 quarters (quarters 1-10)
    - Oldest 10 quarters (quarters 11-20) from the most recent 20 quarters
    
    Args:
        ticker_data: Dictionary containing QuickFS financial data
    
    Returns:
        Tuple of (growth_ratio, recent_10_sum, old_10_sum, recent_periods, old_periods)
        Returns None if insufficient data
    """
    if not ticker_data or 'financials' not in ticker_data:
        return None
    
    financials = ticker_data['financials']
    
    # Use quarterly data for 5-year halfway calculation
    if 'quarterly' not in financials:
        return None
    
    quarterly_data = financials['quarterly']
    
    # Get consecutive quarters (requires 20 for this calculation)
    consecutive_quarters = get_consecutive_quarters(quarterly_data, 'revenue', 20)
    if consecutive_quarters is None:
        return None
    
    # Filter to only positive revenue values
    consecutive_quarters = [(date, rev) for date, rev in consecutive_quarters if rev > 0]
    if len(consecutive_quarters) < 20:
        return None
    
    # Take the most recent 20 consecutive quarters (for 5 years)
    most_recent_20 = consecutive_quarters[:20]
    
    # Most recent 10 quarters (quarters 1-10 of the 20)
    recent_10 = most_recent_20[:10]
    recent_10_sum = sum(rev for _, rev in recent_10)
    recent_periods = [date for date, _ in recent_10]
    
    # Oldest 10 quarters from the most recent 20 (quarters 11-20 of the 20)
    old_10 = most_recent_20[10:20]
    old_10_sum = sum(rev for _, rev in old_10)
    old_periods = [date for date, _ in old_10]
    
    if old_10_sum <= 0:
        return None
    
    # Calculate growth ratio
    growth_ratio = recent_10_sum / old_10_sum
    
    # Return all 20 quarters for display
    all_20_periods = [(date, rev) for date, rev in most_recent_20]
    
    return (growth_ratio, recent_10_sum, old_10_sum, recent_periods, old_periods, all_20_periods)

def calculate_halfway_share_count_growth(ticker_data):
    """
    Calculate 5-year halfway share count growth using quarterly data.
    
    Uses the most recent 20 quarters (5 years) of share count data:
    - Recent 10 quarters: sum of share counts
    - Oldest 10 quarters: sum of share counts
    - Growth ratio = sum(recent 10) / sum(oldest 10)
    
    Note: Quarter 1 is the oldest, Quarter 20 is the newest
    
    Args:
        ticker_data: Dictionary containing QuickFS financial data
    
    Returns:
        Tuple of (growth_ratio, recent_10_sum, old_10_sum, recent_periods, old_periods, all_20_periods)
        Returns None if insufficient data
    """
    if not ticker_data or 'financials' not in ticker_data:
        return None
    
    financials = ticker_data['financials']
    
    # Use quarterly data
    if 'quarterly' not in financials:
        return None
    
    quarterly_data = financials['quarterly']
    
    # Check if share count is available (try different possible field names)
    shares_field = None
    for field_name in ['shares_eop', 'shares_diluted', 'shares_basic', 'shares']:
        if field_name in quarterly_data:
            shares_field = field_name
            break
    
    if not shares_field:
        return None
    
    # Get consecutive quarters (requires 20 for this calculation)
    consecutive_quarters = get_consecutive_quarters(quarterly_data, shares_field, 20)
    if consecutive_quarters is None:
        return None
    
    # Filter to only positive share count values
    consecutive_quarters = [(date, shares) for date, shares in consecutive_quarters if shares > 0]
    if len(consecutive_quarters) < 20:
        return None
    
    # Take the most recent 20 consecutive quarters (for 5 years)
    most_recent_20 = consecutive_quarters[:20]
    
    # Most recent 10 quarters (quarters 1-10 of the 20)
    recent_10 = most_recent_20[:10]
    recent_10_sum = sum(shares for _, shares in recent_10)
    recent_periods = [date for date, _ in recent_10]
    
    # Oldest 10 quarters from the most recent 20 (quarters 11-20 of the 20)
    old_10 = most_recent_20[10:20]
    old_10_sum = sum(shares for _, shares in old_10)
    old_periods = [date for date, _ in old_10]
    
    if old_10_sum <= 0:
        return None
    
    # Calculate growth ratio
    growth_ratio = recent_10_sum / old_10_sum
    
    # Return all 20 quarters for display
    all_20_periods = [(date, shares) for date, shares in most_recent_20]
    
    return (growth_ratio, recent_10_sum, old_10_sum, recent_periods, old_periods, all_20_periods)

def calculate_consistency_of_growth(ticker_data):
    """
    Calculate consistency of growth metric using YoY quarterly revenue growth.
    
    For each quarter in the last 5 years, calculates year-over-year growth
    compared to the same quarter in the previous year, then calculates the
    standard deviation of those growth rates.
    
    Args:
        ticker_data: Dictionary containing QuickFS financial data
    
    Returns:
        Tuple of (stdev, growth_rates, quarters_with_growth)
        Returns None if insufficient data
    """
    if not ticker_data or 'financials' not in ticker_data:
        return None
    
    financials = ticker_data['financials']
    
    # Use quarterly data
    if 'quarterly' not in financials:
        return None
    
    quarterly_data = financials['quarterly']
    
    # Get consecutive quarters (requires 20 for this calculation)
    consecutive_quarters = get_consecutive_quarters(quarterly_data, 'revenue', 20)
    if consecutive_quarters is None:
        return None
    
    # Filter to only positive revenue values
    consecutive_quarters = [(date, rev) for date, rev in consecutive_quarters if rev > 0]
    if len(consecutive_quarters) < 20:
        return None
    
    # Get the last 5 years of consecutive quarters (20 quarters)
    last_5_years = consecutive_quarters[:20]
    
    # Create a dictionary to look up revenue by date (use all consecutive quarters for YoY lookup)
    revenue_by_date = {date: rev for date, rev in consecutive_quarters}
    
    # Calculate YoY growth for each quarter
    growth_rates = []
    quarters_with_growth = []
    
    for date, current_rev in last_5_years:
        # Parse the date to get year and quarter
        try:
            if '-' in date:
                parts = date.split('-')
                year = int(parts[0])
                month = int(parts[1])
                # Determine quarter from month
                quarter = (month - 1) // 3 + 1
                
                # Find the same quarter from the previous year
                prev_year = year - 1
                # Approximate the previous year's quarter date
                # We'll try to find the closest matching date
                prev_year_month = month  # Same month in previous year
                prev_date_candidates = [
                    f"{prev_year}-{prev_year_month:02d}",
                    f"{prev_year}-{prev_year_month-1:02d}" if prev_year_month > 1 else None,
                    f"{prev_year}-{prev_year_month+1:02d}" if prev_year_month < 12 else None,
                ]
                
                # Try to find the previous year's quarter
                prev_rev = None
                prev_date_found = None
                for candidate in prev_date_candidates:
                    if candidate and candidate in revenue_by_date:
                        prev_rev = revenue_by_date[candidate]
                        prev_date_found = candidate
                        break
                
                # If not found, try a broader search - look for any quarter in that year range
                if prev_rev is None:
                    for check_date, check_rev in consecutive_quarters:
                        if '-' in check_date:
                            check_parts = check_date.split('-')
                            check_year = int(check_parts[0])
                            check_month = int(check_parts[1])
                            # Check if it's in the previous year and same quarter
                            if check_year == prev_year:
                                check_quarter = (check_month - 1) // 3 + 1
                                if check_quarter == quarter:
                                    prev_rev = check_rev
                                    prev_date_found = check_date
                                    break
                
                if prev_rev and prev_rev > 0:
                    # Calculate YoY growth
                    yoy_growth = (current_rev - prev_rev) / prev_rev
                    growth_rates.append(yoy_growth)
                    quarters_with_growth.append((date, current_rev, prev_date_found, prev_rev, yoy_growth))
        except (ValueError, IndexError, TypeError):
            # Skip if we can't parse the date
            continue
    
    if len(growth_rates) < 2:
        return None
    
    # Calculate standard deviation of growth rates
    stdev = statistics.stdev(growth_rates)
    
    return (stdev, growth_rates, quarters_with_growth)

def calculate_acceleration_of_growth(ticker_data):
    """
    Calculate acceleration of growth metric using quarterly revenue.
    
    Uses last 21 quarters split into 3 groups of 7 quarters each:
    - sum1 = sum of quarters 1-7 (oldest 7 quarters)
    - sum2 = sum of quarters 8-14 (middle 7 quarters)
    - sum3 = sum of quarters 15-21 (newest 7 quarters)
    
    Then calculates:
    - halfway growth 1 = sum2 / sum1
    - halfway growth 2 = sum3 / sum2
    - acceleration = growth 2 / growth 1
    
    Note: Quarter 1 is the oldest, Quarter 21 is the newest
    
    Args:
        ticker_data: Dictionary containing QuickFS financial data
    
    Returns:
        Tuple of (acceleration, growth1, growth2, sum1, sum2, sum3, all_21_periods)
        where all_21_periods is ordered oldest to newest (quarter 1 to 21)
        Returns None if insufficient data
    """
    if not ticker_data or 'financials' not in ticker_data:
        return None
    
    financials = ticker_data['financials']
    
    # Use quarterly data
    if 'quarterly' not in financials:
        return None
    
    quarterly_data = financials['quarterly']
    
    # Get consecutive quarters (requires 21 for this calculation)
    consecutive_quarters = get_consecutive_quarters(quarterly_data, 'revenue', 21)
    if consecutive_quarters is None:
        return None
    
    # Filter to only positive revenue values
    consecutive_quarters = [(date, rev) for date, rev in consecutive_quarters if rev > 0]
    if len(consecutive_quarters) < 21:
        return None
    
    # Take the most recent 21 consecutive quarters (most recent first)
    most_recent_21 = consecutive_quarters[:21]
    
    # Reverse the list so oldest is first (for display purposes)
    # This way quarter 1 = oldest, quarter 21 = newest
    oldest_to_newest_21 = list(reversed(most_recent_21))
    
    # Split into 3 groups of 7 quarters each
    # Quarters 1-7 (oldest of the 21) - these are indices 0-6 in reversed list
    quarters_1_7 = oldest_to_newest_21[:7]
    sum1 = sum(rev for _, rev in quarters_1_7)
    
    # Quarters 8-14 (middle) - these are indices 7-13 in reversed list
    quarters_8_14 = oldest_to_newest_21[7:14]
    sum2 = sum(rev for _, rev in quarters_8_14)
    
    # Quarters 15-21 (newest) - these are indices 14-20 in reversed list
    quarters_15_21 = oldest_to_newest_21[14:21]
    sum3 = sum(rev for _, rev in quarters_15_21)
    
    if sum1 <= 0 or sum2 <= 0:
        return None
    
    # Calculate growth rates
    growth1 = sum2 / sum1
    growth2 = sum3 / sum2 if sum2 > 0 else None
    
    if growth2 is None:
        return None
    
    # Calculate acceleration
    acceleration = growth2 / growth1 if growth1 > 0 else None
    
    if acceleration is None:
        return None
    
    return (acceleration, growth1, growth2, sum1, sum2, sum3, oldest_to_newest_21)

def calculate_operating_margin_growth(ticker_data):
    """
    Calculate operating margin growth using quarterly data.
    
    Takes the last 20 quarters and calculates operating margin for:
    - Quarters 1-10 (oldest 10): sum(operating_income) / sum(revenue)
    - Quarters 11-20 (newest 10): sum(operating_income) / sum(revenue)
    
    Operating margin growth = operating_margin_2 - operating_margin_1
    
    Note: Quarter 1 is the oldest, Quarter 20 is the newest
    
    Args:
        ticker_data: Dictionary containing QuickFS financial data
    
    Returns:
        Tuple of (margin_growth, margin1, margin2, op_income_sum1, op_income_sum2, revenue_sum1, revenue_sum2, all_20_periods)
        Returns None if insufficient data
    """
    if not ticker_data or 'financials' not in ticker_data:
        return None
    
    financials = ticker_data['financials']
    
    # Use quarterly data
    if 'quarterly' not in financials:
        return None
    
    quarterly_data = financials['quarterly']
    
    if 'revenue' not in quarterly_data or 'operating_income' not in quarterly_data:
        return None
    
    # Get consecutive quarters for revenue (requires 20 for this calculation)
    consecutive_revenue = get_consecutive_quarters(quarterly_data, 'revenue', 20)
    if consecutive_revenue is None:
        return None
    
    # Get consecutive quarters for operating income
    consecutive_op_inc = get_consecutive_quarters(quarterly_data, 'operating_income', 20)
    if consecutive_op_inc is None:
        return None
    
    # Match up consecutive quarters - they should have the same dates
    # Create a dictionary for operating income by date
    op_inc_by_date = {date: val for date, val in consecutive_op_inc}
    
    # Filter to only include quarters where both revenue and operating income are available
    valid_data = []
    for date, rev in consecutive_revenue:
        if date in op_inc_by_date and rev > 0:
            op_inc = op_inc_by_date[date]
            if op_inc is not None:
                valid_data.append((date, rev, op_inc))
    
    # Need at least 20 consecutive quarters with both revenue and operating income
    if len(valid_data) < 20:
        return None
    
    # Take the most recent 20 consecutive quarters
    most_recent_20 = valid_data[:20]
    
    # Reverse the list so oldest is first (for display purposes)
    # This way quarter 1 = oldest, quarter 20 = newest
    oldest_to_newest_20 = list(reversed(most_recent_20))
    
    # Quarters 1-10 (oldest 10)
    quarters_1_10 = oldest_to_newest_20[:10]
    revenue_sum1 = sum(rev for _, rev, _ in quarters_1_10)
    op_income_sum1 = sum(op_inc for _, _, op_inc in quarters_1_10)
    
    # Quarters 11-20 (newest 10)
    quarters_11_20 = oldest_to_newest_20[10:20]
    revenue_sum2 = sum(rev for _, rev, _ in quarters_11_20)
    op_income_sum2 = sum(op_inc for _, _, op_inc in quarters_11_20)
    
    if revenue_sum1 <= 0 or revenue_sum2 <= 0:
        return None
    
    # Calculate operating margins
    margin1 = op_income_sum1 / revenue_sum1
    margin2 = op_income_sum2 / revenue_sum2
    
    # Calculate operating margin growth (difference, not ratio)
    margin_growth = margin2 - margin1
    
    return (margin_growth, margin1, margin2, op_income_sum1, op_income_sum2, revenue_sum1, revenue_sum2, oldest_to_newest_20)

def calculate_gross_margin_growth(ticker_data):
    """
    Calculate gross margin growth using quarterly data.
    
    Takes the last 20 quarters and calculates gross margin for:
    - Quarters 1-10 (oldest 10): sum(gross_profit) / sum(revenue)
    - Quarters 11-20 (newest 10): sum(gross_profit) / sum(revenue)
    
    Gross margin growth = gross_margin_2 - gross_margin_1
    
    Note: Quarter 1 is the oldest, Quarter 20 is the newest
    
    Args:
        ticker_data: Dictionary containing QuickFS financial data
    
    Returns:
        Tuple of (margin_growth, margin1, margin2, gross_profit_sum1, gross_profit_sum2, revenue_sum1, revenue_sum2, all_20_periods)
        Returns None if insufficient data
    """
    if not ticker_data or 'financials' not in ticker_data:
        return None
    
    financials = ticker_data['financials']
    
    # Use quarterly data
    if 'quarterly' not in financials:
        return None
    
    quarterly_data = financials['quarterly']
    
    if 'revenue' not in quarterly_data or 'gross_profit' not in quarterly_data:
        return None
    
    # Get consecutive quarters for revenue (requires 20 for this calculation)
    consecutive_revenue = get_consecutive_quarters(quarterly_data, 'revenue', 20)
    if consecutive_revenue is None:
        return None
    
    # Get consecutive quarters for gross profit
    consecutive_gross_profit = get_consecutive_quarters(quarterly_data, 'gross_profit', 20)
    if consecutive_gross_profit is None:
        return None
    
    # Match up consecutive quarters - they should have the same dates
    # Create a dictionary for gross profit by date
    gross_profit_by_date = {date: val for date, val in consecutive_gross_profit}
    
    # Filter to only include quarters where both revenue and gross profit are available
    valid_data = []
    for date, rev in consecutive_revenue:
        if date in gross_profit_by_date and rev > 0:
            gp = gross_profit_by_date[date]
            if gp is not None:
                valid_data.append((date, rev, gp))
    
    # Need at least 20 consecutive quarters with both revenue and gross profit
    if len(valid_data) < 20:
        return None
    
    # Take the most recent 20 consecutive quarters
    most_recent_20 = valid_data[:20]
    
    # Reverse the list so oldest is first (for display purposes)
    # This way quarter 1 = oldest, quarter 20 = newest
    oldest_to_newest_20 = list(reversed(most_recent_20))
    
    # Quarters 1-10 (oldest 10)
    quarters_1_10 = oldest_to_newest_20[:10]
    revenue_sum1 = sum(rev for _, rev, _ in quarters_1_10)
    gross_profit_sum1 = sum(gp for _, _, gp in quarters_1_10)
    
    # Quarters 11-20 (newest 10)
    quarters_11_20 = oldest_to_newest_20[10:20]
    revenue_sum2 = sum(rev for _, rev, _ in quarters_11_20)
    gross_profit_sum2 = sum(gp for _, _, gp in quarters_11_20)
    
    if revenue_sum1 <= 0 or revenue_sum2 <= 0:
        return None
    
    # Calculate gross margins
    margin1 = gross_profit_sum1 / revenue_sum1
    margin2 = gross_profit_sum2 / revenue_sum2
    
    # Calculate gross margin growth (difference, not ratio)
    margin_growth = margin2 - margin1
    
    return (margin_growth, margin1, margin2, gross_profit_sum1, gross_profit_sum2, revenue_sum1, revenue_sum2, oldest_to_newest_20)

def calculate_operating_margin_consistency(ticker_data):
    """
    Calculate operating margin consistency using the most recent 20 quarters.
    
    Splits the last 20 quarters into 5 groups of 4 quarters each:
    - Group 1: Quarters 1-4 (oldest)
    - Group 2: Quarters 5-8
    - Group 3: Quarters 9-12
    - Group 4: Quarters 13-16
    - Group 5: Quarters 17-20 (newest)
    
    Calculates operating margin for each group: sum(operating_income) / sum(revenue)
    Then calculates the standard deviation of those 5 operating margins.
    
    This avoids seasonality by ensuring each group contains one quarter from each season,
    even if groups span across year boundaries.
    
    Args:
        ticker_data: Dictionary containing QuickFS financial data
    
    Returns:
        Tuple of (stdev, avg_margin, margins_with_data)
        where margins_with_data is a list of (group_num, operating_margin, total_revenue, total_operating_income, quarters_list)
        Returns None if insufficient data
    """
    if not ticker_data or 'financials' not in ticker_data:
        return None
    
    financials = ticker_data['financials']
    
    # Use quarterly data
    if 'quarterly' not in financials:
        return None
    
    quarterly_data = financials['quarterly']
    
    if 'revenue' not in quarterly_data or 'operating_income' not in quarterly_data:
        return None
    
    # Get consecutive quarters for revenue (requires 20 for this calculation)
    consecutive_revenue = get_consecutive_quarters(quarterly_data, 'revenue', 20)
    if consecutive_revenue is None:
        return None
    
    # Get consecutive quarters for operating income
    consecutive_op_inc = get_consecutive_quarters(quarterly_data, 'operating_income', 20)
    if consecutive_op_inc is None:
        return None
    
    # Match up consecutive quarters - they should have the same dates
    op_inc_by_date = {date: val for date, val in consecutive_op_inc}
    
    # Filter to only include quarters where both revenue and operating income are available
    valid_data = []
    for date, rev in consecutive_revenue:
        if date in op_inc_by_date and rev > 0:
            op_inc = op_inc_by_date[date]
            if op_inc is not None:
                valid_data.append((date, rev, op_inc))
    
    # Need at least 20 consecutive quarters with both revenue and operating income
    if len(valid_data) < 20:
        return None
    
    # Take the most recent 20 consecutive quarters
    most_recent_20 = valid_data[:20]
    
    # Reverse the list so oldest is first (for display purposes)
    # This way quarter 1 = oldest, quarter 20 = newest
    oldest_to_newest_20 = list(reversed(most_recent_20))
    
    # Split into 5 groups of 4 quarters each
    groups = []
    for i in range(5):
        start_idx = i * 4
        end_idx = start_idx + 4
        group_quarters = oldest_to_newest_20[start_idx:end_idx]
        groups.append(group_quarters)
    
    # Calculate operating margin for each group
    margins = []
    margins_with_data = []
    
    for group_num, group_quarters in enumerate(groups, 1):
        total_revenue = sum(rev for _, rev, _ in group_quarters)
        total_op_income = sum(op_inc for _, _, op_inc in group_quarters)
        
        if total_revenue > 0:
            margin = total_op_income / total_revenue
            margins.append(margin)
            margins_with_data.append((group_num, margin, total_revenue, total_op_income, group_quarters))
    
    if len(margins) < 2:
        return None
    
    # Calculate standard deviation and average
    stdev = statistics.stdev(margins)
    avg_margin = statistics.mean(margins)
    
    return (stdev, avg_margin, margins_with_data)

def calculate_gross_margin_consistency(ticker_data):
    """
    Calculate gross margin consistency using the most recent 20 quarters.
    
    Splits the last 20 quarters into 5 groups of 4 quarters each:
    - Group 1: Quarters 1-4 (oldest)
    - Group 2: Quarters 5-8
    - Group 3: Quarters 9-12
    - Group 4: Quarters 13-16
    - Group 5: Quarters 17-20 (newest)
    
    Calculates gross margin for each group: sum(gross_profit) / sum(revenue)
    Then calculates the standard deviation of those 5 gross margins.
    
    This avoids seasonality by ensuring each group contains one quarter from each season,
    even if groups span across year boundaries.
    
    Args:
        ticker_data: Dictionary containing QuickFS financial data
    
    Returns:
        Tuple of (stdev, avg_margin, margins_with_data)
        where margins_with_data is a list of (group_num, gross_margin, total_revenue, total_gross_profit, quarters_list)
        Returns None if insufficient data
    """
    if not ticker_data or 'financials' not in ticker_data:
        return None
    
    financials = ticker_data['financials']
    
    # Use quarterly data
    if 'quarterly' not in financials:
        return None
    
    quarterly_data = financials['quarterly']
    
    if 'revenue' not in quarterly_data or 'gross_profit' not in quarterly_data:
        return None
    
    # Get consecutive quarters for revenue (requires 20 for this calculation)
    consecutive_revenue = get_consecutive_quarters(quarterly_data, 'revenue', 20)
    if consecutive_revenue is None:
        return None
    
    # Get consecutive quarters for gross profit
    consecutive_gross_profit = get_consecutive_quarters(quarterly_data, 'gross_profit', 20)
    if consecutive_gross_profit is None:
        return None
    
    # Match up consecutive quarters - they should have the same dates
    gross_profit_by_date = {date: val for date, val in consecutive_gross_profit}
    
    # Filter to only include quarters where both revenue and gross profit are available
    valid_data = []
    for date, rev in consecutive_revenue:
        if date in gross_profit_by_date and rev > 0:
            gp = gross_profit_by_date[date]
            if gp is not None:
                valid_data.append((date, rev, gp))
    
    # Need at least 20 consecutive quarters with both revenue and gross profit
    if len(valid_data) < 20:
        return None
    
    # Take the most recent 20 consecutive quarters
    most_recent_20 = valid_data[:20]
    
    # Reverse the list so oldest is first (for display purposes)
    # This way quarter 1 = oldest, quarter 20 = newest
    oldest_to_newest_20 = list(reversed(most_recent_20))
    
    # Split into 5 groups of 4 quarters each
    groups = []
    for i in range(5):
        start_idx = i * 4
        end_idx = start_idx + 4
        group_quarters = oldest_to_newest_20[start_idx:end_idx]
        groups.append(group_quarters)
    
    # Calculate gross margin for each group
    margins = []
    margins_with_data = []
    
    for group_num, group_quarters in enumerate(groups, 1):
        total_revenue = sum(rev for _, rev, _ in group_quarters)
        total_gross_profit = sum(gp for _, _, gp in group_quarters)
        
        if total_revenue > 0:
            margin = total_gross_profit / total_revenue
            margins.append(margin)
            margins_with_data.append((group_num, margin, total_revenue, total_gross_profit, group_quarters))
    
    if len(margins) < 2:
        return None
    
    # Calculate standard deviation and average
    stdev = statistics.stdev(margins)
    avg_margin = statistics.mean(margins)
    
    return (stdev, avg_margin, margins_with_data)

def calculate_ttm_ebit_ppe(ticker_data):
    """
    Calculate TTM EBIT/PPE (Trailing Twelve Months Operating Income / Property Plant & Equipment).
    
    This is a return on capital metric that shows how efficiently a company uses its fixed assets.
    
    TTM EBIT = Sum of operating income for the most recent 4 quarters
    PPE = Property Plant & Equipment from the most recent quarter
    
    Args:
        ticker_data: Dictionary containing QuickFS financial data
    
    Returns:
        Tuple of (ratio, ttm_ebit, ppe, most_recent_quarter_date, quarters_used)
        where quarters_used is a list of (date, operating_income) for the 4 quarters
        Returns None if insufficient data
    """
    if not ticker_data or 'financials' not in ticker_data:
        return None
    
    financials = ticker_data['financials']
    
    # Use quarterly data
    if 'quarterly' not in financials:
        return None
    
    quarterly_data = financials['quarterly']
    
    if 'operating_income' not in quarterly_data:
        return None
    
    # Check if PPE is available (try different possible field names)
    ppe_field = None
    for field_name in ['ppe_net', 'ppe', 'property_plant_equipment', 'net_ppe', 'fixed_assets']:
        if field_name in quarterly_data:
            ppe_field = field_name
            break
    
    if not ppe_field:
        return None
    
    # Get consecutive quarters for operating income (requires 4 for TTM calculation)
    consecutive_op_inc = get_consecutive_quarters(quarterly_data, 'operating_income', 4)
    if consecutive_op_inc is None:
        return None
    
    # Get most recent 4 consecutive quarters for TTM EBIT
    ttm_quarters = consecutive_op_inc[:4]
    ttm_ebit = sum(op_inc for _, op_inc in ttm_quarters)
    
    # Get PPE from the most recent quarter (same date as first quarter in TTM)
    most_recent_date = ttm_quarters[0][0]
    
    # Get consecutive quarters for PPE to find the most recent value
    consecutive_ppe = get_consecutive_quarters(quarterly_data, ppe_field, 1)
    if consecutive_ppe is None or len(consecutive_ppe) == 0:
        return None
    
    # Get PPE from the most recent quarter
    ppe_date, ppe = consecutive_ppe[0]
    
    # Verify it's the same date as our TTM most recent date
    if ppe_date != most_recent_date:
        # Try to find matching date in PPE data
        ppe_by_date = {date: val for date, val in consecutive_ppe}
        if most_recent_date not in ppe_by_date:
            return None
        ppe = ppe_by_date[most_recent_date]
    
    if ppe is None or ppe <= 0:
        return None
    
    # Calculate ratio
    ratio = ttm_ebit / ppe
    
    return (ratio, ttm_ebit, ppe, most_recent_date, ttm_quarters)

def calculate_net_debt_to_ttm_operating_income(ticker_data):
    """
    Calculate Net Debt to TTM Operating Income ratio.
    
    This metric is generally "lower is better", with special handling:
    - If TTM operating income is positive and net debt is positive: regular calculation (lower is better)
    - If TTM operating income is positive and net debt is negative (cash position): 
      regular calculation will be negative, which is good (lower is better)
    - If TTM operating income is negative: assign 1000 to reflect bad income and debt load
    
    TTM Operating Income = Sum of operating income for the most recent 4 quarters
    Net Debt = Net debt from the most recent quarter
    
    Args:
        ticker_data: Dictionary containing QuickFS financial data
    
    Returns:
        Tuple of (ratio, ttm_operating_income, net_debt, most_recent_quarter_date, quarters_used)
        where quarters_used is a list of (date, operating_income) for the 4 quarters
        Returns None if insufficient data
    """
    if not ticker_data or 'financials' not in ticker_data:
        return None
    
    financials = ticker_data['financials']
    
    # Use quarterly data
    if 'quarterly' not in financials:
        return None
    
    quarterly_data = financials['quarterly']
    
    if 'operating_income' not in quarterly_data:
        return None
    
    # Check if net debt is available
    if 'net_debt' not in quarterly_data:
        return None
    
    # Get consecutive quarters for operating income (requires 4 for TTM calculation)
    consecutive_op_inc = get_consecutive_quarters(quarterly_data, 'operating_income', 4)
    if consecutive_op_inc is None:
        return None
    
    # Get most recent 4 consecutive quarters for TTM Operating Income
    ttm_quarters = consecutive_op_inc[:4]
    ttm_operating_income = sum(op_inc for _, op_inc in ttm_quarters)
    
    # Get Net Debt from the most recent quarter (same date as first quarter in TTM)
    most_recent_date = ttm_quarters[0][0]
    
    # Get consecutive quarters for net debt to find the most recent value
    consecutive_net_debt = get_consecutive_quarters(quarterly_data, 'net_debt', 1)
    if consecutive_net_debt is None or len(consecutive_net_debt) == 0:
        return None
    
    # Get net debt from the most recent quarter
    net_debt_date, net_debt = consecutive_net_debt[0]
    
    # Verify it's the same date as our TTM most recent date
    if net_debt_date != most_recent_date:
        # Try to find matching date in net debt data
        net_debt_by_date = {date: val for date, val in consecutive_net_debt}
        if most_recent_date not in net_debt_by_date:
            return None
        net_debt = net_debt_by_date[most_recent_date]
    
    if net_debt is None:
        return None
    
    # Apply special logic:
    # If operating income is negative, assign 1000 to make it bad relative to other numbers
    if ttm_operating_income <= 0:
        ratio = 1000.0
    else:
        # Regular calculation: net_debt / ttm_operating_income
        # If net_debt is negative (cash position), ratio will be negative (good, since lower is better)
        # If net_debt is positive, ratio will be positive (lower is better)
        ratio = net_debt / ttm_operating_income
    
    return (ratio, ttm_operating_income, net_debt, most_recent_date, ttm_quarters)

def calculate_total_past_return(ticker_data):
    """
    Calculate total past return with dividend reinvestment.
    
    Starts at the very first available data point and goes to the most recent.
    Includes reinvested dividends along the way.
    
    Algorithm:
    - Start with 1 share
    - For each period (oldest to newest):
      - Receive dividend per share
      - Use dividend to buy more shares at the period_end_price
      - Track total shares owned
    - Calculate total return = (final_shares * final_price) / (1 * initial_price) - 1
    
    Args:
        ticker_data: Dictionary containing QuickFS financial data
    
    Returns:
        Tuple of (total_return, total_return_multiplier, initial_price, initial_date, 
                  final_price, final_date, final_shares, periods_with_data)
        where periods_with_data is a list of (date, price, dividend, shares_before, 
        dividend_received, shares_purchased, shares_after)
        Returns None if insufficient data
    """
    if not ticker_data or 'financials' not in ticker_data:
        return None
    
    financials = ticker_data['financials']
    
    # Use quarterly data
    if 'quarterly' not in financials:
        return None
    
    quarterly_data = financials['quarterly']
    
    if 'period_end_price' not in quarterly_data:
        return None
    
    prices = quarterly_data['period_end_price']
    dates = quarterly_data.get('period_end_date', [])
    dividends = quarterly_data.get('dividends', [])
    
    # Get consecutive quarters for price data (need at least 2 for start and end)
    consecutive_prices = get_consecutive_quarters(quarterly_data, 'period_end_price', 2)
    if consecutive_prices is None:
        return None
    
    # Filter to only positive prices
    consecutive_prices = [(date, price) for date, price in consecutive_prices if price > 0]
    if len(consecutive_prices) < 2:
        return None
    
    # Create a dictionary for dividends by date
    dividend_by_date = {}
    if dividends and dates:
        for date, div in zip(dates, dividends):
            if date and div is not None:
                dividend_by_date[date] = div if div is not None else 0.0
    
    # Build valid data with prices and dividends
    valid_data = []
    for date, price in consecutive_prices:
        dividend = dividend_by_date.get(date, 0.0)
        valid_data.append((date, price, dividend))
    
    # Need at least 2 data points (start and end)
    if len(valid_data) < 2:
        return None
    
    # Sort by date (oldest to newest) - consecutive_prices is already most recent first, so reverse
    valid_data.sort(key=lambda x: x[0])
    
    initial_date, initial_price, initial_dividend = valid_data[0]
    final_date, final_price, final_dividend = valid_data[-1]
    
    # Start with 1 share at the initial price
    shares_owned = 1.0
    
    periods_with_data = []
    
    # Process each period (oldest to newest)
    # Dividend reinvestment logic:
    # - At the start of period i, we own shares_owned shares
    # - During period i, we receive dividend per share * shares_owned
    # - We immediately reinvest the dividend at period i's end price to buy more shares
    # - At the end of period i, we own shares_owned + new_shares_from_dividend
    
    for i, (date, price, dividend) in enumerate(valid_data):
        shares_before = shares_owned
        
        # Receive dividend based on shares owned at the start of this period
        dividend_received = shares_before * dividend
        
        # Reinvest dividend to buy more shares at this period's end price
        if price > 0 and dividend_received > 0:
            shares_purchased = dividend_received / price
        else:
            shares_purchased = 0.0
        
        shares_owned += shares_purchased
        shares_after = shares_owned
        
        periods_with_data.append((date, price, dividend, shares_before, 
                                  dividend_received, shares_purchased, shares_after))
    
    # Calculate total return
    # Final value = shares owned at end * final price
    final_value = shares_owned * final_price
    initial_value = 1.0 * initial_price
    
    total_return_multiplier = final_value / initial_value
    total_return = total_return_multiplier - 1.0
    
    return (total_return, total_return_multiplier, initial_price, initial_date,
            final_price, final_date, shares_owned, periods_with_data)

def format_revenue(revenue):
    """Format revenue as billions with appropriate suffix."""
    if revenue >= 1e9:
        return f"${revenue / 1e9:.2f}B"
    elif revenue >= 1e6:
        return f"${revenue / 1e6:.2f}M"
    else:
        return f"${revenue:,.0f}"

def format_shares(shares):
    """Format share count with appropriate suffix."""
    if shares >= 1e9:
        return f"{shares / 1e9:.2f}B"
    elif shares >= 1e6:
        return f"{shares / 1e6:.2f}M"
    else:
        return f"{shares:,.0f}"

def main():
    """Main function to calculate revenue growth."""
    print("=" * 80)
    print("5-Year Revenue Growth Calculator")
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
        
        print(f"\nFetching data for {ticker}...")
        
        # Get ticker data
        ticker_data = get_ticker_data(ticker)
        
        if not ticker_data:
            print(f"✗ No data found for {ticker}")
            print("  Make sure the ticker exists in the database.")
            continue
        
        # Calculate 5-year growth
        result = calculate_5y_revenue_growth(ticker_data)
        
        # Calculate 5-year halfway growth
        halfway_result = calculate_5y_halfway_revenue_growth(ticker_data)
        
        # Calculate consistency of growth
        consistency_result = calculate_consistency_of_growth(ticker_data)
        
        # Calculate acceleration of growth
        acceleration_result = calculate_acceleration_of_growth(ticker_data)
        
        # Calculate operating margin growth
        margin_growth_result = calculate_operating_margin_growth(ticker_data)
        
        # Calculate gross margin growth
        gross_margin_growth_result = calculate_gross_margin_growth(ticker_data)
        
        # Calculate operating margin consistency
        margin_consistency_result = calculate_operating_margin_consistency(ticker_data)
        
        # Calculate gross margin consistency
        gross_margin_consistency_result = calculate_gross_margin_consistency(ticker_data)
        
        # Calculate TTM EBIT/PPE
        ttm_ebit_ppe_result = calculate_ttm_ebit_ppe(ticker_data)
        
        # Calculate total past return with dividend reinvestment
        total_return_result = calculate_total_past_return(ticker_data)
        
        # Calculate halfway share count growth
        share_count_growth_result = calculate_halfway_share_count_growth(ticker_data)
        
        # Calculate Net Debt to TTM Operating Income
        net_debt_ttm_result = calculate_net_debt_to_ttm_operating_income(ticker_data)
        
        if result is None and halfway_result is None and consistency_result is None and acceleration_result is None and margin_growth_result is None and gross_margin_growth_result is None and margin_consistency_result is None and gross_margin_consistency_result is None and ttm_ebit_ppe_result is None and total_return_result is None and share_count_growth_result is None and net_debt_ttm_result is None:
            print(f"✗ Insufficient data to calculate revenue growth metrics for {ticker}")
            continue
        
        # Display results
        print()
        print("=" * 80)
        print(f"5-YEAR REVENUE GROWTH: {ticker}")
        print("=" * 80)
        print()
        
        # Display 5-year CAGR if available
        if result:
            growth_rate, current_rev_sum, old_rev_sum, current_periods, old_periods, years_diff = result
            print(f"5-YEAR CAGR (Quarterly Data):")
            print(f"  Using most recent 4 quarters vs 4 quarters from ~5 years ago")
            print(f"  Period: {old_periods[-1]} to {old_periods[0]} → {current_periods[-1]} to {current_periods[0]}")
            print(f"  Time period: {years_diff:.2f} years")
            print(f"  Revenue (5 years ago, 4 quarters): {format_revenue(old_rev_sum)}")
            print(f"  Revenue (most recent, 4 quarters): {format_revenue(current_rev_sum)}")
            print(f"  CAGR: {growth_rate * 100:.2f}%")
            print(f"  Total Growth: {((current_rev_sum / old_rev_sum) - 1) * 100:.2f}%")
            print()
            print(f"  Quarters used:")
            print(f"    5 years ago: {old_periods[0]} to {old_periods[-1]}")
            print(f"    Most recent: {current_periods[0]} to {current_periods[-1]}")
            print()
        else:
            print("5-YEAR CAGR: Insufficient quarterly data (need at least 20 quarters)")
            print()
        
        # Display 5-year halfway growth if available
        if halfway_result:
            growth_ratio, recent_sum, old_sum, recent_periods, old_periods, all_20_periods = halfway_result
            print(f"5-YEAR HALFWAY GROWTH (Quarterly Data):")
            print(f"  Formula: Sum of recent 10 quarters / Sum of oldest 10 quarters")
            print(f"  Oldest 10 quarters: {old_periods[-1]} to {old_periods[0]}")
            print(f"  Recent 10 quarters: {recent_periods[-1]} to {recent_periods[0]}")
            print(f"  Sum of oldest 10 quarters: {format_revenue(old_sum)}")
            print(f"  Sum of recent 10 quarters: {format_revenue(recent_sum)}")
            print(f"  Growth Ratio: {growth_ratio:.2f}x")
            print(f"  Growth Percentage: {(growth_ratio - 1) * 100:.2f}%")
            print()
            print(f"  Revenue for all 20 quarters (most recent first):")
            print(f"  {'-' * 70}")
            print(f"  {'Quarter':<12} {'Revenue':>15} {'Group':<10}")
            print(f"  {'-' * 70}")
            # Display quarters 1-10 (recent)
            for i, (date, rev) in enumerate(all_20_periods[:10], 1):
                print(f"  {date:<12} {format_revenue(rev):>15} {'Recent 10':<10}")
            # Display quarters 11-20 (oldest of the 20)
            for i, (date, rev) in enumerate(all_20_periods[10:20], 11):
                print(f"  {date:<12} {format_revenue(rev):>15} {'Oldest 10':<10}")
            print(f"  {'-' * 70}")
        else:
            print("5-YEAR HALFWAY GROWTH: Insufficient quarterly data (need at least 20 quarters)")
        
        # Display halfway share count growth if available
        if share_count_growth_result:
            growth_ratio, recent_sum, old_sum, recent_periods, old_periods, all_20_periods = share_count_growth_result
            print()
            print(f"5-YEAR HALFWAY SHARE COUNT GROWTH (Quarterly Data):")
            print(f"  Formula: Sum of recent 10 quarters / Sum of oldest 10 quarters")
            print(f"  Oldest 10 quarters: {old_periods[-1]} to {old_periods[0]}")
            print(f"  Recent 10 quarters: {recent_periods[-1]} to {recent_periods[0]}")
            print(f"  Sum of oldest 10 quarters: {format_shares(old_sum)} shares")
            print(f"  Sum of recent 10 quarters: {format_shares(recent_sum)} shares")
            print(f"  Growth Ratio: {growth_ratio:.4f}x")
            print(f"  Growth Percentage: {(growth_ratio - 1) * 100:.2f}%")
            if growth_ratio < 1.0:
                print(f"  Note: Share count decreased (share buybacks exceeded issuances)")
            elif growth_ratio > 1.0:
                print(f"  Note: Share count increased (share issuances exceeded buybacks)")
            else:
                print(f"  Note: Share count remained stable")
            print()
            print(f"  Share count for all 20 quarters (most recent first):")
            print(f"  {'-' * 70}")
            print(f"  {'Quarter':<12} {'Share Count':>20} {'Group':<10}")
            print(f"  {'-' * 70}")
            # Display quarters 1-10 (recent)
            for i, (date, shares) in enumerate(all_20_periods[:10], 1):
                print(f"  {date:<12} {format_shares(shares):>20} {'Recent 10':<10}")
            # Display quarters 11-20 (oldest of the 20)
            for i, (date, shares) in enumerate(all_20_periods[10:20], 11):
                print(f"  {date:<12} {format_shares(shares):>20} {'Oldest 10':<10}")
            print(f"  {'-' * 70}")
        else:
            print()
            print("5-YEAR HALFWAY SHARE COUNT GROWTH: Insufficient quarterly data (need at least 20 quarters with share count)")
        
        # Display consistency of growth if available
        if consistency_result:
            stdev, growth_rates, quarters_with_growth = consistency_result
            avg_growth = statistics.mean(growth_rates)
            print()
            print(f"CONSISTENCY OF GROWTH (YoY Quarterly Revenue Growth):")
            print(f"  Calculated standard deviation of year-over-year quarterly growth rates")
            print(f"  Number of YoY comparisons: {len(growth_rates)}")
            print(f"  Average YoY Growth: {avg_growth * 100:.2f}%")
            print(f"  Standard Deviation: {stdev * 100:.2f}%")
            print(f"  Coefficient of Variation: {(stdev / abs(avg_growth) * 100) if avg_growth != 0 else 'N/A':.2f}%")
            print()
            print(f"  Year-over-Year Quarterly Growth Rates:")
            print(f"  {'-' * 85}")
            print(f"  {'Quarter':<12} {'Revenue':>15} {'Prev Year':<12} {'Prev Revenue':>15} {'YoY Growth':>12}")
            print(f"  {'-' * 85}")
            for date, current_rev, prev_date, prev_rev, yoy_growth in quarters_with_growth:
                print(f"  {date:<12} {format_revenue(current_rev):>15} {prev_date or 'N/A':<12} {format_revenue(prev_rev) if prev_rev else 'N/A':>15} {yoy_growth * 100:>11.2f}%")
            print(f"  {'-' * 85}")
        else:
            print()
            print("CONSISTENCY OF GROWTH: Insufficient quarterly data (need at least 20 quarters for YoY comparisons)")
        
        # Display acceleration of growth if available
        if acceleration_result:
            acceleration, growth1, growth2, sum1, sum2, sum3, all_21_periods = acceleration_result
            print()
            print(f"ACCELERATION OF GROWTH (Quarterly Revenue):")
            print(f"  Using last 21 quarters split into 3 groups of 7 quarters each")
            print()
            print(f"  Quarters 1-7 (oldest): {all_21_periods[0][0]} to {all_21_periods[6][0]}")
            print(f"    Sum: {format_revenue(sum1)}")
            print(f"  Quarters 8-14 (middle): {all_21_periods[7][0]} to {all_21_periods[13][0]}")
            print(f"    Sum: {format_revenue(sum2)}")
            print(f"  Quarters 15-21 (newest): {all_21_periods[14][0]} to {all_21_periods[20][0]}")
            print(f"    Sum: {format_revenue(sum3)}")
            print()
            print(f"  Halfway Growth 1 (sum2 / sum1): {growth1:.4f}x ({(growth1 - 1) * 100:.2f}%)")
            print(f"  Halfway Growth 2 (sum3 / sum2): {growth2:.4f}x ({(growth2 - 1) * 100:.2f}%)")
            print(f"  Acceleration (growth2 / growth1): {acceleration:.4f}x")
            print()
            print(f"  Revenue for all 21 quarters (oldest to newest):")
            print(f"  {'-' * 85}")
            print(f"  {'Quarter':<4} {'Date':<12} {'Revenue':>15} {'Group':<15}")
            print(f"  {'-' * 85}")
            # Quarters 1-7 (oldest)
            for i, (date, rev) in enumerate(all_21_periods[:7], 1):
                print(f"  {i:<4} {date:<12} {format_revenue(rev):>15} {'Quarters 1-7':<15}")
            # Quarters 8-14 (middle)
            for i, (date, rev) in enumerate(all_21_periods[7:14], 8):
                print(f"  {i:<4} {date:<12} {format_revenue(rev):>15} {'Quarters 8-14':<15}")
            # Quarters 15-21 (newest)
            for i, (date, rev) in enumerate(all_21_periods[14:21], 15):
                print(f"  {i:<4} {date:<12} {format_revenue(rev):>15} {'Quarters 15-21':<15}")
            print(f"  {'-' * 85}")
        else:
            print()
            print("ACCELERATION OF GROWTH: Insufficient quarterly data (need at least 21 quarters)")
        
        # Display operating margin growth if available
        if margin_growth_result:
            margin_growth, margin1, margin2, op_income_sum1, op_income_sum2, revenue_sum1, revenue_sum2, all_20_periods = margin_growth_result
            print()
            print(f"OPERATING MARGIN GROWTH (Quarterly Data):")
            print(f"  Using last 20 quarters split into 2 groups of 10 quarters each")
            print()
            print(f"  Quarters 1-10 (oldest): {all_20_periods[0][0]} to {all_20_periods[9][0]}")
            print(f"    Sum of Operating Income: {format_revenue(op_income_sum1)}")
            print(f"    Sum of Revenue: {format_revenue(revenue_sum1)}")
            print(f"    Operating Margin: {margin1 * 100:.2f}%")
            print(f"  Quarters 11-20 (newest): {all_20_periods[10][0]} to {all_20_periods[19][0]}")
            print(f"    Sum of Operating Income: {format_revenue(op_income_sum2)}")
            print(f"    Sum of Revenue: {format_revenue(revenue_sum2)}")
            print(f"    Operating Margin: {margin2 * 100:.2f}%")
            print()
            print(f"  Operating Margin Growth (Margin 2 - Margin 1): {margin_growth * 100:.2f} percentage points")
            print()
            print(f"  Revenue and Operating Income for all 20 quarters (oldest to newest):")
            print(f"  {'-' * 100}")
            print(f"  {'Quarter':<4} {'Date':<12} {'Revenue':>15} {'Op Income':>15} {'Margin':>12} {'Group':<15}")
            print(f"  {'-' * 100}")
            # Quarters 1-10 (oldest)
            for i, (date, rev, op_inc) in enumerate(all_20_periods[:10], 1):
                margin = (op_inc / rev * 100) if rev > 0 else 0
                print(f"  {i:<4} {date:<12} {format_revenue(rev):>15} {format_revenue(op_inc):>15} {margin:>11.2f}% {'Quarters 1-10':<15}")
            # Quarters 11-20 (newest)
            for i, (date, rev, op_inc) in enumerate(all_20_periods[10:20], 11):
                margin = (op_inc / rev * 100) if rev > 0 else 0
                print(f"  {i:<4} {date:<12} {format_revenue(rev):>15} {format_revenue(op_inc):>15} {margin:>11.2f}% {'Quarters 11-20':<15}")
            print(f"  {'-' * 100}")
        else:
            print()
            print("OPERATING MARGIN GROWTH: Insufficient quarterly data (need at least 20 quarters with operating income)")
        
        # Display gross margin growth if available
        if gross_margin_growth_result:
            margin_growth, margin1, margin2, gross_profit_sum1, gross_profit_sum2, revenue_sum1, revenue_sum2, all_20_periods = gross_margin_growth_result
            print()
            print(f"GROSS MARGIN GROWTH (Quarterly Data):")
            print(f"  Using last 20 quarters split into 2 groups of 10 quarters each")
            print()
            print(f"  Quarters 1-10 (oldest): {all_20_periods[0][0]} to {all_20_periods[9][0]}")
            print(f"    Sum of Gross Profit: {format_revenue(gross_profit_sum1)}")
            print(f"    Sum of Revenue: {format_revenue(revenue_sum1)}")
            print(f"    Gross Margin: {margin1 * 100:.2f}%")
            print(f"  Quarters 11-20 (newest): {all_20_periods[10][0]} to {all_20_periods[19][0]}")
            print(f"    Sum of Gross Profit: {format_revenue(gross_profit_sum2)}")
            print(f"    Sum of Revenue: {format_revenue(revenue_sum2)}")
            print(f"    Gross Margin: {margin2 * 100:.2f}%")
            print()
            print(f"  Gross Margin Growth (Margin 2 - Margin 1): {margin_growth * 100:.2f} percentage points")
            print()
            print(f"  Revenue and Gross Profit for all 20 quarters (oldest to newest):")
            print(f"  {'-' * 100}")
            print(f"  {'Quarter':<4} {'Date':<12} {'Revenue':>15} {'Gross Profit':>15} {'Margin':>12} {'Group':<15}")
            print(f"  {'-' * 100}")
            # Quarters 1-10 (oldest)
            for i, (date, rev, gp) in enumerate(all_20_periods[:10], 1):
                margin = (gp / rev * 100) if rev > 0 else 0
                print(f"  {i:<4} {date:<12} {format_revenue(rev):>15} {format_revenue(gp):>15} {margin:>11.2f}% {'Quarters 1-10':<15}")
            # Quarters 11-20 (newest)
            for i, (date, rev, gp) in enumerate(all_20_periods[10:20], 11):
                margin = (gp / rev * 100) if rev > 0 else 0
                print(f"  {i:<4} {date:<12} {format_revenue(rev):>15} {format_revenue(gp):>15} {margin:>11.2f}% {'Quarters 11-20':<15}")
            print(f"  {'-' * 100}")
        else:
            print()
            print("GROSS MARGIN GROWTH: Insufficient quarterly data (need at least 20 quarters with gross profit)")
        
        # Display operating margin consistency if available
        if margin_consistency_result:
            stdev, avg_margin, margins_with_data = margin_consistency_result
            print()
            print(f"OPERATING MARGIN CONSISTENCY (Most Recent 20 Quarters):")
            print(f"  Calculated standard deviation of operating margins for 5 groups of 4 quarters each")
            print(f"  Using the most recent 20 quarters, split into 5 groups")
            print(f"  Number of groups: {len(margins_with_data)}")
            print(f"  Average Operating Margin: {avg_margin * 100:.2f}%")
            print(f"  Standard Deviation: {stdev * 100:.2f} percentage points")
            print(f"  Coefficient of Variation: {(stdev / abs(avg_margin) * 100) if avg_margin != 0 else 'N/A':.2f}%")
            print()
            print(f"  Operating Margin for each group (oldest to newest):")
            print(f"  {'-' * 110}")
            print(f"  {'Group':<7} {'Quarters':<20} {'Revenue':>18} {'Op Income':>18} {'Margin':>12} {'Deviation':>12}")
            print(f"  {'-' * 110}")
            for group_num, margin, total_rev, total_op_inc, quarters_list in margins_with_data:
                deviation = (margin - avg_margin) * 100
                # Show quarter range (oldest to newest in group)
                oldest_quarter = quarters_list[0][0]
                newest_quarter = quarters_list[-1][0]
                quarter_range = f"{oldest_quarter} to {newest_quarter}"
                print(f"  {group_num:<7} {quarter_range:<20} {format_revenue(total_rev):>18} {format_revenue(total_op_inc):>18} {margin * 100:>11.2f}% {deviation:>+11.2f}pp")
            print(f"  {'-' * 110}")
        else:
            print()
            print("OPERATING MARGIN CONSISTENCY: Insufficient quarterly data (need at least 20 quarters with operating income)")
        
        # Display gross margin consistency if available
        if gross_margin_consistency_result:
            stdev, avg_margin, margins_with_data = gross_margin_consistency_result
            print()
            print(f"GROSS MARGIN CONSISTENCY (Most Recent 20 Quarters):")
            print(f"  Calculated standard deviation of gross margins for 5 groups of 4 quarters each")
            print(f"  Using the most recent 20 quarters, split into 5 groups")
            print(f"  Number of groups: {len(margins_with_data)}")
            print(f"  Average Gross Margin: {avg_margin * 100:.2f}%")
            print(f"  Standard Deviation: {stdev * 100:.2f} percentage points")
            print(f"  Coefficient of Variation: {(stdev / abs(avg_margin) * 100) if avg_margin != 0 else 'N/A':.2f}%")
            print()
            print(f"  Gross Margin for each group (oldest to newest):")
            print(f"  {'-' * 110}")
            print(f"  {'Group':<7} {'Quarters':<20} {'Revenue':>18} {'Gross Profit':>18} {'Margin':>12} {'Deviation':>12}")
            print(f"  {'-' * 110}")
            for group_num, margin, total_rev, total_gross_profit, quarters_list in margins_with_data:
                deviation = (margin - avg_margin) * 100
                # Show quarter range (oldest to newest in group)
                oldest_quarter = quarters_list[0][0]
                newest_quarter = quarters_list[-1][0]
                quarter_range = f"{oldest_quarter} to {newest_quarter}"
                print(f"  {group_num:<7} {quarter_range:<20} {format_revenue(total_rev):>18} {format_revenue(total_gross_profit):>18} {margin * 100:>11.2f}% {deviation:>+11.2f}pp")
            print(f"  {'-' * 110}")
        else:
            print()
            print("GROSS MARGIN CONSISTENCY: Insufficient quarterly data (need at least 20 quarters with gross profit)")
        
        # Display TTM EBIT/PPE if available
        if ttm_ebit_ppe_result:
            ratio, ttm_ebit, ppe, most_recent_date, ttm_quarters = ttm_ebit_ppe_result
            print()
            print(f"TTM EBIT/PPE (Return on Capital):")
            print(f"  Trailing Twelve Months Operating Income / Property Plant & Equipment")
            print(f"  This metric shows how efficiently a company uses its fixed assets")
            print()
            print(f"  Most Recent Quarter (for PPE): {most_recent_date}")
            print(f"  Property Plant & Equipment: {format_revenue(ppe)}")
            print()
            print(f"  TTM Operating Income (sum of last 4 quarters): {format_revenue(ttm_ebit)}")
            print(f"  TTM EBIT/PPE Ratio: {ratio:.4f} ({ratio * 100:.2f}%)")
            print()
            print(f"  Quarters used for TTM calculation (most recent first):")
            print(f"  {'-' * 70}")
            print(f"  {'Quarter':<12} {'Operating Income':>20}")
            print(f"  {'-' * 70}")
            for date, op_inc in ttm_quarters:
                print(f"  {date:<12} {format_revenue(op_inc):>20}")
            print(f"  {'-' * 70}")
            print(f"  {'TTM Total':<12} {format_revenue(ttm_ebit):>20}")
            print(f"  {'-' * 70}")
        else:
            print()
            print("TTM EBIT/PPE: Insufficient quarterly data (need at least 4 quarters with operating income and PPE)")
        
        # Display Net Debt to TTM Operating Income if available
        if net_debt_ttm_result:
            ratio, ttm_operating_income, net_debt, most_recent_date, ttm_quarters = net_debt_ttm_result
            print()
            print(f"NET DEBT TO TTM OPERATING INCOME:")
            print(f"  Net Debt / Trailing Twelve Months Operating Income")
            print(f"  This metric is lower the better (reflects debt burden relative to earnings)")
            print()
            print(f"  Most Recent Quarter (for Net Debt): {most_recent_date}")
            if net_debt >= 0:
                print(f"  Net Debt: {format_revenue(net_debt)}")
            else:
                print(f"  Net Debt (cash position): {format_revenue(-net_debt)} (negative net debt means cash > debt)")
            print()
            print(f"  TTM Operating Income (sum of last 4 quarters): {format_revenue(ttm_operating_income)}")
            
            # Explain the ratio based on the calculation
            if ttm_operating_income <= 0:
                print(f"  Ratio: {ratio:.2f} (assigned 1000 due to negative operating income)")
                print(f"  Interpretation: Company has negative operating income, indicating poor profitability")
            elif net_debt < 0:
                print(f"  Ratio: {ratio:.4f} (negative ratio reflects cash position)")
                print(f"  Interpretation: Company has net cash (cash > debt), which is excellent")
            else:
                print(f"  Ratio: {ratio:.4f}")
                print(f"  Interpretation: Lower is better - indicates less debt relative to operating income")
            
            print()
            print(f"  Quarters used for TTM calculation (most recent first):")
            print(f"  {'-' * 70}")
            print(f"  {'Quarter':<12} {'Operating Income':>20}")
            print(f"  {'-' * 70}")
            for date, op_inc in ttm_quarters:
                print(f"  {date:<12} {format_revenue(op_inc):>20}")
            print(f"  {'-' * 70}")
            print(f"  {'TTM Total':<12} {format_revenue(ttm_operating_income):>20}")
            print(f"  {'-' * 70}")
        else:
            print()
            print("NET DEBT TO TTM OPERATING INCOME: Insufficient quarterly data (need at least 4 quarters with operating income and net debt)")
        
        # Display total past return if available
        if total_return_result:
            total_return, total_return_multiplier, initial_price, initial_date, final_price, final_date, final_shares, periods_with_data = total_return_result
            print()
            print(f"TOTAL PAST RETURN (With Dividend Reinvestment):")
            print(f"  Calculates total return from first available data point to most recent")
            print(f"  Includes reinvested dividends along the way")
            print()
            print(f"  Start Date: {initial_date}")
            print(f"  Initial Price: ${initial_price:.2f}")
            print(f"  Initial Investment: ${initial_price:.2f} (1 share)")
            print()
            print(f"  End Date: {final_date}")
            print(f"  Final Price: ${final_price:.2f}")
            print(f"  Final Shares Owned: {final_shares:.6f} shares")
            print(f"  Final Value: ${final_shares * final_price:.2f}")
            print()
            print(f"  Total Return: {total_return * 100:.2f}%")
            print(f"  Total Return Multiplier: {total_return_multiplier:.4f}x")
            print(f"  Number of Periods: {len(periods_with_data)}")
            print()
            # Show summary of first 5 and last 5 periods, plus key milestones
            print(f"  Key Periods (first 5, last 5, and every 20th period):")
            print(f"  {'-' * 120}")
            print(f"  {'Period':<7} {'Date':<12} {'Price':>12} {'Div/Share':>12} {'Shares Before':>15} {'Div Received':>15} {'Shares Purchased':>18} {'Shares After':>15}")
            print(f"  {'-' * 120}")
            
            # Show first 5
            for i, (date, price, dividend, shares_before, div_received, shares_purchased, shares_after) in enumerate(periods_with_data[:5]):
                print(f"  {i+1:<7} {date:<12} ${price:>11.2f} ${dividend:>11.4f} {shares_before:>15.6f} ${div_received:>14.2f} {shares_purchased:>18.6f} {shares_after:>15.6f}")
            
            # Show every 20th period (if there are more than 10 periods)
            if len(periods_with_data) > 10:
                print(f"  {'...':<7} {'...':<12} {'...':>12} {'...':>12} {'...':>15} {'...':>15} {'...':>18} {'...':>15}")
                for i in range(19, len(periods_with_data) - 5, 20):
                    date, price, dividend, shares_before, div_received, shares_purchased, shares_after = periods_with_data[i]
                    print(f"  {i+1:<7} {date:<12} ${price:>11.2f} ${dividend:>11.4f} {shares_before:>15.6f} ${div_received:>14.2f} {shares_purchased:>18.6f} {shares_after:>15.6f}")
                print(f"  {'...':<7} {'...':<12} {'...':>12} {'...':>12} {'...':>15} {'...':>15} {'...':>18} {'...':>15}")
            
            # Show last 5
            for i, (date, price, dividend, shares_before, div_received, shares_purchased, shares_after) in enumerate(periods_with_data[-5:], len(periods_with_data) - 4):
                print(f"  {i:<7} {date:<12} ${price:>11.2f} ${dividend:>11.4f} {shares_before:>15.6f} ${div_received:>14.2f} {shares_purchased:>18.6f} {shares_after:>15.6f}")
            
            print(f"  {'-' * 120}")
        else:
            print()
            print("TOTAL PAST RETURN: Insufficient data (need at least 2 periods with price data)")
        
        # Display summary of all metrics (just final numbers)
        print()
        print("=" * 80)
        print("METRIC SUMMARY")
        print("=" * 80)
        print()
        
        metrics_list = []
        
        # 5-Year CAGR
        if result:
            growth_rate, current_rev_sum, old_rev_sum, current_periods, old_periods, years_diff = result
            metrics_list.append(f"5-Year CAGR: {growth_rate * 100:.2f}%")
        else:
            metrics_list.append("5-Year CAGR: N/A")
        
        # 5-Year Halfway Growth
        if halfway_result:
            growth_ratio, recent_sum, old_sum, recent_periods, old_periods, all_20_periods = halfway_result
            metrics_list.append(f"5-Year Halfway Growth: {growth_ratio:.4f}x ({(growth_ratio - 1) * 100:.2f}%)")
        else:
            metrics_list.append("5-Year Halfway Growth: N/A")
        
        # 5-Year Halfway Share Count Growth
        if share_count_growth_result:
            growth_ratio, recent_sum, old_sum, recent_periods, old_periods, all_20_periods = share_count_growth_result
            metrics_list.append(f"5-Year Halfway Share Count Growth: {growth_ratio:.4f}x ({(growth_ratio - 1) * 100:.2f}%)")
        else:
            metrics_list.append("5-Year Halfway Share Count Growth: N/A")
        
        # Consistency of Growth
        if consistency_result:
            stdev, growth_rates, quarters_with_growth = consistency_result
            avg_growth = statistics.mean(growth_rates)
            metrics_list.append(f"Consistency of Growth (YoY Stdev): {stdev * 100:.2f}% (Avg: {avg_growth * 100:.2f}%)")
        else:
            metrics_list.append("Consistency of Growth: N/A")
        
        # Acceleration of Growth
        if acceleration_result:
            acceleration, growth1, growth2, sum1, sum2, sum3, all_21_periods = acceleration_result
            metrics_list.append(f"Acceleration of Growth: {acceleration:.4f}x")
        else:
            metrics_list.append("Acceleration of Growth: N/A")
        
        # Operating Margin Growth
        if margin_growth_result:
            margin_growth, margin1, margin2, op_income_sum1, op_income_sum2, revenue_sum1, revenue_sum2, all_20_periods = margin_growth_result
            metrics_list.append(f"Operating Margin Growth: {margin_growth * 100:.2f} pp")
        else:
            metrics_list.append("Operating Margin Growth: N/A")
        
        # Gross Margin Growth
        if gross_margin_growth_result:
            margin_growth, margin1, margin2, gross_profit_sum1, gross_profit_sum2, revenue_sum1, revenue_sum2, all_20_periods = gross_margin_growth_result
            metrics_list.append(f"Gross Margin Growth: {margin_growth * 100:.2f} pp")
        else:
            metrics_list.append("Gross Margin Growth: N/A")
        
        # Operating Margin Consistency
        if margin_consistency_result:
            stdev, avg_margin, margins_with_data = margin_consistency_result
            metrics_list.append(f"Operating Margin Consistency (Stdev): {stdev * 100:.2f} pp")
        else:
            metrics_list.append("Operating Margin Consistency: N/A")
        
        # Gross Margin Consistency
        if gross_margin_consistency_result:
            stdev, avg_margin, margins_with_data = gross_margin_consistency_result
            metrics_list.append(f"Gross Margin Consistency (Stdev): {stdev * 100:.2f} pp")
        else:
            metrics_list.append("Gross Margin Consistency: N/A")
        
        # TTM EBIT/PPE
        if ttm_ebit_ppe_result:
            ratio, ttm_ebit, ppe, most_recent_date, ttm_quarters = ttm_ebit_ppe_result
            metrics_list.append(f"TTM EBIT/PPE: {ratio:.4f} ({ratio * 100:.2f}%)")
        else:
            metrics_list.append("TTM EBIT/PPE: N/A")
        
        # Net Debt to TTM Operating Income
        if net_debt_ttm_result:
            ratio, ttm_operating_income, net_debt, most_recent_date, ttm_quarters = net_debt_ttm_result
            if ttm_operating_income <= 0:
                metrics_list.append(f"Net Debt to TTM Operating Income: {ratio:.2f} (negative income)")
            else:
                metrics_list.append(f"Net Debt to TTM Operating Income: {ratio:.4f}")
        else:
            metrics_list.append("Net Debt to TTM Operating Income: N/A")
        
        # Total Past Return
        if total_return_result:
            total_return, total_return_multiplier, initial_price, initial_date, final_price, final_date, final_shares, periods_with_data = total_return_result
            metrics_list.append(f"Total Past Return (with dividends): {total_return * 100:.2f}% ({total_return_multiplier:.4f}x)")
        else:
            metrics_list.append("Total Past Return: N/A")
        
        # Print all metrics
        for i, metric in enumerate(metrics_list, 1):
            print(f"{i}. {metric}")
        
        print()
        print("=" * 80)
        print()

if __name__ == '__main__':
    main()

