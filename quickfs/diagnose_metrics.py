#!/usr/bin/env python3
"""
Diagnostic tool to check which QuickFS metrics fail for a ticker and why.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from get_one import (
    get_ticker_data,
    calculate_5y_revenue_growth,
    calculate_5y_halfway_revenue_growth,
    calculate_halfway_share_count_growth,
    calculate_consistency_of_growth,
    calculate_acceleration_of_growth,
    calculate_operating_margin_growth,
    calculate_gross_margin_growth,
    calculate_operating_margin_consistency,
    calculate_gross_margin_consistency,
    calculate_ttm_ebit_ppe,
    calculate_net_debt_to_ttm_operating_income,
    calculate_total_past_return,
    get_consecutive_quarters,
    get_previous_quarter
)

def format_quarter_table(quarters_data, title, show_gap_warning=False):
    """Format quarter data as a table."""
    if not quarters_data:
        return []
    
    lines = []
    lines.append(f"  {title}:")
    lines.append("  " + "-" * 60)
    lines.append(f"  {'Quarter':<12} {'Value':<20}")
    lines.append("  " + "-" * 60)
    
    for date, val in quarters_data:
        # Format the value
        if isinstance(val, (int, float)):
            if abs(val) >= 1e9:
                val_str = f"${val/1e9:.2f}B"
            elif abs(val) >= 1e6:
                val_str = f"${val/1e6:.2f}M"
            elif abs(val) >= 1e3:
                val_str = f"${val/1e3:.2f}K"
            else:
                val_str = f"${val:.2f}"
        else:
            val_str = str(val)
        
        lines.append(f"  {date:<12} {val_str:<20}")
    
    if show_gap_warning and len(quarters_data) > 0:
        lines.append("  " + "-" * 60)
        lines.append("  ⚠ Gap detected - quarters are not consecutive")
    
    lines.append("")
    
    return lines

def check_data_availability(ticker_data, field_name):
    """Check if a field has data and count consecutive quarters."""
    if not ticker_data or 'financials' not in ticker_data:
        return None, "No financials data"
    
    financials = ticker_data['financials']
    if 'quarterly' not in financials:
        return None, "No quarterly data"
    
    quarterly_data = financials['quarterly']
    
    if field_name not in quarterly_data:
        return None, f"Field '{field_name}' not found"
    
    values = quarterly_data[field_name]
    if not isinstance(values, list):
        return None, f"Field '{field_name}' is not a list"
    
    dates = quarterly_data.get('period_end_date', [])
    
    # Count valid values
    valid_count = sum(1 for v in values if v is not None)
    positive_count = sum(1 for v in values if v is not None and (not isinstance(v, (int, float)) or v > 0))
    
    # Check consecutive quarters
    consecutive = get_consecutive_quarters(quarterly_data, field_name, 1)
    consecutive_count = len(consecutive) if consecutive else 0
    
    # Get all valid data points (for display)
    all_valid = [(date, val) for date, val in zip(dates, values) if date is not None and val is not None]
    all_valid.sort(key=lambda x: x[0], reverse=True)  # Most recent first
    
    return {
        'total_values': len(values) if values else 0,
        'valid_values': valid_count,
        'positive_values': positive_count,
        'consecutive_quarters': consecutive_count,
        'all_valid_data': all_valid[:20] if len(all_valid) > 20 else all_valid,  # Show up to 20 most recent
        'consecutive_data': consecutive[:20] if consecutive and len(consecutive) > 20 else (consecutive if consecutive else [])
    }, None

def diagnose_metric(metric_name, calculation_func, ticker_data, required_quarters=None):
    """Try to calculate a metric and diagnose why it fails if it does."""
    try:
        result = calculation_func(ticker_data)
        if result is not None:
            return True, "Success", None
        else:
            # Metric failed - diagnose why
            reasons = []
            
            # Check basic data availability
            if not ticker_data or 'financials' not in ticker_data:
                return False, "Failed", "No financials data in ticker_data"
            
            financials = ticker_data['financials']
            if 'quarterly' not in financials:
                return False, "Failed", "No quarterly data available"
            
            quarterly_data = financials['quarterly']
            
            # Check specific field requirements based on metric name
            metric_lower = metric_name.lower().replace(' ', '_').replace('-', '_')
            
            if 'revenue' in metric_lower:
                data_info, error = check_data_availability(ticker_data, 'revenue')
                if error:
                    reasons.append(error)
                elif data_info:
                    if data_info['consecutive_quarters'] < (required_quarters or 20):
                        reasons.append(f"Only {data_info['consecutive_quarters']} consecutive quarters of revenue data (need {required_quarters or 20})")
                    elif data_info['valid_values'] == 0:
                        reasons.append("No valid revenue data")
            
            if 'share_count' in metric_lower or ('share' in metric_lower and 'growth' in metric_lower):
                for field in ['shares_eop', 'shares_diluted', 'shares_basic']:
                    data_info, error = check_data_availability(ticker_data, field)
                    if not error and data_info and data_info['valid_values'] > 0:
                        if data_info['consecutive_quarters'] < (required_quarters or 20):
                            reasons.append(f"Only {data_info['consecutive_quarters']} consecutive quarters of {field} data (need {required_quarters or 20})")
                        break
                else:
                    reasons.append("No share count data found (checked shares_eop, shares_diluted, shares_basic)")
            
            if 'operating_margin' in metric_lower or ('operating_income' in metric_lower and 'net_debt' not in metric_lower):
                # Check both revenue and operating income
                rev_info, rev_error = check_data_availability(ticker_data, 'revenue')
                op_inc_info, op_inc_error = check_data_availability(ticker_data, 'operating_income')
                
                if 'ttm' in metric_lower:
                    required = 4
                else:
                    required = required_quarters or 20
                
                # Check revenue
                if rev_error:
                    reasons.append(f"Revenue: {rev_error}")
                elif rev_info:
                    if rev_info['consecutive_quarters'] < required:
                        reasons.append(f"Revenue: only {rev_info['consecutive_quarters']} consecutive quarters (need {required})")
                        # Show quarter details if there's a gap (show table if few consecutive quarters)
                        if rev_info.get('consecutive_data') and len(rev_info['consecutive_data']) < 10:
                            table_lines = format_quarter_table(rev_info['consecutive_data'], "Revenue Consecutive Quarters")
                            reasons.extend(table_lines)
                            # Show what comes after (the gap)
                            if rev_info.get('all_valid_data') and len(rev_info['all_valid_data']) > len(rev_info['consecutive_data']):
                                gap_data = rev_info['all_valid_data'][len(rev_info['consecutive_data']):len(rev_info['consecutive_data'])+10]
                                gap_table_lines = format_quarter_table(gap_data, "Next Data Points (Gap Detected)", show_gap_warning=True)
                                reasons.extend(gap_table_lines)
                else:
                    reasons.append("Revenue: no data found")
                
                # Check operating income
                if op_inc_error:
                    reasons.append(f"Operating income: {op_inc_error}")
                elif op_inc_info:
                    if op_inc_info['consecutive_quarters'] < required:
                        reasons.append(f"Operating income: only {op_inc_info['consecutive_quarters']} consecutive quarters (need {required})")
                        # Show quarter details if there's a gap (show table if few consecutive quarters)
                        if op_inc_info.get('consecutive_data') and len(op_inc_info['consecutive_data']) < 10:
                            table_lines = format_quarter_table(op_inc_info['consecutive_data'], "Operating Income Consecutive Quarters")
                            reasons.extend(table_lines)
                            # Show what comes after (the gap)
                            if op_inc_info.get('all_valid_data') and len(op_inc_info['all_valid_data']) > len(op_inc_info['consecutive_data']):
                                gap_data = op_inc_info['all_valid_data'][len(op_inc_info['consecutive_data']):len(op_inc_info['consecutive_data'])+10]
                                gap_table_lines = format_quarter_table(gap_data, "Next Data Points (Gap Detected)", show_gap_warning=True)
                                reasons.extend(gap_table_lines)
                    elif op_inc_info['valid_values'] == 0:
                        reasons.append("Operating income: no valid data")
                else:
                    reasons.append("Operating income: no data found")
                
                # Check if dates match (both fields need matching dates)
                if rev_info and op_inc_info and rev_info['consecutive_quarters'] > 0 and op_inc_info['consecutive_quarters'] > 0:
                    # Get consecutive quarters for both to check matching dates
                    rev_consecutive = get_consecutive_quarters(ticker_data['financials']['quarterly'], 'revenue', 1)
                    op_inc_consecutive = get_consecutive_quarters(ticker_data['financials']['quarterly'], 'operating_income', 1)
                    if rev_consecutive and op_inc_consecutive:
                        rev_dates = {date for date, _ in rev_consecutive[:required]}
                        op_inc_dates = {date for date, _ in op_inc_consecutive[:required]}
                        matching_dates = rev_dates & op_inc_dates
                        if len(matching_dates) < required:
                            reasons.append(f"Only {len(matching_dates)} matching dates between revenue and operating_income (need {required})")
            
            if 'gross_margin' in metric_lower or 'gross_profit' in metric_lower:
                # Check both revenue and gross profit
                rev_info, rev_error = check_data_availability(ticker_data, 'revenue')
                gp_info, gp_error = check_data_availability(ticker_data, 'gross_profit')
                
                required = required_quarters or 20
                
                # Check revenue
                if rev_error:
                    reasons.append(f"Revenue: {rev_error}")
                elif rev_info:
                    if rev_info['consecutive_quarters'] < required:
                        reasons.append(f"Revenue: only {rev_info['consecutive_quarters']} consecutive quarters (need {required})")
                        # Show quarter details if there's a gap (show table if few consecutive quarters)
                        if rev_info.get('consecutive_data') and len(rev_info['consecutive_data']) < 10:
                            table_lines = format_quarter_table(rev_info['consecutive_data'], "Revenue Consecutive Quarters")
                            reasons.extend(table_lines)
                            # Show what comes after (the gap)
                            if rev_info.get('all_valid_data') and len(rev_info['all_valid_data']) > len(rev_info['consecutive_data']):
                                gap_data = rev_info['all_valid_data'][len(rev_info['consecutive_data']):len(rev_info['consecutive_data'])+10]
                                gap_table_lines = format_quarter_table(gap_data, "Next Data Points (Gap Detected)", show_gap_warning=True)
                                reasons.extend(gap_table_lines)
                else:
                    reasons.append("Revenue: no data found")
                
                # Check gross profit
                if gp_error:
                    reasons.append(f"Gross profit: {gp_error}")
                elif gp_info:
                    if gp_info['consecutive_quarters'] < required:
                        reasons.append(f"Gross profit: only {gp_info['consecutive_quarters']} consecutive quarters (need {required})")
                        # Show quarter details if there's a gap (show table if few consecutive quarters)
                        if gp_info.get('consecutive_data') and len(gp_info['consecutive_data']) < 10:
                            table_lines = format_quarter_table(gp_info['consecutive_data'], "Gross Profit Consecutive Quarters")
                            reasons.extend(table_lines)
                            # Show what comes after (the gap)
                            if gp_info.get('all_valid_data') and len(gp_info['all_valid_data']) > len(gp_info['consecutive_data']):
                                gap_data = gp_info['all_valid_data'][len(gp_info['consecutive_data']):len(gp_info['consecutive_data'])+10]
                                gap_table_lines = format_quarter_table(gap_data, "Next Data Points (Gap Detected)", show_gap_warning=True)
                                reasons.extend(gap_table_lines)
                    elif gp_info['valid_values'] == 0:
                        reasons.append("Gross profit: no valid data")
                else:
                    reasons.append("Gross profit: no data found")
                
                # Check if dates match (both fields need matching dates)
                if rev_info and gp_info and rev_info['consecutive_quarters'] > 0 and gp_info['consecutive_quarters'] > 0:
                    # Get consecutive quarters for both to check matching dates
                    rev_consecutive = get_consecutive_quarters(ticker_data['financials']['quarterly'], 'revenue', 1)
                    gp_consecutive = get_consecutive_quarters(ticker_data['financials']['quarterly'], 'gross_profit', 1)
                    if rev_consecutive and gp_consecutive:
                        rev_dates = {date for date, _ in rev_consecutive[:required]}
                        gp_dates = {date for date, _ in gp_consecutive[:required]}
                        matching_dates = rev_dates & gp_dates
                        if len(matching_dates) < required:
                            reasons.append(f"Only {len(matching_dates)} matching dates between revenue and gross_profit (need {required})")
            
            if 'ppe' in metric_lower or 'ebit_ppe' in metric_lower:
                # Check both operating income and PPE
                op_inc_info, op_inc_error = check_data_availability(ticker_data, 'operating_income')
                if op_inc_error:
                    reasons.append(f"Operating income: {op_inc_error}")
                elif op_inc_info:
                    if op_inc_info['consecutive_quarters'] < 4:
                        reasons.append(f"Only {op_inc_info['consecutive_quarters']} consecutive quarters of operating_income data (need 4 for TTM)")
                
                ppe_found = False
                ppe_field_used = None
                for field in ['ppe_net', 'ppe', 'property_plant_equipment', 'net_ppe', 'fixed_assets']:
                    data_info, error = check_data_availability(ticker_data, field)
                    if not error and data_info and data_info['valid_values'] > 0:
                        ppe_found = True
                        ppe_field_used = field
                        if data_info['consecutive_quarters'] < 1:
                            reasons.append(f"PPE field '{field}' found but has no consecutive quarters")
                        break
                if not ppe_found:
                    reasons.append("No PPE data found (checked ppe_net, ppe, property_plant_equipment, net_ppe, fixed_assets)")
            
            if 'net_debt' in metric_lower:
                op_inc_info, op_inc_error = check_data_availability(ticker_data, 'operating_income')
                net_debt_info, net_debt_error = check_data_availability(ticker_data, 'net_debt')
                
                if op_inc_error:
                    reasons.append(f"Operating income: {op_inc_error}")
                elif op_inc_info:
                    if op_inc_info['consecutive_quarters'] < 4:
                        reasons.append(f"Operating income: only {op_inc_info['consecutive_quarters']} consecutive quarters (need 4 for TTM)")
                    elif op_inc_info['valid_values'] == 0:
                        reasons.append("Operating income: no valid data")
                else:
                    reasons.append("Operating income: no data found")
                
                if net_debt_error:
                    reasons.append(f"Net debt: {net_debt_error}")
                elif net_debt_info:
                    if net_debt_info['valid_values'] == 0:
                        reasons.append("Net debt: no valid data")
                else:
                    reasons.append("Net debt: no data found")
            
            if 'total_past_return' in metric_lower or ('total_return' in metric_lower and 'past' in metric_lower):
                data_info, error = check_data_availability(ticker_data, 'period_end_price')
                if error:
                    reasons.append(error)
                elif data_info:
                    if data_info['consecutive_quarters'] < 2:
                        reasons.append(f"Only {data_info['consecutive_quarters']} consecutive quarters of price data (need at least 2)")
                
                # Check dividends (optional but good to know)
                div_info, _ = check_data_availability(ticker_data, 'dividends')
                if div_info and div_info['valid_values'] == 0:
                    reasons.append("Note: No dividend data (this is okay, metric can still calculate)")
            
            # If no reasons found, try to provide a general diagnosis
            if not reasons:
                # Check if it's a general data availability issue
                if not ticker_data or 'financials' not in ticker_data:
                    reasons.append("No financials data available")
                elif 'quarterly' not in ticker_data['financials']:
                    reasons.append("No quarterly data available")
                else:
                    reasons.append("Unknown reason - calculation returned None (check data availability summary below)")
            
            # Join reasons - if any are lists (tables), flatten them
            flat_reasons = []
            for r in reasons:
                if isinstance(r, list):
                    flat_reasons.extend(r)
                else:
                    flat_reasons.append(r)
            
            return False, "Failed", flat_reasons
    
    except Exception as e:
        return False, "Error", f"Exception occurred: {str(e)}"

def main():
    """Main function to diagnose metrics for a ticker."""
    print("=" * 80)
    print("QuickFS Metrics Diagnostic Tool")
    print("=" * 80)
    print()
    
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
            print(f"✗ No QuickFS data found for {ticker}")
            print("  Make sure the ticker exists in the database.")
            continue
        
        print(f"✓ Data loaded for {ticker}")
        print()
        
        # Define all metrics to check
        metrics_to_check = [
            ("5-Year Revenue CAGR", calculate_5y_revenue_growth, 20),
            ("5-Year Halfway Revenue Growth", calculate_5y_halfway_revenue_growth, 20),
            ("Revenue Growth Consistency", calculate_consistency_of_growth, 20),
            ("Revenue Growth Acceleration", calculate_acceleration_of_growth, 21),
            ("Operating Margin Growth", calculate_operating_margin_growth, 20),
            ("Gross Margin Growth", calculate_gross_margin_growth, 20),
            ("Operating Margin Consistency", calculate_operating_margin_consistency, 20),
            ("Gross Margin Consistency", calculate_gross_margin_consistency, 20),
            ("Share Count Halfway Growth", calculate_halfway_share_count_growth, 20),
            ("TTM EBIT/PPE", calculate_ttm_ebit_ppe, 4),
            ("Net Debt to TTM Operating Income", calculate_net_debt_to_ttm_operating_income, 4),
            ("Total Past Return", calculate_total_past_return, 2),
        ]
        
        print("=" * 80)
        print(f"METRIC DIAGNOSTICS FOR {ticker}")
        print("=" * 80)
        print()
        
        success_count = 0
        fail_count = 0
        
        for metric_name, calc_func, required_q in metrics_to_check:
            success, status, reason = diagnose_metric(metric_name, calc_func, ticker_data, required_q)
            
            if success:
                print(f"✓ {metric_name}: SUCCESS")
                success_count += 1
            else:
                print(f"✗ {metric_name}: {status}")
                if reason:
                    # If reason contains multiple lines (tables), print each line separately
                    if isinstance(reason, list):
                        for line in reason:
                            print(line)
                    else:
                        # Split by semicolon but preserve table formatting
                        if ";   " in reason and "------------------------------------------------------------" in reason:
                            # This is a table format - split and print properly
                            parts = reason.split(";   ")
                            for part in parts:
                                if part.strip():
                                    print(f"  {part}")
                        else:
                            # Regular reason - split by semicolon
                            for part in reason.split("; "):
                                if part.strip():
                                    print(f"  {part}")
                fail_count += 1
        
        print()
        print("=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total metrics: {len(metrics_to_check)}")
        print(f"Successful: {success_count}")
        print(f"Failed: {fail_count}")
        print()
        
        # Show data availability summary
        print("=" * 80)
        print("DATA AVAILABILITY SUMMARY")
        print("=" * 80)
        print()
        
        if ticker_data and 'financials' in ticker_data and 'quarterly' in ticker_data['financials']:
            quarterly = ticker_data['financials']['quarterly']
            
            key_fields = [
                'revenue', 'operating_income', 'gross_profit', 
                'shares_eop', 'ppe_net', 'net_debt', 'period_end_price', 'dividends'
            ]
            
            print(f"{'Field':<25} {'Total':<10} {'Valid':<10} {'Consecutive Q':<15}")
            print("-" * 80)
            
            for field in key_fields:
                data_info, error = check_data_availability(ticker_data, field)
                if error:
                    print(f"{field:<25} {'N/A':<10} {'N/A':<10} {'N/A':<15}")
                elif data_info:
                    print(f"{field:<25} {data_info['total_values']:<10} {data_info['valid_values']:<10} {data_info['consecutive_quarters']:<15}")
                else:
                    print(f"{field:<25} {'Not found':<10} {'-':<10} {'-':<15}")
        
        print()

if __name__ == '__main__':
    main()

