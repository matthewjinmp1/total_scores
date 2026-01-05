#!/usr/bin/env python3
"""
Calculate all QuickFS metrics for all stocks and save to database.
"""

import sqlite3
import json
import os
from datetime import datetime
import sys

# Import all calculation functions from get_one.py
# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)

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
    calculate_total_past_return
)

# Database paths
QUICKFS_DB = os.path.join(os.path.dirname(__file__), "data.db")
METRICS_DB = os.path.join(os.path.dirname(__file__), "metrics.db")

def init_metrics_db():
    """Initialize the metrics database with table to store calculated metrics."""
    conn = sqlite3.connect(METRICS_DB)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS quickfs_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            calculated_at TEXT NOT NULL,
            -- Revenue Growth Metrics
            revenue_5y_cagr REAL,
            revenue_5y_halfway_growth REAL,
            revenue_growth_consistency REAL,
            revenue_growth_acceleration REAL,
            -- Margin Metrics
            operating_margin_growth REAL,
            gross_margin_growth REAL,
            operating_margin_consistency REAL,
            gross_margin_consistency REAL,
            -- Share Count Metrics
            share_count_halfway_growth REAL,
            -- Return on Capital Metrics
            ttm_ebit_ppe REAL,
            -- Debt Metrics
            net_debt_to_ttm_operating_income REAL,
            -- Return Metrics
            total_past_return REAL,
            total_past_return_multiplier REAL,
            -- Error tracking
            error TEXT,
            UNIQUE(ticker, calculated_at)
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker ON quickfs_metrics(ticker)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_calculated_at ON quickfs_metrics(calculated_at)')
    
    conn.commit()
    conn.close()
    print(f"Initialized metrics database: {METRICS_DB}")

def get_all_tickers():
    """Get all unique tickers from QuickFS database that have data."""
    if not os.path.exists(QUICKFS_DB):
        print(f"Error: Database not found at {QUICKFS_DB}")
        return []
    
    conn = sqlite3.connect(QUICKFS_DB)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT DISTINCT ticker FROM quickfs_data 
            WHERE data_type = 'full'
            ORDER BY ticker
        ''')
        tickers = [row[0] for row in cursor.fetchall()]
        return tickers
    finally:
        conn.close()

def calculate_all_metrics_for_ticker(ticker):
    """Calculate all metrics for a single ticker."""
    ticker_data = get_ticker_data(ticker)
    
    if not ticker_data:
        return None, f"No data found for {ticker}"
    
    metrics = {
        'ticker': ticker,
        'calculated_at': datetime.now().isoformat()
    }
    errors = []
    
    try:
        # Revenue Growth Metrics
        result = calculate_5y_revenue_growth(ticker_data)
        if result:
            growth_rate, _, _, _, _, _ = result
            metrics['revenue_5y_cagr'] = growth_rate
        else:
            errors.append("revenue_5y_cagr")
        
        result = calculate_5y_halfway_revenue_growth(ticker_data)
        if result:
            growth_ratio, _, _, _, _, _ = result
            metrics['revenue_5y_halfway_growth'] = growth_ratio
        else:
            errors.append("revenue_5y_halfway_growth")
        
        result = calculate_consistency_of_growth(ticker_data)
        if result:
            stdev, _, _ = result
            metrics['revenue_growth_consistency'] = stdev
        else:
            errors.append("revenue_growth_consistency")
        
        result = calculate_acceleration_of_growth(ticker_data)
        if result:
            acceleration, _, _, _, _, _, _ = result
            metrics['revenue_growth_acceleration'] = acceleration
        else:
            errors.append("revenue_growth_acceleration")
        
        # Margin Metrics
        result = calculate_operating_margin_growth(ticker_data)
        if result:
            margin_growth, _, _, _, _, _, _, _ = result
            metrics['operating_margin_growth'] = margin_growth
        else:
            errors.append("operating_margin_growth")
        
        result = calculate_gross_margin_growth(ticker_data)
        if result:
            margin_growth, _, _, _, _, _, _, _ = result
            metrics['gross_margin_growth'] = margin_growth
        else:
            errors.append("gross_margin_growth")
        
        result = calculate_operating_margin_consistency(ticker_data)
        if result:
            stdev, _, _ = result
            metrics['operating_margin_consistency'] = stdev
        else:
            errors.append("operating_margin_consistency")
        
        result = calculate_gross_margin_consistency(ticker_data)
        if result:
            stdev, _, _ = result
            metrics['gross_margin_consistency'] = stdev
        else:
            errors.append("gross_margin_consistency")
        
        # Share Count Metrics
        result = calculate_halfway_share_count_growth(ticker_data)
        if result:
            growth_ratio, _, _, _, _, _ = result
            metrics['share_count_halfway_growth'] = growth_ratio
        else:
            errors.append("share_count_halfway_growth")
        
        # Return on Capital Metrics
        result = calculate_ttm_ebit_ppe(ticker_data)
        if result:
            ratio, _, _, _, _ = result
            metrics['ttm_ebit_ppe'] = ratio
        else:
            errors.append("ttm_ebit_ppe")
        
        # Debt Metrics
        result = calculate_net_debt_to_ttm_operating_income(ticker_data)
        if result:
            ratio, _, _, _, _ = result
            metrics['net_debt_to_ttm_operating_income'] = ratio
        else:
            errors.append("net_debt_to_ttm_operating_income")
        
        # Return Metrics
        result = calculate_total_past_return(ticker_data)
        if result:
            total_return, total_return_multiplier, _, _, _, _, _, _ = result
            metrics['total_past_return'] = total_return
            metrics['total_past_return_multiplier'] = total_return_multiplier
        else:
            errors.append("total_past_return")
        
        if errors:
            metrics['error'] = f"Missing: {', '.join(errors)}"
        
        return metrics, None
        
    except Exception as e:
        return None, f"Error calculating metrics: {str(e)}"

def save_metrics(metrics):
    """Save calculated metrics to the database."""
    if not metrics:
        return False
    
    conn = sqlite3.connect(METRICS_DB)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO quickfs_metrics (
                ticker, calculated_at,
                revenue_5y_cagr, revenue_5y_halfway_growth, revenue_growth_consistency, revenue_growth_acceleration,
                operating_margin_growth, gross_margin_growth,
                operating_margin_consistency, gross_margin_consistency,
                share_count_halfway_growth,
                ttm_ebit_ppe,
                net_debt_to_ttm_operating_income,
                total_past_return, total_past_return_multiplier,
                error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            metrics['ticker'],
            metrics['calculated_at'],
            metrics.get('revenue_5y_cagr'),
            metrics.get('revenue_5y_halfway_growth'),
            metrics.get('revenue_growth_consistency'),
            metrics.get('revenue_growth_acceleration'),
            metrics.get('operating_margin_growth'),
            metrics.get('gross_margin_growth'),
            metrics.get('operating_margin_consistency'),
            metrics.get('gross_margin_consistency'),
            metrics.get('share_count_halfway_growth'),
            metrics.get('ttm_ebit_ppe'),
            metrics.get('net_debt_to_ttm_operating_income'),
            metrics.get('total_past_return'),
            metrics.get('total_past_return_multiplier'),
            metrics.get('error')
        ))
        
        conn.commit()
        return True
    except Exception as e:
        print(f"Error saving metrics for {metrics['ticker']}: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def main():
    """Main function to calculate metrics for all tickers."""
    print("=" * 80)
    print("QuickFS Metrics Calculator - All Stocks")
    print("=" * 80)
    print()
    
    # Initialize database
    init_metrics_db()
    
    # Get all tickers
    tickers = get_all_tickers()
    
    if not tickers:
        print("No tickers found in database.")
        return
    
    print(f"Found {len(tickers)} tickers")
    print(f"Tickers: {', '.join(tickers[:10])}{'...' if len(tickers) > 10 else ''}")
    print()
    print("Starting metric calculations...")
    print("-" * 80)
    
    success_count = 0
    error_count = 0
    skip_count = 0
    
    # Track failures
    companies_with_failures = []  # List of (ticker, failed_metrics_list)
    metric_failure_counts = {}  # metric_name -> count
    
    # Define all metric names for tracking
    all_metric_names = [
        'revenue_5y_cagr',
        'revenue_5y_halfway_growth',
        'revenue_growth_consistency',
        'revenue_growth_acceleration',
        'operating_margin_growth',
        'gross_margin_growth',
        'operating_margin_consistency',
        'gross_margin_consistency',
        'share_count_halfway_growth',
        'ttm_ebit_ppe',
        'net_debt_to_ttm_operating_income',
        'total_past_return'
    ]
    
    # Initialize failure counts
    for metric_name in all_metric_names:
        metric_failure_counts[metric_name] = 0
    
    for i, ticker in enumerate(tickers, 1):
        print(f"[{i}/{len(tickers)}] Processing {ticker}...", end=' ')
        
        metrics, error = calculate_all_metrics_for_ticker(ticker)
        
        if error and not metrics:
            print(f"✗ {error}")
            error_count += 1
            continue
        
        if not metrics:
            print("✗ No data")
            skip_count += 1
            continue
        
        # Track failures
        failed_metrics = []
        
        # Check for None values in metrics (failed calculations)
        for metric_name in all_metric_names:
            if metric_name not in metrics or metrics[metric_name] is None:
                failed_metrics.append(metric_name)
                metric_failure_counts[metric_name] += 1
        
        # Track companies with failures
        if failed_metrics:
            companies_with_failures.append((ticker, failed_metrics))
        
        # Save metrics
        if save_metrics(metrics):
            error_msg = f" ({metrics.get('error', '')})" if metrics.get('error') else ""
            print(f"✓ Saved{error_msg}")
            success_count += 1
        else:
            print("✗ Save failed")
            error_count += 1
    
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total tickers: {len(tickers)}")
    print(f"Successfully calculated and saved: {success_count}")
    print(f"Errors: {error_count}")
    print(f"Skipped (no data): {skip_count}")
    print()
    print(f"Metrics saved to: {METRICS_DB}")
    
    # Display failure statistics
    if companies_with_failures or any(count > 0 for count in metric_failure_counts.values()):
        print()
        print("=" * 80)
        print("FAILURE STATISTICS")
        print("=" * 80)
        
        # Show which metrics failed the most
        print("\nMetric Failure Counts (sorted by frequency):")
        print("-" * 80)
        sorted_failures = sorted(metric_failure_counts.items(), key=lambda x: x[1], reverse=True)
        
        # Format metric names for display
        metric_display_names = {
            'revenue_5y_cagr': '5-Year Revenue CAGR',
            'revenue_5y_halfway_growth': '5-Year Halfway Revenue Growth',
            'revenue_growth_consistency': 'Revenue Growth Consistency',
            'revenue_growth_acceleration': 'Revenue Growth Acceleration',
            'operating_margin_growth': 'Operating Margin Growth',
            'gross_margin_growth': 'Gross Margin Growth',
            'operating_margin_consistency': 'Operating Margin Consistency',
            'gross_margin_consistency': 'Gross Margin Consistency',
            'share_count_halfway_growth': 'Share Count Halfway Growth',
            'ttm_ebit_ppe': 'TTM EBIT/PPE',
            'net_debt_to_ttm_operating_income': 'Net Debt to TTM Operating Income',
            'total_past_return': 'Total Past Return'
        }
        
        print(f"{'Metric':<45} {'Failures':<10} {'% of Total':<12}")
        print("-" * 80)
        
        total_companies = len(tickers)
        for metric_name, count in sorted_failures:
            if count > 0:
                display_name = metric_display_names.get(metric_name, metric_name)
                percentage = (count / total_companies * 100) if total_companies > 0 else 0
                print(f"{display_name:<45} {count:<10} {percentage:.1f}%")
        
        # Show companies with failures
        if companies_with_failures:
            print()
            print(f"Companies with at least one failing metric ({len(companies_with_failures)} total):")
            print("-" * 80)
            
            # Sort by number of failures (most failures first)
            companies_with_failures.sort(key=lambda x: len(x[1]), reverse=True)
            
            print(f"{'Ticker':<10} {'# Failed':<10} {'Failed Metrics'}")
            print("-" * 80)
            
            for ticker, failed_metrics in companies_with_failures:
                metric_names_short = [metric_display_names.get(m, m) for m in failed_metrics]
                # Truncate if too long
                metrics_str = ', '.join(metric_names_short)
                if len(metrics_str) > 65:
                    metrics_str = metrics_str[:62] + "..."
                print(f"{ticker:<10} {len(failed_metrics):<10} {metrics_str}")
        
        print()

if __name__ == '__main__':
    main()

