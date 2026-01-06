#!/usr/bin/env python3
"""
Fill null values in dataroma_metrics database by re-scraping tickers.
"""

import sys
import os
import sqlite3
import time
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataroma.get_one import scrape_dataroma_stock

# Database path
METRICS_DB = os.path.join(os.path.dirname(__file__), 'metrics.db')


def get_available_metrics():
    """Get list of available metric columns from the database."""
    if not os.path.exists(METRICS_DB):
        print(f"Error: {METRICS_DB} not found")
        return []
    
    conn = sqlite3.connect(METRICS_DB)
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dataroma_metrics'")
    if not cursor.fetchone():
        conn.close()
        print(f"Error: 'dataroma_metrics' table not found in {METRICS_DB}")
        return []
    
    # Get column names
    cursor.execute("PRAGMA table_info(dataroma_metrics)")
    columns = cursor.fetchall()
    conn.close()
    
    # Filter out non-metric columns (id, ticker, error, scraped_at, company_name)
    exclude_cols = {'id', 'ticker', 'error', 'scraped_at', 'company_name'}
    metrics = [col[1] for col in columns if col[1] not in exclude_cols]
    
    return metrics


def get_tickers_with_null_metric(metric_name):
    """Get all tickers that have null values for the specified metric."""
    if not os.path.exists(METRICS_DB):
        return []
    
    conn = sqlite3.connect(METRICS_DB)
    cursor = conn.cursor()
    
    # Get the latest record for each ticker with null in the specified metric
    cursor.execute(f'''
        SELECT DISTINCT ticker
        FROM dataroma_metrics d1
        WHERE {metric_name} IS NULL
        AND scraped_at = (
            SELECT MAX(scraped_at)
            FROM dataroma_metrics d2
            WHERE d2.ticker = d1.ticker
        )
        ORDER BY ticker
    ''')
    
    tickers = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    return tickers


def update_metric_for_ticker(ticker, metric_name, new_value):
    """Update the specified metric for a ticker in the latest record."""
    conn = sqlite3.connect(METRICS_DB)
    cursor = conn.cursor()
    
    # Get the latest record ID for this ticker
    cursor.execute('''
        SELECT id FROM dataroma_metrics
        WHERE ticker = ?
        AND scraped_at = (
            SELECT MAX(scraped_at)
            FROM dataroma_metrics
            WHERE ticker = ?
        )
    ''', (ticker, ticker))
    
    result = cursor.fetchone()
    if result:
        record_id = result[0]
        # Update the metric
        cursor.execute(f'''
            UPDATE dataroma_metrics
            SET {metric_name} = ?
            WHERE id = ?
        ''', (new_value, record_id))
        conn.commit()
        conn.close()
        return True
    
    conn.close()
    return False


def main():
    """Main function to fill null values."""
    print("=" * 80)
    print("FILL NULL VALUES IN DATAROMA METRICS")
    print("=" * 80)
    print()
    
    # Get available metrics
    metrics = get_available_metrics()
    
    if not metrics:
        print("No metrics found in database.")
        return
    
    # Display available metrics
    print("Available metrics:")
    for i, metric in enumerate(metrics, 1):
        # Count how many nulls exist for this metric
        conn = sqlite3.connect(METRICS_DB)
        cursor = conn.cursor()
        cursor.execute(f'''
            SELECT COUNT(DISTINCT ticker)
            FROM dataroma_metrics d1
            WHERE {metric} IS NULL
            AND scraped_at = (
                SELECT MAX(scraped_at)
                FROM dataroma_metrics d2
                WHERE d2.ticker = d1.ticker
            )
        ''')
        null_count = cursor.fetchone()[0]
        conn.close()
        
        print(f"  {i}. {metric} ({null_count} tickers with null values)")
    
    print()
    
    # Ask user to select metric
    while True:
        try:
            choice = input(f"Select metric to fill (1-{len(metrics)}): ").strip()
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(metrics):
                selected_metric = metrics[choice_idx]
                break
            else:
                print(f"Please enter a number between 1 and {len(metrics)}")
        except ValueError:
            print("Please enter a valid number")
        except KeyboardInterrupt:
            print("\nCancelled.")
            return
    
    print()
    print(f"Selected metric: {selected_metric}")
    print()
    
    # Get tickers with null values
    tickers = get_tickers_with_null_metric(selected_metric)
    
    if not tickers:
        print(f"No tickers found with null values for {selected_metric}")
        return
    
    print(f"Found {len(tickers)} tickers with null values for {selected_metric}")
    print(f"Tickers: {', '.join(tickers[:10])}{'...' if len(tickers) > 10 else ''}")
    print()
    
    # Confirm before proceeding
    response = input(f"Re-scrape these {len(tickers)} tickers? (y/n): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
        return
    
    print()
    print("Re-scraping tickers...")
    print()
    
    # Re-scrape and update
    success_count = 0
    error_count = 0
    updated_count = 0
    no_change_count = 0
    
    for i, ticker in enumerate(tickers, 1):
        print(f"[{i}/{len(tickers)}] Processing {ticker}...", end=' ', flush=True)
        
        # Scrape data
        data = scrape_dataroma_stock(ticker)
        
        if data.get('error'):
            print(f"✗ {data['error']}")
            error_count += 1
            continue
        
        # Get the new value for the selected metric
        new_value = data.get(selected_metric)
        
        if new_value is None:
            print("✗ No data found")
            no_change_count += 1
            continue
        
        # Update the database
        if update_metric_for_ticker(ticker, selected_metric, new_value):
            print(f"✓ Updated {selected_metric} = {new_value}")
            updated_count += 1
            success_count += 1
        else:
            print("✗ Update failed")
            error_count += 1
        
        # Rate limiting
        time.sleep(0.5)
    
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Successfully updated: {updated_count}")
    print(f"Errors: {error_count}")
    print(f"No change (still null): {no_change_count}")
    print()


if __name__ == "__main__":
    main()

