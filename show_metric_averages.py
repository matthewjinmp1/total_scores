#!/usr/bin/env python3
"""
Display the average percentile for each metric in the all_scores database.
"""

import sqlite3
import os
import pandas as pd

# Database path
ALL_SCORES_DB = os.path.join(os.path.dirname(__file__), "all_scores.db")

def format_metric_name(metric_name):
    """Format metric name for display."""
    name = metric_name.replace('_normalized', '').replace('_percentile', '').replace('_', ' ')
    return name.title()

def is_reverse_metric(metric_name):
    """Check if a metric is a reverse metric (lower is better)."""
    base_name = metric_name.replace('_normalized', '').replace('_percentile', '')
    
    AI_REVERSE_METRICS = [
        'disruption_risk', 'riskiness_score', 'competition_intensity',
        'bargaining_power_of_customers', 'bargaining_power_of_suppliers',
        'size_well_known_score'
    ]
    
    FINVIZ_REVERSE_METRICS = ['short_interest_percent', 'forward_pe', 'recommendation']
    
    if base_name in AI_REVERSE_METRICS:
        return True
    if base_name in FINVIZ_REVERSE_METRICS or base_name == 'recommendation_score':
        return True
    
    return False

def main():
    """Main function."""
    print("=" * 100)
    print("METRIC PERCENTILE AVERAGES")
    print("=" * 100)
    print()
    
    if not os.path.exists(ALL_SCORES_DB):
        print(f"Error: Database not found: {ALL_SCORES_DB}")
        print("Please run calculate_total_scores.py first to generate the database.")
        return
    
    # Connect to database
    conn = sqlite3.connect(ALL_SCORES_DB)
    cursor = conn.cursor()
    
    # Get all columns from all_scores table
    cursor.execute("PRAGMA table_info(all_scores)")
    columns = cursor.fetchall()
    
    # Filter to only metric columns (normalized or percentile)
    metric_columns = []
    for col in columns:
        col_name = col[1]
        if ('_normalized' in col_name or '_percentile' in col_name) and col_name not in ['total_score']:
            metric_columns.append(col_name)
    
    if not metric_columns:
        print("No metric columns found in database.")
        conn.close()
        return
    
    # Load data
    query = f"SELECT {', '.join(metric_columns)} FROM all_scores"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Calculate averages
    results = []
    for metric in sorted(metric_columns):
        if metric not in df.columns:
            continue
        
        avg_percentile = df[metric].mean() * 100
        deviation = abs(avg_percentile - 50.0)
        
        results.append({
            'metric': metric,
            'display_name': format_metric_name(metric),
            'average': avg_percentile,
            'deviation': deviation,
            'is_reverse': is_reverse_metric(metric)
        })
    
    # Separate AI and Finviz metrics
    ai_metrics = [r for r in results if '_normalized' in r['metric']]
    finviz_metrics = [r for r in results if '_percentile' in r['metric']]
    
    # Display results
    print(f"Total metrics: {len(results)}")
    print(f"AI Score Metrics: {len(ai_metrics)}")
    print(f"Finviz Metrics: {len(finviz_metrics)}")
    print()
    print("=" * 100)
    print(f"{'Metric':<45} {'Average %':<12} {'Deviation':<12} {'Type':<15}")
    print("=" * 100)
    
    print("\nAI Score Metrics:")
    print("-" * 100)
    for result in ai_metrics:
        metric_type = "reverse" if result['is_reverse'] else "normal"
        status = "✓" if result['deviation'] <= 5.0 else "✗"
        print(f"{status} {result['display_name']:<42} {result['average']:>6.2f}%     {result['deviation']:>6.2f}%     {metric_type:<15}")
    
    print("\nFinviz Metrics:")
    print("-" * 100)
    for result in finviz_metrics:
        metric_type = "reverse" if result['is_reverse'] else "normal"
        status = "✓" if result['deviation'] <= 5.0 else "✗"
        print(f"{status} {result['display_name']:<42} {result['average']:>6.2f}%     {result['deviation']:>6.2f}%     {metric_type:<15}")
    
    print()
    print("=" * 100)
    
    # Summary statistics
    all_deviations = [r['deviation'] for r in results]
    max_deviation = max(all_deviations)
    min_deviation = min(all_deviations)
    avg_deviation = sum(all_deviations) / len(all_deviations)
    
    print("\nSummary:")
    print(f"  Average deviation from 50%: {avg_deviation:.2f}%")
    print(f"  Maximum deviation: {max_deviation:.2f}%")
    print(f"  Minimum deviation: {min_deviation:.2f}%")
    
    # Count metrics within tolerance
    tolerance = 5.0
    within_tolerance = sum(1 for r in results if r['deviation'] <= tolerance)
    print(f"\n  Metrics within ±{tolerance}% of 50%: {within_tolerance}/{len(results)} ({within_tolerance/len(results)*100:.1f}%)")
    
    print()

if __name__ == '__main__':
    main()

