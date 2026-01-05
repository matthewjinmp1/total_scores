#!/usr/bin/env python3
"""
Master script to recalculate all metrics and final scores.
This script runs all metric calculations in the correct order using existing data.

Order of operations:
1. Calculate QuickFS metrics (from quickfs/data.db -> quickfs/metrics.db)
2. Calculate final composite scores (from all sources -> all_scores.db)

This script uses existing data and does NOT fetch new data from APIs.

Usage:
    python3 recalculate_all_metrics.py              # Non-interactive mode (runs automatically, default)
    python3 recalculate_all_metrics.py --prompt     # Interactive mode (prompts for confirmation)
    python3 recalculate_all_metrics.py -p           # Interactive mode (prompts for confirmation)
    python3 recalculate_all_metrics.py --interactive # Interactive mode (prompts for confirmation)
"""

import sys
import os
import subprocess

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def run_quickfs_calculations(skip_prompt=False):
    """Run QuickFS metric calculations."""
    print("=" * 80)
    print("STEP 1: Calculating QuickFS Metrics")
    print("=" * 80)
    print()
    
    # Import and run QuickFS calculations
    quickfs_dir = os.path.join(os.path.dirname(__file__), 'quickfs')
    sys.path.insert(0, quickfs_dir)
    
    try:
        from calculate_all_metrics import (
            init_metrics_db,
            get_all_tickers,
            calculate_all_metrics_for_ticker,
            save_metrics,
            METRICS_DB
        )
        
        # Initialize database
        init_metrics_db()
        
        # Get all tickers
        tickers = get_all_tickers()
        
        if not tickers:
            print("⚠ No tickers found in QuickFS database.")
            print("   Make sure quickfs/data.db exists and has data.")
            return False
        
        print(f"Found {len(tickers)} tickers to process")
        print(f"Tickers: {', '.join(tickers[:10])}{'...' if len(tickers) > 10 else ''}")
        print()
        
        if not skip_prompt:
            try:
                response = input(f"Calculate metrics for all {len(tickers)} tickers? (y/n): ").strip().lower()
                if response != 'y':
                    print("Cancelled.")
                    return False
            except (EOFError, KeyboardInterrupt):
                print("\nCancelled.")
                return False
        
        print()
        print("Starting QuickFS metric calculations...")
        print("-" * 80)
        
        success_count = 0
        error_count = 0
        skip_count = 0
        
        # Track failures
        companies_with_failures = []
        metric_failure_counts = {}
        
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
            print(f"[{i}/{len(tickers)}] Processing {ticker}...", end=' ', flush=True)
            
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
            for metric_name in all_metric_names:
                if metric_name not in metrics or metrics[metric_name] is None:
                    failed_metrics.append(metric_name)
                    metric_failure_counts[metric_name] += 1
            
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
        print("-" * 80)
        print(f"QuickFS Metrics Summary:")
        print(f"  Successfully calculated: {success_count}")
        print(f"  Errors: {error_count}")
        print(f"  Skipped (no data): {skip_count}")
        print(f"  Metrics saved to: {METRICS_DB}")
        
        if companies_with_failures:
            print(f"\n  ⚠ {len(companies_with_failures)} companies had at least one failing metric")
        
        print()
        return True
        
    except ImportError as e:
        print(f"❌ Error importing QuickFS calculation module: {e}")
        return False
    except Exception as e:
        print(f"❌ Error during QuickFS calculations: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_total_scores_calculation():
    """Run final composite score calculations."""
    print("=" * 80)
    print("STEP 2: Calculating Final Composite Scores")
    print("=" * 80)
    print()
    
    try:
        import sqlite3
        
        # Check if ai_scores.db exists and has a scores table
        ai_scores_db = os.path.join(os.path.dirname(__file__), 'ai_scores.db')
        if os.path.exists(ai_scores_db):
            conn_check = sqlite3.connect(ai_scores_db)
            cursor_check = conn_check.cursor()
            cursor_check.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='scores'")
            table_exists = cursor_check.fetchone() is not None
            conn_check.close()
            
            if not table_exists:
                print("⚠ ai_scores.db exists but does not have a 'scores' table.")
                print("   Skipping total scores calculation.")
                print("   The scores table needs to be created and populated first.")
                return False
        
        from calculate_total_scores import (
            get_overlapping_companies,
            get_ai_score_columns,
            normalize_ai_scores,
            calculate_total_scores,
            display_results,
            save_results
        )
        
        print("Loading data from databases...")
        
        # Get overlapping companies
        df = get_overlapping_companies()
        
        if df is None or len(df) == 0:
            print("⚠ No overlapping companies found between AI scores and Finviz databases.")
            print("   Make sure both ai_scores.db and finviz/finviz.db exist and have data.")
            return False
        
        print(f"Found {len(df)} overlapping companies")
        
        # Get all score columns
        score_columns = get_ai_score_columns()
        print(f"Found {len(score_columns)} AI score metrics")
        
        # Normalize AI scores
        print("\nNormalizing AI scores...")
        df_normalized = normalize_ai_scores(df, score_columns)
        
        # Calculate composite scores
        print("Calculating composite scores...")
        df_scores = calculate_total_scores(df_normalized, score_columns)
        
        # Display results
        print("\n")
        df_display = display_results(df_scores)
        
        # Save results
        output_file = save_results(df_scores)
        
        print(f"\n✓ Final scores saved to: {output_file}")
        print()
        return True
        
    except ImportError as e:
        print(f"❌ Error importing total scores calculation module: {e}")
        return False
    except Exception as e:
        print(f"❌ Error during total scores calculation: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main function to run all calculations."""
    print("=" * 80)
    print("METRIC RECALCULATION MASTER SCRIPT")
    print("=" * 80)
    print()
    print("This script will:")
    print("  1. Recalculate all QuickFS metrics from existing data")
    print("  2. Recalculate final composite scores")
    print()
    print("Note: This uses existing data and does NOT fetch new data from APIs.")
    print()
    
    # Check for required databases
    quickfs_data_db = os.path.join(os.path.dirname(__file__), 'quickfs', 'data.db')
    ai_scores_db = os.path.join(os.path.dirname(__file__), 'ai_scores.db')
    finviz_db = os.path.join(os.path.dirname(__file__), 'finviz', 'finviz.db')
    
    missing_dbs = []
    if not os.path.exists(quickfs_data_db):
        missing_dbs.append(f"  - {quickfs_data_db}")
    if not os.path.exists(ai_scores_db):
        missing_dbs.append(f"  - {ai_scores_db}")
    if not os.path.exists(finviz_db):
        missing_dbs.append(f"  - {finviz_db}")
    
    # Default to skip prompts (run automatically)
    # Can override with --prompt, -p, or --interactive flags if needed
    skip_prompt = not ('--prompt' in sys.argv or '-p' in sys.argv or '--interactive' in sys.argv)
    
    if missing_dbs:
        print("⚠ Warning: The following databases are missing:")
        for db in missing_dbs:
            print(db)
        print()
        if not skip_prompt:
            try:
                response = input("Continue anyway? (y/n): ").strip().lower()
                if response != 'y':
                    print("Cancelled.")
                    return
            except (EOFError, KeyboardInterrupt):
                print("\nCancelled.")
                return
        else:
            print("Continuing automatically...")
            print()
    
    print()
    
    # Step 1: QuickFS metrics
    quickfs_success = run_quickfs_calculations(skip_prompt=skip_prompt)
    
    if not quickfs_success:
        print("\n⚠ QuickFS calculations failed or were skipped.")
        if not skip_prompt:
            try:
                response = input("Continue with total scores calculation anyway? (y/n): ").strip().lower()
                if response != 'y':
                    print("Cancelled.")
                    return
            except (EOFError, KeyboardInterrupt):
                print("\nCancelled.")
                return
        else:
            print("Continuing with total scores calculation...")
            print()
    
    print()
    
    # Step 2: Total scores
    total_scores_success = run_total_scores_calculation()
    
    print()
    print("=" * 80)
    print("RECALCULATION COMPLETE")
    print("=" * 80)
    
    if quickfs_success and total_scores_success:
        print("✓ All calculations completed successfully!")
    elif quickfs_success:
        print("⚠ QuickFS calculations completed, but total scores calculation failed.")
    elif total_scores_success:
        print("⚠ Total scores calculation completed, but QuickFS calculations failed.")
    else:
        print("❌ Some calculations failed. Check the output above for details.")
    
    print()


if __name__ == "__main__":
    main()

