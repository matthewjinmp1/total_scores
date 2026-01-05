#!/usr/bin/env python3
"""
Interactive tool to view stock rankings by individual metrics.
Shows percentile and raw values for each metric.
"""

import sqlite3
import os
import pandas as pd

# Reverse metrics (lower is better)
AI_REVERSE_METRICS = [
    'disruption_risk',
    'riskiness_score',
    'competition_intensity',
    'bargaining_power_of_customers',
    'bargaining_power_of_suppliers',
    'size_well_known_score'
]

FINVIZ_REVERSE_METRICS = [
    'short_interest_percent',
    'forward_pe',
    'recommendation'
]

QUICKFS_REVERSE_METRICS = [
    'revenue_growth_consistency',
    'operating_margin_consistency',
    'gross_margin_consistency',
    'share_count_halfway_growth',
    'net_debt_to_ttm_operating_income'
]

# Database paths
ALL_SCORES_DB = os.path.join(os.path.dirname(__file__), "all_scores.db")
AI_SCORES_DB = os.path.join(os.path.dirname(__file__), "ai_scores.db")
FINVIZ_DB = os.path.join(os.path.dirname(__file__), "finviz", "finviz.db")
QUICKFS_METRICS_DB = os.path.join(os.path.dirname(__file__), "quickfs", "metrics.db")

def get_available_metrics():
    """Get all available metrics from the all_scores database and QuickFS metrics."""
    metrics = []
    
    # Get metrics from all_scores database
    if os.path.exists(ALL_SCORES_DB):
        conn = sqlite3.connect(ALL_SCORES_DB)
        cursor = conn.cursor()
        
        # Get all columns from all_scores table
        cursor.execute("PRAGMA table_info(all_scores)")
        columns = cursor.fetchall()
        
        # Filter to only metric columns (normalized or percentile)
        for col in columns:
            col_name = col[1]
            if ('_normalized' in col_name or '_percentile' in col_name) and col_name not in ['total_score']:
                metrics.append(col_name)
        
        conn.close()
    
    # Get QuickFS metrics (need to add suffix for consistency)
    if os.path.exists(QUICKFS_METRICS_DB):
        conn = sqlite3.connect(QUICKFS_METRICS_DB)
        cursor = conn.cursor()
        
        # Get all numeric columns (excluding id, ticker, calculated_at, error)
        cursor.execute("PRAGMA table_info(quickfs_metrics)")
        columns = cursor.fetchall()
        
        quickfs_metric_cols = [
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
        
        # Add QuickFS metrics with _quickfs suffix to distinguish them
        for col in quickfs_metric_cols:
            metrics.append(f"{col}_quickfs")
        
        conn.close()
    
    return sorted(metrics)

def get_raw_value_mapping():
    """Create a mapping of metric names to their raw value column names in source databases."""
    mapping = {}
    
    # AI score mappings (normalized -> raw)
    ai_raw_mapping = {
        'moat_score_normalized': ('ai_scores', 'moat_score'),
        'barriers_score_normalized': ('ai_scores', 'barriers_score'),
        'disruption_risk_normalized': ('ai_scores', 'disruption_risk'),
        'switching_cost_normalized': ('ai_scores', 'switching_cost'),
        'brand_strength_normalized': ('ai_scores', 'brand_strength'),
        'competition_intensity_normalized': ('ai_scores', 'competition_intensity'),
        'network_effect_normalized': ('ai_scores', 'network_effect'),
        'product_differentiation_normalized': ('ai_scores', 'product_differentiation'),
        'innovativeness_score_normalized': ('ai_scores', 'innovativeness_score'),
        'growth_opportunity_normalized': ('ai_scores', 'growth_opportunity'),
        'riskiness_score_normalized': ('ai_scores', 'riskiness_score'),
        'pricing_power_normalized': ('ai_scores', 'pricing_power'),
        'ambition_score_normalized': ('ai_scores', 'ambition_score'),
        'bargaining_power_of_customers_normalized': ('ai_scores', 'bargaining_power_of_customers'),
        'bargaining_power_of_suppliers_normalized': ('ai_scores', 'bargaining_power_of_suppliers'),
        'product_quality_score_normalized': ('ai_scores', 'product_quality_score'),
        'culture_employee_satisfaction_score_normalized': ('ai_scores', 'culture_employee_satisfaction_score'),
        'trailblazer_score_normalized': ('ai_scores', 'trailblazer_score'),
        'management_quality_score_normalized': ('ai_scores', 'management_quality_score'),
        'ai_knowledge_score_normalized': ('ai_scores', 'ai_knowledge_score'),
        'size_well_known_score_normalized': ('ai_scores', 'size_well_known_score'),
        'ethical_healthy_environmental_score_normalized': ('ai_scores', 'ethical_healthy_environmental_score'),
        'long_term_orientation_score_normalized': ('ai_scores', 'long_term_orientation_score'),
        'execution_ability_score_normalized': ('ai_scores', 'execution_ability_score'),
    }
    
    # Finviz mappings (percentile -> raw)
    finviz_raw_mapping = {
        'short_interest_percent_percentile': ('finviz', 'short_interest_percent'),
        'forward_pe_percentile': ('finviz', 'forward_pe'),
        'eps_growth_next_5y_percentile': ('finviz', 'eps_growth_next_5y'),
        'insider_ownership_percentile': ('finviz', 'insider_ownership'),
        'roa_percentile': ('finviz', 'roa'),
        'roic_percentile': ('finviz', 'roic'),
        'gross_margin_percentile': ('finviz', 'gross_margin'),
        'operating_margin_percentile': ('finviz', 'operating_margin'),
        'perf_10y_percentile': ('finviz', 'perf_10y'),
        'recommendation_score_percentile': ('finviz', 'recommendation'),
        'price_move_percent_percentile': ('finviz', 'price_move_percent'),
    }
    
    mapping.update(ai_raw_mapping)
    mapping.update(finviz_raw_mapping)
    
    return mapping

def calculate_percentile_rank(values, reverse=False):
    """Calculate percentile ranks for a series of values."""
    if len(values) == 0:
        return pd.Series([], dtype=float)
    
    # Remove NaN values for ranking
    valid_values = values.dropna()
    if len(valid_values) == 0:
        return pd.Series([None] * len(values), index=values.index)
    
    # Calculate ranks using average method (handles ties correctly)
    if reverse:
        ranks = valid_values.rank(method='average', ascending=True)
    else:
        ranks = valid_values.rank(method='average', ascending=True)
    
    # Convert ranks to percentiles (0-1 scale)
    n_valid = len(valid_values)
    percentiles = ranks / n_valid
    
    if reverse:
        percentiles = 1.0 - percentiles
    
    # Create full series with NaN for missing values
    result = pd.Series([None] * len(values), index=values.index, dtype=float)
    result.loc[valid_values.index] = percentiles
    
    return result

def get_metric_rankings(metric_name):
    """Get rankings for a specific metric with percentile and raw values."""
    # Check if this is a QuickFS metric
    if metric_name.endswith('_quickfs'):
        base_name = metric_name.replace('_quickfs', '')
        
        # Load raw values from QuickFS metrics database
        if not os.path.exists(QUICKFS_METRICS_DB):
            return None
        
        # Get company names from all_scores (if available)
        df_companies = pd.DataFrame(columns=['ticker', 'company_name'])
        if os.path.exists(ALL_SCORES_DB):
            conn_total = sqlite3.connect(ALL_SCORES_DB)
            query = "SELECT ticker, company_name FROM all_scores"
            df_companies = pd.read_sql_query(query, conn_total)
            conn_total.close()
        
        # Load QuickFS metrics (include ALL tickers, even those without values)
        conn_quickfs = sqlite3.connect(QUICKFS_METRICS_DB)
        # Get most recent calculation for each ticker (include NULL values)
        query = f"""
            SELECT ticker, {base_name} as raw_value
            FROM quickfs_metrics
            WHERE id IN (
                SELECT MAX(id) FROM quickfs_metrics GROUP BY ticker
            )
        """
        df_quickfs = pd.read_sql_query(query, conn_quickfs)
        conn_quickfs.close()
        
        # Merge with company names (outer join to include all companies)
        df_total = pd.merge(df_companies, df_quickfs, on='ticker', how='outer')
        
        # Separate stocks with and without values
        df_with_values = df_total[df_total['raw_value'].notna()].copy()
        df_without_values = df_total[df_total['raw_value'].isna()].copy()
        
        if len(df_with_values) == 0:
            # No stocks with values - just return all stocks marked as missing
            df_total['percentile'] = None
            return df_total[['ticker', 'company_name', 'percentile', 'raw_value']]
        
        # Calculate percentiles only for stocks with values
        reverse = is_reverse_metric(metric_name)
        df_with_values['percentile'] = calculate_percentile_rank(df_with_values['raw_value'], reverse=reverse)
        
        # Sort by percentile (descending - higher is better)
        df_with_values = df_with_values.sort_values('percentile', ascending=False, na_position='last')
        
        # Add percentile column to df_without_values (all NaN)
        if len(df_without_values) > 0:
            df_without_values['percentile'] = None
        
        # Combine: stocks with values first, then stocks without values
        if len(df_without_values) > 0:
            df_total = pd.concat([df_with_values, df_without_values], ignore_index=True)
        else:
            df_total = df_with_values
        
        return df_total[['ticker', 'company_name', 'percentile', 'raw_value']]
    
    # Original logic for all_scores metrics
    if not os.path.exists(ALL_SCORES_DB):
        return None
    
    # Load ALL stocks from all_scores (including those without this metric)
    # Note: Using f-string is safe here since metric_name comes from our database schema
    conn_total = sqlite3.connect(ALL_SCORES_DB)
    query = f"SELECT ticker, company_name, {metric_name} as percentile FROM all_scores"
    df_total = pd.read_sql_query(query, conn_total)
    conn_total.close()
    
    # Separate stocks with and without values
    df_with_values = df_total[df_total['percentile'].notna()].copy()
    df_without_values = df_total[df_total['percentile'].isna()].copy()
    
    if len(df_with_values) == 0:
        # No stocks with values - return all as missing
        df_total['raw_value'] = None
        return df_total[['ticker', 'company_name', 'percentile', 'raw_value']]
    
    # Get raw value mapping
    raw_mapping = get_raw_value_mapping()
    
    if metric_name not in raw_mapping:
        # If no raw mapping, just return percentile data
        return df_total[['ticker', 'company_name', 'percentile']]
    
    db_source, raw_col = raw_mapping[metric_name]
    
    # Load raw values
    raw_values = {}
    
    if db_source == 'ai_scores':
        conn_ai = sqlite3.connect(AI_SCORES_DB)
        # Get most recent scores for each ticker
        query = f"""
            SELECT ticker, {raw_col} as raw_value
            FROM scores
            WHERE id IN (
                SELECT MAX(id) FROM scores GROUP BY ticker
            )
        """
        df_raw = pd.read_sql_query(query, conn_ai)
        conn_ai.close()
        raw_values = dict(zip(df_raw['ticker'], df_raw['raw_value']))
    
    elif db_source == 'finviz':
        if os.path.exists(FINVIZ_DB):
            conn_finviz = sqlite3.connect(FINVIZ_DB)
            query = f"SELECT ticker, {raw_col} as raw_value FROM short_interest WHERE {raw_col} IS NOT NULL"
            df_raw = pd.read_sql_query(query, conn_finviz)
            conn_finviz.close()
            raw_values = dict(zip(df_raw['ticker'], df_raw['raw_value']))
    
    # Sort stocks with values by percentile (descending)
    df_with_values = df_with_values.sort_values('percentile', ascending=False, na_position='last')
    
    # Add raw values to stocks with values
    df_with_values['raw_value'] = df_with_values['ticker'].map(raw_values)
    
    # Add raw values to stocks without values (will be NaN/None)
    if len(df_without_values) > 0:
        df_without_values['raw_value'] = None
    
    # Combine: stocks with values first, then stocks without values
    if len(df_without_values) > 0:
        df_total = pd.concat([df_with_values, df_without_values], ignore_index=True)
    else:
        df_total = df_with_values
    
    return df_total[['ticker', 'company_name', 'percentile', 'raw_value']]

def format_metric_name(metric_name):
    """Format metric name for display."""
    name = metric_name.replace('_normalized', '').replace('_percentile', '').replace('_quickfs', '').replace('_', ' ')
    return name.title()

def is_reverse_metric(metric_name):
    """Check if a metric is a reverse metric (lower is better)."""
    # Extract base name without _normalized or _percentile or _quickfs suffix
    base_name = metric_name.replace('_normalized', '').replace('_percentile', '').replace('_quickfs', '')
    
    # Check AI reverse metrics
    if base_name in AI_REVERSE_METRICS:
        return True
    
    # Check Finviz reverse metrics
    if base_name in FINVIZ_REVERSE_METRICS:
        return True
    
    # Check QuickFS reverse metrics
    if base_name in QUICKFS_REVERSE_METRICS:
        return True
    
    # Special case for recommendation_score
    if base_name == 'recommendation_score' or base_name == 'recommendation':
        return True
    
    return False

def display_rankings(df, metric_name):
    """Display rankings in a formatted table."""
    if df is None:
        print(f"No data available for {format_metric_name(metric_name)} (DataFrame is None)")
        return
    if len(df) == 0:
        print(f"No data available for {format_metric_name(metric_name)} (DataFrame is empty)")
        return
    
    # Separate stocks with and without values
    df_with_values = df[df['percentile'].notna()].copy()
    df_without_values = df[df['percentile'].isna()].copy()
    
    # Check if raw_value column exists
    has_raw_value = 'raw_value' in df.columns
    
    print("\n" + "=" * 100)
    metric_display_name = format_metric_name(metric_name).upper()
    reverse_indicator = " (REVERSE)" if is_reverse_metric(metric_name) else ""
    print(f"RANKINGS: {metric_display_name}{reverse_indicator}")
    print("=" * 100)
    if has_raw_value:
        print(f"{'Rank':<6} {'Ticker':<8} {'Company':<35} {'Percentile':<12} {'Raw Value':<15}")
    else:
        print(f"{'Rank':<6} {'Ticker':<8} {'Company':<35} {'Percentile':<12}")
    print("-" * 100)
    
    rank = 1
    
    # Display stocks with values
    for idx, row in df_with_values.iterrows():
        ticker = row['ticker']
        company = row['company_name'] or '-'
        company = company[:35] if len(str(company)) > 35 else company
        
        percentile = row['percentile']
        raw_value = row['raw_value'] if has_raw_value else None
        
        # Format percentile as percentage
        percentile_str = f"{percentile*100:.1f}%"
        
        # Format raw value
        if pd.isna(raw_value):
            raw_str = "N/A"
        elif isinstance(raw_value, (int, float)):
            # Check if it's a recommendation text
            if metric_name == 'recommendation_score_percentile':
                # Convert score back to recommendation text
                rec_map = {1.0: 'Strong Buy', 2.0: 'Buy', 3.0: 'Hold', 4.0: 'Sell', 5.0: 'Strong Sell'}
                raw_str = rec_map.get(raw_value, str(raw_value))
            elif 'forward_pe' in metric_name.lower() or ('_pe' in metric_name.lower() and 'percentile' in metric_name.lower()):
                # Forward P/E is a ratio, not a percentage
                raw_str = f"{raw_value:.2f}"
            elif 'ebit_ppe' in metric_name.lower():
                # EBIT/PPE is a ratio
                raw_str = f"{raw_value:.4f}"
            elif 'net_debt_to_ttm' in metric_name.lower():
                # Net debt ratio
                if raw_value >= 1000:
                    raw_str = f"{raw_value:.0f} (neg income)"
                else:
                    raw_str = f"{raw_value:.4f}"
            elif 'cagr' in metric_name.lower():
                # CAGR as percentage
                raw_str = f"{raw_value * 100:.2f}%"
            elif 'total_past_return' in metric_name.lower():
                # Total return as percentage
                raw_str = f"{raw_value * 100:.2f}%"
            elif 'total_past_return_multiplier' in metric_name.lower():
                # Multiplier
                raw_str = f"{raw_value:.4f}x"
            elif 'growth' in metric_name.lower() and 'consistency' in metric_name.lower():
                # Consistency (standard deviation) as percentage
                raw_str = f"{raw_value * 100:.2f}%"
            elif 'consistency' in metric_name.lower():
                # Margin consistency (standard deviation) as percentage points
                raw_str = f"{raw_value * 100:.2f} pp"
            elif 'growth' in metric_name.lower() and 'acceleration' in metric_name.lower():
                # Acceleration as ratio
                raw_str = f"{raw_value:.4f}x"
            elif 'growth' in metric_name.lower() and 'halfway' in metric_name.lower():
                # Halfway growth as ratio
                raw_str = f"{raw_value:.4f}x"
            elif 'margin_growth' in metric_name.lower():
                # Margin growth as percentage points
                raw_str = f"{raw_value * 100:.2f} pp"
            elif 'percent' in metric_name.lower() or 'margin' in metric_name.lower() or 'roa' in metric_name.lower() or 'roic' in metric_name.lower():
                raw_str = f"{raw_value:.2f}%"
            elif 'price_move' in metric_name.lower():
                raw_str = f"{raw_value:.2f}%"
            else:
                # AI scores (0-10 scale) or default
                raw_str = f"{raw_value:.4f}"
        else:
            raw_str = str(raw_value)
        
        if has_raw_value:
            print(f"{rank:<6} {ticker:<8} {company:<35} {percentile_str:<12} {raw_str:<15}")
        else:
            print(f"{rank:<6} {ticker:<8} {company:<35} {percentile_str:<12}")
        rank += 1
    
    # Display stocks without values
    if len(df_without_values) > 0:
        print("-" * 100)
        print(f"\nStocks without {format_metric_name(metric_name)} data ({len(df_without_values)} total):")
        print("-" * 100)
        print(f"{'Ticker':<8} {'Company':<35}")
        print("-" * 100)
        
        # Sort by ticker for easier reading
        df_without_values = df_without_values.sort_values('ticker')
        
        for idx, row in df_without_values.iterrows():
            ticker = row['ticker']
            company = row['company_name'] or '-'
            company = company[:35] if len(str(company)) > 35 else company
            print(f"{ticker:<8} {company:<35}")
    
    print("=" * 100)
    
    # Calculate and display average percentile (only for stocks with values)
    if len(df_with_values) > 0:
        avg_percentile = df_with_values['percentile'].mean() * 100
        print(f"\nTotal stocks with data: {len(df_with_values)}")
        print(f"Total stocks without data: {len(df_without_values)}")
        print(f"Total stocks: {len(df)}")
        print(f"Average percentile (stocks with data): {avg_percentile:.2f}%")
    else:
        print(f"\nTotal stocks: {len(df)}")
        print(f"Note: No stocks have data for this metric")

def main():
    """Main interactive loop."""
    print("=" * 100)
    print("METRIC RANKING VIEWER")
    print("=" * 100)
    print()
    
    # Get available metrics
    metrics = get_available_metrics()
    
    if metrics is None or len(metrics) == 0:
        print("Error: Could not find metrics in all_scores.db")
        print("Please run calculate_total_scores.py first to generate the database.")
        return
    
    while True:
        print("\nAvailable Metrics:")
        print("-" * 100)
        
        # Display metrics in groups (matching the display order)
        ai_metrics = sorted([m for m in metrics if '_normalized' in m])
        finviz_metrics = sorted([m for m in metrics if '_percentile' in m])
        quickfs_metrics = sorted([m for m in metrics if '_quickfs' in m])
        
        # Create display order list (AI first, then Finviz, then QuickFS)
        display_order = ai_metrics + finviz_metrics + quickfs_metrics
        
        print("\nAI Score Metrics:")
        for idx, metric in enumerate(ai_metrics, 1):
            reverse_marker = " (reverse)" if is_reverse_metric(metric) else ""
            print(f"  {idx:2d}. {format_metric_name(metric)}{reverse_marker}")
        
        print("\nFinviz Metrics:")
        finviz_start = len(ai_metrics) + 1
        for idx, metric in enumerate(finviz_metrics, finviz_start):
            reverse_marker = " (reverse)" if is_reverse_metric(metric) else ""
            print(f"  {idx:2d}. {format_metric_name(metric)}{reverse_marker}")
        
        if quickfs_metrics:
            print("\nQuickFS Metrics:")
            quickfs_start = len(ai_metrics) + len(finviz_metrics) + 1
            for idx, metric in enumerate(quickfs_metrics, quickfs_start):
                reverse_marker = " (reverse)" if is_reverse_metric(metric) else ""
                print(f"  {idx:2d}. {format_metric_name(metric)}{reverse_marker}")
        
        print("\n" + "-" * 100)
        print("Enter metric number to view rankings (or 'quit' to exit): ", end='')
        
        user_input = input().strip()
        
        if user_input.lower() in ['quit', 'exit', 'q']:
            print("Goodbye!")
            break
        
        try:
            metric_num = int(user_input)
            if 1 <= metric_num <= len(display_order):
                selected_metric = display_order[metric_num - 1]
                try:
                    df = get_metric_rankings(selected_metric)
                    display_rankings(df, selected_metric)
                except Exception as e:
                    print(f"Error getting rankings for {format_metric_name(selected_metric)}: {str(e)}")
                    import traceback
                    traceback.print_exc()
            else:
                print(f"Invalid number. Please enter a number between 1 and {len(display_order)}")
        except ValueError:
            print("Invalid input. Please enter a number.")
        except Exception as e:
            print(f"Error: {str(e)}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()

