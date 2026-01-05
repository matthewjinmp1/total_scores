#!/usr/bin/env python3
"""
Calculate total composite scores for companies that overlap between
the short interest database and the AI scores database.
"""

import sqlite3
import os
import pandas as pd

# Database paths
AI_SCORES_DB = os.path.join(os.path.dirname(__file__), "ai_scores.db")
FINVIZ_DB = os.path.join(os.path.dirname(__file__), "finviz", "finviz.db")
QUICKFS_METRICS_DB = os.path.join(os.path.dirname(__file__), "quickfs", "metrics.db")

# Metrics that need to be reversed (lower is better, so we flip them)
REVERSE_METRICS = [
    'disruption_risk',
    'riskiness_score',
    'competition_intensity',
    'bargaining_power_of_customers',
    'bargaining_power_of_suppliers',
    'size_well_known_score'
]

# QuickFS metrics that need to be reversed (lower is better)
QUICKFS_REVERSE_METRICS = [
    'revenue_growth_consistency',
    'operating_margin_consistency',
    'gross_margin_consistency',
    'share_count_halfway_growth',
    'net_debt_to_ttm_operating_income'
]

def get_ai_score_columns():
    """Get all score columns from the ai_scores database."""
    if not os.path.exists(AI_SCORES_DB):
        return []
    
    conn = sqlite3.connect(AI_SCORES_DB)
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='scores'")
    table_exists = cursor.fetchone() is not None
    
    if not table_exists:
        conn.close()
        return []
    
    # Get column names
    cursor.execute("PRAGMA table_info(scores)")
    columns = cursor.fetchall()
    
    # Extract score columns (exclude id, ticker, company_name, model, timestamp, total_score)
    exclude = {'id', 'ticker', 'company_name', 'model', 'timestamp', 'total_score'}
    score_columns = [col[1] for col in columns if col[1] not in exclude and col[2] == 'REAL']
    
    conn.close()
    return score_columns

def convert_recommendation_to_score(recommendation_val):
    """
    Convert recommendation to numeric score (lower is better).
    Handles both text (Strong Buy = 1, Buy = 2, Hold = 3, Sell = 4, Strong Sell = 5)
    and numeric values (already scores, just return as-is).
    """
    if recommendation_val is None or pd.isna(recommendation_val):
        return None
    
    # If it's already a numeric value, return it (Finviz stores numeric recommendations)
    try:
        numeric_val = float(recommendation_val)
        # Ensure it's in a reasonable range (1-5 scale)
        if 0 < numeric_val <= 5:
            return numeric_val
    except (ValueError, TypeError):
        pass
    
    # If it's text, convert it
    rec_upper = str(recommendation_val).strip().upper()
    
    if 'STRONG BUY' in rec_upper or rec_upper == 'STRONG BUY':
        return 1.0
    elif 'BUY' in rec_upper and 'STRONG' not in rec_upper:
        return 2.0
    elif 'HOLD' in rec_upper:
        return 3.0
    elif 'SELL' in rec_upper and 'STRONG' not in rec_upper:
        return 4.0
    elif 'STRONG SELL' in rec_upper:
        return 5.0
    else:
        return None  # Unknown recommendation

def get_overlapping_companies():
    """
    Get companies that exist in ALL three databases (AI scores, Finviz, QuickFS).
    Returns a DataFrame with all metrics data.
    Only includes stocks that have data in all three databases.
    """
    # Check that all databases exist
    if not os.path.exists(AI_SCORES_DB):
        print(f"Error: AI scores database not found at {AI_SCORES_DB}")
        return None
    
    if not os.path.exists(FINVIZ_DB):
        print(f"Error: Finviz database not found at {FINVIZ_DB}")
        return None
    
    if not os.path.exists(QUICKFS_METRICS_DB):
        print(f"Error: QuickFS metrics database not found at {QUICKFS_METRICS_DB}")
        return None
    
    # Load AI scores
    conn_ai = sqlite3.connect(AI_SCORES_DB)
    cursor_ai = conn_ai.cursor()
    
    cursor_ai.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='scores'")
    ai_table_exists = cursor_ai.fetchone() is not None
    
    if not ai_table_exists:
        conn_ai.close()
        print(f"Error: 'scores' table not found in {AI_SCORES_DB}")
        return None
    
    query_ai = """
        SELECT *
        FROM scores
        WHERE id IN (
            SELECT MAX(id)
            FROM scores
            GROUP BY ticker
        )
    """
    df_ai = pd.read_sql_query(query_ai, conn_ai)
    conn_ai.close()
    
    if len(df_ai) == 0:
        print("Error: No data found in AI scores database")
        return None
    
    # Load Finviz metrics
    conn_finviz = sqlite3.connect(FINVIZ_DB)
    cursor_finviz = conn_finviz.cursor()
    
    cursor_finviz.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='short_interest'")
    finviz_table_exists = cursor_finviz.fetchone() is not None
    
    if not finviz_table_exists:
        conn_finviz.close()
        print(f"Error: 'short_interest' table not found in {FINVIZ_DB}")
        return None
    
    query_finviz = """
        SELECT ticker, short_interest_percent, forward_pe, eps_growth_next_5y,
               insider_ownership, roa, roic, gross_margin, operating_margin,
               perf_10y, recommendation, price_move_percent
        FROM short_interest
        WHERE error IS NULL
    """
    df_finviz = pd.read_sql_query(query_finviz, conn_finviz)
    conn_finviz.close()
    
    if len(df_finviz) == 0:
        print("Error: No data found in Finviz database")
        return None
    
    # Convert recommendation text to numeric score
    df_finviz['recommendation_score'] = df_finviz['recommendation'].apply(convert_recommendation_to_score)
    
    # Load QuickFS metrics
    conn_quickfs = sqlite3.connect(QUICKFS_METRICS_DB)
    cursor_quickfs = conn_quickfs.cursor()
    
    cursor_quickfs.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='quickfs_metrics'")
    quickfs_table_exists = cursor_quickfs.fetchone() is not None
    
    if not quickfs_table_exists:
        conn_quickfs.close()
        print(f"Error: 'quickfs_metrics' table not found in {QUICKFS_METRICS_DB}")
        return None
    
    query_quickfs = """
        SELECT ticker, revenue_5y_cagr, revenue_5y_halfway_growth, revenue_growth_consistency,
               revenue_growth_acceleration, operating_margin_growth, gross_margin_growth,
               operating_margin_consistency, gross_margin_consistency, share_count_halfway_growth,
               ttm_ebit_ppe, net_debt_to_ttm_operating_income, total_past_return
        FROM quickfs_metrics
        WHERE id IN (
            SELECT MAX(id) FROM quickfs_metrics GROUP BY ticker
        )
    """
    df_quickfs = pd.read_sql_query(query_quickfs, conn_quickfs)
    conn_quickfs.close()
    
    if len(df_quickfs) == 0:
        print("Error: No data found in QuickFS metrics database")
        return None
    
    # Find intersection of tickers (stocks that exist in ALL three databases)
    tickers_ai = set(df_ai['ticker'])
    tickers_finviz = set(df_finviz['ticker'])
    tickers_quickfs = set(df_quickfs['ticker'])
    
    overlapping_tickers = tickers_ai & tickers_finviz & tickers_quickfs
    
    if len(overlapping_tickers) == 0:
        print("Error: No companies found that exist in all three databases")
        print(f"  AI scores: {len(tickers_ai)} tickers")
        print(f"  Finviz: {len(tickers_finviz)} tickers")
        print(f"  QuickFS: {len(tickers_quickfs)} tickers")
        return None
    
    print(f"Found {len(overlapping_tickers)} companies that exist in all three databases")
    
    # Start with AI scores and merge with inner joins to ensure only overlapping tickers
    df_merged = df_ai[df_ai['ticker'].isin(overlapping_tickers)].copy()
    
    # Merge Finviz data (inner join - only keep tickers that exist in both)
    df_merged = df_merged.merge(df_finviz, on='ticker', how='inner')
    
    # Merge QuickFS data (inner join - only keep tickers that exist in both)
    df_merged = df_merged.merge(df_quickfs, on='ticker', how='inner')
    
    # Ensure company_name exists (use ticker if missing)
    if 'company_name' not in df_merged.columns:
        df_merged['company_name'] = df_merged['ticker']
    else:
        df_merged['company_name'] = df_merged['company_name'].fillna(df_merged['ticker'])
    
    return df_merged

def normalize_ai_scores(df, score_columns):
    """
    Convert AI scores to percentile ranks (0-1 scale).
    Percentiles are calculated relative to all stocks in the dataset.
    Also handle reverse metrics where lower is better.
    """
    df_normalized = df.copy()
    
    # Only process if we have score columns
    if not score_columns:
        return df_normalized
    
    for col in score_columns:
        if col not in df_normalized.columns:
            continue
        
        # Calculate percentile rank for this metric
        # For reverse metrics, we calculate percentile then flip it
        # (lower original values = higher percentile after reversal)
        is_reverse = col in REVERSE_METRICS
        percentile = calculate_percentile_score(df_normalized[col], reverse=is_reverse)
        
        df_normalized[f'{col}_normalized'] = percentile
    
    return df_normalized

def calculate_percentile_score(values, reverse=False):
    """
    Calculate percentile scores for a series of values.
    Uses midpoint ranking for tied values so average percentile is 50%.
    If reverse=True, lower values get higher percentiles (good for short interest).
    
    Example: If 50 stocks have score 10 and 50 have score 9:
    - Score 10 stocks get percentile ~75% (midpoint of top 50%)
    - Score 9 stocks get percentile ~25% (midpoint of bottom 50%)
    """
    # Calculate percentile rank (0-1 scale) using pandas rank
    # method='average' gives the average rank for tied values (midpoint)
    # ascending=True: lower values get lower ranks (1, 2, ...)
    # For tied values, rank is the average of the positions they occupy
    ranks = values.rank(method='average', na_option='keep', ascending=True)
    n_valid = len(values.dropna())
    
    if n_valid == 0:
        return pd.Series([0.5] * len(values), index=values.index)
    
    # Convert ranks to percentiles (0-1 scale)
    # With ascending=True, lower values have lower ranks (rank 1 = lowest value)
    # Higher values have higher ranks (rank n = highest value)
    # So: percentile = rank / n
    # This gives: rank n -> 100%, rank 1 -> 1/n, average -> ~50%
    percentiles = ranks / n_valid
    
    # Handle NaN values - set to median percentile (0.5)
    percentiles = percentiles.fillna(0.5)
    
    # If reverse, flip the percentile (lower values = higher percentile)
    if reverse:
        percentiles = 1.0 - percentiles
    
    return percentiles

def calculate_total_scores(df_normalized, score_columns):
    """
    Calculate total composite scores.
    All metrics are weighted equally.
    Includes AI scores (normalized 0-1) and finviz metrics (percentile ranked).
    """
    df_scores = df_normalized.copy()
    
    # Get all normalized AI score columns
    normalized_cols = [f'{col}_normalized' for col in score_columns if f'{col}_normalized' in df_scores.columns]
    
    # Finviz metrics - higher is better (normal percentile)
    finviz_higher_better = [
        'eps_growth_next_5y',
        'insider_ownership',
        'roa',
        'roic',
        'gross_margin',
        'operating_margin',
        'perf_10y',
        'price_move_percent'
    ]
    
    # Finviz metrics - lower is better (reverse percentile)
    finviz_lower_better = [
        'short_interest_percent',
        'forward_pe',
        'recommendation_score'
    ]
    
    # Calculate percentiles for finviz metrics
    finviz_percentile_cols = []
    
    # Higher is better metrics
    for metric in finviz_higher_better:
        if metric in df_scores.columns:
            percentile_col = f'{metric}_percentile'
            df_scores[percentile_col] = calculate_percentile_score(
                df_scores[metric],
                reverse=False  # Higher values = higher percentile
            )
            finviz_percentile_cols.append(percentile_col)
    
    # Lower is better metrics
    for metric in finviz_lower_better:
        if metric in df_scores.columns:
            percentile_col = f'{metric}_percentile'
            df_scores[percentile_col] = calculate_percentile_score(
                df_scores[metric],
                reverse=True  # Lower values = higher percentile
            )
            finviz_percentile_cols.append(percentile_col)
    
    # QuickFS metrics - all the metrics we want to include
    quickfs_metrics_list = [
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
    
    # Calculate percentiles for QuickFS metrics
    quickfs_percentile_cols = []
    
    for metric in quickfs_metrics_list:
        if metric in df_scores.columns:
            is_reverse = metric in QUICKFS_REVERSE_METRICS
            percentile_col = f'{metric}_percentile'
            df_scores[percentile_col] = calculate_percentile_score(
                df_scores[metric],
                reverse=is_reverse
            )
            quickfs_percentile_cols.append(percentile_col)
    
    # Combine all metrics (AI normalized + finviz percentiles + quickfs percentiles)
    all_metrics = normalized_cols + finviz_percentile_cols + quickfs_percentile_cols
    
    # Calculate total score as average of all normalized metrics
    # Handle NaN values by only averaging non-NaN values
    valid_metrics = []
    for metric in all_metrics:
        if metric in df_scores.columns:
            valid_metrics.append(metric)
    
    if len(valid_metrics) == 0:
        print("Warning: No valid metrics found for total score calculation.")
        df_scores['total_score'] = 0.0
        return df_scores
    
    # Calculate total score (equal weight average)
    df_scores['total_score'] = df_scores[valid_metrics].mean(axis=1)
    
    # Count how many metrics were used for each company
    df_scores['metrics_count'] = df_scores[valid_metrics].count(axis=1)
    
    return df_scores

def display_results(df_scores):
    """Display the results sorted by total score in an organized way."""
    # Sort by total score descending
    df_display = df_scores.copy()
    df_display = df_display.sort_values('total_score', ascending=False)
    
    print("=" * 120)
    print("TOTAL COMPOSITE SCORES")
    print("=" * 120)
    metric_cols = [c for c in df_display.columns if '_normalized' in c or '_percentile' in c]
    print(f"\nTotal companies: {len(df_display)}")
    print(f"Metrics included: {len(metric_cols)}")
    print(f"AI Reverse metrics: {', '.join(REVERSE_METRICS)}")
    print(f"Finviz Reverse metrics: short_interest, forward_pe, recommendation")
    print(f"QuickFS Reverse metrics: {', '.join(QUICKFS_REVERSE_METRICS)}")
    print("\n" + "=" * 120)
    
    # Create a clean summary table with key metrics
    summary_cols = [
        'ticker', 
        'company_name', 
        'total_score',
        'short_interest_percentile',
        'moat_score_normalized',
        'barriers_score_normalized',
        'growth_opportunity_normalized',
        'riskiness_score_normalized',
        'disruption_risk_normalized',
        'competition_intensity_normalized'
    ]
    
    # Filter to only include columns that exist
    available_summary_cols = [col for col in summary_cols if col in df_display.columns]
    
    df_summary = df_display[available_summary_cols].copy()
    
    # Round all numeric columns to 3 decimal places
    numeric_cols = df_summary.select_dtypes(include=['float64', 'int64']).columns
    df_summary[numeric_cols] = df_summary[numeric_cols].round(3)
    
    # Shorten column names for display
    df_summary.columns = df_summary.columns.str.replace('_normalized', '').str.replace('_', ' ').str.title()
    df_summary = df_summary.rename(columns={
        'Ticker': 'Ticker',
        'Company Name': 'Company',
        'Total Score': 'Total',
        'Short Interest Percentile': 'Short %',
        'Moat Score': 'Moat',
        'Barriers Score': 'Barriers',
        'Growth Opportunity': 'Growth',
        'Riskiness Score': 'Risk',
        'Disruption Risk': 'Disrupt',
        'Competition Intensity': 'Competition'
    })
    
    print("\nTOP 50 COMPANIES - SUMMARY VIEW")
    print("-" * 120)
    print(df_summary.head(50).to_string(index=False))
    
    # Detailed breakdown for top 10 companies
    print("\n" + "=" * 120)
    print("DETAILED BREAKDOWN - TOP 10 COMPANIES")
    print("=" * 120)
    
    metric_cols = [col for col in df_display.columns if '_normalized' in col or '_percentile' in col]
    
    for idx, row in df_display.head(10).iterrows():
        ticker = row['ticker']
        company = row['company_name']
        total = row['total_score']
        
        print(f"\n{ticker:6} - {company:40} | Total Score: {total:.3f}")
        print("-" * 120)
        
        # Group metrics by category
        categories = {
            'Core Strengths': [
                'moat_score_normalized', 'barriers_score_normalized', 'brand_strength_normalized',
                'pricing_power_normalized', 'switching_cost_normalized'
            ],
            'Innovation & Growth': [
                'innovativeness_score_normalized', 'growth_opportunity_normalized', 
                'ambition_score_normalized', 'trailblazer_score_normalized'
            ],
            'Product & Quality': [
                'product_differentiation_normalized', 'product_quality_score_normalized',
                'network_effect_normalized'
            ],
            'Risk Factors (reversed)': [
                'disruption_risk_normalized', 'riskiness_score_normalized',
                'competition_intensity_normalized'
            ],
            'Market Position': [
                'bargaining_power_of_customers_normalized', 'bargaining_power_of_suppliers_normalized',
                'size_well_known_score_normalized'
            ],
            'Management & Culture': [
                'management_quality_score_normalized', 'culture_employee_satisfaction_score_normalized',
                'long_term_orientation_score_normalized', 'execution_ability_score_normalized'
            ],
            'Other': [
                'ai_knowledge_score_normalized', 'ethical_healthy_environmental_score_normalized'
            ],
            'Finviz Metrics': [
                'short_interest_percent_percentile', 'forward_pe_percentile', 
                'eps_growth_next_5y_percentile', 'insider_ownership_percentile',
                'roa_percentile', 'roic_percentile', 'gross_margin_percentile',
                'operating_margin_percentile', 'perf_10y_percentile',
                'recommendation_score_percentile', 'price_move_percent_percentile'
            ]
        }
        
        for category, metrics in categories.items():
            category_values = []
            for metric in metrics:
                if metric in row and pd.notna(row[metric]):
                    # Clean metric name - handle both _normalized and _percentile suffixes
                    clean_name = metric.replace('_normalized', '').replace('_percentile', '').replace('_', ' ').title()
                    clean_name = clean_name[:25]  # Limit length
                    value = f"{row[metric]:.3f}"
                    category_values.append(f"{clean_name:30} {value:>6}")
            
            if category_values:
                print(f"  {category}:")
                for val in category_values:
                    print(f"    {val}")
    
    print("\n" + "=" * 120)
    print(f"\nSUMMARY STATISTICS")
    print("-" * 120)
    stats_df = df_display[['total_score', 'metrics_count']].describe()
    print(stats_df.to_string())
    
    return df_display

def save_results(df_scores):
    """Save results to a SQLite database."""
    output_file = os.path.join(os.path.dirname(__file__), "all_scores.db")
    
    # Select columns to save (excluding total_score since it's calculated dynamically)
    save_cols = ['ticker', 'company_name', 'metrics_count']
    metric_cols = [col for col in df_scores.columns if '_normalized' in col or '_percentile' in col]
    
    df_save = df_scores[save_cols + metric_cols].copy()
    # Sort by ticker for consistent output (total_score is dynamic, not saved)
    df_save = df_save.sort_values('ticker', ascending=True)
    
    # Connect to database and save
    conn = sqlite3.connect(output_file)
    
    # Drop existing table if it exists and create new one
    df_save.to_sql('all_scores', conn, if_exists='replace', index=False)
    
    # Create index on ticker for faster lookups
    cursor = conn.cursor()
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker ON all_scores(ticker)')
    conn.commit()
    conn.close()
    
    print(f"\nResults saved to: {output_file}")
    print(f"  Table: all_scores")
    print(f"  Records: {len(df_save)}")
    return output_file

def main():
    """Main function."""
    print("Loading data from databases...")
    print("(Only including stocks that exist in ALL three databases: AI scores, Finviz, QuickFS)")
    
    # Get overlapping companies (must exist in all three databases)
    df = get_overlapping_companies()
    
    if df is None or len(df) == 0:
        print("\nCannot proceed: No companies found in all three databases.")
        return
    
    print(f"Processing {len(df)} companies with data from all three databases")
    
    # Get all score columns (should exist since we require AI scores)
    score_columns = get_ai_score_columns()
    if score_columns:
        print(f"Found {len(score_columns)} AI score metrics")
    else:
        print("Warning: No AI score metrics found")
    
    # Normalize AI scores (0-10 to 0-1, handle reverse metrics)
    print("\nNormalizing AI scores...")
    df_normalized = normalize_ai_scores(df, score_columns)
    
    # Calculate percentile for short interest and total scores
    print("Calculating composite scores...")
    df_scores = calculate_total_scores(df_normalized, score_columns)
    
    # Display results
    print("\n")
    df_display = display_results(df_scores)
    
    # Save results
    save_results(df_scores)
    
    print("\nDone!")

if __name__ == "__main__":
    main()

