#!/usr/bin/env python3
"""
Web application to display total composite scores for companies.
"""

from flask import Flask, render_template, jsonify, request
import sqlite3
import os
from datetime import datetime

app = Flask(__name__)

# Database path
DB_PATH = os.path.join(os.path.dirname(__file__), "all_scores.db")

def get_db_connection():
    """Get database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    """Main page showing all companies."""
    return render_template('index.html')

@app.route('/api/companies')
def get_companies():
    """API endpoint to get all companies with optional sorting and filtering."""
    import pandas as pd
    
    conn = get_db_connection()
    
    # Get query parameters
    sort_by = request.args.get('sort', 'total_score')
    order = request.args.get('order', 'desc')
    search = request.args.get('search', '').strip()
    limit = request.args.get('limit', type=int)
    
    # FIRST: Load ALL companies
    # Note: total_score is calculated dynamically on the frontend based on selected metrics
    # We'll use a default calculation here for initial rank/percentile (frontend will recalculate)
    all_companies_df = pd.read_sql_query("SELECT ticker FROM all_scores", conn)
    
    # Note: rank and percentile are now calculated dynamically on the frontend
    # based on the selected metrics, so we don't need to calculate them here
    overall_rank_map = {}
    overall_percentile_map = {}
    
    # Build query for filtered/sorted results
    query = "SELECT * FROM all_scores WHERE 1=1"
    params = []
    
    # Add search filter
    if search:
        query += " AND (ticker LIKE ? OR company_name LIKE ?)"
        search_term = f"%{search}%"
        params.extend([search_term, search_term])
    
    # Add sorting (total_score is dynamic, so default to ticker)
    valid_sort_columns = ['ticker', 'company_name', 'metrics_count']
    if sort_by in valid_sort_columns:
        order_sql = 'DESC' if order.lower() == 'desc' else 'ASC'
        query += f" ORDER BY {sort_by} {order_sql}"
    else:
        query += " ORDER BY ticker ASC"
    
    # Add limit
    if limit:
        query += f" LIMIT {limit}"
    
    companies = conn.execute(query, params).fetchall()
    conn.close()
    
    # Convert rows to dictionaries and map QuickFS metrics from _percentile to _quickfs
    # QuickFS metrics are stored in all_scores.db with _percentile suffix but frontend expects _quickfs
    quickfs_metrics_base = [
        'revenue_5y_cagr', 'revenue_5y_halfway_growth', 'revenue_growth_consistency',
        'revenue_growth_acceleration', 'operating_margin_growth', 'gross_margin_growth',
        'operating_margin_consistency', 'gross_margin_consistency', 'share_count_halfway_growth',
        'ttm_ebit_ppe', 'net_debt_to_ttm_operating_income', 'total_past_return'
    ]
    
    # Dataroma metrics are stored in all_scores.db with _percentile suffix but frontend expects _dataroma
    dataroma_metrics_base = [
        'ownership_count', 'portfolio_percent', 'price_move_percent',
        'net_buys', 'net_dollars_percent_of_market_cap'
    ]
    
    result = []
    for row in companies:
        company_dict = dict(row)
        
        # Map QuickFS metrics from _percentile suffix to _quickfs suffix (for frontend compatibility)
        for metric_base in quickfs_metrics_base:
            percentile_key = f'{metric_base}_percentile'
            quickfs_key = f'{metric_base}_quickfs'
            if percentile_key in company_dict:
                # Use percentile value, or 0.5 if None/NaN
                value = company_dict[percentile_key]
                company_dict[quickfs_key] = 0.5 if value is None or pd.isna(value) else value
            else:
                # Metric not in database, assign 0.5
                company_dict[quickfs_key] = 0.5
        
        # Map Dataroma metrics from _percentile suffix to _dataroma suffix (for frontend compatibility)
        for metric_base in dataroma_metrics_base:
            percentile_key = f'{metric_base}_percentile'
            dataroma_key = f'{metric_base}_dataroma'
            if percentile_key in company_dict:
                # Use percentile value, or 0.5 if None/NaN
                value = company_dict[percentile_key]
                company_dict[dataroma_key] = 0.5 if value is None or pd.isna(value) else value
            else:
                # Metric not in database, assign 0.5
                company_dict[dataroma_key] = 0.5
        
        result.append(company_dict)
    
    return jsonify(result)

@app.route('/api/company/<ticker>')
def get_company(ticker):
    """API endpoint to get detailed information for a specific company."""
    import sqlite3 as sqlite3_module
    import os as os_module
    
    conn = get_db_connection()
    company = conn.execute(
        'SELECT * FROM all_scores WHERE ticker = ?',
        (ticker.upper(),)
    ).fetchone()
    conn.close()
    
    if not company:
        return jsonify({'error': 'Company not found'}), 404
    
    company_dict = dict(company)
    
    # Get raw values from source databases
    ticker_upper = ticker.upper()
    
    # Get AI score raw values
    ai_scores_db = os_module.path.join(os_module.path.dirname(__file__), "ai_scores.db")
    if os_module.path.exists(ai_scores_db):
        conn_ai = sqlite3_module.connect(ai_scores_db)
        conn_ai.row_factory = sqlite3_module.Row
        cursor_ai = conn_ai.cursor()
        cursor_ai.execute("""
            SELECT * FROM scores 
            WHERE ticker = ? AND id = (SELECT MAX(id) FROM scores WHERE ticker = ?)
        """, (ticker_upper, ticker_upper))
        ai_row = cursor_ai.fetchone()
        if ai_row:
            ai_data = dict(ai_row)
            # Add raw values with _raw suffix
            for key, value in ai_data.items():
                if key not in ['id', 'ticker', 'company_name', 'model', 'timestamp', 'total_score']:
                    company_dict[f'{key}_raw'] = value
        conn_ai.close()
    
    # Get Finviz raw values
    finviz_db = os_module.path.join(os_module.path.dirname(__file__), "finviz", "finviz.db")
    if os_module.path.exists(finviz_db):
        conn_finviz = sqlite3_module.connect(finviz_db)
        conn_finviz.row_factory = sqlite3_module.Row
        finviz_row = conn_finviz.execute(
            'SELECT * FROM short_interest WHERE ticker = ?',
            (ticker_upper,)
        ).fetchone()
        if finviz_row:
            finviz_data = dict(finviz_row)
            # Map Finviz columns to raw values
            finviz_mapping = {
                'short_interest_percent': 'short_interest_percent_raw',
                'forward_pe': 'forward_pe_raw',
                'eps_growth_next_5y': 'eps_growth_next_5y_raw',
                'insider_ownership': 'insider_ownership_raw',
                'roa': 'roa_raw',
                'roic': 'roic_raw',
                'gross_margin': 'gross_margin_raw',
                'operating_margin': 'operating_margin_raw',
                'perf_10y': 'perf_10y_raw',
                'recommendation': 'recommendation_raw',
                'price_move_percent': 'price_move_percent_raw'
            }
            for key, raw_key in finviz_mapping.items():
                if key in finviz_data:
                    company_dict[raw_key] = finviz_data[key]
        conn_finviz.close()
    
    # QuickFS metrics are already in all_scores.db with _percentile suffix
    # Map them to _quickfs suffix for frontend compatibility
    quickfs_metrics_base = [
        'revenue_5y_cagr', 'revenue_5y_halfway_growth', 'revenue_growth_consistency',
        'revenue_growth_acceleration', 'operating_margin_growth', 'gross_margin_growth',
        'operating_margin_consistency', 'gross_margin_consistency', 'share_count_halfway_growth',
        'ttm_ebit_ppe', 'net_debt_to_ttm_operating_income', 'total_past_return'
    ]
    
    for metric_base in quickfs_metrics_base:
        percentile_key = f'{metric_base}_percentile'
        quickfs_key = f'{metric_base}_quickfs'
        if percentile_key in company_dict:
            # Use percentile value, or 0.5 if None/NaN
            value = company_dict[percentile_key]
            company_dict[quickfs_key] = 0.5 if value is None or pd.isna(value) else value
        else:
            # Metric not in database, assign 0.5
            company_dict[quickfs_key] = 0.5
    
    # Dataroma metrics are already in all_scores.db with _percentile suffix
    # Map them to _dataroma suffix for frontend compatibility
    dataroma_metrics_base = [
        'ownership_count', 'portfolio_percent', 'price_move_percent',
        'net_buys', 'net_dollars_percent_of_market_cap'
    ]
    
    for metric_base in dataroma_metrics_base:
        percentile_key = f'{metric_base}_percentile'
        dataroma_key = f'{metric_base}_dataroma'
        if percentile_key in company_dict:
            # Use percentile value, or 0.5 if None/NaN
            value = company_dict[percentile_key]
            company_dict[dataroma_key] = 0.5 if value is None or pd.isna(value) else value
        else:
            # Metric not in database, assign 0.5
            company_dict[dataroma_key] = 0.5
    
    return jsonify(company_dict)

@app.route('/api/stats')
def get_stats():
    """API endpoint to get summary statistics."""
    conn = get_db_connection()
    stats = conn.execute('''
        SELECT 
            COUNT(*) as total_companies,
            AVG(metrics_count) as avg_metrics
        FROM all_scores
    ''').fetchone()
    conn.close()
    
    return jsonify(dict(stats))

if __name__ == '__main__':
    # Check if database exists
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        print("Please run calculate_total_scores.py first to generate the database.")
    else:
        port = int(os.environ.get('PORT', 8080))
        print(f"\n🚀 Starting server on http://localhost:{port}")
        print("   Press Ctrl+C to stop\n")
        app.run(debug=True, host='0.0.0.0', port=port)

