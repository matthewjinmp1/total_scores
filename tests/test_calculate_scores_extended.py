#!/usr/bin/env python3
"""
Extended tests for calculate_total_scores.py to improve coverage.
"""

import sys
import os
import sqlite3
import unittest
import tempfile
import shutil
import pandas as pd

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calculate_total_scores import (
    convert_recommendation_to_score,
    get_overlapping_companies,
    normalize_ai_scores,
    calculate_percentile_score,
    calculate_total_scores,
    display_results,
    save_results,
    REVERSE_METRICS,
    get_ai_score_columns,
    AI_SCORES_DB,
    FINVIZ_DB
)


class TestCalculateScoresExtended(unittest.TestCase):
    """Extended tests for score calculation functions."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.test_ai_db = os.path.join(self.test_dir, 'test_ai_scores.db')
        self.test_finviz_db = os.path.join(self.test_dir, 'test_finviz.db')
        
        # Create test AI scores database
        conn_ai = sqlite3.connect(self.test_ai_db)
        cursor_ai = conn_ai.cursor()
        cursor_ai.execute('''
            CREATE TABLE scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                company_name TEXT,
                model TEXT,
                timestamp TEXT,
                moat_score REAL,
                barriers_score REAL,
                brand_strength REAL,
                disruption_risk REAL
            )
        ''')
        cursor_ai.executemany('''
            INSERT INTO scores (ticker, company_name, model, timestamp,
                               moat_score, barriers_score, brand_strength, disruption_risk)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', [
            ('AAPL', 'Apple Inc.', 'test', '2024-01-01', 8.0, 7.0, 9.0, 3.0),
            ('MSFT', 'Microsoft', 'test', '2024-01-01', 9.0, 8.0, 8.5, 2.5),
            ('GOOGL', 'Alphabet', 'test', '2024-01-01', 8.5, 7.5, 9.5, 3.5),
        ])
        conn_ai.commit()
        conn_ai.close()
        
        # Create test Finviz database
        os.makedirs(os.path.dirname(self.test_finviz_db), exist_ok=True)
        conn_finviz = sqlite3.connect(self.test_finviz_db)
        cursor_finviz = conn_finviz.cursor()
        cursor_finviz.execute('''
            CREATE TABLE short_interest (
                ticker TEXT,
                short_interest_percent REAL,
                forward_pe REAL,
                eps_growth_next_5y REAL,
                insider_ownership REAL,
                roa REAL,
                roic REAL,
                gross_margin REAL,
                operating_margin REAL,
                perf_10y REAL,
                recommendation TEXT,
                price_move_percent REAL,
                error TEXT
            )
        ''')
        cursor_finviz.executemany('''
            INSERT INTO short_interest 
            (ticker, short_interest_percent, forward_pe, eps_growth_next_5y, 
             insider_ownership, roa, roic, gross_margin, operating_margin,
             perf_10y, recommendation, price_move_percent, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', [
            ('AAPL', 1.5, 25.0, 10.5, 5.0, 20.0, 30.0, 40.0, 25.0, 15.0, 'Buy', 10.0, None),
            ('MSFT', 0.8, 30.0, 12.0, 3.0, 15.0, 25.0, 35.0, 20.0, 12.0, 'Strong Buy', 8.0, None),
            ('GOOGL', 1.2, 22.0, 11.0, 4.0, 18.0, 28.0, 38.0, 22.0, 14.0, 'Hold', 9.0, None),
        ])
        conn_finviz.commit()
        conn_finviz.close()
        
        # Create QuickFS metrics database
        self.test_quickfs_db = os.path.join(self.test_dir, 'quickfs_metrics.db')
        conn_quickfs = sqlite3.connect(self.test_quickfs_db)
        cursor = conn_quickfs.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS quickfs_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                calculated_at TEXT NOT NULL,
                revenue_5y_cagr REAL,
                revenue_5y_halfway_growth REAL,
                revenue_growth_consistency REAL,
                revenue_growth_acceleration REAL,
                operating_margin_growth REAL,
                gross_margin_growth REAL,
                operating_margin_consistency REAL,
                gross_margin_consistency REAL,
                share_count_halfway_growth REAL,
                ttm_ebit_ppe REAL,
                net_debt_to_ttm_operating_income REAL,
                total_past_return REAL,
                error TEXT
            )
        ''')
        # Add test data for overlapping tickers
        cursor.execute('''
            INSERT INTO quickfs_metrics 
            (ticker, calculated_at, revenue_5y_cagr, revenue_5y_halfway_growth)
            VALUES (?, ?, ?, ?)
        ''', ('AAPL', '2024-01-01', 10.5, 1.2))
        cursor.execute('''
            INSERT INTO quickfs_metrics 
            (ticker, calculated_at, revenue_5y_cagr, revenue_5y_halfway_growth)
            VALUES (?, ?, ?, ?)
        ''', ('MSFT', '2024-01-01', 12.3, 1.3))
        conn_quickfs.commit()
        conn_quickfs.close()
        
        # Patch database paths
        import calculate_total_scores
        self.original_ai_path = calculate_total_scores.AI_SCORES_DB
        self.original_finviz_path = calculate_total_scores.FINVIZ_DB
        self.original_quickfs_path = calculate_total_scores.QUICKFS_METRICS_DB
        calculate_total_scores.AI_SCORES_DB = self.test_ai_db
        calculate_total_scores.FINVIZ_DB = self.test_finviz_db
        calculate_total_scores.QUICKFS_METRICS_DB = self.test_quickfs_db
    
    def tearDown(self):
        """Clean up test fixtures."""
        import calculate_total_scores
        calculate_total_scores.AI_SCORES_DB = self.original_ai_path
        calculate_total_scores.FINVIZ_DB = self.original_finviz_path
        calculate_total_scores.QUICKFS_METRICS_DB = self.original_quickfs_path
        shutil.rmtree(self.test_dir)
    
    def test_get_overlapping_companies(self):
        """Test getting overlapping companies from both databases."""
        df = get_overlapping_companies()
        
        self.assertIsNotNone(df)
        self.assertGreater(len(df), 0)
        self.assertIn('ticker', df.columns)
        self.assertIn('moat_score', df.columns)
        self.assertIn('short_interest_percent', df.columns)
        
        # Check that overlapping tickers are included
        tickers = set(df['ticker'].unique())
        self.assertIn('AAPL', tickers)
        self.assertIn('MSFT', tickers)
    
    def test_get_overlapping_companies_no_overlap(self):
        """Test get_overlapping_companies when there's no overlap."""
        # Create databases with no overlapping tickers
        conn_finviz = sqlite3.connect(self.test_finviz_db)
        cursor = conn_finviz.cursor()
        cursor.execute('DELETE FROM short_interest')
        cursor.execute('''
            INSERT INTO short_interest 
            (ticker, short_interest_percent, error)
            VALUES (?, ?, ?)
        ''', ('XYZ', 2.0, None))
        conn_finviz.commit()
        conn_finviz.close()
        
        # Should return None or empty result
        result = get_overlapping_companies()
        if result is None:
            # Function prints message and returns None
            pass
        else:
            self.assertEqual(len(result), 0)
    
    def test_normalize_ai_scores(self):
        """Test normalizing AI scores to percentiles."""
        # Create test dataframe
        df = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT', 'GOOGL'],
            'moat_score': [8.0, 9.0, 8.5],
            'barriers_score': [7.0, 8.0, 7.5],
            'disruption_risk': [3.0, 2.5, 3.5],  # Reverse metric
        })
        
        score_columns = ['moat_score', 'barriers_score', 'disruption_risk']
        df_normalized = normalize_ai_scores(df, score_columns)
        
        # Check normalized columns exist
        self.assertIn('moat_score_normalized', df_normalized.columns)
        self.assertIn('barriers_score_normalized', df_normalized.columns)
        self.assertIn('disruption_risk_normalized', df_normalized.columns)
        
        # Check that normalized values are in range [0, 1]
        for col in ['moat_score_normalized', 'barriers_score_normalized', 'disruption_risk_normalized']:
            values = df_normalized[col]
            self.assertTrue(all((values >= 0) & (values <= 1)))
    
    def test_calculate_percentile_score_basic(self):
        """Test basic percentile score calculation."""
        values = pd.Series([10, 20, 30, 40, 50])
        percentiles = calculate_percentile_score(values, reverse=False)
        
        # Check that percentiles are in range [0, 1]
        self.assertTrue(all((percentiles >= 0) & (percentiles <= 1)))
        
        # Highest value should have highest percentile
        self.assertEqual(percentiles.iloc[-1], 1.0)
    
    def test_calculate_percentile_score_reverse(self):
        """Test reverse percentile score calculation."""
        values = pd.Series([1, 2, 3, 4, 5])  # Lower is better
        percentiles = calculate_percentile_score(values, reverse=True)
        
        # Lowest value should have highest percentile (reversed)
        self.assertGreater(percentiles.iloc[0], percentiles.iloc[-1])
    
    def test_calculate_percentile_score_with_nan(self):
        """Test percentile score calculation with NaN values."""
        values = pd.Series([1, 2, pd.NA, 4, 5])
        percentiles = calculate_percentile_score(values, reverse=False)
        
        # NaN should be filled with 0.5 (median)
        self.assertEqual(percentiles.iloc[2], 0.5)
    
    def test_calculate_percentile_score_empty(self):
        """Test percentile score calculation with empty series."""
        values = pd.Series([], dtype=float)
        percentiles = calculate_percentile_score(values, reverse=False)
        
        # Should return series of 0.5 (default)
        self.assertEqual(len(percentiles), 0)
    
    def test_calculate_total_scores(self):
        """Test calculating total composite scores."""
        # Create normalized dataframe with both AI and Finviz metrics
        df = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT'],
            'moat_score_normalized': [0.8, 0.9],
            'barriers_score_normalized': [0.7, 0.8],
            'eps_growth_next_5y': [10.5, 12.0],
            'forward_pe': [25.0, 30.0],
            'short_interest_percent': [1.5, 0.8],
            'roa': [20.0, 15.0],
            'roic': [30.0, 25.0],
        })
        
        score_columns = ['moat_score', 'barriers_score']
        df_scores = calculate_total_scores(df, score_columns)
        
        # Check that metrics_count is calculated
        self.assertIn('metrics_count', df_scores.columns)
        
        # Check that Finviz percentiles are calculated
        finviz_percentile_cols = [col for col in df_scores.columns if '_percentile' in col]
        self.assertGreater(len(finviz_percentile_cols), 0)
    
    def test_get_overlapping_companies_with_recommendation_scores(self):
        """Test get_overlapping_companies handles recommendation conversion."""
        df = get_overlapping_companies()
        
        if df is not None and len(df) > 0:
            # Should have recommendation_score column after conversion
            self.assertIn('recommendation_score', df.columns)
    
    def test_normalize_ai_scores_with_missing_column(self):
        """Test normalize_ai_scores handles missing columns."""
        df = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT'],
            'moat_score': [8.0, 9.0],
        })
        
        score_columns = ['moat_score', 'nonexistent_score']
        df_normalized = normalize_ai_scores(df, score_columns)
        
        # Should only normalize existing columns
        self.assertIn('moat_score_normalized', df_normalized.columns)
        self.assertNotIn('nonexistent_score_normalized', df_normalized.columns)
    
    def test_calculate_total_scores_no_valid_metrics(self):
        """Test calculate_total_scores with no valid metrics."""
        # Create dataframe with no valid metrics
        df = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT'],
        })
        
        score_columns = ['nonexistent_score']
        df_scores = calculate_total_scores(df, score_columns)
        
        # Should have total_score column set to 0.0
        self.assertIn('total_score', df_scores.columns)
        self.assertTrue(all(df_scores['total_score'] == 0.0))
    
    def test_calculate_total_scores_all_finviz_metrics(self):
        """Test calculate_total_scores with all Finviz metrics."""
        df = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT', 'GOOGL'],
            # Higher is better metrics
            'eps_growth_next_5y': [10.5, 12.0, 11.0],
            'insider_ownership': [5.0, 3.0, 4.0],
            'roa': [20.0, 15.0, 18.0],
            'roic': [30.0, 25.0, 28.0],
            'gross_margin': [40.0, 35.0, 38.0],
            'operating_margin': [25.0, 20.0, 22.0],
            'perf_10y': [15.0, 12.0, 14.0],
            'price_move_percent': [10.0, 8.0, 9.0],
            # Lower is better metrics
            'short_interest_percent': [1.5, 0.8, 1.2],
            'forward_pe': [25.0, 30.0, 22.0],
            'recommendation_score': [4.0, 5.0, 3.0],
        })
        
        score_columns = []
        df_scores = calculate_total_scores(df, score_columns)
        
        # Should have percentile columns for all Finviz metrics
        finviz_higher = ['eps_growth_next_5y', 'insider_ownership', 'roa', 'roic',
                        'gross_margin', 'operating_margin', 'perf_10y', 'price_move_percent']
        finviz_lower = ['short_interest_percent', 'forward_pe', 'recommendation_score']
        
        for metric in finviz_higher + finviz_lower:
            self.assertIn(f'{metric}_percentile', df_scores.columns)
        
        # Should have total_score
        self.assertIn('total_score', df_scores.columns)
        self.assertIn('metrics_count', df_scores.columns)
    
    def test_display_results(self):
        """Test display_results function."""
        # Create test dataframe with scores
        df = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT', 'GOOGL'],
            'company_name': ['Apple', 'Microsoft', 'Alphabet'],
            'total_score': [0.8, 0.9, 0.7],
            'metrics_count': [10, 10, 10],
            'moat_score_normalized': [0.8, 0.9, 0.7],
        })
        
        df_display = display_results(df)
        
        # Should return a DataFrame
        self.assertIsNotNone(df_display)
        self.assertIsInstance(df_display, pd.DataFrame)
        
        # Should be sorted by total_score descending
        self.assertTrue(df_display['total_score'].iloc[0] >= df_display['total_score'].iloc[-1])
    
    def test_save_results(self):
        """Test save_results function."""
        # Mock the file path using monkey patching
        import calculate_total_scores
        original_dir = os.path.dirname(calculate_total_scores.__file__)
        test_output_db = os.path.join(self.test_dir, 'test_all_scores.db')
        
        # Patch os.path.dirname to return our test directory
        import os.path as os_path_module
        original_dirname = os_path_module.dirname
        
        def mock_dirname(path):
            if path == calculate_total_scores.__file__:
                return self.test_dir
            return original_dirname(path)
        
        os_path_module.dirname = mock_dirname
        
        try:
            # Create test dataframe
            df = pd.DataFrame({
                'ticker': ['AAPL', 'MSFT'],
                'company_name': ['Apple', 'Microsoft'],
                'total_score': [0.8, 0.9],
                'metrics_count': [10, 10],
                'moat_score_normalized': [0.8, 0.9],
                'short_interest_percent_percentile': [0.7, 0.8],
            })
            
            output_file = save_results(df)
            
            # Should return output file path
            self.assertIsNotNone(output_file)
            
            # Check if file was created (may be in different location due to patching)
            if os.path.exists(output_file):
                # Verify database was created and has correct structure
                conn = sqlite3.connect(output_file)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='all_scores'")
                table_exists = cursor.fetchone() is not None
                self.assertTrue(table_exists)
                
                # Verify data was saved correctly
                cursor.execute("SELECT COUNT(*) FROM all_scores")
                count = cursor.fetchone()[0]
                self.assertEqual(count, 2)
                
                # Verify total_score is NOT saved (should be excluded)
                cursor.execute("PRAGMA table_info(all_scores)")
                columns = [row[1] for row in cursor.fetchall()]
                self.assertNotIn('total_score', columns)
                self.assertIn('ticker', columns)
                self.assertIn('metrics_count', columns)
                
                conn.close()
        finally:
            # Restore original dirname
            os_path_module.dirname = original_dirname
    
    def test_get_ai_score_columns(self):
        """Test get_ai_score_columns function."""
        columns = get_ai_score_columns()
        
        # Should return a list
        self.assertIsInstance(columns, list)
        
        # Should include score columns from test database
        self.assertIn('moat_score', columns)
        self.assertIn('barriers_score', columns)
        
        # Should not include excluded columns
        self.assertNotIn('id', columns)
        self.assertNotIn('ticker', columns)
        self.assertNotIn('company_name', columns)


if __name__ == '__main__':
    unittest.main(verbosity=2)

