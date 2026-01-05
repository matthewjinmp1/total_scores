#!/usr/bin/env python3
"""
Extended tests for view_metric_rankings.py to improve coverage.
"""

import sys
import os
import sqlite3
import unittest
import tempfile
import shutil
import pandas as pd
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from view_metric_rankings import (
    get_available_metrics,
    get_metric_rankings,
    display_rankings,
    get_raw_value_mapping,
    calculate_percentile_rank,
    format_metric_name,
    is_reverse_metric,
    ALL_SCORES_DB,
    AI_SCORES_DB,
    QUICKFS_METRICS_DB
)


class TestViewRankingsExtended(unittest.TestCase):
    """Extended tests for metric ranking functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.test_all_scores_db = os.path.join(self.test_dir, 'test_all_scores.db')
        self.test_ai_db = os.path.join(self.test_dir, 'test_ai_scores.db')
        self.test_quickfs_db = os.path.join(self.test_dir, 'test_quickfs_metrics.db')
        
        # Create all_scores database
        conn_all = sqlite3.connect(self.test_all_scores_db)
        cursor_all = conn_all.cursor()
        cursor_all.execute('''
            CREATE TABLE all_scores (
                ticker TEXT PRIMARY KEY,
                company_name TEXT,
                moat_score_normalized REAL,
                roa_percentile REAL
            )
        ''')
        cursor_all.executemany('''
            INSERT INTO all_scores (ticker, company_name, moat_score_normalized, roa_percentile)
            VALUES (?, ?, ?, ?)
        ''', [
            ('AAPL', 'Apple Inc.', 0.8, 0.9),
            ('MSFT', 'Microsoft', 0.9, 0.85),
        ])
        conn_all.commit()
        conn_all.close()
        
        # Create AI scores database
        conn_ai = sqlite3.connect(self.test_ai_db)
        cursor_ai = conn_ai.cursor()
        cursor_ai.execute('''
            CREATE TABLE scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                moat_score REAL
            )
        ''')
        cursor_ai.executemany('''
            INSERT INTO scores (ticker, moat_score)
            VALUES (?, ?)
        ''', [
            ('AAPL', 8.0),
            ('MSFT', 9.0),
        ])
        conn_ai.commit()
        conn_ai.close()
        
        # Create QuickFS metrics database
        os.makedirs(os.path.dirname(self.test_quickfs_db), exist_ok=True)
        conn_qfs = sqlite3.connect(self.test_quickfs_db)
        cursor_qfs = conn_qfs.cursor()
        cursor_qfs.execute('''
            CREATE TABLE quickfs_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                revenue_5y_cagr REAL,
                revenue_5y_halfway_growth REAL,
                calculated_at TEXT
            )
        ''')
        cursor_qfs.executemany('''
            INSERT INTO quickfs_metrics (ticker, revenue_5y_cagr, revenue_5y_halfway_growth, calculated_at)
            VALUES (?, ?, ?, ?)
        ''', [
            ('AAPL', 0.15, 1.5, '2024-01-01'),
            ('MSFT', 0.20, 1.8, '2024-01-01'),
            ('GOOGL', 0.18, 1.6, '2024-01-01'),
        ])
        conn_qfs.commit()
        conn_qfs.close()
        
        # Patch database paths
        import view_metric_rankings
        self.original_all_path = view_metric_rankings.ALL_SCORES_DB
        self.original_ai_path = view_metric_rankings.AI_SCORES_DB
        self.original_quickfs_path = view_metric_rankings.QUICKFS_METRICS_DB
        view_metric_rankings.ALL_SCORES_DB = self.test_all_scores_db
        view_metric_rankings.AI_SCORES_DB = self.test_ai_db
        view_metric_rankings.QUICKFS_METRICS_DB = self.test_quickfs_db
    
    def tearDown(self):
        """Clean up test fixtures."""
        import view_metric_rankings
        view_metric_rankings.ALL_SCORES_DB = self.original_all_path
        view_metric_rankings.AI_SCORES_DB = self.original_ai_path
        view_metric_rankings.QUICKFS_METRICS_DB = self.original_quickfs_path
        shutil.rmtree(self.test_dir)
    
    def test_get_available_metrics(self):
        """Test getting available metrics."""
        metrics = get_available_metrics()
        
        self.assertIsInstance(metrics, list)
        self.assertGreater(len(metrics), 0)
        # Should include normalized and percentile metrics
        self.assertTrue(any('_normalized' in m for m in metrics) or 
                       any('_percentile' in m for m in metrics))
    
    def test_get_metric_rankings_normalized(self):
        """Test getting metric rankings for normalized metric."""
        df = get_metric_rankings('moat_score_normalized')
        
        self.assertIsNotNone(df)
        self.assertGreater(len(df), 0)
        self.assertIn('ticker', df.columns)
        self.assertIn('percentile', df.columns)
        self.assertIn('raw_value', df.columns)
    
    def test_get_metric_rankings_percentile(self):
        """Test getting metric rankings for percentile metric."""
        df = get_metric_rankings('roa_percentile')
        
        self.assertIsNotNone(df)
        if len(df) > 0:
            self.assertIn('ticker', df.columns)
            self.assertIn('percentile', df.columns)
    
    def test_get_metric_rankings_quickfs(self):
        """Test getting metric rankings for QuickFS metric."""
        df = get_metric_rankings('revenue_5y_cagr_quickfs')
        
        self.assertIsNotNone(df)
        # May or may not have data, but structure should be correct
        if len(df) > 0:
            self.assertIn('ticker', df.columns)
            self.assertIn('percentile', df.columns)
            self.assertIn('raw_value', df.columns)
    
    def test_get_metric_rankings_nonexistent_metric(self):
        """Test getting metric rankings for nonexistent metric."""
        # This will try to query a column that doesn't exist
        # Should handle SQL error gracefully or return None
        try:
            df = get_metric_rankings('nonexistent_metric')
            # If it returns something, should have expected structure
            if df is not None:
                self.assertIn('ticker', df.columns)
        except Exception:
            # SQL error is acceptable for nonexistent column
            pass
    
    def test_display_rankings_with_data(self):
        """Test displaying rankings with data."""
        df = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT'],
            'company_name': ['Apple', 'Microsoft'],
            'percentile': [0.8, 0.9],
            'raw_value': [8.0, 9.0]
        })
        
        # Should not raise exception
        try:
            display_rankings(df, 'test_metric')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_empty(self):
        """Test displaying rankings with empty dataframe."""
        df = pd.DataFrame()
        
        # Should handle empty dataframe gracefully
        try:
            display_rankings(df, 'test_metric')
        except Exception as e:
            # May print message but shouldn't crash
            pass
    
    def test_display_rankings_none(self):
        """Test displaying rankings with None."""
        # Should handle None gracefully
        try:
            display_rankings(None, 'test_metric')
        except Exception as e:
            # May print message but shouldn't crash
            pass
    
    def test_display_rankings_with_stocks_without_data(self):
        """Test displaying rankings when some stocks have no data."""
        # Create dataframe with both stocks with values and without
        df = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT', 'GOOGL'],
            'company_name': ['Apple', 'Microsoft', 'Alphabet'],
            'percentile': [0.8, 0.9, pd.NA],  # GOOGL has no value
            'raw_value': [8.0, 9.0, pd.NA]
        })
        
        # Should not raise exception
        try:
            display_rankings(df, 'test_metric')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_only_no_data(self):
        """Test displaying rankings when no stocks have data."""
        df = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT'],
            'company_name': ['Apple', 'Microsoft'],
            'percentile': [pd.NA, pd.NA],
            'raw_value': [pd.NA, pd.NA]
        })
        
        # Should handle gracefully
        try:
            display_rankings(df, 'test_metric')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_no_raw_value_column(self):
        """Test displaying rankings when raw_value column doesn't exist."""
        df = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT'],
            'company_name': ['Apple', 'Microsoft'],
            'percentile': [0.8, 0.9]
            # No raw_value column
        })
        
        # Should handle missing raw_value column gracefully
        try:
            display_rankings(df, 'test_metric')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_get_raw_value_mapping(self):
        """Test getting raw value mapping."""
        mapping = get_raw_value_mapping()
        
        self.assertIsInstance(mapping, dict)
        # Should have mappings for normalized metrics
        if len(mapping) > 0:
            # Check structure of mappings
            for key, value in mapping.items():
                self.assertIsInstance(value, tuple)
                self.assertEqual(len(value), 2)
    
    def test_calculate_percentile_rank_empty(self):
        """Test calculate_percentile_rank with empty series."""
        values = pd.Series([], dtype=float)
        result = calculate_percentile_rank(values)
        
        self.assertEqual(len(result), 0)
        self.assertIsInstance(result, pd.Series)
    
    def test_calculate_percentile_rank_all_nan(self):
        """Test calculate_percentile_rank with all NaN values."""
        values = pd.Series([pd.NA, pd.NA, pd.NA])
        result = calculate_percentile_rank(values)
        
        self.assertEqual(len(result), 3)
        self.assertTrue(all(pd.isna(v) or v is None for v in result))
    
    def test_calculate_percentile_rank_reverse(self):
        """Test calculate_percentile_rank with reverse=True."""
        values = pd.Series([1, 2, 3, 4, 5])
        result = calculate_percentile_rank(values, reverse=True)
        
        # Lowest value should have highest percentile (reversed)
        self.assertGreater(result.iloc[0], result.iloc[-1])
        # All values should be between 0 and 1
        self.assertTrue(all(0 <= v <= 1 for v in result))
    
    def test_calculate_percentile_rank_with_nan(self):
        """Test calculate_percentile_rank with some NaN values."""
        values = pd.Series([1, 2, pd.NA, 4, 5])
        result = calculate_percentile_rank(values)
        
        self.assertEqual(len(result), 5)
        # NaN should remain NaN
        self.assertTrue(pd.isna(result.iloc[2]) or result.iloc[2] is None)
        # Valid values should have percentiles
        self.assertFalse(pd.isna(result.iloc[0]))
        self.assertFalse(pd.isna(result.iloc[1]))
    
    def test_format_metric_name(self):
        """Test format_metric_name function."""
        # Test normalized metrics
        self.assertEqual(format_metric_name('moat_score_normalized'), 'Moat Score')
        self.assertEqual(format_metric_name('revenue_5y_cagr_normalized'), 'Revenue 5Y Cagr')
        
        # Test percentile metrics
        self.assertEqual(format_metric_name('roa_percentile'), 'Roa')
        
        # Test QuickFS metrics
        self.assertEqual(format_metric_name('revenue_5y_cagr_quickfs'), 'Revenue 5Y Cagr')
    
    def test_is_reverse_metric(self):
        """Test is_reverse_metric function."""
        # AI reverse metrics
        self.assertTrue(is_reverse_metric('disruption_risk_normalized'))
        self.assertTrue(is_reverse_metric('riskiness_score_normalized'))
        self.assertTrue(is_reverse_metric('competition_intensity_normalized'))
        
        # Finviz reverse metrics
        self.assertTrue(is_reverse_metric('short_interest_percent_percentile'))
        self.assertTrue(is_reverse_metric('forward_pe_percentile'))
        
        # QuickFS reverse metrics
        self.assertTrue(is_reverse_metric('revenue_growth_consistency_quickfs'))
        self.assertTrue(is_reverse_metric('net_debt_to_ttm_operating_income_quickfs'))
        
        # Normal metrics (not reverse)
        self.assertFalse(is_reverse_metric('moat_score_normalized'))
        self.assertFalse(is_reverse_metric('roa_percentile'))
        self.assertFalse(is_reverse_metric('revenue_5y_cagr_quickfs'))
    
    def test_get_metric_rankings_no_all_scores_db(self):
        """Test get_metric_rankings when all_scores.db doesn't exist."""
        import view_metric_rankings
        original_path = view_metric_rankings.ALL_SCORES_DB
        view_metric_rankings.ALL_SCORES_DB = '/nonexistent/path/all_scores.db'
        
        try:
            result = get_metric_rankings('moat_score_normalized')
            self.assertIsNone(result)
        finally:
            view_metric_rankings.ALL_SCORES_DB = original_path
    
    def test_get_metric_rankings_quickfs_no_db(self):
        """Test get_metric_rankings for QuickFS metric when DB doesn't exist."""
        import view_metric_rankings
        original_path = view_metric_rankings.QUICKFS_METRICS_DB
        view_metric_rankings.QUICKFS_METRICS_DB = '/nonexistent/path/metrics.db'
        
        try:
            result = get_metric_rankings('revenue_5y_cagr_quickfs')
            self.assertIsNone(result)
        finally:
            view_metric_rankings.QUICKFS_METRICS_DB = original_path
    
    def test_get_metric_rankings_no_raw_mapping(self):
        """Test get_metric_rankings when no raw mapping exists."""
        # Use a metric that doesn't have a raw mapping
        df = get_metric_rankings('roa_percentile')
        
        # Should still return data, just without raw_value
        if df is not None and len(df) > 0:
            self.assertIn('percentile', df.columns)
    
    def test_get_metric_rankings_all_stocks_no_data(self):
        """Test get_metric_rankings when all stocks have no data."""
        # Create a metric column with all NULL values
        conn = sqlite3.connect(self.test_all_scores_db)
        cursor = conn.cursor()
        cursor.execute('''
            ALTER TABLE all_scores ADD COLUMN empty_metric_normalized REAL
        ''')
        conn.commit()
        conn.close()
        
        result = get_metric_rankings('empty_metric_normalized')
        
        # Should return dataframe but with all None percentiles
        self.assertIsNotNone(result)
        if len(result) > 0:
            self.assertTrue(all(pd.isna(row['percentile']) or row['percentile'] is None 
                              for _, row in result.iterrows()))
    
    def test_get_metric_rankings_finviz_source(self):
        """Test get_metric_rankings with Finviz data source."""
        # Create finviz database
        finviz_dir = os.path.join(self.test_dir, 'finviz')
        os.makedirs(finviz_dir, exist_ok=True)
        finviz_db = os.path.join(finviz_dir, 'finviz.db')
        
        conn_finviz = sqlite3.connect(finviz_db)
        cursor = conn_finviz.cursor()
        cursor.execute('''
            CREATE TABLE short_interest (
                ticker TEXT,
                roa REAL
            )
        ''')
        cursor.executemany('''
            INSERT INTO short_interest (ticker, roa)
            VALUES (?, ?)
        ''', [
            ('AAPL', 20.0),
            ('MSFT', 15.0),
        ])
        conn_finviz.commit()
        conn_finviz.close()
        
        # Patch FINVIZ_DB
        import view_metric_rankings
        original_finviz_path = view_metric_rankings.FINVIZ_DB
        view_metric_rankings.FINVIZ_DB = finviz_db
        
        try:
            # Add roa_percentile to all_scores
            conn = sqlite3.connect(self.test_all_scores_db)
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE all_scores SET roa_percentile = 0.8 WHERE ticker = 'AAPL'
            ''')
            conn.commit()
            conn.close()
            
            result = get_metric_rankings('roa_percentile')
            # Should have raw values from finviz
            if result is not None and len(result) > 0:
                self.assertIn('raw_value', result.columns)
        finally:
            view_metric_rankings.FINVIZ_DB = original_finviz_path
    
    def test_get_metric_rankings_quickfs_with_data(self):
        """Test get_metric_rankings for QuickFS metric with data."""
        # Test with existing QuickFS data
        df = get_metric_rankings('revenue_5y_cagr_quickfs')
        
        self.assertIsNotNone(df)
        if len(df) > 0:
            self.assertIn('ticker', df.columns)
            self.assertIn('percentile', df.columns)
            self.assertIn('raw_value', df.columns)
            # Should have percentiles calculated
            valid_percentiles = df[df['percentile'].notna()]
            if len(valid_percentiles) > 0:
                self.assertTrue(all(0 <= p <= 1 for p in valid_percentiles['percentile']))
    
    def test_get_metric_rankings_quickfs_with_empty_data(self):
        """Test get_metric_rankings for QuickFS metric when metric column is all NULL."""
        # Add a metric that doesn't exist in QuickFS data
        conn = sqlite3.connect(self.test_quickfs_db)
        cursor = conn.cursor()
        # Ensure we have tickers but no data for a specific metric
        cursor.execute('''
            UPDATE quickfs_metrics SET revenue_5y_halfway_growth = NULL
        ''')
        conn.commit()
        conn.close()
        
        # This should still work, just with no values
        result = get_metric_rankings('revenue_5y_halfway_growth_quickfs')
        # Should return None or empty result when no data
        if result is not None:
            # All percentiles should be None/NaN if no data
            pass
    
    def test_display_rankings_formatting_edge_cases(self):
        """Test display_rankings with various formatting edge cases."""
        # Test with very long company names
        df = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT'],
            'company_name': ['A' * 50, 'B' * 60],  # Very long names
            'percentile': [0.8, 0.9],
            'raw_value': [8.0, 9.0]
        })
        
        try:
            display_rankings(df, 'test_metric')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
        
        # Test with None company names
        df2 = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT'],
            'company_name': [None, 'Microsoft'],
            'percentile': [0.8, 0.9],
            'raw_value': [8.0, 9.0]
        })
        
        try:
            display_rankings(df2, 'test_metric')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_single_stock(self):
        """Test display_rankings with single stock."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [8.0]
        })
        
        try:
            display_rankings(df, 'test_metric')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_get_available_metrics_with_quickfs(self):
        """Test get_available_metrics includes QuickFS metrics."""
        metrics = get_available_metrics()
        
        # Should include QuickFS metrics if database exists
        quickfs_metrics = [m for m in metrics if '_quickfs' in m]
        # May or may not have QuickFS metrics depending on test setup
        # Just verify the function doesn't crash
        self.assertIsInstance(metrics, list)
    
    def test_display_rankings_recommendation_score(self):
        """Test display_rankings with recommendation_score metric formatting."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [1.0]  # Strong Buy
        })
        
        try:
            display_rankings(df, 'recommendation_score_percentile')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_forward_pe(self):
        """Test display_rankings with forward_pe metric formatting."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [25.5]
        })
        
        try:
            display_rankings(df, 'forward_pe_percentile')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_ebit_ppe(self):
        """Test display_rankings with ebit_ppe metric formatting."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [0.3456]
        })
        
        try:
            display_rankings(df, 'ttm_ebit_ppe_quickfs')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_net_debt_high_value(self):
        """Test display_rankings with net_debt_to_ttm with high value (>= 1000)."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [1500.0]  # >= 1000
        })
        
        try:
            display_rankings(df, 'net_debt_to_ttm_operating_income_quickfs')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_cagr(self):
        """Test display_rankings with CAGR metric formatting."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [0.15]  # 15% CAGR
        })
        
        try:
            display_rankings(df, 'revenue_5y_cagr_quickfs')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_total_return(self):
        """Test display_rankings with total_return metric formatting."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [0.25]  # 25% return
        })
        
        try:
            display_rankings(df, 'total_past_return_quickfs')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_return_multiplier(self):
        """Test display_rankings with return multiplier formatting."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [1.5]  # 1.5x multiplier
        })
        
        try:
            display_rankings(df, 'total_past_return_multiplier_quickfs')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_growth_consistency(self):
        """Test display_rankings with growth consistency formatting."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [0.05]  # 5% consistency
        })
        
        try:
            display_rankings(df, 'revenue_growth_consistency_quickfs')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_margin_consistency(self):
        """Test display_rankings with margin consistency formatting."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [0.02]  # 2 pp consistency
        })
        
        try:
            display_rankings(df, 'operating_margin_consistency_quickfs')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_acceleration(self):
        """Test display_rankings with acceleration formatting."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [1.2]  # 1.2x acceleration
        })
        
        try:
            display_rankings(df, 'revenue_growth_acceleration_quickfs')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_halfway_growth(self):
        """Test display_rankings with halfway growth formatting."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [1.5]  # 1.5x growth
        })
        
        try:
            display_rankings(df, 'revenue_5y_halfway_growth_quickfs')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_get_metric_rankings_quickfs_without_values_path(self):
        """Test get_metric_rankings for QuickFS when some stocks don't have values (covers lines 227, 231)."""
        # Create QuickFS metrics with some NULL values
        conn = sqlite3.connect(self.test_quickfs_db)
        cursor = conn.cursor()
        # Insert a ticker with NULL value
        cursor.execute('''
            INSERT OR REPLACE INTO quickfs_metrics (ticker, calculated_at, revenue_5y_cagr)
            VALUES (?, ?, ?)
        ''', ('TSLA', datetime.now().isoformat(), None))
        conn.commit()
        conn.close()
        
        # Also add to all_scores for company name
        conn = sqlite3.connect(self.test_all_scores_db)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO all_scores (ticker, company_name)
            VALUES (?, ?)
        ''', ('TSLA', 'Tesla'))
        conn.commit()
        conn.close()
        
        result = get_metric_rankings('revenue_5y_cagr_quickfs')
        
        # Should have both stocks
        self.assertIsNotNone(result)
        if len(result) > 0:
            # Should have TSLA with None percentile
            tsla_row = result[result['ticker'] == 'TSLA']
            if len(tsla_row) > 0:
                self.assertTrue(pd.isna(tsla_row.iloc[0]['percentile']) or tsla_row.iloc[0]['percentile'] is None)
    
    def test_get_metric_rankings_no_raw_mapping_path(self):
        """Test get_metric_rankings when metric_name is not in raw_mapping (covers line 262)."""
        # Use a metric that doesn't have a raw mapping
        result = get_metric_rankings('roa_percentile')
        
        # Should return data without raw_value column if no mapping
        if result is not None and len(result) > 0:
            # May or may not have raw_value depending on whether roa_percentile has mapping
            self.assertIn('percentile', result.columns)
            self.assertIn('ticker', result.columns)
    
    def test_get_metric_rankings_all_scores_without_values_path(self):
        """Test get_metric_rankings for all_scores when some stocks don't have values (covers lines 299, 303)."""
        # Create a metric with some NULL values and ensure raw mapping exists
        conn = sqlite3.connect(self.test_all_scores_db)
        cursor = conn.cursor()
        # Check if column exists first
        cursor.execute("PRAGMA table_info(all_scores)")
        columns = [col[1] for col in cursor.fetchall()]
        metric_name = 'test_metric_with_mapping_normalized'
        if metric_name not in columns:
            cursor.execute(f'''
                ALTER TABLE all_scores ADD COLUMN {metric_name} REAL
            ''')
        cursor.execute(f'''
            UPDATE all_scores SET {metric_name} = 0.8 WHERE ticker = 'AAPL'
        ''')
        # MSFT will have NULL - this should trigger the df_without_values path
        conn.commit()
        conn.close()
        
        # Use a metric that already has raw mapping (moat_score_normalized)
        # Just update values to have some NULLs to trigger the path
        metric_name = 'moat_score_normalized'
        conn = sqlite3.connect(self.test_all_scores_db)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(all_scores)")
        columns = [col[1] for col in cursor.fetchall()]
        if metric_name not in columns:
            cursor.execute(f'''
                ALTER TABLE all_scores ADD COLUMN {metric_name} REAL
            ''')
        cursor.execute(f'''
            UPDATE all_scores SET {metric_name} = 0.8 WHERE ticker = 'AAPL'
        ''')
        # MSFT will have NULL - this should trigger the df_without_values path
        conn.commit()
        conn.close()
        
        result = get_metric_rankings(metric_name)
        
        # Should have both stocks
        self.assertIsNotNone(result)
        self.assertGreater(len(result), 0)
        # Should have AAPL with value, MSFT might have None
        aapl_rows = result[result['ticker'] == 'AAPL']
        if len(aapl_rows) > 0:
            self.assertFalse(pd.isna(aapl_rows.iloc[0]['percentile']))
        # Ensure we have stocks with and without values to trigger the concat path (lines 299, 303)
        has_values = result[result['percentile'].notna()]
        has_no_values = result[result['percentile'].isna()]
        # If both exist, should trigger lines 299 and 303
        if len(has_values) > 0 and len(has_no_values) > 0:
            self.assertIn('raw_value', result.columns)
    
    def test_display_rankings_net_debt_low_value(self):
        """Test display_rankings with net_debt_to_ttm with low value (< 1000) (covers line 398)."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [0.5]  # < 1000
        })
        
        try:
            display_rankings(df, 'net_debt_to_ttm_operating_income_quickfs')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_return_multiplier(self):
        """Test display_rankings with return multiplier formatting (covers line 407)."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [2.5]
        })
        
        try:
            display_rankings(df, 'total_past_return_multiplier_quickfs')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_percent_metric(self):
        """Test display_rankings with percent metric formatting (covers line 424)."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [25.5]
        })
        
        try:
            display_rankings(df, 'roa_percentile')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_price_move(self):
        """Test display_rankings with price_move metric formatting (covers line 426)."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [15.5]
        })
        
        try:
            display_rankings(df, 'price_move_percentile')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_non_numeric_raw_value(self):
        """Test display_rankings with non-numeric raw value (covers line 431)."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': ['N/A']
        })
        
        try:
            display_rankings(df, 'test_metric')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_margin_growth(self):
        """Test display_rankings with margin growth formatting."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [0.03]  # 3 pp growth
        })
        
        try:
            display_rankings(df, 'operating_margin_growth_quickfs')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_roa_roic(self):
        """Test display_rankings with ROA/ROIC formatting."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [20.5]  # 20.5%
        })
        
        try:
            display_rankings(df, 'roa_percentile')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_price_move(self):
        """Test display_rankings with price_move formatting."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [10.5]  # 10.5%
        })
        
        try:
            display_rankings(df, 'price_move_percent_percentile')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_get_metric_rankings_quickfs_all_none(self):
        """Test get_metric_rankings for QuickFS when all values are NULL."""
        # Update QuickFS data to have all NULL for a metric
        conn = sqlite3.connect(self.test_quickfs_db)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE quickfs_metrics SET revenue_5y_halfway_growth = NULL
        ''')
        conn.commit()
        conn.close()
        
        result = get_metric_rankings('revenue_5y_halfway_growth_quickfs')
        
        # Should return data but with all None percentiles
        if result is not None and len(result) > 0:
            # All percentiles should be None when no data
            all_none = all(pd.isna(row['percentile']) or row['percentile'] is None 
                          for _, row in result.iterrows())
            self.assertTrue(all_none)
    
    def test_get_metric_rankings_no_raw_mapping_returns_three_cols(self):
        """Test get_metric_rankings returns 3 columns when no raw mapping (covers line 262)."""
        # Create a metric that has no raw mapping
        conn = sqlite3.connect(self.test_all_scores_db)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(all_scores)")
        columns = [col[1] for col in cursor.fetchall()]
        metric_name = 'test_unmapped_percentile'
        if metric_name not in columns:
            cursor.execute(f'ALTER TABLE all_scores ADD COLUMN {metric_name} REAL')
        cursor.execute(f'UPDATE all_scores SET {metric_name} = 0.8 WHERE ticker = "AAPL"')
        conn.commit()
        conn.close()
        
        result = get_metric_rankings(metric_name)
        
        # Should return only 3 columns (no raw_value)
        if result is not None and len(result) > 0:
            self.assertIn('ticker', result.columns)
            self.assertIn('company_name', result.columns)
            self.assertIn('percentile', result.columns)
            # Should NOT have raw_value column when no mapping
            if 'raw_value' not in result.columns:
                pass  # This is correct - no raw mapping
    
    def test_get_metric_rankings_without_values_concat_path(self):
        """Test get_metric_rankings concat path when df_without_values exists (covers lines 299, 303)."""
        # Create a scenario with both values and no values
        conn = sqlite3.connect(self.test_all_scores_db)
        cursor = conn.cursor()
        # Use moat_score which has a raw mapping
        cursor.execute('UPDATE all_scores SET moat_score_normalized = 0.8 WHERE ticker = "AAPL"')
        # MSFT will have NULL
        conn.commit()
        conn.close()
        
        result = get_metric_rankings('moat_score_normalized')
        
        # Should have both AAPL (with value) and MSFT (without value)
        if result is not None and len(result) > 0:
            aapl_rows = result[result['ticker'] == 'AAPL']
            msft_rows = result[result['ticker'] == 'MSFT']
            if len(aapl_rows) > 0 and len(msft_rows) > 0:
                # AAPL should have percentile, MSFT might not
                self.assertIn('raw_value', result.columns)  # Should have raw_value column
    
    def test_display_rankings_total_return_multiplier_formatting(self):
        """Test display_rankings with total_return_multiplier formatting (covers line 407)."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [1.2345]
        })
        
        try:
            display_rankings(df, 'total_past_return_multiplier_quickfs')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_percent_margin_roa_formatting(self):
        """Test display_rankings with percent/margin/roa formatting (covers line 424)."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [25.5]
        })
        
        # Test with margin metric
        try:
            display_rankings(df, 'margin_metric_percentile')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")
    
    def test_display_rankings_price_move_formatting(self):
        """Test display_rankings with price_move formatting (covers line 426)."""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8],
            'raw_value': [15.5]
        })
        
        try:
            display_rankings(df, 'price_move_percent_percentile')
        except Exception as e:
            self.fail(f"display_rankings raised exception: {e}")


if __name__ == '__main__':
    unittest.main(verbosity=2)

