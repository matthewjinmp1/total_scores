#!/usr/bin/env python3
"""
Tests for recalculate_all_metrics.py
"""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock, Mock
import sqlite3
import tempfile
import shutil

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestRecalculateAllMetrics(unittest.TestCase):
    """Test cases for recalculate_all_metrics.py"""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @patch('recalculate_all_metrics.sys.path')
    def test_run_quickfs_calculations_success(self, mock_sys_path):
        """Test successful QuickFS calculations."""
        import recalculate_all_metrics
        
        # Mock the calculate_all_metrics module
        mock_calc_module = MagicMock()
        mock_calc_module.init_metrics_db = Mock()
        mock_calc_module.get_all_tickers = Mock(return_value=['AAPL', 'MSFT', 'GOOGL'])
        mock_calc_module.calculate_all_metrics_for_ticker = Mock(side_effect=[
            ({'revenue_5y_cagr': 0.15, 'revenue_5y_halfway_growth': 0.12}, None),
            ({'revenue_5y_cagr': 0.20, 'revenue_5y_halfway_growth': 0.18}, None),
            ({'revenue_5y_cagr': 0.25, 'revenue_5y_halfway_growth': 0.22}, None),
        ])
        mock_calc_module.save_metrics = Mock(return_value=True)
        mock_calc_module.METRICS_DB = 'quickfs/metrics.db'
        
        # Mock the import
        with patch.dict('sys.modules', {'calculate_all_metrics': mock_calc_module}):
            # Mock __import__ to return our mock module
            original_import = __import__
            def mock_import(name, *args, **kwargs):
                if name == 'calculate_all_metrics':
                    return mock_calc_module
                return original_import(name, *args, **kwargs)
            
            with patch('builtins.__import__', side_effect=mock_import):
                result = recalculate_all_metrics.run_quickfs_calculations(skip_prompt=True)
                
                # Verify result
                self.assertTrue(result)
                
                # Verify functions were called
                mock_calc_module.init_metrics_db.assert_called_once()
                mock_calc_module.get_all_tickers.assert_called_once()
                self.assertEqual(mock_calc_module.calculate_all_metrics_for_ticker.call_count, 3)
                self.assertEqual(mock_calc_module.save_metrics.call_count, 3)
    
    def test_run_quickfs_calculations_no_tickers(self):
        """Test QuickFS calculations when no tickers are found."""
        import recalculate_all_metrics
        
        # Mock the calculate_all_metrics module
        mock_calc_module = MagicMock()
        mock_calc_module.init_metrics_db = Mock()
        mock_calc_module.get_all_tickers = Mock(return_value=[])
        mock_calc_module.METRICS_DB = 'quickfs/metrics.db'
        
        # Mock the import
        with patch.dict('sys.modules', {'calculate_all_metrics': mock_calc_module}):
            original_import = __import__
            def mock_import(name, *args, **kwargs):
                if name == 'calculate_all_metrics':
                    return mock_calc_module
                return original_import(name, *args, **kwargs)
            
            with patch('builtins.__import__', side_effect=mock_import):
                result = recalculate_all_metrics.run_quickfs_calculations(skip_prompt=True)
                
                # Verify result
                self.assertFalse(result)
    
    def test_run_quickfs_calculations_import_error(self):
        """Test QuickFS calculations with import error."""
        import recalculate_all_metrics
        
        # Mock import error
        def mock_import(name, *args, **kwargs):
            if name == 'calculate_all_metrics':
                raise ImportError("No module named 'calculate_all_metrics'")
            return __import__(name, *args, **kwargs)
        
        with patch('builtins.__import__', side_effect=mock_import):
            result = recalculate_all_metrics.run_quickfs_calculations(skip_prompt=True)
            
            # Verify result
            self.assertFalse(result)
    
    @patch('sqlite3.connect')
    @patch('recalculate_all_metrics.os.path.exists')
    def test_run_total_scores_calculation_success(self, mock_exists, mock_connect):
        """Test successful total scores calculation."""
        import recalculate_all_metrics
        import pandas as pd
        
        # Mock database exists and has scores table
        mock_exists.return_value = True
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ('scores',)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Mock calculate_total_scores module
        mock_calc_module = MagicMock()
        mock_df = pd.DataFrame({'ticker': ['AAPL'], 'company_name': ['Apple Inc.']})
        mock_calc_module.get_overlapping_companies = Mock(return_value=mock_df)
        mock_calc_module.get_ai_score_columns = Mock(return_value=['moat_score'])
        mock_calc_module.normalize_ai_scores = Mock(return_value=mock_df)
        mock_calc_module.calculate_total_scores = Mock(return_value=mock_df)
        mock_calc_module.display_results = Mock(return_value=mock_df)
        mock_calc_module.save_results = Mock(return_value='all_scores.db')
        
        # Mock the import
        with patch.dict('sys.modules', {'calculate_total_scores': mock_calc_module}):
            original_import = __import__
            def mock_import(name, *args, **kwargs):
                if name == 'calculate_total_scores':
                    return mock_calc_module
                return original_import(name, *args, **kwargs)
            
            with patch('builtins.__import__', side_effect=mock_import):
                result = recalculate_all_metrics.run_total_scores_calculation()
                
                # Verify result
                self.assertTrue(result)
                
                # Verify functions were called
                mock_calc_module.get_overlapping_companies.assert_called_once()
                mock_calc_module.get_ai_score_columns.assert_called_once()
    
    @patch('sqlite3.connect')
    @patch('recalculate_all_metrics.os.path.exists')
    def test_run_total_scores_calculation_no_scores_table(self, mock_exists, mock_connect):
        """Test total scores calculation when scores table doesn't exist."""
        import recalculate_all_metrics
        
        # Mock database exists but no scores table
        mock_exists.return_value = True
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # Table doesn't exist
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        result = recalculate_all_metrics.run_total_scores_calculation()
        
        # Verify result
        self.assertFalse(result)
    
    @patch('sqlite3.connect')
    @patch('recalculate_all_metrics.os.path.exists')
    def test_run_total_scores_calculation_no_overlapping_companies(self, mock_exists, mock_connect):
        """Test total scores calculation when no overlapping companies."""
        import recalculate_all_metrics
        
        # Mock database exists and has scores table
        mock_exists.return_value = True
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ('scores',)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Mock calculate_total_scores module
        mock_calc_module = MagicMock()
        mock_calc_module.get_overlapping_companies = Mock(return_value=None)
        mock_calc_module.get_ai_score_columns = Mock(return_value=[])
        
        # Mock the import
        with patch.dict('sys.modules', {'calculate_total_scores': mock_calc_module}):
            original_import = __import__
            def mock_import(name, *args, **kwargs):
                if name == 'calculate_total_scores':
                    return mock_calc_module
                return original_import(name, *args, **kwargs)
            
            with patch('builtins.__import__', side_effect=mock_import):
                result = recalculate_all_metrics.run_total_scores_calculation()
                
                # Verify result
                self.assertFalse(result)
    
    @patch('recalculate_all_metrics.run_quickfs_calculations')
    @patch('recalculate_all_metrics.run_total_scores_calculation')
    @patch('recalculate_all_metrics.os.path.exists')
    def test_main_success(self, mock_exists, mock_total_scores, mock_quickfs):
        """Test main function with successful execution."""
        import recalculate_all_metrics
        
        # Setup mocks
        mock_exists.return_value = True
        mock_quickfs.return_value = True
        mock_total_scores.return_value = True
        
        # Mock sys.argv
        with patch('sys.argv', ['recalculate_all_metrics.py']):
            recalculate_all_metrics.main()
            
            # Verify functions were called
            mock_quickfs.assert_called_once_with(skip_prompt=True)
            mock_total_scores.assert_called_once()
    
    @patch('recalculate_all_metrics.run_quickfs_calculations')
    @patch('recalculate_all_metrics.run_total_scores_calculation')
    @patch('recalculate_all_metrics.os.path.exists')
    def test_main_with_prompt_flag(self, mock_exists, mock_total_scores, mock_quickfs):
        """Test main function with --prompt flag."""
        import recalculate_all_metrics
        
        # Setup mocks
        mock_exists.return_value = True
        mock_quickfs.return_value = True
        mock_total_scores.return_value = True
        
        # Mock sys.argv with --prompt flag
        with patch('sys.argv', ['recalculate_all_metrics.py', '--prompt']):
            recalculate_all_metrics.main()
            
            # Verify functions were called with skip_prompt=False
            mock_quickfs.assert_called_once_with(skip_prompt=False)
            mock_total_scores.assert_called_once()
    
    @patch('recalculate_all_metrics.run_quickfs_calculations')
    @patch('recalculate_all_metrics.run_total_scores_calculation')
    @patch('recalculate_all_metrics.os.path.exists')
    def test_main_with_short_prompt_flag(self, mock_exists, mock_total_scores, mock_quickfs):
        """Test main function with -p flag."""
        import recalculate_all_metrics
        
        # Setup mocks
        mock_exists.return_value = True
        mock_quickfs.return_value = True
        mock_total_scores.return_value = True
        
        # Mock sys.argv with -p flag
        with patch('sys.argv', ['recalculate_all_metrics.py', '-p']):
            recalculate_all_metrics.main()
            
            # Verify functions were called with skip_prompt=False
            mock_quickfs.assert_called_once_with(skip_prompt=False)
    
    @patch('recalculate_all_metrics.run_quickfs_calculations')
    @patch('recalculate_all_metrics.run_total_scores_calculation')
    @patch('recalculate_all_metrics.os.path.exists')
    def test_main_quickfs_failure_continues(self, mock_exists, mock_total_scores, mock_quickfs):
        """Test main function continues when QuickFS fails (non-interactive mode)."""
        import recalculate_all_metrics
        
        # Setup mocks
        mock_exists.return_value = True
        mock_quickfs.return_value = False
        mock_total_scores.return_value = True
        
        # Mock sys.argv
        with patch('sys.argv', ['recalculate_all_metrics.py']):
            recalculate_all_metrics.main()
            
            # Verify functions were called
            mock_quickfs.assert_called_once_with(skip_prompt=True)
            mock_total_scores.assert_called_once()  # Should still be called
    
    @patch('recalculate_all_metrics.run_quickfs_calculations')
    @patch('recalculate_all_metrics.run_total_scores_calculation')
    @patch('recalculate_all_metrics.os.path.exists')
    def test_main_missing_databases(self, mock_exists, mock_total_scores, mock_quickfs):
        """Test main function with missing databases."""
        import recalculate_all_metrics
        
        # Setup mocks - simulate missing database
        def exists_side_effect(path):
            return 'quickfs/data.db' not in path  # QuickFS data.db missing
        
        mock_exists.side_effect = exists_side_effect
        mock_quickfs.return_value = True
        mock_total_scores.return_value = True
        
        # Mock sys.argv
        with patch('sys.argv', ['recalculate_all_metrics.py']):
            recalculate_all_metrics.main()
            
            # Verify functions were still called (continues automatically)
            mock_quickfs.assert_called_once_with(skip_prompt=True)
            mock_total_scores.assert_called_once()


if __name__ == '__main__':
    unittest.main()
