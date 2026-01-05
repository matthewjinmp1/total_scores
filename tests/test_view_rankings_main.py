#!/usr/bin/env python3
"""
Tests for view_metric_rankings.py main() function.
"""

import sys
import os
import unittest
import tempfile
import shutil
import sqlite3
from unittest.mock import patch, MagicMock

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import view_metric_rankings


class TestViewRankingsMain(unittest.TestCase):
    """Tests for main() function in view_metric_rankings.py."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.test_all_scores_db = os.path.join(self.test_dir, 'test_all_scores.db')
        
        # Create all_scores database
        conn = sqlite3.connect(self.test_all_scores_db)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE all_scores (
                ticker TEXT PRIMARY KEY,
                company_name TEXT,
                moat_score_normalized REAL
            )
        ''')
        cursor.executemany('''
            INSERT INTO all_scores (ticker, company_name, moat_score_normalized)
            VALUES (?, ?, ?)
        ''', [
            ('AAPL', 'Apple Inc.', 0.8),
            ('MSFT', 'Microsoft', 0.9),
        ])
        conn.commit()
        conn.close()
        
        # Patch database path
        self.original_path = view_metric_rankings.ALL_SCORES_DB
        view_metric_rankings.ALL_SCORES_DB = self.test_all_scores_db
    
    def tearDown(self):
        """Clean up test fixtures."""
        view_metric_rankings.ALL_SCORES_DB = self.original_path
        shutil.rmtree(self.test_dir)
    
    @patch('builtins.input', return_value='quit')
    @patch('sys.stdout')  # Mock stdout instead of print
    def test_main_quit_immediately(self, mock_stdout, mock_input):
        """Test main() function quits immediately with 'quit' input."""
        view_metric_rankings.main()
        # Should have called input
        self.assertGreater(mock_input.call_count, 0)
    
    @patch('builtins.input', side_effect=['1', 'quit'])
    @patch('sys.stdout')
    @patch('view_metric_rankings.get_metric_rankings')
    @patch('view_metric_rankings.display_rankings')
    def test_main_select_metric_then_quit(self, mock_display, mock_get_rankings, mock_stdout, mock_input):
        """Test main() function selects a metric then quits."""
        # Mock return value
        import pandas as pd
        mock_get_rankings.return_value = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8]
        })
        
        view_metric_rankings.main()
        
        # Should have called get_metric_rankings and display_rankings
        self.assertGreater(mock_get_rankings.call_count, 0)
        self.assertGreater(mock_display.call_count, 0)
    
    @patch('builtins.input', side_effect=['invalid', 'quit'])
    @patch('sys.stdout')
    def test_main_invalid_input(self, mock_stdout, mock_input):
        """Test main() function handles invalid input."""
        view_metric_rankings.main()
        # Should have called input multiple times (invalid then quit)
        self.assertGreaterEqual(mock_input.call_count, 2)
    
    @patch('builtins.input', side_effect=['999', 'quit'])
    @patch('sys.stdout')
    def test_main_invalid_metric_number(self, mock_stdout, mock_input):
        """Test main() function handles out-of-range metric number."""
        view_metric_rankings.main()
        # Should have called input multiple times
        self.assertGreaterEqual(mock_input.call_count, 2)
    
    @patch('builtins.input', side_effect=['1', 'quit'])
    @patch('sys.stdout')
    @patch('view_metric_rankings.get_metric_rankings', side_effect=Exception("Test error"))
    def test_main_error_getting_rankings(self, mock_get_rankings, mock_stdout, mock_input):
        """Test main() function handles error when getting rankings."""
        view_metric_rankings.main()
        # Should have handled the error
        self.assertGreater(mock_get_rankings.call_count, 0)
    
    @patch('builtins.input', return_value='quit')
    @patch('sys.stdout')
    def test_main_no_metrics(self, mock_stdout, mock_input):
        """Test main() function when no metrics are available."""
        # Create empty database
        conn = sqlite3.connect(self.test_all_scores_db)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM all_scores')
        conn.commit()
        conn.close()
        
        view_metric_rankings.main()
        # Function should return early when no metrics - input should not be called
    
    @patch('builtins.input', side_effect=['1', 'quit'])
    @patch('sys.stdout')
    @patch('view_metric_rankings.get_metric_rankings')
    @patch('view_metric_rankings.display_rankings')
    def test_main_with_quickfs_metrics(self, mock_display, mock_get_rankings, mock_stdout, mock_input):
        """Test main() function displays QuickFS metrics (covers lines 507-512)."""
        import pandas as pd
        mock_get_rankings.return_value = pd.DataFrame({
            'ticker': ['AAPL'],
            'company_name': ['Apple'],
            'percentile': [0.8]
        })
        
        # Add QuickFS metric to database
        conn = sqlite3.connect(self.test_all_scores_db)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(all_scores)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'revenue_5y_cagr_quickfs' not in columns:
            cursor.execute('ALTER TABLE all_scores ADD COLUMN revenue_5y_cagr_quickfs REAL')
        conn.commit()
        conn.close()
        
        view_metric_rankings.main()
        # Should have attempted to get rankings
        self.assertGreater(mock_get_rankings.call_count, 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)

