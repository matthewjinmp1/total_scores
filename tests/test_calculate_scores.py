#!/usr/bin/env python3
"""
Tests for calculate_total_scores.py functionality.
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
    REVERSE_METRICS,
    get_ai_score_columns
)


class TestCalculateScores(unittest.TestCase):
    """Test score calculation functions."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.test_ai_db = os.path.join(self.test_dir, 'test_ai_scores.db')
        
        # Create test AI scores database
        conn = sqlite3.connect(self.test_ai_db)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                company_name TEXT,
                model TEXT,
                timestamp TEXT,
                moat_score REAL,
                barriers_score REAL,
                brand_strength REAL
            )
        ''')
        conn.commit()
        conn.close()
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.test_dir)
    
    def test_convert_recommendation_strong_buy(self):
        """Test recommendation conversion for Strong Buy."""
        result = convert_recommendation_to_score('Strong Buy')
        self.assertEqual(result, 1.0)
    
    def test_convert_recommendation_buy(self):
        """Test recommendation conversion for Buy."""
        result = convert_recommendation_to_score('Buy')
        self.assertEqual(result, 2.0)
    
    def test_convert_recommendation_hold(self):
        """Test recommendation conversion for Hold."""
        result = convert_recommendation_to_score('Hold')
        self.assertEqual(result, 3.0)
    
    def test_convert_recommendation_sell(self):
        """Test recommendation conversion for Sell."""
        result = convert_recommendation_to_score('Sell')
        self.assertEqual(result, 4.0)
    
    def test_convert_recommendation_strong_sell(self):
        """Test recommendation conversion for Strong Sell."""
        result = convert_recommendation_to_score('Strong Sell')
        self.assertEqual(result, 5.0)
    
    def test_convert_recommendation_numeric(self):
        """Test recommendation conversion for numeric values."""
        self.assertEqual(convert_recommendation_to_score(1.0), 1.0)
        self.assertEqual(convert_recommendation_to_score(2.5), 2.5)
        self.assertEqual(convert_recommendation_to_score(5.0), 5.0)
    
    def test_convert_recommendation_none(self):
        """Test recommendation conversion for None."""
        self.assertIsNone(convert_recommendation_to_score(None))
        self.assertIsNone(convert_recommendation_to_score(pd.NA))
    
    def test_convert_recommendation_invalid(self):
        """Test recommendation conversion for invalid values."""
        self.assertIsNone(convert_recommendation_to_score('Invalid'))
        self.assertIsNone(convert_recommendation_to_score(10.0))  # Out of range
    
    def test_reverse_metrics_list(self):
        """Test that REVERSE_METRICS contains expected metrics."""
        expected_metrics = [
            'disruption_risk',
            'riskiness_score',
            'competition_intensity',
            'bargaining_power_of_customers',
            'bargaining_power_of_suppliers',
            'size_well_known_score'
        ]
        for metric in expected_metrics:
            self.assertIn(metric, REVERSE_METRICS)
    
    def test_get_ai_score_columns(self):
        """Test getting AI score columns from database."""
        # Add some test data
        conn = sqlite3.connect(self.test_ai_db)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO scores (ticker, company_name, model, timestamp,
                               moat_score, barriers_score, brand_strength)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', ('AAPL', 'Apple', 'test', '2024-01-01', 8.0, 7.0, 9.0))
        conn.commit()
        conn.close()
        
        # Temporarily patch the AI_SCORES_DB path
        import calculate_total_scores
        original_path = calculate_total_scores.AI_SCORES_DB
        calculate_total_scores.AI_SCORES_DB = self.test_ai_db
        
        try:
            columns = get_ai_score_columns()
            self.assertIsInstance(columns, list)
            self.assertIn('moat_score', columns)
            self.assertIn('barriers_score', columns)
            self.assertIn('brand_strength', columns)
            # Should not include non-score columns
            self.assertNotIn('id', columns)
            self.assertNotIn('ticker', columns)
            self.assertNotIn('company_name', columns)
        finally:
            calculate_total_scores.AI_SCORES_DB = original_path


if __name__ == '__main__':
    unittest.main(verbosity=2)

