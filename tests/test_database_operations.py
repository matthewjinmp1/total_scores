#!/usr/bin/env python3
"""
Tests for database operations and data integrity.
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


class TestDatabaseOperations(unittest.TestCase):
    """Test database operations and data integrity."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.test_db = os.path.join(self.test_dir, 'test_all_scores.db')
        
        # Create test database
        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE all_scores (
                ticker TEXT PRIMARY KEY,
                company_name TEXT,
                metrics_count INTEGER,
                moat_score_normalized REAL,
                barriers_score_normalized REAL,
                brand_strength_normalized REAL,
                riskiness_score_normalized REAL
            )
        ''')
        
        # Insert test data
        test_data = [
            ('AAPL', 'Apple Inc.', 4, 0.8, 0.7, 0.9, 0.3),
            ('MSFT', 'Microsoft Corporation', 4, 0.9, 0.8, 0.85, 0.25),
            ('GOOGL', 'Alphabet Inc.', 4, 0.85, 0.75, 0.88, 0.35),
        ]
        
        cursor.executemany('''
            INSERT INTO all_scores 
            (ticker, company_name, metrics_count, moat_score_normalized,
             barriers_score_normalized, brand_strength_normalized, riskiness_score_normalized)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', test_data)
        
        conn.commit()
        conn.close()
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.test_dir)
    
    def test_database_connection(self):
        """Test database connection."""
        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM all_scores')
        count = cursor.fetchone()[0]
        conn.close()
        
        self.assertEqual(count, 3)
    
    def test_database_schema(self):
        """Test database schema."""
        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(all_scores)")
        columns = [row[1] for row in cursor.fetchall()]
        conn.close()
        
        expected_columns = [
            'ticker', 'company_name', 'metrics_count',
            'moat_score_normalized', 'barriers_score_normalized',
            'brand_strength_normalized', 'riskiness_score_normalized'
        ]
        
        for col in expected_columns:
            self.assertIn(col, columns)
    
    def test_data_retrieval(self):
        """Test data retrieval from database."""
        conn = sqlite3.connect(self.test_db)
        df = pd.read_sql_query('SELECT * FROM all_scores', conn)
        conn.close()
        
        self.assertEqual(len(df), 3)
        self.assertIn('AAPL', df['ticker'].values)
        self.assertIn('MSFT', df['ticker'].values)
        self.assertIn('GOOGL', df['ticker'].values)
    
    def test_metric_columns_exist(self):
        """Test that metric columns exist."""
        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(all_scores)")
        columns = [row[1] for row in cursor.fetchall()]
        conn.close()
        
        metric_columns = [col for col in columns if '_normalized' in col or '_percentile' in col]
        self.assertGreater(len(metric_columns), 0)
    
    def test_null_handling(self):
        """Test handling of NULL values."""
        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()
        
        # Insert row with NULL metric
        cursor.execute('''
            INSERT INTO all_scores 
            (ticker, company_name, metrics_count, moat_score_normalized)
            VALUES (?, ?, ?, ?)
        ''', ('TEST', 'Test Company', 1, None))
        
        conn.commit()
        
        # Query with NULL
        cursor.execute('''
            SELECT * FROM all_scores WHERE moat_score_normalized IS NULL
        ''')
        rows = cursor.fetchall()
        conn.close()
        
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 'TEST')
    
    def test_case_insensitive_ticker_search(self):
        """Test case-insensitive ticker search."""
        conn = sqlite3.connect(self.test_db)
        
        # Test uppercase
        df1 = pd.read_sql_query(
            "SELECT * FROM all_scores WHERE UPPER(ticker) = UPPER('AAPL')",
            conn
        )
        
        # Test lowercase - also uppercase the comparison string
        df2 = pd.read_sql_query(
            "SELECT * FROM all_scores WHERE UPPER(ticker) = UPPER('aapl')",
            conn
        )
        
        conn.close()
        
        self.assertEqual(len(df1), 1)
        self.assertEqual(len(df2), 1)
        self.assertEqual(df1.iloc[0]['ticker'], df2.iloc[0]['ticker'])
    
    def test_percentile_values_in_range(self):
        """Test that percentile values are in valid range (0-1)."""
        conn = sqlite3.connect(self.test_db)
        df = pd.read_sql_query('SELECT * FROM all_scores', conn)
        conn.close()
        
        # Check normalized columns
        normalized_cols = [col for col in df.columns if '_normalized' in col]
        
        for col in normalized_cols:
            if col in df.columns:
                values = df[col].dropna()
                if len(values) > 0:
                    self.assertTrue(all((values >= 0) & (values <= 1)),
                                  f"Values in {col} are not in range [0, 1]")


if __name__ == '__main__':
    unittest.main(verbosity=2)

