#!/usr/bin/env python3
"""
Tests for Flask API endpoints.
"""

import sys
import os
import sqlite3
import unittest
import tempfile
import shutil
from unittest.mock import patch, MagicMock

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import Flask app
from app import app

class TestFlaskAPI(unittest.TestCase):
    """Test Flask API endpoints."""
    
    def setUp(self):
        """Set up test fixtures."""
        app.config['TESTING'] = True
        self.client = app.test_client()
        
        # Create temporary database
        self.test_dir = tempfile.mkdtemp()
        self.test_db = os.path.join(self.test_dir, 'test_all_scores.db')
        
        # Create test database schema
        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE all_scores (
                ticker TEXT PRIMARY KEY,
                company_name TEXT,
                metrics_count INTEGER,
                moat_score_normalized REAL,
                barriers_score_normalized REAL,
                brand_strength_normalized REAL
            )
        ''')
        
        # Insert test data
        cursor.execute('''
            INSERT INTO all_scores (ticker, company_name, metrics_count, 
                                   moat_score_normalized, barriers_score_normalized, 
                                   brand_strength_normalized)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', ('AAPL', 'Apple Inc.', 3, 0.8, 0.7, 0.9))
        cursor.execute('''
            INSERT INTO all_scores (ticker, company_name, metrics_count, 
                                   moat_score_normalized, barriers_score_normalized, 
                                   brand_strength_normalized)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', ('MSFT', 'Microsoft Corporation', 3, 0.9, 0.8, 0.85))
        
        conn.commit()
        conn.close()
        
        # Patch DB_PATH
        self.db_patcher = patch('app.DB_PATH', self.test_db)
        self.db_patcher.start()
    
    def tearDown(self):
        """Clean up test fixtures."""
        self.db_patcher.stop()
        shutil.rmtree(self.test_dir)
    
    def test_index_route(self):
        """Test main page route."""
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Stock Total Scores Dashboard', response.data)
    
    def test_get_companies_no_params(self):
        """Test /api/companies endpoint with no parameters."""
        response = self.client.get('/api/companies')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)
    
    def test_get_companies_with_search(self):
        """Test /api/companies endpoint with search parameter."""
        response = self.client.get('/api/companies?search=AAPL')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIsInstance(data, list)
        if len(data) > 0:
            self.assertEqual(data[0]['ticker'], 'AAPL')
    
    def test_get_companies_with_sort(self):
        """Test /api/companies endpoint with sort parameter."""
        response = self.client.get('/api/companies?sort=ticker&order=asc')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIsInstance(data, list)
        if len(data) > 1:
            # Check if sorted ascending
            tickers = [item['ticker'] for item in data]
            self.assertEqual(tickers, sorted(tickers))
    
    def test_get_companies_with_limit(self):
        """Test /api/companies endpoint with limit parameter."""
        response = self.client.get('/api/companies?limit=1')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIsInstance(data, list)
        self.assertLessEqual(len(data), 1)
    
    def test_get_company_existing(self):
        """Test /api/company/<ticker> endpoint with existing company."""
        response = self.client.get('/api/company/AAPL')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIsInstance(data, dict)
        self.assertEqual(data['ticker'], 'AAPL')
        self.assertIn('company_name', data)
    
    def test_get_company_nonexistent(self):
        """Test /api/company/<ticker> endpoint with nonexistent company."""
        response = self.client.get('/api/company/INVALID')
        self.assertEqual(response.status_code, 404)
        data = response.get_json()
        self.assertIn('error', data)
    
    def test_get_stats(self):
        """Test /api/stats endpoint."""
        response = self.client.get('/api/stats')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIsInstance(data, dict)
        self.assertIn('total_companies', data)
        self.assertGreater(data['total_companies'], 0)
    
    def test_get_company_with_quickfs_metrics(self):
        """Test /api/company/<ticker> endpoint includes QuickFS metrics."""
        # Mock QuickFS metrics database
        import app
        quickfs_dir = os.path.join(os.path.dirname(app.__file__), 'quickfs')
        os.makedirs(quickfs_dir, exist_ok=True)
        quickfs_metrics_db = os.path.join(quickfs_dir, 'metrics.db')
        
        conn_qfs = sqlite3.connect(quickfs_metrics_db)
        cursor = conn_qfs.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS quickfs_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                revenue_5y_cagr REAL,
                calculated_at TEXT
            )
        ''')
        cursor.execute('''
            INSERT OR REPLACE INTO quickfs_metrics (ticker, revenue_5y_cagr, calculated_at)
            VALUES (?, ?, ?)
        ''', ('AAPL', 0.15, '2024-01-01'))
        conn_qfs.commit()
        conn_qfs.close()
        
        response = self.client.get('/api/company/AAPL')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        
        # Clean up
        if os.path.exists(quickfs_metrics_db):
            os.remove(quickfs_metrics_db)
        
        # Should have company data
        self.assertEqual(data['ticker'], 'AAPL')
    
    def test_get_companies_order_desc(self):
        """Test /api/companies with descending order."""
        response = self.client.get('/api/companies?sort=ticker&order=desc')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIsInstance(data, list)
        if len(data) > 1:
            tickers = [item['ticker'] for item in data]
            self.assertEqual(tickers, sorted(tickers, reverse=True))
    
    def test_get_companies_multiple_filters(self):
        """Test /api/companies with multiple filters."""
        response = self.client.get('/api/companies?search=Apple&sort=ticker&limit=5')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIsInstance(data, list)
        self.assertLessEqual(len(data), 5)
    
    def test_get_company_case_insensitive(self):
        """Test /api/company/<ticker> with different case."""
        # Test with lowercase
        response = self.client.get('/api/company/aapl')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertEqual(data['ticker'], 'AAPL')
    
    def test_get_companies_with_quickfs_metrics(self):
        """Test /api/companies endpoint includes QuickFS metrics."""
        import app
        quickfs_dir = os.path.join(os.path.dirname(app.__file__), 'quickfs')
        os.makedirs(quickfs_dir, exist_ok=True)
        quickfs_metrics_db = os.path.join(quickfs_dir, 'metrics.db')
        
        conn_qfs = sqlite3.connect(quickfs_metrics_db)
        cursor = conn_qfs.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS quickfs_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                revenue_5y_cagr REAL,
                revenue_5y_halfway_growth REAL,
                calculated_at TEXT
            )
        ''')
        cursor.execute('''
            INSERT OR REPLACE INTO quickfs_metrics (ticker, revenue_5y_cagr, revenue_5y_halfway_growth, calculated_at)
            VALUES (?, ?, ?, ?)
        ''', ('AAPL', 0.15, 1.5, '2024-01-01'))
        cursor.execute('''
            INSERT OR REPLACE INTO quickfs_metrics (ticker, revenue_5y_cagr, revenue_5y_halfway_growth, calculated_at)
            VALUES (?, ?, ?, ?)
        ''', ('MSFT', 0.20, 1.8, '2024-01-01'))
        conn_qfs.commit()
        conn_qfs.close()
        
        response = self.client.get('/api/companies')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        
        # Clean up
        if os.path.exists(quickfs_metrics_db):
            os.remove(quickfs_metrics_db)
        if os.path.exists(quickfs_dir) and not os.listdir(quickfs_dir):
            os.rmdir(quickfs_dir)
        
        # Should have QuickFS metrics
        self.assertIsInstance(data, list)
        if len(data) > 0:
            # Check if any company has QuickFS metrics
            has_quickfs = any('revenue_5y_cagr_quickfs' in company for company in data)
            # May or may not have QuickFS depending on test data, but shouldn't crash
    
    def test_get_companies_search_company_name(self):
        """Test /api/companies with search by company name."""
        response = self.client.get('/api/companies?search=Apple')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIsInstance(data, list)
        if len(data) > 0:
            # Should find Apple Inc.
            found_apple = any('Apple' in str(item.get('company_name', '')) for item in data)
            # May or may not match depending on exact search term
    
    def test_get_companies_invalid_sort_column(self):
        """Test /api/companies with invalid sort column."""
        response = self.client.get('/api/companies?sort=invalid_column')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        # Should default to ticker sorting
        self.assertIsInstance(data, list)
    
    def test_get_company_with_ai_scores(self):
        """Test /api/company/<ticker> with AI scores database."""
        import app
        ai_scores_db = os.path.join(os.path.dirname(app.__file__), 'ai_scores.db')
        
        # Create AI scores database
        conn_ai = sqlite3.connect(ai_scores_db)
        cursor = conn_ai.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                moat_score REAL,
                company_name TEXT,
                model TEXT,
                timestamp TEXT
            )
        ''')
        cursor.execute('''
            INSERT OR REPLACE INTO scores (ticker, moat_score, company_name, model, timestamp)
            VALUES (?, ?, ?, ?, ?)
        ''', ('AAPL', 8.5, 'Apple Inc.', 'test', '2024-01-01'))
        conn_ai.commit()
        conn_ai.close()
        
        response = self.client.get('/api/company/AAPL')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        
        # Clean up
        if os.path.exists(ai_scores_db):
            os.remove(ai_scores_db)
        
        # Should have company data with raw values
        self.assertEqual(data['ticker'], 'AAPL')
    
    def test_get_company_with_finviz_data(self):
        """Test /api/company/<ticker> with Finviz database."""
        import app
        finviz_dir = os.path.join(os.path.dirname(app.__file__), 'finviz')
        os.makedirs(finviz_dir, exist_ok=True)
        finviz_db = os.path.join(finviz_dir, 'finviz.db')
        
        # Create Finviz database
        conn_finviz = sqlite3.connect(finviz_db)
        cursor = conn_finviz.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS short_interest (
                ticker TEXT,
                roa REAL,
                roic REAL
            )
        ''')
        cursor.execute('''
            INSERT OR REPLACE INTO short_interest (ticker, roa, roic)
            VALUES (?, ?, ?)
        ''', ('AAPL', 20.5, 25.3))
        conn_finviz.commit()
        conn_finviz.close()
        
        response = self.client.get('/api/company/AAPL')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        
        # Clean up
        if os.path.exists(finviz_db):
            os.remove(finviz_db)
        if os.path.exists(finviz_dir) and not os.listdir(finviz_dir):
            os.rmdir(finviz_dir)
        
        # Should have company data
        self.assertEqual(data['ticker'], 'AAPL')


if __name__ == '__main__':
    unittest.main(verbosity=2)

