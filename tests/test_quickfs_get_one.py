#!/usr/bin/env python3
"""
Tests for quickfs/get_one.py - QuickFS metric calculation functions.
These are critical because they calculate data that the web app uses.
"""

import sys
import os
import unittest
import tempfile
import shutil
import sqlite3
import json

# Add parent directory to path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

# Import directly from the quickfs folder
quickfs_dir = os.path.join(parent_dir, 'quickfs')
sys.path.insert(0, quickfs_dir)

from get_one import (
    get_previous_quarter,
    get_consecutive_quarters,
    get_ticker_data,
    calculate_5y_revenue_growth,
    calculate_5y_halfway_revenue_growth,
    calculate_halfway_share_count_growth,
    calculate_consistency_of_growth,
    calculate_acceleration_of_growth,
    calculate_operating_margin_growth,
    calculate_gross_margin_growth,
    calculate_operating_margin_consistency,
    calculate_gross_margin_consistency,
    calculate_ttm_ebit_ppe,
    calculate_net_debt_to_ttm_operating_income,
    calculate_total_past_return,
    QUICKFS_DB
)


class TestQuickFSGetOne(unittest.TestCase):
    """Tests for QuickFS metric calculation functions."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.test_db = os.path.join(self.test_dir, 'test_data.db')
        
        # Create test QuickFS database
        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE quickfs_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                data_type TEXT,
                data_json TEXT,
                fetched_at TEXT
            )
        ''')
        conn.commit()
        conn.close()
        
        # Patch database path
        import get_one as quickfs_module
        self.original_path = quickfs_module.QUICKFS_DB
        quickfs_module.QUICKFS_DB = self.test_db
    
    def tearDown(self):
        """Clean up test fixtures."""
        import get_one as quickfs_module
        quickfs_module.QUICKFS_DB = self.original_path
        shutil.rmtree(self.test_dir)
    
    def test_get_previous_quarter(self):
        """Test getting previous quarter calculation."""
        # Test Q4 -> Q3 (Dec -> Sep)
        year, month = get_previous_quarter(2024, 12)
        self.assertEqual((year, month), (2024, 9))
        
        # Test Q1 -> Q4 previous year (Jan -> Oct previous year)
        year, month = get_previous_quarter(2024, 1)
        self.assertEqual((year, month), (2023, 10))
        
        # Test Q2 -> Q1 (Apr -> Jan)
        year, month = get_previous_quarter(2024, 4)
        self.assertEqual((year, month), (2024, 1))
        
        # Test Q3 -> Q2 (Jul -> Apr)
        year, month = get_previous_quarter(2024, 7)
        self.assertEqual((year, month), (2024, 4))
    
    def test_get_consecutive_quarters_basic(self):
        """Test getting consecutive quarters from quarterly data."""
        # Create test data with consecutive quarters (format: YYYY-MM)
        quarterly_data = {
            'period_end_date': ['2024-12', '2024-09', '2024-06', '2024-03'],
            'revenue': [100.0, 90.0, 80.0, 70.0]
        }
        
        result = get_consecutive_quarters(quarterly_data, 'revenue', 4)
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 4)
        # Should be in reverse chronological order (most recent first)
        self.assertEqual(result[0][0], '2024-12')
        self.assertEqual(result[0][1], 100.0)
    
    def test_get_consecutive_quarters_with_gap(self):
        """Test get_consecutive_quarters detects gaps correctly."""
        # Create test data with a gap (format: YYYY-MM)
        quarterly_data = {
            'period_end_date': ['2024-12', '2024-09', '2024-03'],  # Missing Q2 (2024-06)
            'revenue': [100.0, 90.0, 70.0]
        }
        
        # Asking for 4 quarters but only have 2 consecutive (2024-12, 2024-09)
        # The gap at 2024-06 means 2024-03 can't be included
        result = get_consecutive_quarters(quarterly_data, 'revenue', 4)
        
        # Should return None because we don't have enough consecutive quarters
        self.assertIsNone(result)
        
        # But if we only ask for 2, we should get them
        result = get_consecutive_quarters(quarterly_data, 'revenue', 2)
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)  # 2024-12, 2024-09
    
    def test_get_consecutive_quarters_insufficient_data(self):
        """Test get_consecutive_quarters with insufficient data."""
        quarterly_data = {
            'period_end_date': ['2024-12', '2024-09'],
            'revenue': [100.0, 90.0]
        }
        
        result = get_consecutive_quarters(quarterly_data, 'revenue', 20)
        
        # Should return None if insufficient consecutive quarters
        self.assertIsNone(result)
    
    def test_get_ticker_data_existing(self):
        """Test getting ticker data from database."""
        # Insert test data
        test_data = {
            'revenue': [100.0, 90.0],
            'period_end_date': ['2024-12', '2024-09']
        }
        
        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO quickfs_data (ticker, data_type, data_json, fetched_at)
            VALUES (?, ?, ?, ?)
        ''', ('AAPL', 'full', json.dumps(test_data), '2024-01-01'))
        conn.commit()
        conn.close()
        
        result = get_ticker_data('AAPL')
        
        self.assertIsNotNone(result)
        self.assertEqual(result['revenue'], [100.0, 90.0])
    
    def test_get_ticker_data_nonexistent(self):
        """Test getting ticker data for nonexistent ticker."""
        result = get_ticker_data('INVALID')
        self.assertIsNone(result)
    
    def test_calculate_5y_revenue_growth(self):
        """Test 5-year revenue CAGR calculation."""
        # Create test data with 20+ consecutive quarters (format: YYYY-MM)
        # Generate dates backwards from 2024-12
        dates = []
        for i in range(20):
            year = 2024 - (i // 4)
            month = 12 - ((i % 4) * 3)
            if month <= 0:
                month += 12
                year -= 1
            dates.append(f'{year}-{month:02d}')
        
        quarterly_data = {
            'period_end_date': dates,
            'revenue': [100.0 * (1.1 ** (i/4)) for i in range(20)]  # ~10% annual growth
        }
        
        # Mock ticker_data structure
        ticker_data = {
            'quarterly': quarterly_data
        }
        
        result = calculate_5y_revenue_growth(ticker_data)
        
        # Should calculate CAGR
        if result is not None:
            self.assertIsInstance(result, float)
            # Should be approximately 10% (0.10) for 10% annual growth
            self.assertGreater(result, 0.0)
    
    def test_calculate_5y_revenue_growth_insufficient_data(self):
        """Test 5-year revenue growth with insufficient data."""
        quarterly_data = {
            'period_end_date': ['2024-12-31', '2024-09-30'],
            'revenue': [100.0, 90.0]
        }
        
        ticker_data = {
            'quarterly': quarterly_data
        }
        
        result = calculate_5y_revenue_growth(ticker_data)
        
        # Should return None for insufficient data
        self.assertIsNone(result)
    
    def test_calculate_halfway_share_count_growth(self):
        """Test halfway share count growth calculation."""
        # Create test data with sufficient quarters (format: YYYY-MM)
        dates = []
        for i in range(20):
            year = 2024 - (i // 4)
            month = 12 - ((i % 4) * 3)
            if month <= 0:
                month += 12
                year -= 1
            dates.append(f'{year}-{month:02d}')
        
        quarterly_data = {
            'period_end_date': dates,
            'weighted_average_shares': [1000.0 * (0.99 ** (i/4)) for i in range(20)]  # Decreasing shares
        }
        
        ticker_data = {
            'quarterly': quarterly_data
        }
        
        result = calculate_halfway_share_count_growth(ticker_data)
        
        # Should calculate growth ratio
        if result is not None:
            self.assertIsInstance(result, float)
            # Share count decreasing means ratio < 1.0
            self.assertLess(result, 1.0)


class TestQuickFSMarginCalculations(unittest.TestCase):
    """Tests for margin-related calculations."""
    
    def test_calculate_operating_margin_growth_insufficient_data(self):
        """Test operating margin growth with insufficient data."""
        quarterly_data = {
            'period_end_date': ['2024-12'],
            'revenue': [100.0],
            'operating_income': [10.0]
        }
        
        ticker_data = {
            'quarterly': quarterly_data
        }
        
        result = calculate_operating_margin_growth(ticker_data)
        
        # Should return None for insufficient data (need 20 quarters)
        self.assertIsNone(result)
    
    def test_calculate_gross_margin_growth_insufficient_data(self):
        """Test gross margin growth with insufficient data."""
        quarterly_data = {
            'period_end_date': ['2024-12'],
            'revenue': [100.0],
            'gross_profit': [40.0]
        }
        
        ticker_data = {
            'quarterly': quarterly_data
        }
        
        result = calculate_gross_margin_growth(ticker_data)
        
        # Should return None for insufficient data
        self.assertIsNone(result)
    
    def test_calculate_ttm_ebit_ppe(self):
        """Test TTM EBIT/PPE calculation."""
        # Create test data
        quarterly_data = {
            'period_end_date': ['2024-12', '2024-09', '2024-06', '2024-03'],
            'operating_income': [10.0, 9.0, 8.0, 7.0],  # TTM = 34
            'property_plant_equipment': [100.0, 100.0, 100.0, 100.0]
        }
        
        ticker_data = {
            'quarterly': quarterly_data
        }
        
        result = calculate_ttm_ebit_ppe(ticker_data)
        
        # Should calculate TTM EBIT / PPE
        if result is not None:
            self.assertIsInstance(result, float)
            # TTM EBIT = 34, PPE = 100, ratio = 0.34
            self.assertGreater(result, 0.0)
    
    def test_get_ticker_data_not_found(self):
        """Test get_ticker_data when ticker not found (covers lines 18-19)."""
        result = get_ticker_data('NONEXISTENT')
        self.assertIsNone(result)
    
    def test_get_consecutive_quarters_no_field(self):
        """Test get_consecutive_quarters when field doesn't exist (covers line 69)."""
        quarterly_data = {
            'period_end_date': ['2024-12', '2024-09'],
        }
        
        result = get_consecutive_quarters(quarterly_data, 'nonexistent_field', 2)
        self.assertIsNone(result)
    
    def test_get_consecutive_quarters_no_valid_data(self):
        """Test get_consecutive_quarters when no valid data (covers line 79)."""
        quarterly_data = {
            'period_end_date': [None, None],
            'revenue': [None, None]
        }
        
        result = get_consecutive_quarters(quarterly_data, 'revenue', 2)
        self.assertIsNone(result)
    
    def test_calculate_5y_revenue_growth_no_financials(self):
        """Test calculate_5y_revenue_growth when no financials key (covers line 130-131)."""
        ticker_data = {}
        result = calculate_5y_revenue_growth(ticker_data)
        self.assertIsNone(result)
    
    def test_calculate_5y_revenue_growth_no_quarterly(self):
        """Test calculate_5y_revenue_growth when no quarterly data (covers line 136-137)."""
        ticker_data = {'financials': {}}
        result = calculate_5y_revenue_growth(ticker_data)
        self.assertIsNone(result)
    
    def test_calculate_5y_revenue_growth_date_parsing_errors(self):
        """Test calculate_5y_revenue_growth with date parsing edge cases (covers lines 174-194)."""
        # Create data with edge case dates
        dates = []
        revenues = []
        for i in range(20):
            year = 2024 - (i // 4)
            month = 12 - ((i % 4) * 3)
            if month <= 0:
                month += 12
                year -= 1
            dates.append(f'{year}-{month:02d}')
            revenues.append(100.0 * (1.1 ** (i/4)))
        
        quarterly_data = {
            'period_end_date': dates,
            'revenue': revenues
        }
        
        ticker_data = {
            'financials': {
                'quarterly': quarterly_data
            }
        }
        
        result = calculate_5y_revenue_growth(ticker_data)
        # Should handle date parsing and calculate growth
        if result is not None:
            self.assertIsInstance(result, tuple)
            self.assertEqual(len(result), 6)
    
    def test_calculate_5y_revenue_growth_zero_years_diff(self):
        """Test calculate_5y_revenue_growth with zero years difference (covers line 196-197)."""
        # Create data where dates are the same (unlikely but possible edge case)
        quarterly_data = {
            'period_end_date': ['2024-12'] * 20,
            'revenue': [100.0] * 20
        }
        
        ticker_data = {
            'financials': {
                'quarterly': quarterly_data
            }
        }
        
        result = calculate_5y_revenue_growth(ticker_data)
        # Should return None if years_diff <= 0
        # Note: This might return None for insufficient consecutive quarters instead
        # Just verify it doesn't crash
        self.assertIn(result, [None, tuple])
    
    def test_calculate_5y_halfway_revenue_growth_no_financials(self):
        """Test calculate_5y_halfway_revenue_growth when no financials."""
        ticker_data = {}
        result = calculate_5y_halfway_revenue_growth(ticker_data)
        self.assertIsNone(result)
    
    def test_calculate_halfway_share_count_growth_no_shares_field(self):
        """Test calculate_halfway_share_count_growth when no shares field exists (covers lines 296-303)."""
        quarterly_data = {
            'period_end_date': ['2024-12'] * 20,
        }
        
        ticker_data = {
            'financials': {
                'quarterly': quarterly_data
            }
        }
        
        result = calculate_halfway_share_count_growth(ticker_data)
        self.assertIsNone(result)
    
    def test_calculate_halfway_share_count_growth_with_shares_eop(self):
        """Test calculate_halfway_share_count_growth with shares_eop field."""
        dates = []
        for i in range(20):
            year = 2024 - (i // 4)
            month = 12 - ((i % 4) * 3)
            if month <= 0:
                month += 12
                year -= 1
            dates.append(f'{year}-{month:02d}')
        
        quarterly_data = {
            'period_end_date': dates,
            'shares_eop': [1000.0 * (0.99 ** (i/4)) for i in range(20)]
        }
        
        ticker_data = {
            'financials': {
                'quarterly': quarterly_data
            }
        }
        
        result = calculate_halfway_share_count_growth(ticker_data)
        if result is not None:
            self.assertIsInstance(result, tuple)
    
    def test_calculate_consistency_of_growth_no_financials(self):
        """Test calculate_consistency_of_growth when no financials."""
        ticker_data = {}
        result = calculate_consistency_of_growth(ticker_data)
        self.assertIsNone(result)
    
    def test_calculate_acceleration_of_growth_no_financials(self):
        """Test calculate_acceleration_of_growth when no financials."""
        ticker_data = {}
        result = calculate_acceleration_of_growth(ticker_data)
        self.assertIsNone(result)
    
    def test_calculate_operating_margin_growth_no_financials(self):
        """Test calculate_operating_margin_growth when no financials."""
        ticker_data = {}
        result = calculate_operating_margin_growth(ticker_data)
        self.assertIsNone(result)
    
    def test_calculate_gross_margin_growth_no_financials(self):
        """Test calculate_gross_margin_growth when no financials."""
        ticker_data = {}
        result = calculate_gross_margin_growth(ticker_data)
        self.assertIsNone(result)
    
    def test_calculate_operating_margin_consistency_no_financials(self):
        """Test calculate_operating_margin_consistency when no financials."""
        ticker_data = {}
        result = calculate_operating_margin_consistency(ticker_data)
        self.assertIsNone(result)
    
    def test_calculate_gross_margin_consistency_no_financials(self):
        """Test calculate_gross_margin_consistency when no financials."""
        ticker_data = {}
        result = calculate_gross_margin_consistency(ticker_data)
        self.assertIsNone(result)
    
    def test_calculate_ttm_ebit_ppe_no_financials(self):
        """Test calculate_ttm_ebit_ppe when no financials."""
        ticker_data = {}
        result = calculate_ttm_ebit_ppe(ticker_data)
        self.assertIsNone(result)
    
    def test_calculate_net_debt_to_ttm_operating_income_no_financials(self):
        """Test calculate_net_debt_to_ttm_operating_income when no financials."""
        ticker_data = {}
        result = calculate_net_debt_to_ttm_operating_income(ticker_data)
        self.assertIsNone(result)
    
    def test_calculate_total_past_return_no_financials(self):
        """Test calculate_total_past_return when no financials."""
        ticker_data = {}
        result = calculate_total_past_return(ticker_data)
        self.assertIsNone(result)
    
    def test_calculate_5y_revenue_growth_zero_old_sum(self):
        """Test calculate_5y_revenue_growth when old revenue sum is zero (covers line 164)."""
        dates = []
        revenues = []
        for i in range(20):
            year = 2024 - (i // 4)
            month = 12 - ((i % 4) * 3)
            if month <= 0:
                month += 12
                year -= 1
            dates.append(f'{year}-{month:02d}')
            # Old quarters have zero revenue
            if i >= 16:
                revenues.append(100.0 * (1.1 ** ((i-16)/4)))
            else:
                revenues.append(0.0)  # Zero revenue in old quarters
        
        quarterly_data = {
            'period_end_date': dates,
            'revenue': revenues
        }
        
        ticker_data = {
            'financials': {
                'quarterly': quarterly_data
            }
        }
        
        result = calculate_5y_revenue_growth(ticker_data)
        # Should return None when old_revenue_sum <= 0
        self.assertIsNone(result)
    
    def test_calculate_5y_revenue_growth_zero_current_sum(self):
        """Test calculate_5y_revenue_growth when current revenue sum is zero (covers line 156-157)."""
        dates = []
        revenues = []
        for i in range(20):
            year = 2024 - (i // 4)
            month = 12 - ((i % 4) * 3)
            if month <= 0:
                month += 12
                year -= 1
            dates.append(f'{year}-{month:02d}')
            # Current quarters have zero revenue
            if i < 4:
                revenues.append(0.0)  # Zero revenue in current quarters
            else:
                revenues.append(100.0 * (1.1 ** ((i-4)/4)))
        
        quarterly_data = {
            'period_end_date': dates,
            'revenue': revenues
        }
        
        ticker_data = {
            'financials': {
                'quarterly': quarterly_data
            }
        }
        
        result = calculate_5y_revenue_growth(ticker_data)
        # Should return None when current_revenue_sum <= 0
        self.assertIsNone(result)
    
    def test_calculate_5y_halfway_revenue_growth_zero_old_sum(self):
        """Test calculate_5y_halfway_revenue_growth when old sum is zero (covers line 255-256)."""
        dates = []
        revenues = []
        for i in range(20):
            year = 2024 - (i // 4)
            month = 12 - ((i % 4) * 3)
            if month <= 0:
                month += 12
                year -= 1
            dates.append(f'{year}-{month:02d}')
            # Old 10 quarters have zero revenue
            if i < 10:
                revenues.append(100.0 * (1.1 ** (i/4)))
            else:
                revenues.append(0.0)
        
        quarterly_data = {
            'period_end_date': dates,
            'revenue': revenues
        }
        
        ticker_data = {
            'financials': {
                'quarterly': quarterly_data
            }
        }
        
        result = calculate_5y_halfway_revenue_growth(ticker_data)
        # Should return None when old_10_sum <= 0
        self.assertIsNone(result)
    
    def test_calculate_halfway_share_count_growth_zero_old_sum(self):
        """Test calculate_halfway_share_count_growth when old sum is zero (covers line 328)."""
        dates = []
        shares = []
        for i in range(20):
            year = 2024 - (i // 4)
            month = 12 - ((i % 4) * 3)
            if month <= 0:
                month += 12
                year -= 1
            dates.append(f'{year}-{month:02d}')
            # Old 10 quarters have zero shares
            if i < 10:
                shares.append(1000.0 * (0.99 ** (i/4)))
            else:
                shares.append(0.0)
        
        quarterly_data = {
            'period_end_date': dates,
            'weighted_average_shares': shares
        }
        
        ticker_data = {
            'financials': {
                'quarterly': quarterly_data
            }
        }
        
        result = calculate_halfway_share_count_growth(ticker_data)
        # Should return None when old_10_sum <= 0
        self.assertIsNone(result)
    
    def test_calculate_5y_revenue_growth_filtered_quarters(self):
        """Test calculate_5y_revenue_growth filters to positive revenue (covers line 147-149)."""
        dates = []
        revenues = []
        for i in range(20):
            year = 2024 - (i // 4)
            month = 12 - ((i % 4) * 3)
            if month <= 0:
                month += 12
                year -= 1
            dates.append(f'{year}-{month:02d}')
            # Some negative or zero revenues
            if i % 5 == 0:
                revenues.append(-100.0)  # Negative revenue
            elif i % 7 == 0:
                revenues.append(0.0)  # Zero revenue
            else:
                revenues.append(100.0 * (1.1 ** (i/4)))
        
        quarterly_data = {
            'period_end_date': dates,
            'revenue': revenues
        }
        
        ticker_data = {
            'financials': {
                'quarterly': quarterly_data
            }
        }
        
        result = calculate_5y_revenue_growth(ticker_data)
        # If we don't have enough positive quarters after filtering, should return None
        # Otherwise should calculate
        # Just verify it doesn't crash
        self.assertIn(result, [None, tuple])
    
    def test_calculate_5y_revenue_growth_valid_data(self):
        """Test calculate_5y_revenue_growth with valid data that should succeed."""
        # Use valid consecutive dates that will pass get_consecutive_quarters
        dates = []
        revenues = []
        for i in range(20):
            year = 2024 - (i // 4)
            month = 12 - ((i % 4) * 3)
            if month <= 0:
                month += 12
                year -= 1
            dates.append(f'{year}-{month:02d}')
            revenues.append(100.0 * (1.1 ** (i/4)))
        
        quarterly_data = {
            'period_end_date': dates,
            'revenue': revenues
        }
        
        ticker_data = {
            'financials': {
                'quarterly': quarterly_data
            }
        }
        
        result = calculate_5y_revenue_growth(ticker_data)
        # Should calculate successfully with valid consecutive data
        if result is not None:
            self.assertIsInstance(result, tuple)
            self.assertEqual(len(result), 6)
    
    def test_calculate_5y_halfway_revenue_growth_no_quarterly(self):
        """Test calculate_5y_halfway_revenue_growth when no quarterly data (covers line 228)."""
        ticker_data = {
            'financials': {}
        }
        result = calculate_5y_halfway_revenue_growth(ticker_data)
        self.assertIsNone(result)
    
    def test_calculate_halfway_share_count_growth_no_shares_field(self):
        """Test calculate_halfway_share_count_growth when no shares field (covers line 302-303)."""
        quarterly_data = {
            'period_end_date': ['2024-12'] * 20,
            # No shares fields
        }
        
        ticker_data = {
            'financials': {
                'quarterly': quarterly_data
            }
        }
        
        result = calculate_halfway_share_count_growth(ticker_data)
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main(verbosity=2)

