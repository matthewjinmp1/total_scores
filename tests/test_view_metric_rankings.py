#!/usr/bin/env python3
"""
Tests for view_metric_rankings.py functionality.
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

from view_metric_rankings import (
    format_metric_name,
    is_reverse_metric,
    calculate_percentile_rank,
    AI_REVERSE_METRICS,
    FINVIZ_REVERSE_METRICS,
    QUICKFS_REVERSE_METRICS
)


class TestViewMetricRankings(unittest.TestCase):
    """Test metric ranking functions."""
    
    def test_format_metric_name_normalized(self):
        """Test formatting metric name with _normalized suffix."""
        result = format_metric_name('moat_score_normalized')
        self.assertEqual(result, 'Moat Score')
    
    def test_format_metric_name_percentile(self):
        """Test formatting metric name with _percentile suffix."""
        result = format_metric_name('roa_percentile')
        self.assertEqual(result, 'Roa')
    
    def test_format_metric_name_quickfs(self):
        """Test formatting metric name with _quickfs suffix."""
        result = format_metric_name('revenue_5y_cagr_quickfs')
        self.assertEqual(result, 'Revenue 5Y Cagr')
    
    def test_format_metric_name_multiple_suffixes(self):
        """Test formatting metric name with multiple suffixes."""
        result = format_metric_name('test_metric_normalized')
        self.assertEqual(result, 'Test Metric')
    
    def test_is_reverse_metric_ai_reverse(self):
        """Test reverse metric detection for AI metrics."""
        self.assertTrue(is_reverse_metric('disruption_risk_normalized'))
        self.assertTrue(is_reverse_metric('riskiness_score_normalized'))
        self.assertFalse(is_reverse_metric('moat_score_normalized'))
    
    def test_is_reverse_metric_finviz_reverse(self):
        """Test reverse metric detection for Finviz metrics."""
        self.assertTrue(is_reverse_metric('short_interest_percent_percentile'))
        self.assertTrue(is_reverse_metric('forward_pe_percentile'))
        self.assertFalse(is_reverse_metric('roa_percentile'))
    
    def test_is_reverse_metric_quickfs_reverse(self):
        """Test reverse metric detection for QuickFS metrics."""
        self.assertTrue(is_reverse_metric('revenue_growth_consistency_quickfs'))
        self.assertTrue(is_reverse_metric('operating_margin_consistency_quickfs'))
        self.assertFalse(is_reverse_metric('revenue_5y_cagr_quickfs'))
    
    def test_is_reverse_metric_recommendation(self):
        """Test reverse metric detection for recommendation."""
        self.assertTrue(is_reverse_metric('recommendation_score_percentile'))
        self.assertTrue(is_reverse_metric('recommendation_percentile'))
    
    def test_calculate_percentile_rank_ascending(self):
        """Test percentile rank calculation (normal, ascending)."""
        values = pd.Series([1, 2, 3, 4, 5])
        result = calculate_percentile_rank(values, reverse=False)
        
        # Highest value should have highest percentile
        self.assertGreater(result.iloc[4], result.iloc[0])
    
    def test_calculate_percentile_rank_reverse(self):
        """Test percentile rank calculation (reverse, lower is better)."""
        values = pd.Series([1, 2, 3, 4, 5])
        result = calculate_percentile_rank(values, reverse=True)
        
        # Lowest value should have highest percentile (reversed)
        self.assertGreater(result.iloc[0], result.iloc[4])
    
    def test_calculate_percentile_rank_with_nan(self):
        """Test percentile rank calculation with NaN values."""
        values = pd.Series([1, 2, pd.NA, 4, 5])
        result = calculate_percentile_rank(values, reverse=False)
        
        # Should handle NaN gracefully
        self.assertEqual(len(result), len(values))
        # NaN position should be None or NaN
        self.assertTrue(pd.isna(result.iloc[2]))
    
    def test_calculate_percentile_rank_empty(self):
        """Test percentile rank calculation with empty series."""
        values = pd.Series([], dtype=float)
        result = calculate_percentile_rank(values, reverse=False)
        
        self.assertEqual(len(result), 0)
    
    def test_ai_reverse_metrics_defined(self):
        """Test that AI reverse metrics list is defined."""
        self.assertIsInstance(AI_REVERSE_METRICS, list)
        self.assertGreater(len(AI_REVERSE_METRICS), 0)
        self.assertIn('disruption_risk', AI_REVERSE_METRICS)
    
    def test_finviz_reverse_metrics_defined(self):
        """Test that Finviz reverse metrics list is defined."""
        self.assertIsInstance(FINVIZ_REVERSE_METRICS, list)
        self.assertGreater(len(FINVIZ_REVERSE_METRICS), 0)
    
    def test_quickfs_reverse_metrics_defined(self):
        """Test that QuickFS reverse metrics list is defined."""
        self.assertIsInstance(QUICKFS_REVERSE_METRICS, list)
        self.assertGreater(len(QUICKFS_REVERSE_METRICS), 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)

