#!/usr/bin/env python3
"""
Tests for metric calculations and percentile computations.
"""

import sys
import os
import unittest
import pandas as pd
import numpy as np

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestMetricCalculations(unittest.TestCase):
    """Test metric calculation functions."""
    
    def test_percentile_calculation_basic(self):
        """Test basic percentile calculation."""
        values = pd.Series([10, 20, 30, 40, 50])
        ranks = values.rank(method='average', ascending=True)
        n_valid = len(values)
        percentiles = ranks / n_valid
        
        # Check that percentiles are in range [0, 1]
        self.assertTrue(all((percentiles >= 0) & (percentiles <= 1)))
        
        # Check that highest value has highest percentile
        self.assertEqual(percentiles.iloc[-1], 1.0)
    
    def test_percentile_calculation_reverse(self):
        """Test reverse percentile calculation (lower is better)."""
        values = pd.Series([1, 2, 3, 4, 5])  # Lower is better
        ranks = values.rank(method='average', ascending=True)
        n_valid = len(values)
        percentiles = ranks / n_valid
        reversed_percentiles = 1.0 - percentiles
        
        # In reverse, lowest value should have highest percentile
        # With 5 values, lowest (1) has rank 1, percentile 0.2, reversed 0.8
        # Highest (5) has rank 5, percentile 1.0, reversed 0.0
        self.assertGreater(reversed_percentiles.iloc[0], reversed_percentiles.iloc[-1])
        # Lowest value should have the highest reversed percentile (not necessarily 1.0)
        self.assertEqual(reversed_percentiles.iloc[0], 0.8)  # 1 - (1/5) = 0.8
        self.assertEqual(reversed_percentiles.iloc[-1], 0.0)  # 1 - (5/5) = 0.0
    
    def test_percentile_calculation_ties(self):
        """Test percentile calculation with tied values."""
        values = pd.Series([10, 20, 20, 30, 40])
        ranks = values.rank(method='average', ascending=True)
        n_valid = len(values)
        percentiles = ranks / n_valid
        
        # Tied values should have same rank
        self.assertEqual(ranks.iloc[1], ranks.iloc[2])
        self.assertEqual(percentiles.iloc[1], percentiles.iloc[2])
    
    def test_percentile_average_approximately_50(self):
        """Test that average percentile is approximately 50%."""
        # Create a series of values
        np.random.seed(42)
        values = pd.Series(np.random.randn(100))
        ranks = values.rank(method='average', ascending=True)
        n_valid = len(values)
        percentiles = ranks / n_valid
        
        avg_percentile = percentiles.mean() * 100
        
        # Average should be close to 50% (within 5% tolerance)
        self.assertAlmostEqual(avg_percentile, 50.0, delta=5.0)
    
    def test_percentile_distribution(self):
        """Test that percentiles are evenly distributed."""
        values = pd.Series(range(1, 101))  # 1 to 100
        ranks = values.rank(method='average', ascending=True)
        n_valid = len(values)
        percentiles = ranks / n_valid
        
        # Check distribution - should have values across the range
        min_percentile = percentiles.min() * 100
        max_percentile = percentiles.max() * 100
        
        self.assertAlmostEqual(min_percentile, 0.5, delta=1.0)  # First value ~0.5%
        self.assertAlmostEqual(max_percentile, 100.0, delta=0.1)  # Last value ~100%
    
    def test_metric_values_not_nan(self):
        """Test that calculated metric values are not NaN (when input is valid)."""
        values = pd.Series([1, 2, 3, 4, 5])
        ranks = values.rank(method='average', ascending=True)
        n_valid = len(values)
        percentiles = ranks / n_valid
        
        # All percentiles should be valid numbers
        self.assertFalse(percentiles.isna().any())
    
    def test_metric_consistency(self):
        """Test that metric calculations are consistent."""
        values1 = pd.Series([1, 2, 3, 4, 5])
        values2 = pd.Series([1, 2, 3, 4, 5])
        
        ranks1 = values1.rank(method='average', ascending=True)
        ranks2 = values2.rank(method='average', ascending=True)
        
        n1 = len(values1)
        n2 = len(values2)
        percentiles1 = ranks1 / n1
        percentiles2 = ranks2 / n2
        
        # Same input should produce same output
        pd.testing.assert_series_equal(percentiles1, percentiles2)


if __name__ == '__main__':
    unittest.main(verbosity=2)

