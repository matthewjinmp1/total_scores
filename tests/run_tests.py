#!/usr/bin/env python3
"""
Run all tests in the tests directory.
"""

import sys
import os
import unittest

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def discover_and_run_tests():
    """Discover and run all tests in the tests directory."""
    # Get the directory containing this script
    test_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(test_dir)
    
    # Discover tests in the tests directory
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=test_dir, pattern='test_*.py')
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Return exit code (0 for success, 1 for failure)
    return 0 if result.wasSuccessful() else 1

if __name__ == '__main__':
    exit_code = discover_and_run_tests()
    sys.exit(exit_code)

