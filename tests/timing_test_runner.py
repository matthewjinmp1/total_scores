#!/usr/bin/env python3
"""
Custom test runner that tracks timing for each test.
"""

import unittest
import time
import sys
import os

class TimingTestResult(unittest.TextTestResult):
    """Test result class that tracks timing for each test."""
    
    def __init__(self, stream, descriptions, verbosity):
        super().__init__(stream, descriptions, verbosity)
        self.test_timings = []
        self.test_start_times = {}
    
    def startTest(self, test):
        """Called when a test starts."""
        self.test_start_times[test] = time.time()
        super().startTest(test)
    
    def addSuccess(self, test):
        """Called when a test succeeds."""
        duration = time.time() - self.test_start_times.get(test, time.time())
        test_id = str(test)
        self.test_timings.append({
            'test': test_id,
            'duration': duration,
            'status': 'ok'
        })
        super().addSuccess(test)
        if test in self.test_start_times:
            del self.test_start_times[test]
    
    def addError(self, test, err):
        """Called when a test raises an error."""
        duration = time.time() - self.test_start_times.get(test, time.time())
        test_id = str(test)
        self.test_timings.append({
            'test': test_id,
            'duration': duration,
            'status': 'ERROR'
        })
        super().addError(test, err)
        if test in self.test_start_times:
            del self.test_start_times[test]
    
    def addFailure(self, test, err):
        """Called when a test fails."""
        duration = time.time() - self.test_start_times.get(test, time.time())
        test_id = str(test)
        self.test_timings.append({
            'test': test_id,
            'duration': duration,
            'status': 'FAIL'
        })
        super().addFailure(test, err)
        if test in self.test_start_times:
            del self.test_start_times[test]
    
    def addSkip(self, test, reason):
        """Called when a test is skipped."""
        duration = time.time() - self.test_start_times.get(test, time.time())
        test_id = str(test)
        self.test_timings.append({
            'test': test_id,
            'duration': duration,
            'status': 'skipped'
        })
        super().addSkip(test, reason)
        if test in self.test_start_times:
            del self.test_start_times[test]

class TimingTestRunner(unittest.TextTestRunner):
    """Test runner that uses TimingTestResult."""
    
    def __init__(self, stream=None, descriptions=True, verbosity=1):
        super().__init__(stream, descriptions, verbosity, resultclass=TimingTestResult)
    
    def run(self, test):
        """Run the test suite and return timing information."""
        result = super().run(test)
        return result, result.test_timings if hasattr(result, 'test_timings') else []

