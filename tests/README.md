# Test Suite

This directory contains comprehensive tests for the stock analysis application.

## Test Files

### `test_app_api.py`
Tests for Flask API endpoints:
- Main page route (`/`)
- Companies endpoint (`/api/companies`) with search, sort, and limit parameters
- Company details endpoint (`/api/company/<ticker>`)
- Stats endpoint (`/api/stats`)

### `test_calculate_scores.py`
Tests for score calculation functions:
- Recommendation to score conversion (Strong Buy, Buy, Hold, Sell, Strong Sell)
- Reverse metrics handling
- AI score column extraction

### `test_view_metric_rankings.py`
Tests for metric ranking functionality:
- Metric name formatting
- Reverse metric detection (AI, Finviz, QuickFS)
- Percentile rank calculations (normal and reverse)
- Handling of NaN values and edge cases

### `test_database_operations.py`
Tests for database operations and data integrity:
- Database connections and schema
- Data retrieval and queries
- NULL value handling
- Case-insensitive searches
- Percentile value validation (range 0-1)

### `test_metric_calculations.py`
Tests for metric calculation logic:
- Basic percentile calculations
- Reverse percentile calculations (lower is better)
- Handling of tied values
- Percentile distribution and averages
- Consistency checks

### `test_percentile_averages.py`
Tests that verify percentile averages are approximately 50%:
- All metrics should have average percentiles around 50%
- No metrics should have NaN averages

## Running Tests

### Run all tests:
```bash
python3 tests/run_tests.py
```

### Run a specific test file:
```bash
python3 -m unittest tests.test_app_api -v
python3 -m unittest tests.test_calculate_scores -v
python3 -m unittest tests.test_view_metric_rankings -v
python3 -m unittest tests.test_database_operations -v
python3 -m unittest tests.test_metric_calculations -v
python3 -m unittest tests.test_percentile_averages -v
```

### Run with more verbose output:
```bash
python3 -m unittest discover tests -v
```

## Test Coverage

The test suite covers:
- ✅ API endpoints and responses
- ✅ Database operations and schema
- ✅ Metric calculations and percentile computations
- ✅ Data validation and edge cases
- ✅ Reverse metrics handling
- ✅ Error handling

## Test Coverage

The coverage report focuses on important files that contribute to the web app functionality:

### Core Web App Files:
- **app.py** - Main Flask web application (API endpoints, routing)
- **calculate_total_scores.py** - Generates the composite scores database used by the web app
- **view_metric_rankings.py** - Metric ranking functionality used by the web app
- **show_metric_averages.py** - Metric averages display

### QuickFS Integration Files:
- **quickfs/get_one.py** - QuickFS metric calculations (used to populate database)
- **quickfs/calculate_all_metrics.py** - Batch QuickFS metric calculation (used to populate database)
- **quickfs/get_data.py** - QuickFS data retrieval (used to populate database)

### Running Coverage Analysis:

Basic coverage report:
```bash
python3 tests/run_coverage.py
```

Generate HTML coverage report:
```bash
python3 tests/run_coverage.py --html
```

Show missing lines in coverage report:
```bash
python3 tests/run_coverage.py --show-missing
```

The coverage report shows:
- ✓ Files with 80%+ coverage (Excellent)
- ⚠ Files with 50-79% coverage (Good, but could improve)
- ✗ Files with <50% coverage (Needs improvement)
- Files marked "Not executed" exist but aren't imported during tests (may need dedicated tests)

The HTML report will be generated in `htmlcov/index.html` - open it in your browser for detailed line-by-line coverage information.

## Requirements

Tests require:
- Python 3.7+
- unittest (built-in)
- pandas
- Flask (for API tests)
- coverage (for coverage analysis)
- sqlite3 (built-in)

Install dependencies:
```bash
pip install -r requirements.txt
```

## Note

Some tests create temporary databases and clean them up automatically. These tests are isolated and won't affect your actual database files.

