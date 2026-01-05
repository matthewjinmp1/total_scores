#!/usr/bin/env python3
"""
Run test coverage analysis for the stock analysis application.
Requires coverage.py: pip install coverage
"""

import sys
import os
import subprocess
import argparse
import re
import time
import unittest

# ANSI color codes (using standard colors, not bright)
class Colors:
    GREEN = '\033[32m'      # Standard green (darker)
    RED = '\033[31m'        # Standard red (darker)
    YELLOW = '\033[33m'     # Standard yellow (darker)
    BLUE = '\033[34m'       # Standard blue (darker)
    MAGENTA = '\033[35m'    # Standard magenta (darker)
    CYAN = '\033[36m'       # Standard cyan (darker)
    WHITE = '\033[37m'      # Standard white (darker)
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'  # Reset color
    # Disable colors if output is redirected or in non-TTY
    if not sys.stdout.isatty():
        GREEN = RED = YELLOW = BLUE = MAGENTA = CYAN = WHITE = BOLD = UNDERLINE = END = ''

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def check_coverage_installed():
    """Check if coverage.py is installed."""
    try:
        import coverage
        return True
    except ImportError:
        return False

def run_coverage(html=False, verbose=False, show_missing=False, measure_test_timing=False):
    """Run coverage analysis."""
    
    if not check_coverage_installed():
        print(f"{Colors.RED}❌ coverage.py is not installed.{Colors.END}")
        print("\nPlease install it with:")
        print(f"  {Colors.CYAN}pip install coverage{Colors.END}")
        print("\nOr add it to requirements.txt and run:")
        print(f"  {Colors.CYAN}pip install -r requirements.txt{Colors.END}")
        return 1
    
    # Get the project root directory
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tests_dir = os.path.join(project_root, 'tests')
    
    # Important files that contribute to web app functionality
    # These are the core files the web app depends on
    important_files = [
        # Main web app (runtime)
        'app.py',
        # Score calculation and database generation (runtime for web app setup)
        'calculate_total_scores.py',
        # Metric viewing and rankings (runtime)
        'view_metric_rankings.py',
        'show_metric_averages.py',
    ]
    
    # Data preparation scripts (not part of web app runtime, but important for data)
    data_prep_files = [
        'quickfs/get_one.py',
        'quickfs/calculate_all_metrics.py',
        'quickfs/get_data.py',
    ]
    
    # Build source specification - include root directory but omit non-essential files
    # We'll use --include to explicitly list important files, or use --source with omit
    omit_patterns = [
        '*/tests/*',
        '*/__pycache__/*',
        '*/venv/*',
        '*/env/*',
        '*.pyc',
        '*/finviz/testing/*',  # Exclude finviz testing files
        '*/finviz/fetch_top_tickers.py',  # Not used by web app
        '*/finviz/get_all.py',  # Data collection, not web app runtime
        '*/quickfs/seasonality.py',  # Analysis tool, not web app
        '*/quickfs/diagnose_metrics.py',  # Diagnostic tool, not web app
    ]
    
    print("=" * 80)
    print("TEST COVERAGE ANALYSIS")
    print("=" * 80)
    print()
    print("Important files for web app runtime:")
    for f in important_files:
        file_path = os.path.join(project_root, f)
        if os.path.exists(file_path):
            print(f"  ✓ {f}")
        else:
            print(f"  ⚠ {f} (not found)")
    
    print("\nData preparation scripts (populate databases, not web app runtime):")
    for f in data_prep_files:
        file_path = os.path.join(project_root, f)
        if os.path.exists(file_path):
            print(f"  • {f}")
        else:
            print(f"  ⚠ {f} (not found)")
    print()
    
    # Build list of files to track
    files_to_track = important_files + data_prep_files
    
    # Build coverage command
    coverage_cmd = [
        sys.executable, '-m', 'coverage', 'run',
        '--source', project_root,
        '--omit', ','.join(omit_patterns),
        '-m', 'unittest', 'discover', '-s', tests_dir, '-p', 'test_*.py', '-v'
    ]
    
    print("Running tests with coverage...")
    if verbose:
        print(f"Command: {' '.join(coverage_cmd)}")
    print()
    
    # Run coverage - capture output and track timing
    test_start_time = time.time()
    result = subprocess.run(
        coverage_cmd,
        cwd=project_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    test_end_time = time.time()
    total_test_time = test_end_time - test_start_time
    
    # Parse test output to extract test information
    test_timings = []
    if result.stdout:
        lines = result.stdout.split('\n')
        i = 0
        while i < len(lines):
            line = lines[i]
            # Look for test definition line: "test_name (test_module.TestClass)"
            test_def_match = re.search(r'^(\w+) \((test_[^)]+)\)', line)
            if test_def_match:
                test_name = test_def_match.group(1)
                test_class = test_def_match.group(2)
                
                # Look at next line for result: "... ok" or "... FAIL" or "... ERROR"
                if i + 1 < len(lines):
                    next_line = lines[i + 1]
                    result_match = re.search(r'\.\.\. (ok|FAIL|ERROR|skipped)', next_line)
                    if result_match:
                        status = result_match.group(1)
                        # Build full test name for unittest
                        # Format: test_module.TestClass.test_name
                        full_name = f"{test_class}.{test_name}"
                        test_timings.append({
                            'name': test_name,
                            'class': test_class,
                            'full_name': full_name,
                            'duration': 0.0,  # Will be filled when we measure individual tests
                            'status': status
                        })
                        i += 2  # Skip both lines
                        continue
            i += 1
    
    # Get actual test counts from output
    test_count_match = re.search(r'Ran (\d+) test', result.stdout)
    num_tests = int(test_count_match.group(1)) if test_count_match else len(test_timings)
    
    if result.returncode != 0:
        print(result.stdout)
        print(f"\n{Colors.RED}❌ Tests failed with exit code {result.returncode}{Colors.END}")
        return result.returncode
    
    # Display test output (filtered if not verbose)
    if verbose:
        # Color code verbose output
        lines = result.stdout.split('\n')
        for line in lines:
            if '...' in line:
                if 'ok' in line:
                    print(f"{Colors.GREEN}{line}{Colors.END}")
                elif 'FAIL' in line or 'ERROR' in line:
                    print(f"{Colors.RED}{line}{Colors.END}")
                elif 'skipped' in line:
                    print(f"{Colors.YELLOW}{line}{Colors.END}")
                else:
                    print(line)
            elif 'Ran' in line and 'test' in line.lower():
                print(f"{Colors.CYAN}{line}{Colors.END}")
            elif line.strip() and 'OK' in line:
                print(f"{Colors.GREEN}{line}{Colors.END}")
            elif line.strip() and ('FAILED' in line or 'ERROR' in line):
                print(f"{Colors.RED}{line}{Colors.END}")
            else:
                print(line)
    else:
        # Show filtered output - just test results and summary with colors
        lines = result.stdout.split('\n')
        for line in lines:
            # Show test results with color coding
            if '...' in line and ('ok' in line or 'FAIL' in line or 'ERROR' in line):
                if 'ok' in line:
                    print(f"{Colors.GREEN}{line}{Colors.END}")
                elif 'FAIL' in line or 'ERROR' in line:
                    print(f"{Colors.RED}{line}{Colors.END}")
                elif 'skipped' in line:
                    print(f"{Colors.YELLOW}{line}{Colors.END}")
                else:
                    print(line)
            # Show summary lines
            elif 'Ran' in line and 'test' in line.lower():
                print(f"{Colors.CYAN}{line}{Colors.END}")
            elif line.strip() and 'OK' in line:
                print(f"{Colors.GREEN}{line}{Colors.END}")
            elif line.strip() and ('FAILED' in line or 'ERROR' in line):
                print(f"{Colors.RED}{line}{Colors.END}")
    
    print()
    print("=" * 80)
    print("COVERAGE REPORT")
    print("=" * 80)
    print()
    
    # Generate report
    report_cmd = [sys.executable, '-m', 'coverage', 'report']
    
    if show_missing:
        report_cmd.append('--show-missing')
    
    # Run report and capture output
    report_result = subprocess.run(
        report_cmd,
        cwd=project_root,
        capture_output=True,
        text=True
    )
    
    # Parse output and add missing files with 0% coverage
    output_lines = report_result.stdout.split('\n')
    reported_files = {}  # Map of basename -> full line for matching
    total_line_idx = -1
    
    # Find which files are already reported and locate TOTAL line
    for i, line in enumerate(output_lines):
        parts = line.split()
        if len(parts) >= 4 and parts[0].endswith('.py'):
            # Store the filename (handle both paths and basenames)
            file_key = parts[0]
            reported_files[os.path.basename(file_key)] = line
            reported_files[file_key] = line
        elif 'TOTAL' in line and len(parts) >= 1:
            total_line_idx = i
    
    # Find files that should be reported but aren't
    missing_files = []
    
    for file_path in files_to_track:
        full_path = os.path.join(project_root, file_path)
        if not os.path.exists(full_path):
            continue
        
        # Check if this file is already in the report
        filename = os.path.basename(file_path)
        file_already_reported = filename in reported_files or file_path in reported_files
        
        if not file_already_reported:
            # Count statements in file - use coverage's method for consistency
            # Coverage counts executable statements, not just lines
            statements = 0
            try:
                # Use coverage API to get statement count for consistency
                import coverage
                cov = coverage.Coverage()
                cov.load()
                
                # Get coverage data for this file
                analysis = cov.analysis2(full_path)
                if analysis:
                    statements = len(analysis[1])  # statements list
                else:
                    # Fallback: count non-comment, non-blank lines
                    with open(full_path, 'r') as f:
                        content = f.read()
                        statements = len([l for l in content.split('\n') 
                                        if l.strip() and not l.strip().startswith('#')])
            except Exception:
                # Fallback: simple line count excluding comments
                try:
                    with open(full_path, 'r') as f:
                        content = f.read()
                        statements = len([l for l in content.split('\n') 
                                        if l.strip() and not l.strip().startswith('#')])
                except:
                    statements = 0
            
            missing_files.append((file_path, statements))
    
    # Rebuild output with missing files inserted before TOTAL
    if missing_files and total_line_idx >= 0:
        new_output = []
        separator_after_files = -1  # Track if there's a separator line before TOTAL
        
        # First pass: identify separator line position
        for i in range(total_line_idx - 1, -1, -1):
            if output_lines[i].strip() == '---':
                separator_after_files = i
                break
        
        # Second pass: rebuild output
        for i, line in enumerate(output_lines):
            # Skip separator line that appears right before TOTAL (if any)
            if i == separator_after_files:
                continue
            
            if i == total_line_idx:
                # Insert missing files before TOTAL (no separator line)
                for file_path, statements in missing_files:
                    # Format path relative to project root
                    display_path = os.path.relpath(os.path.join(project_root, file_path), project_root)
                    new_output.append(f"{display_path:40} {statements:6} {statements:6}    0%")
                
                # Update TOTAL line with new counts
                parts = line.split()
                if len(parts) >= 4:
                    try:
                        old_stmts = int(parts[1])
                        old_missed = int(parts[2])
                        
                        # Add missing file statements
                        total_missing_stmts = sum(s for _, s in missing_files)
                        new_stmts = old_stmts + total_missing_stmts
                        new_missed = old_missed + total_missing_stmts
                        new_cover = ((new_stmts - new_missed) / new_stmts * 100) if new_stmts > 0 else 0
                        
                        new_output.append(f"{'TOTAL':40} {new_stmts:6} {new_missed:6} {new_cover:5.0f}%")
                    except (ValueError, IndexError):
                        new_output.append(line)
                else:
                    new_output.append(line)
            else:
                new_output.append(line)
        
        print('\n'.join(new_output))
    else:
        # No missing files or couldn't find TOTAL, just print original
        print(report_result.stdout)
    
    if report_result.stderr:
        print(report_result.stderr, file=sys.stderr)
    
    # Generate HTML report if requested
    if html:
        print()
        print("=" * 80)
        print("GENERATING HTML COVERAGE REPORT")
        print("=" * 80)
        print()
        
        html_dir = os.path.join(project_root, 'htmlcov')
        html_cmd = [sys.executable, '-m', 'coverage', 'html', '-d', html_dir]
        subprocess.run(html_cmd, cwd=project_root)
        
        html_index = os.path.join(html_dir, 'index.html')
        print(f"\n{Colors.GREEN}✓ HTML coverage report generated at:{Colors.END}")
        print(f"  {Colors.CYAN}{html_index}{Colors.END}")
        print(f"\nOpen it in your browser to view detailed coverage.")
    
    print()
    print("=" * 80)
    print("COVERAGE ANALYSIS COMPLETE")
    print("=" * 80)
    
    # Parse coverage and show important files status
    print()
    print("=" * 80)
    print("IMPORTANT FILES COVERAGE STATUS")
    print("=" * 80)
    print()
    
    if report_result.stdout:
        lines = report_result.stdout.split('\n')
        
        # Create a mapping of basename to full path for all important files
        all_tracked_files = important_files + data_prep_files
        filename_to_path = {os.path.basename(f): f for f in all_tracked_files}
        
        # Parse each file's coverage
        file_coverage = {}
        total_statements = 0
        total_missed = 0
        
        for line in lines:
            # Parse coverage line (format: "filename.py   123   45   63%" or "path/to/filename.py...")
            parts = line.split()
            if len(parts) >= 4 and parts[0].endswith('.py'):
                # Handle both "filename.py" and "path/to/filename.py"
                file_key = parts[0]
                filename = os.path.basename(file_key)
                
                # Check if this is one of our tracked files
                if filename in filename_to_path:
                    try:
                        statements = int(parts[1])
                        missed = int(parts[2])
                        coverage_pct = float(parts[3].rstrip('%'))
                        file_path = filename_to_path[filename]
                        file_coverage[file_path] = {
                            'statements': statements,
                            'missed': missed,
                            'coverage': coverage_pct
                        }
                        # Only count web app runtime files in totals
                        if file_path in important_files:
                            total_statements += statements
                            total_missed += missed
                    except (ValueError, IndexError):
                        pass
        
        # Display web app runtime files coverage
        print("Web App Runtime Files:")
        for file_path in sorted(important_files):
            if file_path in file_coverage:
                info = file_coverage[file_path]
                if info['coverage'] >= 80:
                    status = f"{Colors.GREEN}✓{Colors.END}"
                    color = Colors.GREEN
                elif info['coverage'] >= 50:
                    status = f"{Colors.YELLOW}⚠{Colors.END}"
                    color = Colors.YELLOW
                else:
                    status = f"{Colors.RED}✗{Colors.END}"
                    color = Colors.RED
                print(f"{status} {color}{file_path:45}{Colors.END} {color}{info['coverage']:5.1f}%{Colors.END} ({info['statements'] - info['missed']}/{info['statements']} statements)")
            else:
                # Check if file exists but wasn't executed (no coverage)
                full_path = os.path.join(project_root, file_path)
                if os.path.exists(full_path):
                    print(f"{Colors.RED}✗{Colors.END} {Colors.RED}{file_path:45}{Colors.END} {Colors.RED}Not executed (0%) - File exists but wasn't imported/executed during tests{Colors.END}")
                else:
                    print(f"{Colors.RED}✗{Colors.END} {Colors.RED}{file_path:45}{Colors.END} {Colors.RED}Not found{Colors.END}")
        
        # Display data preparation scripts (informational)
        print("\nData Preparation Scripts (not web app runtime):")
        for file_path in sorted(data_prep_files):
            if file_path in file_coverage:
                info = file_coverage[file_path]
                if info['coverage'] >= 80:
                    status = f"{Colors.GREEN}✓{Colors.END}"
                    color = Colors.GREEN
                elif info['coverage'] >= 50:
                    status = f"{Colors.YELLOW}⚠{Colors.END}"
                    color = Colors.YELLOW
                else:
                    status = f"{Colors.RED}✗{Colors.END}"
                    color = Colors.RED
                print(f"{status} {color}{file_path:45}{Colors.END} {color}{info['coverage']:5.1f}%{Colors.END} ({info['statements'] - info['missed']}/{info['statements']} statements)")
            else:
                full_path = os.path.join(project_root, file_path)
                if os.path.exists(full_path):
                    print(f"{Colors.CYAN}•{Colors.END} {file_path:45} {Colors.CYAN}Not executed - Data prep script (not part of web app runtime){Colors.END}")
                else:
                    print(f"{Colors.RED}✗{Colors.END} {Colors.RED}{file_path:45}{Colors.END} {Colors.RED}Not found{Colors.END}")
        
        # Calculate and show total coverage
        print()
        print("-" * 80)
        if total_statements > 0:
            total_coverage = ((total_statements - total_missed) / total_statements) * 100
            if total_coverage >= 80:
                status_icon = f"{Colors.GREEN}✓{Colors.END}"
                color = Colors.GREEN
            elif total_coverage >= 50:
                status_icon = f"{Colors.YELLOW}⚠{Colors.END}"
                color = Colors.YELLOW
            else:
                status_icon = f"{Colors.RED}✗{Colors.END}"
                color = Colors.RED
            print(f"{status_icon} {color}Total (Important Files):{Colors.END} {color}{total_coverage:5.1f}%{Colors.END} ({total_statements - total_missed}/{total_statements} statements)")
        
        # Also show overall total from report
        for line in lines:
            if 'TOTAL' in line:
                parts = line.split()
                if len(parts) >= 4:
                    try:
                        overall_coverage = float(parts[-1].rstrip('%'))
                        if overall_coverage >= 80:
                            overall_color = Colors.GREEN
                        elif overall_coverage >= 50:
                            overall_color = Colors.YELLOW
                        else:
                            overall_color = Colors.RED
                        print(f"  {overall_color}Overall (All Files): {parts[-1]}{Colors.END}")
                    except (ValueError, IndexError):
                        pass
                break
    
    # Display slowest tests
    # Only run individual test timing if there are fewer than 50 tests (to avoid long runtime)
    if test_timings and len(test_timings) > 0 and total_test_time > 0:
        print()
        print("=" * 80)
        print("TEST EXECUTION TIMING")
        print("=" * 80)
        print()
        
        # Calculate average time per test
        avg_time_per_test = total_test_time / len(test_timings)
        
        # Only run individual test timing if requested or if there aren't too many tests
        # Default threshold: 150 tests (reasonable for most projects)
        # For fewer tests, always measure individual timing for useful insights
        measure_individual = measure_test_timing or len(test_timings) <= 150
        
        if measure_individual:
            print("Measuring individual test execution times...")
            
            # Run each test individually to get accurate timing
            for i, test_info in enumerate(test_timings, 1):
                # Build test spec: tests.module.Class.method
                test_full_name = test_info['full_name']
                
                # Run individual test with timing
                individual_cmd = [
                    sys.executable, '-m', 'unittest', test_full_name, '-q'
                ]
                
                start = time.time()
                individual_result = subprocess.run(
                    individual_cmd,
                    cwd=project_root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                duration = time.time() - start
                
                test_info['duration'] = duration
                
                # Show progress
                if i % 10 == 0 or duration > 0.5:
                    print(f"  Progress: {i}/{len(test_timings)} tests measured...", end='\r')
            
            print()  # New line after progress
            
            # Sort by duration (slowest first)
            sorted_tests = sorted(test_timings, key=lambda x: x['duration'], reverse=True)
            
            # Show top 10 slowest tests
            top_n = min(10, len(sorted_tests))
            
            print(f"\nTop {top_n} slowest tests (out of {len(test_timings)} total):")
            print()
            print(f"{'Rank':<6} {'Duration':<12} {'Status':<8} {'Test Name'}")
            print("-" * 80)
            
            for idx, test_info in enumerate(sorted_tests[:top_n], 1):
                duration_str = f"{test_info['duration']:.3f}s"
                status = test_info['status']
                # Color code status
                if status == 'ok':
                    status_color = Colors.GREEN
                elif status == 'FAIL':
                    status_color = Colors.RED
                elif status == 'ERROR':
                    status_color = Colors.RED
                else:
                    status_color = Colors.YELLOW
                
                # Show full test name (may wrap, but that's okay)
                test_name = test_info['full_name']
                # Truncate only if extremely long
                if len(test_name) > 70:
                    test_name = test_name[:67] + "..."
                print(f"{idx:<6} {duration_str:<12} {status_color}{status:<8}{Colors.END} {test_name}")
        else:
            print(f"Note: {len(test_timings)} tests found. Individual timing skipped (would take too long).")
            print("Showing summary statistics instead:")
            print()
            # Assign average duration to all tests for display purposes
            for test_info in test_timings:
                test_info['duration'] = avg_time_per_test
        
        # Show total test time and statistics
        print()
        print("-" * 80)
        print(f"Total test execution time: {total_test_time:.2f}s")
        print(f"Average test time: {avg_time_per_test:.3f}s")
        print(f"Number of tests: {len(test_timings)}")
        if measure_individual and len(test_timings) > 0:
            max_time = max(t['duration'] for t in test_timings)
            min_time = min(t['duration'] for t in test_timings)
            print(f"Fastest test: {min_time:.3f}s")
            print(f"Slowest test: {max_time:.3f}s")
    
    return 0

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Run test coverage analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 tests/run_coverage.py
  python3 tests/run_coverage.py --html
  python3 tests/run_coverage.py --html --show-missing
  python3 tests/run_coverage.py --verbose
        """
    )
    
    parser.add_argument(
        '--html',
        action='store_true',
        help='Generate HTML coverage report'
    )
    
    parser.add_argument(
        '--show-missing',
        action='store_true',
        help='Show missing line numbers in report'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose output'
    )
    
    parser.add_argument(
        '--measure-test-timing',
        action='store_true',
        help='Measure individual test execution times (can be slow with many tests)'
    )
    
    args = parser.parse_args()
    
    exit_code = run_coverage(
        html=args.html,
        verbose=args.verbose,
        show_missing=args.show_missing,
        measure_test_timing=args.measure_test_timing
    )
    
    sys.exit(exit_code)

if __name__ == '__main__':
    main()

