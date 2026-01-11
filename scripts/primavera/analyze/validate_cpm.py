#!/usr/bin/env python3
"""
CPM Engine Validation Test.

Compares CPM calculations against P6's stored values to validate accuracy.
Produces a detailed report highlighting matches and mismatches.

Usage:
    python scripts/primavera/analyze/validate_cpm.py [--file-id N] [--output FILE]
    python scripts/primavera/analyze/validate_cpm.py --help
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
from typing import Optional
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from scripts.primavera.analyze.data_loader import (
    load_schedule,
    get_latest_file_id,
    list_schedule_versions,
)
from scripts.primavera.analyze.cpm.engine import CPMEngine


class CPMValidationResult:
    """Results from CPM validation against P6."""

    def __init__(self):
        self.file_id: int = 0
        self.data_date: Optional[datetime] = None
        self.total_tasks: int = 0
        self.compared_tasks: int = 0
        self.completed_tasks: int = 0
        self.in_progress_tasks: int = 0
        self.not_started_tasks: int = 0

        # Per-field results: {field: {'matches': [], 'mismatches': []}}
        self.field_results: dict = defaultdict(lambda: {'matches': [], 'mismatches': []})

        # Mismatch details for reporting
        self.mismatch_details: list = []

    def add_comparison(self, task, field: str, calculated, p6_value,
                       tolerance_hours: float = 1.0):
        """Add a field comparison result."""
        if calculated is None or p6_value is None:
            return

        # Calculate difference in hours
        if isinstance(calculated, datetime):
            diff_hours = abs((calculated - p6_value).total_seconds()) / 3600
        else:
            diff_hours = abs(calculated - p6_value)

        is_match = diff_hours <= tolerance_hours

        result = {
            'task_id': task.task_id,
            'task_code': task.task_code,
            'task_name': task.task_name[:50],
            'status': task.status,
            'calendar_id': task.calendar_id,
            'calculated': calculated,
            'p6_value': p6_value,
            'diff_hours': diff_hours,
            'is_match': is_match,
        }

        if is_match:
            self.field_results[field]['matches'].append(result)
        else:
            self.field_results[field]['mismatches'].append(result)
            self.mismatch_details.append({**result, 'field': field})

    def get_match_rate(self, field: str) -> float:
        """Get match rate for a field as percentage."""
        matches = len(self.field_results[field]['matches'])
        mismatches = len(self.field_results[field]['mismatches'])
        total = matches + mismatches
        if total == 0:
            return 0.0
        return (matches / total) * 100

    def get_summary_df(self) -> pd.DataFrame:
        """Get summary statistics as DataFrame."""
        rows = []
        for field in ['early_start', 'early_finish', 'late_start', 'late_finish', 'total_float']:
            matches = len(self.field_results[field]['matches'])
            mismatches = len(self.field_results[field]['mismatches'])
            total = matches + mismatches
            rate = (matches / total * 100) if total > 0 else 0

            rows.append({
                'Field': field,
                'Compared': total,
                'Matches': matches,
                'Mismatches': mismatches,
                'Match Rate': f"{rate:.1f}%",
            })

        return pd.DataFrame(rows)

    def get_mismatch_df(self) -> pd.DataFrame:
        """Get mismatch details as DataFrame."""
        if not self.mismatch_details:
            return pd.DataFrame()

        df = pd.DataFrame(self.mismatch_details)

        # Format datetime columns
        for col in ['calculated', 'p6_value']:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: x.strftime('%Y-%m-%d %H:%M') if isinstance(x, datetime) else x
                )

        # Sort by diff_hours descending
        df = df.sort_values('diff_hours', ascending=False)

        return df


def validate_cpm(file_id: int, verbose: bool = True) -> CPMValidationResult:
    """
    Validate CPM calculations against P6 stored values.

    Args:
        file_id: Schedule version to validate
        verbose: Print progress messages

    Returns:
        CPMValidationResult with detailed comparison data
    """
    result = CPMValidationResult()
    result.file_id = file_id

    # Load schedule
    if verbose:
        print(f"\n{'='*80}")
        print("CPM ENGINE VALIDATION TEST")
        print(f"{'='*80}")
        print(f"\nLoading schedule file_id={file_id}...")

    network, calendars, project_info = load_schedule(file_id, verbose=verbose)
    result.data_date = project_info.get('data_date')
    result.total_tasks = len(network.tasks)

    if verbose:
        print(f"\nProject Info:")
        print(f"  Data Date: {result.data_date}")
        print(f"  Total Tasks: {result.total_tasks}")

    # Count by status
    for task in network.tasks.values():
        if task.is_completed():
            result.completed_tasks += 1
        elif task.is_in_progress():
            result.in_progress_tasks += 1
        else:
            result.not_started_tasks += 1

    if verbose:
        print(f"  Completed: {result.completed_tasks}")
        print(f"  In Progress: {result.in_progress_tasks}")
        print(f"  Not Started: {result.not_started_tasks}")

    # Run CPM
    if verbose:
        print("\nRunning CPM engine...")

    engine = CPMEngine(network, calendars)
    cpm_result = engine.run(data_date=result.data_date)

    if verbose:
        print(f"  Project Start: {cpm_result.project_start}")
        print(f"  Project Finish: {cpm_result.project_finish}")
        print(f"  Critical Path: {len(cpm_result.critical_path)} tasks")

    # Compare with P6 values
    if verbose:
        print("\nComparing with P6 stored values...")

    for task in network.tasks.values():
        # Skip completed tasks (dates are actuals, not calculated)
        if task.is_completed():
            continue

        # Skip tasks without P6 values
        if task.p6_early_finish is None:
            continue

        result.compared_tasks += 1

        # Compare each field
        result.add_comparison(task, 'early_start', task.early_start, task.p6_early_start)
        result.add_comparison(task, 'early_finish', task.early_finish, task.p6_early_finish)
        result.add_comparison(task, 'late_start', task.late_start, task.p6_late_start)
        result.add_comparison(task, 'late_finish', task.late_finish, task.p6_late_finish)

        # Float uses 8-hour (1 day) tolerance
        if task.total_float_hours is not None and task.p6_total_float_hours is not None:
            result.add_comparison(task, 'total_float',
                                  task.total_float_hours, task.p6_total_float_hours,
                                  tolerance_hours=8.0)

    if verbose:
        print(f"  Compared: {result.compared_tasks} tasks (excluding completed)")

    return result


def print_report(result: CPMValidationResult, show_mismatches: int = 20):
    """Print validation report to console."""

    print(f"\n{'='*80}")
    print("VALIDATION RESULTS")
    print(f"{'='*80}")

    print(f"\nSchedule: file_id={result.file_id}")
    print(f"Data Date: {result.data_date}")
    print(f"Tasks Compared: {result.compared_tasks} (of {result.total_tasks} total)")
    print(f"  - Completed (excluded): {result.completed_tasks}")
    print(f"  - In Progress: {result.in_progress_tasks}")
    print(f"  - Not Started: {result.not_started_tasks}")

    # Summary table
    print(f"\n{'='*80}")
    print("ACCURACY SUMMARY")
    print(f"{'='*80}")

    summary = result.get_summary_df()
    print(f"\n{summary.to_string(index=False)}")

    # Overall accuracy
    total_matches = sum(len(result.field_results[f]['matches'])
                       for f in ['early_start', 'early_finish', 'late_start', 'late_finish'])
    total_comparisons = sum(len(result.field_results[f]['matches']) + len(result.field_results[f]['mismatches'])
                           for f in ['early_start', 'early_finish', 'late_start', 'late_finish'])
    overall_rate = (total_matches / total_comparisons * 100) if total_comparisons > 0 else 0

    print(f"\nOverall Date Match Rate: {overall_rate:.1f}%")
    print(f"Total Comparisons: {total_comparisons}")

    # Mismatch analysis
    mismatches = result.mismatch_details
    if mismatches:
        print(f"\n{'='*80}")
        print(f"MISMATCH ANALYSIS ({len(mismatches)} total)")
        print(f"{'='*80}")

        # Group by field
        by_field = defaultdict(list)
        for m in mismatches:
            by_field[m['field']].append(m)

        for field, items in sorted(by_field.items()):
            print(f"\n--- {field.upper()} ({len(items)} mismatches) ---")

            # Show distribution of difference magnitudes
            diffs = [m['diff_hours'] for m in items]
            print(f"  Difference range: {min(diffs):.1f} - {max(diffs):.1f} hours")
            print(f"  Avg difference: {sum(diffs)/len(diffs):.1f} hours")

            # Count by magnitude
            bands = [
                (1, 8, '1-8 hrs (< 1 day)'),
                (8, 24, '8-24 hrs (1-3 days)'),
                (24, 80, '24-80 hrs (3-10 days)'),
                (80, float('inf'), '>80 hrs (>10 days)'),
            ]
            for low, high, label in bands:
                count = sum(1 for d in diffs if low < d <= high)
                if count > 0:
                    print(f"    {label}: {count}")

        # Show sample mismatches
        if show_mismatches > 0:
            print(f"\n{'='*80}")
            print(f"SAMPLE MISMATCHES (top {min(show_mismatches, len(mismatches))} by difference)")
            print(f"{'='*80}")

            df = result.get_mismatch_df()
            display_cols = ['task_code', 'field', 'calculated', 'p6_value', 'diff_hours', 'status']
            print(f"\n{df[display_cols].head(show_mismatches).to_string(index=False)}")
    else:
        print("\nNo mismatches found - perfect match with P6!")

    # Calendar analysis for mismatches
    if mismatches:
        print(f"\n{'='*80}")
        print("MISMATCH BY CALENDAR")
        print(f"{'='*80}")

        by_cal = defaultdict(int)
        for m in mismatches:
            by_cal[m['calendar_id']] += 1

        for cal_id, count in sorted(by_cal.items(), key=lambda x: -x[1]):
            print(f"  {cal_id}: {count} mismatches")

    # Status analysis
    if mismatches:
        print(f"\n{'='*80}")
        print("MISMATCH BY STATUS")
        print(f"{'='*80}")

        by_status = defaultdict(int)
        for m in mismatches:
            by_status[m['status']] += 1

        for status, count in sorted(by_status.items(), key=lambda x: -x[1]):
            print(f"  {status}: {count} mismatches")


def save_report(result: CPMValidationResult, output_path: Path):
    """Save validation report to files."""

    # Save summary
    summary_path = output_path.with_suffix('.summary.txt')
    with open(summary_path, 'w') as f:
        f.write(f"CPM Validation Report\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Schedule: file_id={result.file_id}\n")
        f.write(f"Data Date: {result.data_date}\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n\n")

        f.write(f"Tasks Compared: {result.compared_tasks}\n")
        f.write(f"  Completed (excluded): {result.completed_tasks}\n")
        f.write(f"  In Progress: {result.in_progress_tasks}\n")
        f.write(f"  Not Started: {result.not_started_tasks}\n\n")

        f.write("Accuracy Summary:\n")
        summary = result.get_summary_df()
        f.write(summary.to_string(index=False))
        f.write("\n")

    print(f"\nSummary saved to: {summary_path}")

    # Save mismatch details to CSV
    if result.mismatch_details:
        mismatch_path = output_path.with_suffix('.mismatches.csv')
        df = result.get_mismatch_df()
        df.to_csv(mismatch_path, index=False)
        print(f"Mismatches saved to: {mismatch_path}")

    # Save all match/mismatch data for analysis
    all_data_path = output_path.with_suffix('.all_comparisons.csv')
    all_rows = []
    for field, data in result.field_results.items():
        for item in data['matches']:
            all_rows.append({**item, 'field': field, 'result': 'match'})
        for item in data['mismatches']:
            all_rows.append({**item, 'field': field, 'result': 'mismatch'})

    if all_rows:
        all_df = pd.DataFrame(all_rows)
        all_df.to_csv(all_data_path, index=False)
        print(f"All comparisons saved to: {all_data_path}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Validate CPM engine against P6 stored values',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python validate_cpm.py                    # Validate latest schedule
  python validate_cpm.py --file-id 64       # Validate specific schedule
  python validate_cpm.py --list-schedules   # List available schedules
  python validate_cpm.py --output report    # Save report to files
        """
    )

    parser.add_argument('--file-id', type=int, default=None,
                        help='Schedule file_id to validate (default: latest)')
    parser.add_argument('--list-schedules', action='store_true',
                        help='List available schedule versions')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='Output file path prefix for saved reports')
    parser.add_argument('--show-mismatches', type=int, default=20,
                        help='Number of sample mismatches to show (default: 20)')
    parser.add_argument('--quiet', '-q', action='store_true',
                        help='Minimal output')

    args = parser.parse_args()

    # List schedules
    if args.list_schedules:
        versions = list_schedule_versions()
        print("\nAvailable schedule versions:")
        print(versions.to_string())
        return 0

    # Get file_id
    file_id = args.file_id or get_latest_file_id()

    # Run validation
    result = validate_cpm(file_id, verbose=not args.quiet)

    # Print report
    if not args.quiet:
        print_report(result, show_mismatches=args.show_mismatches)

    # Save report if requested
    if args.output:
        output_path = Path(args.output)
        save_report(result, output_path)

    # Return exit code based on match rate
    match_rate = result.get_match_rate('early_finish')
    if match_rate >= 95.0:
        print(f"\n[PASS] Early finish match rate {match_rate:.1f}% >= 95%")
        return 0
    else:
        print(f"\n[WARN] Early finish match rate {match_rate:.1f}% < 95%")
        return 1


if __name__ == "__main__":
    sys.exit(main())
