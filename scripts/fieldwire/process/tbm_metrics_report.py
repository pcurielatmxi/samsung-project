#!/usr/bin/env python3
"""
TBM Metrics Report Generator

Calculates and reports TBM (Toolbox Meeting) labor metrics from raw Fieldwire data.

METRIC DEFINITIONS (from Field Team):
=====================================

TBM Actual:
    SUM of TBM Manpower WHERE Category = "Manpower Count"
    Meaning: Total headcount at morning Toolbox Meeting (all workers present at start of day)

TBM Planned:
    SUM of TBM Manpower WHERE Status = "TBM" AND Category != "Manpower Count"
    Meaning: Planned worker deployment across all work locations

Verified:
    SUM of (Direct MP + Indirect MP) WHERE Status = "TBM" AND Category != "Manpower Count" AND TBM Manpower > 0
    Meaning: Workers found at locations that HAD planned manpower

Unverified:
    SUM of (Direct MP + Indirect MP) WHERE Status = "TBM" AND Category != "Manpower Count" AND (TBM Manpower = 0 OR NULL)
    Meaning: Workers found at locations with NO planned manpower (showed up somewhere unexpected)

Total Found:
    Verified + Unverified
    Meaning: All workers found at any work location during audits

Not Found:
    TBM Actual - Total Found
    Meaning: Workers counted at morning TBM but not observed at any work location

LPI % (Labor Planning Index):
    Verified / TBM Planned × 100
    Target: 80%
    Meaning: How well planned labor deployment matches actual field execution


WORKER FLOW:
============

    Morning TBM (TBM Actual)
        │
        ├──► Found at work locations (Total Found)
        │       ├──► At PLANNED locations (Verified) ✓
        │       └──► At UNPLANNED locations (Unverified) ⚠
        │
        └──► NOT found at any location (Not Found) ✗


Usage:
    python -m scripts.fieldwire.process.tbm_metrics_report
    python -m scripts.fieldwire.process.tbm_metrics_report --output report.csv
    python -m scripts.fieldwire.process.tbm_metrics_report --by-company
    python -m scripts.fieldwire.process.tbm_metrics_report --by-date
"""

import argparse
import csv
import sys
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.config.settings import settings


def parse_numeric(val: str) -> float:
    """Parse numeric value, returning 0 for empty/invalid."""
    if not val or val.strip() in ('', '""'):
        return 0.0
    try:
        return float(val.strip().strip('"'))
    except ValueError:
        return 0.0


def parse_date(val: str) -> Optional[str]:
    """Parse date value, return YYYY-MM-DD or None."""
    if not val or val.strip() in ('', '""'):
        return None
    val = val.strip().strip('"')
    if len(val) >= 10 and val[4] == '-' and val[7] == '-':
        return val[:10]
    return None


def read_fieldwire_data(filepath: Path) -> tuple[list[str], list[list[str]]]:
    """
    Read Fieldwire CSV file (UTF-16LE encoded).

    Returns: (headers, data_rows)
    """
    with open(filepath, 'r', encoding='utf-16-le') as f:
        content = f.read()
        if content.startswith('\ufeff'):
            content = content[1:]

    reader = csv.reader(StringIO(content), delimiter='\t')
    rows = list(reader)

    # Row 4 (index 3) is column headers, data starts at row 5 (index 4)
    headers = rows[3]
    data = rows[4:]

    return headers, data


def find_latest_input_file(input_dir: Path) -> Optional[Path]:
    """Find the most recent Fieldwire CSV dump file."""
    csv_files = list(input_dir.glob('Samsung_-_Progress_Tracking*.csv'))
    if not csv_files:
        return None
    csv_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return csv_files[0]


def calculate_metrics(headers: list[str], data: list[list[str]],
                      filter_company: str = None, filter_date: str = None) -> dict:
    """
    Calculate TBM metrics from raw data.

    Args:
        headers: Column headers
        data: Data rows
        filter_company: Optional company filter
        filter_date: Optional date filter (YYYY-MM-DD)

    Returns:
        Dict with all calculated metrics
    """
    # Find column indices
    status_idx = headers.index('Status')
    category_idx = headers.index('Category')
    tbm_mp_idx = headers.index('TBM Manpower')
    direct_mp_idx = headers.index('Direct Manpower')
    indirect_mp_idx = headers.index('Indirect Manpower')
    company_idx = headers.index('Company')
    date_idx = headers.index('Start date')

    # Initialize counters
    tbm_actual = 0.0
    tbm_planned = 0.0
    verified = 0.0
    unverified = 0.0
    tbm_location_count = 0
    manpower_count_records = 0

    for row in data:
        if len(row) <= max(status_idx, category_idx, tbm_mp_idx, direct_mp_idx, indirect_mp_idx, company_idx, date_idx):
            continue

        status = row[status_idx].strip().strip('"')
        category = row[category_idx].strip().strip('"')
        company = row[company_idx].strip().strip('"')
        date = parse_date(row[date_idx])
        tbm_mp = parse_numeric(row[tbm_mp_idx])
        direct_mp = parse_numeric(row[direct_mp_idx])
        indirect_mp = parse_numeric(row[indirect_mp_idx])
        actual_mp = direct_mp + indirect_mp

        # Apply filters
        if filter_company and company != filter_company:
            continue
        if filter_date and date != filter_date:
            continue

        # TBM Actual = Manpower Count category
        if category == 'Manpower Count':
            tbm_actual += tbm_mp
            manpower_count_records += 1

        # TBM Planned, Verified, Unverified = Status="TBM" AND Category != "Manpower Count"
        elif status == 'TBM' and category != 'Manpower Count':
            tbm_location_count += 1
            tbm_planned += tbm_mp

            if tbm_mp > 0:
                verified += actual_mp
            else:
                unverified += actual_mp

    # Calculate derived metrics
    total_found = verified + unverified
    not_found = tbm_actual - total_found
    lpi_pct = (verified / tbm_planned * 100) if tbm_planned > 0 else 0.0

    found_pct = (total_found / tbm_actual * 100) if tbm_actual > 0 else 0.0
    not_found_pct = (not_found / tbm_actual * 100) if tbm_actual > 0 else 0.0
    verified_of_found_pct = (verified / total_found * 100) if total_found > 0 else 0.0

    return {
        'tbm_location_count': tbm_location_count,
        'manpower_count_records': manpower_count_records,
        'tbm_actual': tbm_actual,
        'tbm_planned': tbm_planned,
        'verified': verified,
        'unverified': unverified,
        'total_found': total_found,
        'not_found': not_found,
        'lpi_pct': lpi_pct,
        'found_pct': found_pct,
        'not_found_pct': not_found_pct,
        'verified_of_found_pct': verified_of_found_pct,
    }


def calculate_metrics_by_company(headers: list[str], data: list[list[str]]) -> list[dict]:
    """Calculate metrics grouped by company."""
    company_idx = headers.index('Company')

    # Get unique companies
    companies = set()
    for row in data:
        if len(row) > company_idx:
            company = row[company_idx].strip().strip('"')
            if company:
                companies.add(company)

    results = []
    for company in sorted(companies):
        metrics = calculate_metrics(headers, data, filter_company=company)
        if metrics['tbm_actual'] > 0 or metrics['tbm_planned'] > 0:
            metrics['company'] = company
            results.append(metrics)

    return results


def calculate_metrics_by_date(headers: list[str], data: list[list[str]]) -> list[dict]:
    """Calculate metrics grouped by date."""
    date_idx = headers.index('Start date')

    # Get unique dates
    dates = set()
    for row in data:
        if len(row) > date_idx:
            date = parse_date(row[date_idx])
            if date:
                dates.add(date)

    results = []
    for date in sorted(dates):
        metrics = calculate_metrics(headers, data, filter_date=date)
        if metrics['tbm_actual'] > 0 or metrics['tbm_planned'] > 0:
            metrics['date'] = date
            results.append(metrics)

    return results


def print_report(metrics: dict, title: str = "TBM METRICS REPORT"):
    """Print formatted metrics report to stdout."""
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)
    print()
    print(f"TBM Location Records:    {metrics['tbm_location_count']:,}")
    print(f"Manpower Count Records:  {metrics['manpower_count_records']:,}")
    print()
    print("-" * 70)
    print(f"{'METRIC':<35} {'VALUE':>15} {'NOTES':<20}")
    print("-" * 70)
    print(f"{'TBM Actual (morning headcount)':<35} {metrics['tbm_actual']:>15,.1f}")
    print(f"{'TBM Planned (deployment plan)':<35} {metrics['tbm_planned']:>15,.1f}")
    print()
    print(f"{'Verified (at planned locations)':<35} {metrics['verified']:>15,.1f}")
    print(f"{'Unverified (at unplanned locations)':<35} {metrics['unverified']:>15,.1f}")
    print(f"{'Total Found':<35} {metrics['total_found']:>15,.1f} ({metrics['found_pct']:.1f}% of Actual)")
    print(f"{'Not Found':<35} {metrics['not_found']:>15,.1f} ({metrics['not_found_pct']:.1f}% of Actual)")
    print()
    print(f"{'LPI % (Verified / Planned)':<35} {metrics['lpi_pct']:>14.1f}% {'Target: 80%':<20}")
    print("-" * 70)
    print()
    print("Worker Flow:")
    print(f"  Morning TBM: {metrics['tbm_actual']:,.1f}")
    print(f"      │")
    print(f"      ├─► Found: {metrics['total_found']:,.1f} ({metrics['found_pct']:.1f}%)")
    print(f"      │     ├─► Verified:   {metrics['verified']:,.1f}")
    print(f"      │     └─► Unverified: {metrics['unverified']:,.1f}")
    print(f"      │")
    print(f"      └─► Not Found: {metrics['not_found']:,.1f} ({metrics['not_found_pct']:.1f}%)")
    print()


def print_company_report(results: list[dict]):
    """Print metrics by company."""
    print()
    print("=" * 100)
    print("TBM METRICS BY COMPANY")
    print("=" * 100)
    print()
    print(f"{'Company':<20} {'TBM Actual':>12} {'TBM Planned':>12} {'Verified':>10} {'Unverified':>10} {'Not Found':>10} {'LPI %':>8}")
    print("-" * 100)

    for r in results:
        print(f"{r['company']:<20} {r['tbm_actual']:>12,.1f} {r['tbm_planned']:>12,.1f} {r['verified']:>10,.1f} {r['unverified']:>10,.1f} {r['not_found']:>10,.1f} {r['lpi_pct']:>7.1f}%")

    print("-" * 100)
    print()


def print_date_report(results: list[dict]):
    """Print metrics by date."""
    print()
    print("=" * 100)
    print("TBM METRICS BY DATE")
    print("=" * 100)
    print()
    print(f"{'Date':<12} {'TBM Actual':>12} {'TBM Planned':>12} {'Verified':>10} {'Unverified':>10} {'Not Found':>10} {'LPI %':>8}")
    print("-" * 100)

    for r in results:
        print(f"{r['date']:<12} {r['tbm_actual']:>12,.1f} {r['tbm_planned']:>12,.1f} {r['verified']:>10,.1f} {r['unverified']:>10,.1f} {r['not_found']:>10,.1f} {r['lpi_pct']:>7.1f}%")

    print("-" * 100)
    print()


def write_csv_report(metrics: dict, output_path: Path):
    """Write metrics to CSV file."""
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Metric', 'Value', 'Description'])
        writer.writerow(['tbm_actual', metrics['tbm_actual'], 'Headcount at morning TBM'])
        writer.writerow(['tbm_planned', metrics['tbm_planned'], 'Planned deployment across locations'])
        writer.writerow(['verified', metrics['verified'], 'Found at planned locations'])
        writer.writerow(['unverified', metrics['unverified'], 'Found at unplanned locations'])
        writer.writerow(['total_found', metrics['total_found'], 'Verified + Unverified'])
        writer.writerow(['not_found', metrics['not_found'], 'At TBM but not found at locations'])
        writer.writerow(['lpi_pct', metrics['lpi_pct'], 'Verified / TBM Planned × 100'])
        writer.writerow(['found_pct', metrics['found_pct'], 'Total Found / TBM Actual × 100'])
        writer.writerow(['not_found_pct', metrics['not_found_pct'], 'Not Found / TBM Actual × 100'])
    print(f"Report written to: {output_path}")


def write_csv_by_group(results: list[dict], output_path: Path, group_col: str):
    """Write grouped metrics to CSV file."""
    if not results:
        print("No data to write.")
        return

    fieldnames = [group_col, 'tbm_actual', 'tbm_planned', 'verified', 'unverified',
                  'total_found', 'not_found', 'lpi_pct', 'found_pct', 'not_found_pct']

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)
    print(f"Report written to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Generate TBM metrics report from Fieldwire data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--input', '-i',
        type=Path,
        help='Input CSV file (default: latest in processed/fieldwire/)'
    )
    parser.add_argument(
        '--output', '-o',
        type=Path,
        help='Output CSV file (optional)'
    )
    parser.add_argument(
        '--by-company',
        action='store_true',
        help='Show metrics grouped by company'
    )
    parser.add_argument(
        '--by-date',
        action='store_true',
        help='Show metrics grouped by date'
    )
    parser.add_argument(
        '--company',
        type=str,
        help='Filter to specific company'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Filter to specific date (YYYY-MM-DD)'
    )
    args = parser.parse_args()

    # Determine input file
    if args.input:
        input_file = args.input
    else:
        input_dir = settings.DATA_DIR / 'processed' / 'fieldwire'
        input_file = find_latest_input_file(input_dir)
        if not input_file:
            print(f"Error: No Fieldwire CSV files found in {input_dir}")
            sys.exit(1)

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        sys.exit(1)

    print(f"Input: {input_file.name}")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Read data
    headers, data = read_fieldwire_data(input_file)
    print(f"Total rows: {len(data):,}")

    # Generate report based on options
    if args.by_company:
        results = calculate_metrics_by_company(headers, data)
        if args.output:
            write_csv_by_group(results, args.output, 'company')
        else:
            print_company_report(results)

    elif args.by_date:
        results = calculate_metrics_by_date(headers, data)
        if args.output:
            write_csv_by_group(results, args.output, 'date')
        else:
            print_date_report(results)

    else:
        metrics = calculate_metrics(headers, data,
                                   filter_company=args.company,
                                   filter_date=args.date)

        title = "TBM METRICS REPORT"
        if args.company:
            title += f" - {args.company}"
        if args.date:
            title += f" - {args.date}"

        if args.output:
            write_csv_report(metrics, args.output)
        else:
            print_report(metrics, title)


if __name__ == '__main__':
    main()
