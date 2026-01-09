#!/usr/bin/env python3
"""Test script for monthly report data loaders.

Usage:
    python -m scripts.integrated_analysis.monthly_reports.test_loaders 2024-03
    python -m scripts.integrated_analysis.monthly_reports.test_loaders --all
"""

import argparse
import sys
from pathlib import Path
from datetime import date

_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from scripts.integrated_analysis.monthly_reports.data_loaders import (
    get_monthly_period,
    load_schedule_data,
    load_labor_data,
    load_quality_data,
    load_narrative_data,
)


def test_period(year_month: str) -> None:
    """Test all loaders for a specific period."""
    print(f"\n{'='*60}")
    print(f"Testing period: {year_month}")
    print('='*60)

    period = get_monthly_period(year_month)
    print(f"Period: {period.start_date} to {period.end_date}")

    # Test each loader
    print("\n--- Schedule Data (P6) ---")
    schedule = load_schedule_data(period)
    print(f"Tasks: {len(schedule['tasks']):,}")
    print(f"Snapshots: {schedule['snapshots']}")
    print(f"Availability: {schedule['availability'].to_dict()}")

    if not schedule['tasks'].empty:
        df = schedule['tasks']
        print(f"  Completed this period: {df['completed_this_period'].sum():,}")
        print(f"  Started this period: {df['started_this_period'].sum():,}")
        if 'building' in df.columns:
            print(f"  Buildings: {df['building'].value_counts().head(3).to_dict()}")

    print("\n--- Quality Data (RABA + PSI) ---")
    quality = load_quality_data(period)
    print(f"Combined inspections: {len(quality['inspections']):,}")
    print(f"RABA: {len(quality['raba']):,}")
    print(f"PSI: {len(quality['psi']):,}")
    for avail in quality['availability']:
        print(f"  {avail.source}: {avail.to_dict()}")

    if not quality['inspections'].empty:
        df = quality['inspections']
        if 'outcome_normalized' in df.columns:
            print(f"  Outcomes: {df['outcome_normalized'].value_counts().to_dict()}")
        if 'dim_company_id' in df.columns:
            coverage = df['dim_company_id'].notna().mean() * 100
            print(f"  Company ID coverage: {coverage:.1f}%")

    print("\n--- Labor Data (ProjectSight + TBM + Weekly) ---")
    labor = load_labor_data(period)
    print(f"Combined entries: {len(labor['labor']):,}")
    print(f"ProjectSight: {len(labor['projectsight']):,}")
    print(f"TBM: {len(labor['tbm']):,}")
    print(f"Weekly Reports: {len(labor['weekly_reports']):,}")
    for avail in labor['availability']:
        print(f"  {avail.source}: {avail.to_dict()}")

    if not labor['labor'].empty:
        df = labor['labor']
        if 'hours' in df.columns:
            print(f"  Total hours: {df['hours'].sum():,.0f}")
        if 'source' in df.columns:
            print(f"  By source: {df.groupby('source')['hours'].sum().to_dict() if 'hours' in df.columns else df['source'].value_counts().to_dict()}")

    print("\n--- Narrative Data ---")
    narratives = load_narrative_data(period)
    print(f"Statements in period: {len(narratives['statements']):,}")
    print(f"Undated statements: {len(narratives['statements_undated']):,}")
    print(f"Source documents: {len(narratives['documents']):,}")
    print(f"Availability: {narratives['availability'].to_dict()}")

    if not narratives['statements'].empty:
        df = narratives['statements']
        if 'category' in df.columns:
            print(f"  Categories: {df['category'].value_counts().head(5).to_dict()}")
        if 'impact_days' in df.columns:
            with_impact = df['impact_days'].notna()
            print(f"  Statements with impact: {with_impact.sum()}")
            if with_impact.any():
                print(f"  Total impact days: {df.loc[with_impact, 'impact_days'].sum():.0f}")

    print("\n" + "="*60)


def find_periods_with_data() -> list:
    """Find all periods that have data in at least one source."""
    # Test a range of periods
    periods = []
    for year in range(2022, 2026):
        for month in range(1, 13):
            if year == 2022 and month < 5:
                continue  # Project starts around May 2022
            if year == 2025 and month > 12:
                continue
            periods.append(f"{year}-{month:02d}")
    return periods


def main():
    parser = argparse.ArgumentParser(description="Test monthly report data loaders")
    parser.add_argument(
        'period',
        nargs='?',
        default=None,
        help='Period in YYYY-MM format (e.g., 2024-03)'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Test all periods with potential data'
    )
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Show summary only (record counts per period)'
    )

    args = parser.parse_args()

    if args.all:
        periods = find_periods_with_data()
        print(f"Testing {len(periods)} periods...")

        if args.summary:
            print("\nPeriod Summary:")
            print("-" * 80)
            print(f"{'Period':<10} {'P6':>10} {'Quality':>10} {'Labor':>10} {'Narratives':>10}")
            print("-" * 80)

            for year_month in periods:
                period = get_monthly_period(year_month)
                schedule = load_schedule_data(period)
                quality = load_quality_data(period)
                labor = load_labor_data(period)
                narratives = load_narrative_data(period)

                print(f"{year_month:<10} {len(schedule['tasks']):>10,} {len(quality['inspections']):>10,} {len(labor['labor']):>10,} {len(narratives['statements']):>10,}")
        else:
            for year_month in periods:
                test_period(year_month)
    elif args.period:
        test_period(args.period)
    else:
        # Default: test a sample period
        test_period('2024-06')


if __name__ == '__main__':
    main()
