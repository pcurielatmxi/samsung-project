#!/usr/bin/env python3
"""
Calculate LPI (Labor Planning Index) and idle time metrics from TBM data.

Input:
- tbm_audits_enriched.csv (or tbm_audits.csv)
- manpower_counts.csv

Output:
- lpi_summary.csv: Daily LPI by contractor
- lpi_weekly.csv: Weekly LPI by contractor
- idle_analysis.csv: Idle time breakdown by category

LPI Calculation:
- LPI = Verified Workers / Planned Workers
- Target: 80%
- Verified = workers with Active checklist observation at planned location
"""

import argparse
import csv
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.config.settings import settings


def get_week_start(date_str: str) -> Optional[str]:
    """Get the Monday of the week for a given date."""
    if not date_str or pd.isna(date_str):
        return None
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        # Get Monday of that week
        monday = dt - timedelta(days=dt.weekday())
        return monday.strftime('%Y-%m-%d')
    except ValueError:
        return None


def calculate_daily_lpi(tbm_df: pd.DataFrame, manpower_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily LPI by contractor.

    Strategy:
    1. Get planned headcount from START records in manpower_counts
    2. Get verified count from TBM records with Active checklist
    3. LPI = Verified / Planned

    Args:
        tbm_df: TBM audit records (enriched)
        manpower_df: Daily manpower counts

    Returns:
        DataFrame with daily LPI by contractor
    """
    results = []

    # Filter to START records for planned headcount
    start_records = manpower_df[manpower_df['count_type'] == 'START'].copy()

    # Get unique dates and companies from START records
    for _, start in start_records.iterrows():
        date = start['date']
        company = start['company']
        planned = start['tbm_manpower'] or 0

        if not date or not company or planned == 0:
            continue

        # Filter TBM records for this date and company
        day_records = tbm_df[
            (tbm_df['start_date'] == date) &
            (tbm_df['company'] == company)
        ]

        # Count verified (active observations)
        verified = day_records['is_active'].sum() if 'is_active' in day_records.columns else 0

        # Sum actual manpower observed
        actual_direct = day_records['direct_manpower'].sum() if 'direct_manpower' in day_records.columns else 0
        actual_indirect = day_records['indirect_manpower'].sum() if 'indirect_manpower' in day_records.columns else 0
        actual_total = actual_direct + actual_indirect

        # Count checklist observations
        num_records = len(day_records)
        num_active = day_records['is_active'].sum() if 'is_active' in day_records.columns else 0
        num_passive = day_records['is_passive'].sum() if 'is_passive' in day_records.columns else 0
        num_obstructed = day_records['is_obstructed'].sum() if 'is_obstructed' in day_records.columns else 0
        num_meeting = day_records['is_meeting'].sum() if 'is_meeting' in day_records.columns else 0
        num_no_manpower = day_records['is_no_manpower'].sum() if 'is_no_manpower' in day_records.columns else 0
        num_not_started = day_records['is_not_started'].sum() if 'is_not_started' in day_records.columns else 0

        # Calculate LPI
        # Use verified observations as percentage of total START headcount
        lpi = verified / planned if planned > 0 else 0

        # Get END count if available
        end_record = manpower_df[
            (manpower_df['count_type'] == 'END') &
            (manpower_df['date'] == date) &
            (manpower_df['company'] == company)
        ]
        end_count = end_record['tbm_manpower'].iloc[0] if len(end_record) > 0 else None

        results.append({
            'date': date,
            'company': company,
            'planned_headcount': planned,
            'end_headcount': end_count,
            'tbm_locations': num_records,
            'verified_count': verified,
            'actual_direct': actual_direct,
            'actual_indirect': actual_indirect,
            'actual_total': actual_total,
            'lpi': lpi,
            'lpi_pct': round(lpi * 100, 1),
            # Idle breakdown
            'active_count': num_active,
            'passive_count': num_passive,
            'obstructed_count': num_obstructed,
            'meeting_count': num_meeting,
            'no_manpower_count': num_no_manpower,
            'not_started_count': num_not_started,
        })

    return pd.DataFrame(results)


def calculate_weekly_lpi(daily_lpi: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate daily LPI to weekly summaries.

    Args:
        daily_lpi: Daily LPI DataFrame

    Returns:
        Weekly LPI DataFrame
    """
    if daily_lpi.empty:
        return pd.DataFrame()

    # Add week start column
    daily_lpi = daily_lpi.copy()
    daily_lpi['week_start'] = daily_lpi['date'].apply(get_week_start)

    # Aggregate by week and company
    weekly = daily_lpi.groupby(['week_start', 'company']).agg({
        'planned_headcount': 'sum',
        'end_headcount': 'sum',
        'tbm_locations': 'sum',
        'verified_count': 'sum',
        'actual_direct': 'sum',
        'actual_indirect': 'sum',
        'actual_total': 'sum',
        'active_count': 'sum',
        'passive_count': 'sum',
        'obstructed_count': 'sum',
        'meeting_count': 'sum',
        'no_manpower_count': 'sum',
        'not_started_count': 'sum',
        'date': 'count',  # Number of days
    }).rename(columns={'date': 'days_with_data'}).reset_index()

    # Recalculate LPI from aggregated values
    weekly['lpi'] = weekly['verified_count'] / weekly['planned_headcount']
    weekly['lpi_pct'] = (weekly['lpi'] * 100).round(1)

    return weekly


def calculate_idle_analysis(tbm_df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze idle time by category and company.

    Uses checklist observations to categorize worker status.

    Args:
        tbm_df: TBM audit records

    Returns:
        Idle analysis DataFrame
    """
    results = []

    # Group by company and date
    for (company, date), group in tbm_df.groupby(['company', 'start_date']):
        if not company or not date:
            continue

        total_records = len(group)

        # Count by observation type
        active = group['is_active'].sum() if 'is_active' in group.columns else 0
        passive = group['is_passive'].sum() if 'is_passive' in group.columns else 0
        obstructed = group['is_obstructed'].sum() if 'is_obstructed' in group.columns else 0
        meeting = group['is_meeting'].sum() if 'is_meeting' in group.columns else 0
        no_manpower = group['is_no_manpower'].sum() if 'is_no_manpower' in group.columns else 0
        not_started = group['is_not_started'].sum() if 'is_not_started' in group.columns else 0

        # Total observations made
        total_observations = active + passive + obstructed + meeting + no_manpower + not_started

        # Idle = all non-active observations
        idle_total = passive + obstructed + meeting + no_manpower + not_started

        # Calculate percentages
        if total_observations > 0:
            active_pct = active / total_observations * 100
            passive_pct = passive / total_observations * 100
            obstructed_pct = obstructed / total_observations * 100
            meeting_pct = meeting / total_observations * 100
            no_manpower_pct = no_manpower / total_observations * 100
            not_started_pct = not_started / total_observations * 100
            idle_pct = idle_total / total_observations * 100
        else:
            active_pct = passive_pct = obstructed_pct = meeting_pct = 0
            no_manpower_pct = not_started_pct = idle_pct = 0

        results.append({
            'date': date,
            'company': company,
            'total_locations': total_records,
            'total_observations': total_observations,
            'active_count': active,
            'active_pct': round(active_pct, 1),
            'passive_count': passive,
            'passive_pct': round(passive_pct, 1),
            'obstructed_count': obstructed,
            'obstructed_pct': round(obstructed_pct, 1),
            'meeting_count': meeting,
            'meeting_pct': round(meeting_pct, 1),
            'no_manpower_count': no_manpower,
            'no_manpower_pct': round(no_manpower_pct, 1),
            'not_started_count': not_started,
            'not_started_pct': round(not_started_pct, 1),
            'idle_total': idle_total,
            'idle_pct': round(idle_pct, 1),
        })

    return pd.DataFrame(results)


def main():
    parser = argparse.ArgumentParser(
        description='Calculate LPI and idle metrics from TBM data'
    )
    parser.add_argument(
        '--tbm-input', '-t',
        type=Path,
        help='TBM audit CSV (default: tbm_audits_enriched.csv or tbm_audits.csv)'
    )
    parser.add_argument(
        '--manpower-input', '-m',
        type=Path,
        help='Manpower counts CSV (default: manpower_counts.csv)'
    )
    parser.add_argument(
        '--output-dir', '-o',
        type=Path,
        help='Output directory (default: processed/fieldwire/)'
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Show what would be done without writing files'
    )
    args = parser.parse_args()

    # Determine input files
    data_dir = settings.DATA_DIR / 'processed' / 'fieldwire'

    if args.tbm_input:
        tbm_file = args.tbm_input
    else:
        # Prefer enriched, fall back to raw
        enriched = data_dir / 'tbm_audits_enriched.csv'
        raw = data_dir / 'tbm_audits.csv'
        tbm_file = enriched if enriched.exists() else raw

    if not tbm_file.exists():
        print(f"Error: TBM audit file not found: {tbm_file}")
        print("Run parse_fieldwire.py first")
        sys.exit(1)

    if args.manpower_input:
        manpower_file = args.manpower_input
    else:
        manpower_file = data_dir / 'manpower_counts.csv'

    if not manpower_file.exists():
        print(f"Error: Manpower counts file not found: {manpower_file}")
        print("Run parse_fieldwire.py first")
        sys.exit(1)

    print(f"TBM input: {tbm_file}")
    print(f"Manpower input: {manpower_file}")

    # Determine output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = data_dir

    # Read input files
    print("\nReading input files...")
    tbm_df = pd.read_csv(tbm_file)
    manpower_df = pd.read_csv(manpower_file)

    print(f"  TBM records: {len(tbm_df)}")
    print(f"  Manpower count records: {len(manpower_df)}")

    # Calculate daily LPI
    print("\nCalculating daily LPI...")
    daily_lpi = calculate_daily_lpi(tbm_df, manpower_df)
    print(f"  Daily LPI records: {len(daily_lpi)}")

    if not daily_lpi.empty:
        # Summary statistics
        print("\n" + "=" * 60)
        print("DAILY LPI SUMMARY")
        print("=" * 60)

        # Overall totals
        total_planned = daily_lpi['planned_headcount'].sum()
        total_verified = daily_lpi['verified_count'].sum()
        overall_lpi = total_verified / total_planned if total_planned > 0 else 0

        print(f"\nOverall:")
        print(f"  Total Planned: {total_planned:,.0f}")
        print(f"  Total Verified: {total_verified:,.0f}")
        print(f"  Overall LPI: {overall_lpi*100:.1f}%")

        # By company
        print("\nBy Company:")
        company_summary = daily_lpi.groupby('company').agg({
            'planned_headcount': 'sum',
            'verified_count': 'sum',
            'tbm_locations': 'sum',
        }).reset_index()
        company_summary['lpi'] = company_summary['verified_count'] / company_summary['planned_headcount']

        for _, row in company_summary.iterrows():
            print(f"  {row['company']}:")
            print(f"    Planned: {row['planned_headcount']:,.0f}")
            print(f"    Verified: {row['verified_count']:,.0f}")
            print(f"    Locations: {row['tbm_locations']:,.0f}")
            print(f"    LPI: {row['lpi']*100:.1f}%")

        # Date range
        dates = daily_lpi['date'].dropna()
        if len(dates) > 0:
            print(f"\nDate range: {dates.min()} to {dates.max()}")
            print(f"Days with data: {len(dates.unique())}")

    # Calculate weekly LPI
    print("\nCalculating weekly LPI...")
    weekly_lpi = calculate_weekly_lpi(daily_lpi)
    print(f"  Weekly LPI records: {len(weekly_lpi)}")

    # Calculate idle analysis
    print("\nCalculating idle time analysis...")
    idle_analysis = calculate_idle_analysis(tbm_df)
    print(f"  Idle analysis records: {len(idle_analysis)}")

    if not idle_analysis.empty:
        print("\n" + "=" * 60)
        print("IDLE TIME SUMMARY")
        print("=" * 60)

        # Overall idle breakdown
        totals = idle_analysis.agg({
            'total_observations': 'sum',
            'active_count': 'sum',
            'passive_count': 'sum',
            'obstructed_count': 'sum',
            'meeting_count': 'sum',
            'no_manpower_count': 'sum',
            'not_started_count': 'sum',
            'idle_total': 'sum',
        })

        total_obs = totals['total_observations']
        if total_obs > 0:
            print(f"\nTotal Observations: {total_obs:,.0f}")
            print(f"\nBreakdown:")
            print(f"  Active:        {totals['active_count']:>6,.0f} ({totals['active_count']/total_obs*100:>5.1f}%)")
            print(f"  Passive:       {totals['passive_count']:>6,.0f} ({totals['passive_count']/total_obs*100:>5.1f}%)")
            print(f"  Obstructed:    {totals['obstructed_count']:>6,.0f} ({totals['obstructed_count']/total_obs*100:>5.1f}%)")
            print(f"  Meeting:       {totals['meeting_count']:>6,.0f} ({totals['meeting_count']/total_obs*100:>5.1f}%)")
            print(f"  No Manpower:   {totals['no_manpower_count']:>6,.0f} ({totals['no_manpower_count']/total_obs*100:>5.1f}%)")
            print(f"  Not Started:   {totals['not_started_count']:>6,.0f} ({totals['not_started_count']/total_obs*100:>5.1f}%)")
            print(f"  ─────────────────────────")
            print(f"  TOTAL IDLE:    {totals['idle_total']:>6,.0f} ({totals['idle_total']/total_obs*100:>5.1f}%)")

    if args.dry_run:
        print("\n[Dry run - no files written]")
        return

    # Write output files
    print("\nWriting output files...")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Daily LPI
    daily_file = output_dir / 'lpi_summary.csv'
    daily_lpi.to_csv(daily_file, index=False)
    print(f"  {daily_file}")

    # Weekly LPI
    weekly_file = output_dir / 'lpi_weekly.csv'
    weekly_lpi.to_csv(weekly_file, index=False)
    print(f"  {weekly_file}")

    # Idle analysis
    idle_file = output_dir / 'idle_analysis.csv'
    idle_analysis.to_csv(idle_file, index=False)
    print(f"  {idle_file}")

    print("\nDone!")


if __name__ == '__main__':
    main()
