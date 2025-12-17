#!/usr/bin/env python3
"""
Integrated Weekly Summary Analysis

Joins data across P6, TBM, and Quality sources to answer:
- Which tasks were completed by contractor/trade?
- What quality issues were present?
- How many hours were worked?
- All with location context where available.

Usage:
    python scripts/integrated_analysis/weekly_summary.py --week 2025-W10
    python scripts/integrated_analysis/weekly_summary.py --start 2025-03-01 --end 2025-03-31
"""

import sys
from pathlib import Path
import pandas as pd
import argparse
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings


def load_dimension_tables():
    """Load dimension and mapping tables."""
    base_path = project_root / 'scripts/integrated_analysis'

    dims = {
        'company': pd.read_csv(base_path / 'dimensions/dim_company.csv'),
        'location': pd.read_csv(base_path / 'dimensions/dim_location.csv'),
        'trade': pd.read_csv(base_path / 'dimensions/dim_trade.csv'),
        'aliases': pd.read_csv(base_path / 'mappings/map_company_aliases.csv'),
    }

    # Build alias lookup
    dims['alias_lookup'] = {}
    for source in dims['aliases']['source'].unique():
        source_aliases = dims['aliases'][dims['aliases']['source'] == source]
        dims['alias_lookup'][source] = dict(zip(
            source_aliases['alias'].str.upper().str.strip(),
            source_aliases['company_id']
        ))

    return dims


def load_p6_data():
    """Load P6 tasks with taxonomy."""
    # Load taxonomy
    taxonomy = pd.read_csv(
        Settings.PRIMAVERA_DERIVED_DIR / 'task_taxonomy.csv',
        low_memory=False
    )

    # Load tasks for dates and status
    tasks = pd.read_csv(
        Settings.PRIMAVERA_PROCESSED_DIR / 'task.csv',
        low_memory=False,
        usecols=['task_id', 'task_code', 'task_name', 'act_end_date', 'target_end_date',
                 'status_code', 'total_float_hr_cnt', 'phys_complete_pct']
    )

    # Merge
    merged = taxonomy.merge(tasks, on='task_id', how='left')

    # Parse dates
    merged['act_end_date'] = pd.to_datetime(merged['act_end_date'], errors='coerce')
    merged['target_end_date'] = pd.to_datetime(merged['target_end_date'], errors='coerce')

    # Create location_id
    merged['location_id'] = merged.apply(
        lambda r: f"{r['building']}-{r['level']}"
        if pd.notna(r['building']) and pd.notna(r['level']) else None,
        axis=1
    )

    return merged


def load_tbm_data(dims):
    """Load TBM work entries with company mapping."""
    tbm_path = Settings.TBM_PROCESSED_DIR / 'work_entries.csv'
    if not tbm_path.exists():
        return pd.DataFrame()

    tbm = pd.read_csv(tbm_path, low_memory=False)

    # Parse date
    tbm['report_date'] = pd.to_datetime(tbm['report_date'], errors='coerce')

    # Map company
    tbm['subcontractor'] = tbm['tier2_sc'].fillna(tbm['subcontractor_file'])
    tbm['company_upper'] = tbm['subcontractor'].fillna('').str.upper().str.strip()

    tbm_lookup = dims['alias_lookup'].get('TBM', {})
    p6_lookup = dims['alias_lookup'].get('P6', {})

    tbm['company_id'] = tbm['company_upper'].map(tbm_lookup)
    unmapped = tbm['company_id'].isna()
    tbm.loc[unmapped, 'company_id'] = tbm.loc[unmapped, 'company_upper'].map(p6_lookup)

    # Normalize location
    def normalize_level(level):
        if pd.isna(level):
            return None
        level = str(level).upper().strip()
        level_map = {
            '1': '1F', '2': '2F', '3': '3F', '4': '4F', '5': '5F', '6': '6F',
            'L1': '1F', 'L2': '2F', 'L3': '3F', 'L4': '4F', 'L5': '5F', 'L6': '6F',
            'ROOF': 'ROOF', 'RF': 'ROOF', 'B1': 'B1', 'UG': 'UG',
        }
        return level_map.get(level, level)

    tbm['level_norm'] = tbm['location_level'].apply(normalize_level)
    tbm['location_id'] = tbm.apply(
        lambda r: f"{str(r['location_building']).upper()}-{r['level_norm']}"
        if pd.notna(r['location_building']) and pd.notna(r['level_norm']) else None,
        axis=1
    )

    return tbm


def load_quality_data(dims):
    """Load Quality inspection records with company mapping."""
    quality_dir = Settings.PROCESSED_DATA_DIR / 'quality'

    yates_path = quality_dir / 'yates_all_inspections.csv'
    if not yates_path.exists():
        return pd.DataFrame()

    yates = pd.read_csv(yates_path, low_memory=False)

    # Parse date
    yates['Date'] = pd.to_datetime(yates['Date'], errors='coerce')

    # Map company
    yates['contractor'] = yates['Contractor_Normalized'].fillna(yates['Contractor'])
    yates['company_upper'] = yates['contractor'].fillna('').str.upper().str.strip()

    quality_lookup = dims['alias_lookup'].get('QUALITY', {})
    p6_lookup = dims['alias_lookup'].get('P6', {})

    yates['company_id'] = yates['company_upper'].map(quality_lookup)
    unmapped = yates['company_id'].isna()
    yates.loc[unmapped, 'company_id'] = yates.loc[unmapped, 'company_upper'].map(p6_lookup)

    return yates


def get_week_range(week_str):
    """Parse week string (YYYY-Www) to date range."""
    year, week = week_str.split('-W')
    # Get Monday of the week
    start = datetime.strptime(f'{year}-W{week}-1', '%G-W%V-%u')
    end = start + timedelta(days=6)
    return start, end


def summarize_week(start_date, end_date, p6_df, tbm_df, quality_df, dims):
    """Generate summary for a date range."""
    print(f"\n{'='*70}")
    print(f"WEEKLY SUMMARY: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"{'='*70}")

    # Build company name lookup
    company_names = dict(zip(dims['company']['company_id'], dims['company']['canonical_name']))
    trade_names = dict(zip(dims['trade']['trade_id'], dims['trade']['trade_name']))

    # === P6 Tasks Completed ===
    print("\n--- P6 TASKS COMPLETED ---")
    completed = p6_df[
        (p6_df['act_end_date'] >= start_date) &
        (p6_df['act_end_date'] <= end_date)
    ].copy()

    if len(completed) > 0:
        # Group by contractor and trade
        completed['company_name'] = completed['sub_contractor'].fillna('Unknown')

        by_contractor = completed.groupby('company_name').agg({
            'task_id': 'count',
            'trade_name': lambda x: ', '.join(x.dropna().unique()[:3]),
            'location_id': lambda x: ', '.join(x.dropna().unique()[:3])
        }).reset_index()
        by_contractor.columns = ['Contractor', 'Tasks', 'Trades', 'Locations']
        by_contractor = by_contractor.sort_values('Tasks', ascending=False).head(15)

        print(f"\nTasks completed: {len(completed):,}")
        print("\nBy Contractor (top 15):")
        print(by_contractor.to_string(index=False))

        # Summary by trade
        by_trade = completed.groupby('trade_name').agg({
            'task_id': 'count',
            'building': lambda x: ', '.join(x.dropna().unique()[:3])
        }).reset_index()
        by_trade.columns = ['Trade', 'Tasks', 'Buildings']
        by_trade = by_trade.sort_values('Tasks', ascending=False).head(10)
        print("\nBy Trade (top 10):")
        print(by_trade.to_string(index=False))
    else:
        print("  No P6 task completions in this period")

    # === TBM Work Activities ===
    print("\n--- TBM WORK ACTIVITIES ---")
    tbm_week = tbm_df[
        (tbm_df['report_date'] >= start_date) &
        (tbm_df['report_date'] <= end_date)
    ].copy() if len(tbm_df) > 0 else pd.DataFrame()

    if len(tbm_week) > 0:
        # Map company names
        tbm_week['company_name'] = tbm_week['company_id'].map(company_names).fillna(tbm_week['subcontractor'])

        # Estimate hours (8 hours per employee per day)
        tbm_week['estimated_hours'] = tbm_week['num_employees'].fillna(0) * 8

        by_contractor = tbm_week.groupby('company_name').agg({
            'num_employees': 'sum',
            'estimated_hours': 'sum',
            'location_id': lambda x: ', '.join(x.dropna().unique()[:3]),
            'work_activities': lambda x: '; '.join(x.dropna().unique()[:2])
        }).reset_index()
        by_contractor.columns = ['Contractor', 'Headcount', 'Est. Hours', 'Locations', 'Activities']
        by_contractor = by_contractor.sort_values('Est. Hours', ascending=False).head(15)

        print(f"\nTBM entries: {len(tbm_week):,}")
        print(f"Total headcount: {tbm_week['num_employees'].sum():,.0f}")
        print(f"Estimated hours: {tbm_week['estimated_hours'].sum():,.0f}")
        print("\nBy Contractor (top 15):")
        print(by_contractor.to_string(index=False))

        # By location
        by_location = tbm_week.groupby('location_id').agg({
            'num_employees': 'sum',
            'company_name': lambda x: ', '.join(x.dropna().unique()[:3])
        }).reset_index()
        by_location.columns = ['Location', 'Headcount', 'Contractors']
        by_location = by_location.sort_values('Headcount', ascending=False).head(10)
        print("\nBy Location (top 10):")
        print(by_location.to_string(index=False))
    else:
        print("  No TBM data in this period")

    # === Quality Issues ===
    print("\n--- QUALITY INSPECTIONS ---")
    qual_week = quality_df[
        (quality_df['Date'] >= start_date) &
        (quality_df['Date'] <= end_date)
    ].copy() if len(quality_df) > 0 else pd.DataFrame()

    if len(qual_week) > 0:
        # Map company names
        qual_week['company_name'] = qual_week['company_id'].map(company_names).fillna(qual_week['contractor'])

        # Status breakdown
        status_counts = qual_week['Status_Normalized'].value_counts()
        print(f"\nInspections: {len(qual_week):,}")
        print("\nBy Status:")
        for status, count in status_counts.items():
            print(f"  {status}: {count:,}")

        # Failed/Issues by contractor
        issues = qual_week[qual_week['Status_Normalized'].isin(['Failed', 'Rejected', 'Open'])]
        if len(issues) > 0:
            by_contractor = issues.groupby('company_name').agg({
                'row_index': 'count',
                'Category': lambda x: ', '.join(x.dropna().unique()[:2]),
                'Location': lambda x: ', '.join(x.dropna().unique()[:2])
            }).reset_index()
            by_contractor.columns = ['Contractor', 'Issues', 'Categories', 'Locations']
            by_contractor = by_contractor.sort_values('Issues', ascending=False).head(10)
            print(f"\nQuality Issues by Contractor (top 10):")
            print(by_contractor.to_string(index=False))

        # By category
        by_category = qual_week.groupby('Category').size().reset_index(name='Count')
        by_category = by_category.sort_values('Count', ascending=False).head(10)
        print("\nBy Category (top 10):")
        print(by_category.to_string(index=False))
    else:
        print("  No Quality inspections in this period")

    # === Cross-Source: Company Activity Summary ===
    print("\n--- CROSS-SOURCE: COMPANY ACTIVITY SUMMARY ---")

    # Collect all company activities
    company_summary = []

    # P6 completed tasks by company
    if len(completed) > 0:
        p6_lookup = dims['alias_lookup'].get('P6', {})
        completed['company_id'] = completed['sub_contractor'].fillna('').str.upper().map(p6_lookup)
        p6_by_company = completed.groupby('company_id').agg({
            'task_id': 'count'
        }).reset_index()
        p6_by_company.columns = ['company_id', 'p6_tasks_completed']
        company_summary.append(p6_by_company)

    # TBM hours by company
    if len(tbm_week) > 0:
        tbm_by_company = tbm_week.groupby('company_id').agg({
            'estimated_hours': 'sum'
        }).reset_index()
        tbm_by_company.columns = ['company_id', 'tbm_hours']
        company_summary.append(tbm_by_company)

    # Quality issues by company
    if len(qual_week) > 0:
        qual_by_company = qual_week.groupby('company_id').agg({
            'row_index': 'count'
        }).reset_index()
        qual_by_company.columns = ['company_id', 'quality_inspections']
        company_summary.append(qual_by_company)

    if company_summary:
        # Merge all
        from functools import reduce
        merged = reduce(
            lambda left, right: pd.merge(left, right, on='company_id', how='outer'),
            company_summary
        )
        merged = merged[merged['company_id'].notna() & (merged['company_id'] > 0)]
        merged['company_name'] = merged['company_id'].map(company_names)
        merged = merged.fillna(0)

        # Sort by activity
        merged['total_activity'] = (
            merged.get('p6_tasks_completed', 0) +
            merged.get('tbm_hours', 0) / 100 +
            merged.get('quality_inspections', 0)
        )
        merged = merged.sort_values('total_activity', ascending=False).head(20)

        cols = ['company_name']
        if 'p6_tasks_completed' in merged.columns:
            cols.append('p6_tasks_completed')
        if 'tbm_hours' in merged.columns:
            cols.append('tbm_hours')
        if 'quality_inspections' in merged.columns:
            cols.append('quality_inspections')

        print("\nCompany Activity (top 20):")
        print(merged[cols].to_string(index=False))


def main():
    parser = argparse.ArgumentParser(description='Generate weekly integrated summary')
    parser.add_argument('--week', help='Week in YYYY-Www format (e.g., 2025-W10)')
    parser.add_argument('--start', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', help='End date (YYYY-MM-DD)')
    args = parser.parse_args()

    # Determine date range
    if args.week:
        start_date, end_date = get_week_range(args.week)
    elif args.start and args.end:
        start_date = pd.to_datetime(args.start)
        end_date = pd.to_datetime(args.end)
    else:
        # Default: show a sample week with data (March 2025 has TBM data)
        start_date = pd.to_datetime('2025-03-10')
        end_date = pd.to_datetime('2025-03-16')

    print("Loading data...")
    dims = load_dimension_tables()
    p6_df = load_p6_data()
    tbm_df = load_tbm_data(dims)
    quality_df = load_quality_data(dims)

    print(f"Loaded: P6 {len(p6_df):,} tasks, TBM {len(tbm_df):,} entries, Quality {len(quality_df):,} inspections")

    summarize_week(start_date, end_date, p6_df, tbm_df, quality_df, dims)


if __name__ == "__main__":
    main()
