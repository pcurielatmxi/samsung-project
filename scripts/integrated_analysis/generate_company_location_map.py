#!/usr/bin/env python3
"""
Generate Company-Location Mapping Table

Extracts company work distribution by location and time period from:
1. P6 Primavera task taxonomy (sub_contractor + building/level)
2. Quality records (Contractor + Location)
3. TBM daily plans (subcontractor + building/level)

Output: map_company_location.csv

Usage:
    python scripts/integrated_analysis/generate_company_location_map.py
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings


def load_company_aliases() -> dict:
    """Load P6 company aliases and return mapping to company_id."""
    aliases_path = project_root / 'scripts/integrated_analysis/mappings/map_company_aliases.csv'
    aliases = pd.read_csv(aliases_path)

    # Filter to P6 source
    p6_aliases = aliases[aliases['source'] == 'P6']

    # Create mapping: alias (uppercase) -> company_id
    return dict(zip(p6_aliases['alias'].str.upper(), p6_aliases['company_id']))


def extract_from_p6(alias_map: dict) -> pd.DataFrame:
    """
    Extract company-location pairs from P6 taxonomy.

    Returns DataFrame with columns:
        company_id, location_id, period_month, record_count, source
    """
    print("\n--- Extracting from P6 Taxonomy ---")

    # Load taxonomy and tasks
    taxonomy = pd.read_csv(Settings.PRIMAVERA_PROCESSED_DIR / 'p6_task_taxonomy.csv', low_memory=False)
    tasks = pd.read_csv(Settings.PRIMAVERA_PROCESSED_DIR / 'task.csv', low_memory=False)

    # Merge to get dates
    df = taxonomy.merge(
        tasks[['task_id', 'target_start_date']],
        on='task_id',
        how='left'
    )

    # Filter to records with company and location
    has_company = df['sub_contractor'].notna()
    has_building = df['building'].notna()
    has_level = df['level'].notna()
    usable = has_company & has_building & has_level

    df = df[usable].copy()
    print(f"  Usable records: {len(df):,}")

    # Create location_id
    df['location_id'] = df['building'] + '-' + df['level']

    # Map company to company_id
    df['company_id'] = df['sub_contractor'].str.upper().map(alias_map)

    # Filter out unmapped companies
    unmapped = df['company_id'].isna()
    if unmapped.any():
        print(f"  Unmapped companies: {unmapped.sum():,}")
        df = df[~unmapped].copy()

    # Parse date and extract month
    df['target_start_date'] = pd.to_datetime(df['target_start_date'], errors='coerce')
    df['period_month'] = df['target_start_date'].dt.to_period('M')

    # Aggregate by company, location, month
    grouped = df.groupby(['company_id', 'location_id', 'period_month']).size().reset_index(name='record_count')
    grouped['source'] = 'P6'

    print(f"  Generated {len(grouped):,} company-location-month mappings")

    return grouped


def extract_from_tbm(alias_map: dict) -> pd.DataFrame:
    """
    Extract company-location pairs from TBM daily plans.

    Returns DataFrame with columns:
        company_id, location_id, period_month, record_count, source
    """
    print("\n--- Extracting from TBM ---")

    tbm_path = Settings.TBM_PROCESSED_DIR / 'work_entries.csv'
    if not tbm_path.exists():
        print("  TBM data not found, skipping")
        return pd.DataFrame()

    tbm = pd.read_csv(tbm_path, low_memory=False)
    print(f"  Total TBM records: {len(tbm):,}")

    # Use tier2_sc as subcontractor (or subcontractor_file as fallback)
    tbm['subcontractor'] = tbm['tier2_sc'].fillna(tbm['subcontractor_file'])

    # Filter to records with company and location
    has_company = tbm['subcontractor'].notna()
    has_building = tbm['location_building'].notna()
    has_level = tbm['location_level'].notna()
    usable = has_company & has_building & has_level

    df = tbm[usable].copy()
    print(f"  Usable records: {len(df):,}")

    if len(df) == 0:
        return pd.DataFrame()

    # Create location_id (normalize level format)
    def normalize_level(level):
        if pd.isna(level):
            return None
        level = str(level).upper().strip()
        # Map common formats
        level_map = {
            '1': '1F', '2': '2F', '3': '3F', '4': '4F', '5': '5F', '6': '6F',
            'L1': '1F', 'L2': '2F', 'L3': '3F', 'L4': '4F', 'L5': '5F', 'L6': '6F',
            '1F': '1F', '2F': '2F', '3F': '3F', '4F': '4F', '5F': '5F', '6F': '6F',
            'ROOF': 'ROOF', 'RF': 'ROOF', 'B1': 'B1', 'UG': 'UG',
        }
        return level_map.get(level, level)

    df['level_norm'] = df['location_level'].apply(normalize_level)
    df['location_id'] = df['location_building'].str.upper() + '-' + df['level_norm']

    # Load TBM aliases for company mapping
    aliases_path = project_root / 'scripts/integrated_analysis/mappings/map_company_aliases.csv'
    aliases = pd.read_csv(aliases_path)
    tbm_aliases = aliases[aliases['source'] == 'TBM']
    tbm_alias_map = dict(zip(tbm_aliases['alias'].str.upper(), tbm_aliases['company_id']))

    # Map company - try exact match first, then partial
    df['subcontractor_upper'] = df['subcontractor'].str.upper().str.strip()
    df['company_id'] = df['subcontractor_upper'].map(tbm_alias_map)

    # For unmapped, try P6 aliases
    unmapped_mask = df['company_id'].isna()
    df.loc[unmapped_mask, 'company_id'] = df.loc[unmapped_mask, 'subcontractor_upper'].map(alias_map)

    # Filter out still unmapped
    still_unmapped = df['company_id'].isna()
    if still_unmapped.any():
        print(f"  Unmapped companies: {still_unmapped.sum():,}")
        df = df[~still_unmapped].copy()

    # Parse date and extract month
    df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')
    df['period_month'] = df['report_date'].dt.to_period('M')

    # Aggregate
    grouped = df.groupby(['company_id', 'location_id', 'period_month']).size().reset_index(name='record_count')
    grouped['source'] = 'TBM'

    print(f"  Generated {len(grouped):,} company-location-month mappings")

    return grouped


def extract_from_quality(alias_map: dict) -> pd.DataFrame:
    """
    Extract company-location pairs from Quality records.

    Returns DataFrame with columns:
        company_id, location_id, period_month, record_count, source
    """
    print("\n--- Extracting from Quality ---")

    quality_dir = Settings.PROCESSED_DATA_DIR / 'quality'

    all_records = []

    # Load Yates inspections
    yates_path = quality_dir / 'yates_all_inspections.csv'
    if yates_path.exists():
        yates = pd.read_csv(yates_path, low_memory=False)
        print(f"  Yates records: {len(yates):,}")

        # Use Contractor_Normalized or Contractor
        yates['contractor'] = yates['Contractor_Normalized'].fillna(yates['Contractor'])

        # Parse Location for building/level (format varies)
        # Common patterns: "FAB-L1", "SUE L2", "Building: FAB, Level: 1"
        def parse_location(loc):
            if pd.isna(loc):
                return None, None
            loc = str(loc).upper()

            # Try pattern: "FAB-L1", "SUE-1F"
            import re
            match = re.search(r'(FAB|SUE|SUW|FIZ|CUB|GCS|SUP)[\s\-]*(L?\d|ROOF|B1)', loc)
            if match:
                building = match.group(1)
                level = match.group(2)
                # Normalize level
                if level.startswith('L'):
                    level = level[1:] + 'F'
                elif level.isdigit():
                    level = level + 'F'
                return building, level

            # Try just building
            for bldg in ['FAB', 'SUE', 'SUW', 'FIZ', 'CUB', 'GCS', 'SUP']:
                if bldg in loc:
                    return bldg, None

            return None, None

        yates[['building', 'level']] = yates['Location'].apply(
            lambda x: pd.Series(parse_location(x))
        )

        # Filter to usable records
        has_company = yates['contractor'].notna()
        has_location = yates['building'].notna() & yates['level'].notna()
        usable = has_company & has_location

        df = yates[usable].copy()
        print(f"  Yates usable: {len(df):,}")

        if len(df) > 0:
            df['location_id'] = df['building'] + '-' + df['level']

            # Load Quality aliases
            aliases_path = project_root / 'scripts/integrated_analysis/mappings/map_company_aliases.csv'
            aliases = pd.read_csv(aliases_path)
            quality_aliases = aliases[aliases['source'] == 'QUALITY']
            quality_alias_map = dict(zip(quality_aliases['alias'].str.upper(), quality_aliases['company_id']))

            # Map company
            df['contractor_upper'] = df['contractor'].str.upper().str.strip()
            df['company_id'] = df['contractor_upper'].map(quality_alias_map)

            # Fallback to P6 aliases
            unmapped = df['company_id'].isna()
            df.loc[unmapped, 'company_id'] = df.loc[unmapped, 'contractor_upper'].map(alias_map)

            # Parse date
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df['period_month'] = df['Date'].dt.to_period('M')

            # Filter out unmapped
            df = df[df['company_id'].notna()].copy()

            if len(df) > 0:
                grouped = df.groupby(['company_id', 'location_id', 'period_month']).size().reset_index(name='record_count')
                grouped['source'] = 'QUALITY'
                all_records.append(grouped)

    if all_records:
        result = pd.concat(all_records, ignore_index=True)
        print(f"  Generated {len(result):,} company-location-month mappings")
        return result

    return pd.DataFrame()


def aggregate_by_quarter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate monthly data to quarterly periods and calculate percentages.

    Returns DataFrame with:
        company_id, location_id, period_start, period_end, pct_of_work, record_count, source
    """
    if len(df) == 0:
        return pd.DataFrame()

    # Convert company_id to int and filter out placeholder (0)
    df['company_id'] = df['company_id'].astype(int)
    df = df[df['company_id'] > 0].copy()

    if len(df) == 0:
        return pd.DataFrame()

    # Convert period to quarter
    df['period_month'] = df['period_month'].astype(str)
    df['period_date'] = pd.to_datetime(df['period_month'])
    df['quarter'] = df['period_date'].dt.to_period('Q')

    # Aggregate by company, location, quarter (combine sources)
    grouped = df.groupby(['company_id', 'location_id', 'quarter']).agg({
        'record_count': 'sum',
        'source': lambda x: ','.join(sorted(set(x)))
    }).reset_index()

    # Calculate percentage of work per company per quarter
    totals = grouped.groupby(['company_id', 'quarter'])['record_count'].transform('sum')
    grouped['pct_of_work'] = (grouped['record_count'] / totals * 100).round(1)

    # Convert quarter to date range
    grouped['period_start'] = grouped['quarter'].apply(lambda q: q.start_time.date())
    grouped['period_end'] = grouped['quarter'].apply(lambda q: q.end_time.date())

    # Ensure company_id is int in output
    grouped['company_id'] = grouped['company_id'].astype(int)

    # Select and order columns
    result = grouped[[
        'company_id', 'location_id', 'period_start', 'period_end',
        'pct_of_work', 'record_count', 'source'
    ]].sort_values(['company_id', 'period_start', 'pct_of_work'], ascending=[True, True, False])

    return result


def main():
    print("=" * 70)
    print("GENERATING COMPANY-LOCATION MAPPING TABLE")
    print("=" * 70)

    # Load company alias mapping
    alias_map = load_company_aliases()
    print(f"Loaded {len(alias_map)} P6 company aliases")

    # Extract from each source
    p6_data = extract_from_p6(alias_map)
    tbm_data = extract_from_tbm(alias_map)
    quality_data = extract_from_quality(alias_map)

    # Combine all sources
    all_data = pd.concat([p6_data, tbm_data, quality_data], ignore_index=True)
    print(f"\nTotal monthly mappings: {len(all_data):,}")

    # Aggregate to quarterly
    print("\n--- Aggregating to Quarterly Periods ---")
    quarterly = aggregate_by_quarter(all_data)
    print(f"Quarterly mappings: {len(quarterly):,}")

    # Summary statistics
    print("\n--- Summary ---")
    print(f"Unique companies: {quarterly['company_id'].nunique()}")
    print(f"Unique locations: {quarterly['location_id'].nunique()}")
    print(f"Date range: {quarterly['period_start'].min()} to {quarterly['period_end'].max()}")

    # Source distribution
    source_counts = quarterly['source'].value_counts()
    print("\nSource distribution:")
    for src, count in source_counts.items():
        print(f"  {src}: {count:,}")

    # Save output
    output_path = project_root / 'scripts/integrated_analysis/mappings/map_company_location.csv'
    quarterly.to_csv(output_path, index=False)
    print(f"\nSaved to: {output_path}")
    print(f"Records: {len(quarterly):,}")


if __name__ == "__main__":
    main()
