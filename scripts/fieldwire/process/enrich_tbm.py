#!/usr/bin/env python3
"""
Enrich TBM audit records with dimension IDs for integration.

Input: tbm_audits.csv from parse_fieldwire.py
Output: tbm_audits_enriched.csv with dimension IDs

Adds:
- dim_location_id: Location FK from building + level
- building_level: String for display (e.g., "FAB-1F")
- dim_company_id: Company FK
- dim_trade_id: Trade FK (inferred from category)
- dim_trade_code: Trade code for readability
"""

import argparse
import csv
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.config.settings import settings
from scripts.shared.dimension_lookup import (
    get_company_id,
    get_trade_id,
    get_trade_code,
)
from scripts.shared.company_standardization import standardize_company
from scripts.integrated_analysis.location import enrich_location


# Fieldwire category to trade mapping
# Maps work categories from Fieldwire to standardized trade IDs
CATEGORY_TO_TRADE = {
    'Firestop': 'firestop',
    'Drywall': 'drywall',
    'Tape & Float': 'drywall',  # Part of drywall finishing
    'Framing': 'drywall',  # Metal framing = drywall trade
    'Scaffold': 'general',  # Scaffold support
    'Expansion Joint': 'architectural',
    'Miscellaneous': 'general',
    'Control Joints': 'drywall',
    'Cleaning': 'general',
}


def get_trade_from_category(category) -> Optional[str]:
    """Get trade name from Fieldwire category."""
    if not category or pd.isna(category):
        return None
    return CATEGORY_TO_TRADE.get(str(category), 'general')


def enrich_record(record: dict) -> dict:
    """
    Enrich a single TBM record with dimension IDs.

    Args:
        record: Raw TBM record dict

    Returns:
        Enriched record with dimension IDs added
    """
    enriched = record.copy()

    company = record.get('company')
    category = record.get('category')

    # Standardize company name
    if company and not pd.isna(company):
        company_str = str(company).strip()
        if company_str:
            company_std = standardize_company(company_str)
            enriched['company_standardized'] = company_std
        else:
            company_std = None
            enriched['company_standardized'] = None
    else:
        company_std = None
        enriched['company_standardized'] = None

    # Centralized location enrichment
    loc = enrich_location(
        building=record.get('building'),
        level=record.get('level'),
        grid=None,  # Fieldwire TBM doesn't have grid in this format
        source='FIELDWIRE_TBM'
    )
    enriched['dim_location_id'] = loc.dim_location_id
    enriched['building_level'] = loc.building_level

    # Get company dimension
    if company_std:
        enriched['dim_company_id'] = get_company_id(company_std)
    else:
        enriched['dim_company_id'] = None

    # Get trade dimension from category
    trade_name = get_trade_from_category(category)
    if trade_name:
        enriched['dim_trade_id'] = get_trade_id(trade_name)
        if enriched['dim_trade_id']:
            enriched['dim_trade_code'] = get_trade_code(enriched['dim_trade_id'])
        else:
            enriched['dim_trade_code'] = None
    else:
        enriched['dim_trade_id'] = None
        enriched['dim_trade_code'] = None

    return enriched


def main():
    parser = argparse.ArgumentParser(
        description='Enrich TBM audit records with dimension IDs'
    )
    parser.add_argument(
        '--input', '-i',
        type=Path,
        help='Input CSV file (default: processed/fieldwire/tbm_audits.csv)'
    )
    parser.add_argument(
        '--output', '-o',
        type=Path,
        help='Output CSV file (default: processed/fieldwire/tbm_audits_enriched.csv)'
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Show what would be done without writing files'
    )
    args = parser.parse_args()

    # Determine input file
    if args.input:
        input_file = args.input
    else:
        input_file = settings.DATA_DIR / 'processed' / 'fieldwire' / 'tbm_audits.csv'

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        print("Run parse_fieldwire.py first to generate tbm_audits.csv")
        sys.exit(1)

    print(f"Input file: {input_file}")

    # Determine output file
    if args.output:
        output_file = args.output
    else:
        output_file = settings.DATA_DIR / 'processed' / 'fieldwire' / 'tbm_audits_enriched.csv'

    print(f"Output file: {output_file}")

    # Read input
    print("\nReading TBM audit records...")
    df = pd.read_csv(input_file)
    print(f"  Total records: {len(df)}")

    # Enrich records
    print("\nEnriching records with dimension IDs...")
    enriched_records = []

    for _, row in df.iterrows():
        record = row.to_dict()
        enriched = enrich_record(record)
        enriched_records.append(enriched)

    enriched_df = pd.DataFrame(enriched_records)

    # Coverage statistics
    print("\nDimension coverage:")

    total = len(enriched_df)

    loc_mapped = enriched_df['dim_location_id'].notna().sum()
    print(f"  Location: {loc_mapped}/{total} ({100*loc_mapped/total:.1f}%)")

    comp_mapped = enriched_df['dim_company_id'].notna().sum()
    print(f"  Company:  {comp_mapped}/{total} ({100*comp_mapped/total:.1f}%)")

    trade_mapped = enriched_df['dim_trade_id'].notna().sum()
    print(f"  Trade:    {trade_mapped}/{total} ({100*trade_mapped/total:.1f}%)")

    # Summary by company
    print("\nRecords by company (standardized):")
    company_counts = enriched_df['company_standardized'].value_counts()
    for company, count in company_counts.head(10).items():
        company_id = enriched_df[enriched_df['company_standardized'] == company]['dim_company_id'].iloc[0]
        id_str = f"(id={company_id})" if pd.notna(company_id) else "(no match)"
        print(f"  {company}: {count} {id_str}")

    # Summary by building_level
    print("\nRecords by building_level:")
    bl_counts = enriched_df['building_level'].value_counts()
    for bl, count in bl_counts.head(10).items():
        loc_id = enriched_df[enriched_df['building_level'] == bl]['dim_location_id'].iloc[0]
        id_str = f"(id={loc_id})" if pd.notna(loc_id) else "(no match)"
        print(f"  {bl}: {count} {id_str}")

    if args.dry_run:
        print("\n[Dry run - no files written]")
        return

    # Write output
    print(f"\nWriting {len(enriched_df)} enriched records to {output_file}...")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Define column order
    columns = [
        # Original fields
        'id', 'title', 'status', 'category', 'start_date',
        'tier_1', 'tier_2', 'tier_3', 'building', 'level', 'company',
        'location_id', 'activity_name', 'activity_id', 'wbs_code',
        'tbm_manpower', 'direct_manpower', 'indirect_manpower', 'total_idle_hours',
        'tag_1', 'tag_2',
        'is_active', 'is_passive', 'is_obstructed', 'is_meeting',
        'is_no_manpower', 'is_not_started', 'inspector', 'observation_date',
        'created', 'last_updated',
        # Enrichment fields
        'company_standardized', 'building_level',
        'dim_location_id', 'dim_company_id', 'dim_trade_id', 'dim_trade_code',
    ]

    # Only include columns that exist
    columns = [c for c in columns if c in enriched_df.columns]

    enriched_df[columns].to_csv(output_file, index=False)

    print("\nDone!")


if __name__ == '__main__':
    main()
