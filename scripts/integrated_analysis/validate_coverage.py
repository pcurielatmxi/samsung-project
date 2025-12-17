#!/usr/bin/env python3
"""
Validate Coverage of Dimension and Mapping Tables

Measures how well the integrated analysis mappings cover actual data:
1. Company resolution rate per source
2. Location coverage per source
3. Cross-source joinability

Output:
- validation/coverage_report.csv
- validation/unmapped_entities.csv

Usage:
    python scripts/integrated_analysis/validate_coverage.py
"""

import sys
from pathlib import Path
import pandas as pd
from collections import defaultdict

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings


def load_dimension_tables():
    """Load all dimension and mapping tables."""
    base_path = project_root / 'scripts/integrated_analysis'

    tables = {
        'dim_company': pd.read_csv(base_path / 'dimensions/dim_company.csv'),
        'dim_location': pd.read_csv(base_path / 'dimensions/dim_location.csv'),
        'dim_trade': pd.read_csv(base_path / 'dimensions/dim_trade.csv'),
        'map_company_aliases': pd.read_csv(base_path / 'mappings/map_company_aliases.csv'),
        'map_company_location': pd.read_csv(base_path / 'mappings/map_company_location.csv'),
        'map_location_codes': pd.read_csv(base_path / 'mappings/map_location_codes.csv'),
    }

    print("Loaded dimension tables:")
    for name, df in tables.items():
        print(f"  {name}: {len(df):,} rows")

    return tables


def build_alias_lookup(aliases_df):
    """Build lookup dictionaries from alias mapping."""
    # Group by source for source-specific lookups
    lookups = {}
    for source in aliases_df['source'].unique():
        source_aliases = aliases_df[aliases_df['source'] == source]
        lookups[source] = dict(zip(
            source_aliases['alias'].str.upper().str.strip(),
            source_aliases['company_id']
        ))

    # Combined lookup (all sources)
    lookups['ALL'] = dict(zip(
        aliases_df['alias'].str.upper().str.strip(),
        aliases_df['company_id']
    ))

    return lookups


def validate_p6_taxonomy(alias_lookup, location_set):
    """Validate P6 task taxonomy coverage."""
    print("\n--- Validating P6 Task Taxonomy ---")

    taxonomy_path = Settings.PRIMAVERA_DERIVED_DIR / 'task_taxonomy.csv'
    if not taxonomy_path.exists():
        print("  WARNING: task_taxonomy.csv not found")
        return None

    taxonomy = pd.read_csv(taxonomy_path, low_memory=False)
    total = len(taxonomy)
    print(f"  Total tasks: {total:,}")

    results = {'source': 'P6_TAXONOMY', 'total_records': total}
    unmapped = []

    # Company coverage (sub_contractor field)
    has_company = taxonomy['sub_contractor'].notna()
    results['has_company_field'] = has_company.sum()
    results['pct_has_company_field'] = round(has_company.sum() / total * 100, 1)

    # Check if companies map to aliases
    p6_lookup = alias_lookup.get('P6', {})
    taxonomy['company_upper'] = taxonomy['sub_contractor'].fillna('').str.upper().str.strip()
    taxonomy['company_id'] = taxonomy['company_upper'].map(p6_lookup)

    mapped_company = taxonomy['company_id'].notna()
    results['company_mapped'] = mapped_company.sum()
    results['pct_company_mapped'] = round(mapped_company.sum() / total * 100, 1)

    # Track unmapped companies
    unmapped_companies = taxonomy[has_company & ~mapped_company]['sub_contractor'].dropna().unique()
    for co in unmapped_companies:
        unmapped.append({'source': 'P6', 'entity_type': 'company', 'value': co})

    # Location coverage (building + level)
    has_building = taxonomy['building'].notna()
    has_level = taxonomy['level'].notna()
    has_location = has_building & has_level
    results['has_location_field'] = has_location.sum()
    results['pct_has_location_field'] = round(has_location.sum() / total * 100, 1)

    # Check if locations are valid
    taxonomy['location_id'] = taxonomy.apply(
        lambda r: f"{r['building']}-{r['level']}" if pd.notna(r['building']) and pd.notna(r['level']) else None,
        axis=1
    )
    taxonomy['location_valid'] = taxonomy['location_id'].isin(location_set)
    valid_location = taxonomy['location_valid'] & has_location
    results['location_valid'] = valid_location.sum()
    results['pct_location_valid'] = round(valid_location.sum() / total * 100, 1)

    # Track unmapped locations
    unmapped_locs = taxonomy[has_location & ~taxonomy['location_valid']]['location_id'].dropna().unique()
    for loc in unmapped_locs:
        unmapped.append({'source': 'P6', 'entity_type': 'location', 'value': loc})

    # Trade coverage
    has_trade = taxonomy['trade_id'].notna()
    results['has_trade'] = has_trade.sum()
    results['pct_has_trade'] = round(has_trade.sum() / total * 100, 1)

    print(f"  Company field: {results['has_company_field']:,} ({results['pct_has_company_field']}%)")
    print(f"  Company mapped: {results['company_mapped']:,} ({results['pct_company_mapped']}%)")
    print(f"  Location field: {results['has_location_field']:,} ({results['pct_has_location_field']}%)")
    print(f"  Location valid: {results['location_valid']:,} ({results['pct_location_valid']}%)")
    print(f"  Trade assigned: {results['has_trade']:,} ({results['pct_has_trade']}%)")

    return results, unmapped


def validate_tbm(alias_lookup, location_set, location_code_map):
    """Validate TBM work entries coverage."""
    print("\n--- Validating TBM Work Entries ---")

    tbm_path = Settings.TBM_PROCESSED_DIR / 'work_entries.csv'
    if not tbm_path.exists():
        print("  WARNING: work_entries.csv not found")
        return None

    tbm = pd.read_csv(tbm_path, low_memory=False)
    total = len(tbm)
    print(f"  Total records: {total:,}")

    results = {'source': 'TBM', 'total_records': total}
    unmapped = []

    # Company coverage
    tbm['subcontractor'] = tbm['tier2_sc'].fillna(tbm['subcontractor_file'])
    has_company = tbm['subcontractor'].notna()
    results['has_company_field'] = has_company.sum()
    results['pct_has_company_field'] = round(has_company.sum() / total * 100, 1)

    # Check company mapping (try TBM aliases, then P6)
    tbm_lookup = alias_lookup.get('TBM', {})
    p6_lookup = alias_lookup.get('P6', {})

    tbm['company_upper'] = tbm['subcontractor'].fillna('').str.upper().str.strip()
    tbm['company_id'] = tbm['company_upper'].map(tbm_lookup)
    unmapped_mask = tbm['company_id'].isna()
    tbm.loc[unmapped_mask, 'company_id'] = tbm.loc[unmapped_mask, 'company_upper'].map(p6_lookup)

    mapped_company = tbm['company_id'].notna() & has_company
    results['company_mapped'] = mapped_company.sum()
    results['pct_company_mapped'] = round(mapped_company.sum() / has_company.sum() * 100, 1) if has_company.sum() > 0 else 0

    # Track unmapped
    unmapped_companies = tbm[has_company & ~mapped_company]['subcontractor'].dropna().unique()
    for co in unmapped_companies:
        unmapped.append({'source': 'TBM', 'entity_type': 'company', 'value': co})

    # Location coverage
    has_building = tbm['location_building'].notna()
    has_level = tbm['location_level'].notna()
    has_location = has_building & has_level
    results['has_location_field'] = has_location.sum()
    results['pct_has_location_field'] = round(has_location.sum() / total * 100, 1)

    # Normalize and validate locations
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
    tbm['location_id_raw'] = tbm.apply(
        lambda r: f"{str(r['location_building']).upper()}-{r['level_norm']}"
        if pd.notna(r['location_building']) and pd.notna(r['level_norm']) else None,
        axis=1
    )

    # Also try location code mapping for non-standard formats
    def resolve_location(raw_loc):
        if pd.isna(raw_loc):
            return None
        # First check direct match in dim_location
        if raw_loc in location_set:
            return raw_loc
        # Then check location code mapping
        mapped = location_code_map.get(raw_loc)
        if mapped and mapped in location_set:
            return mapped
        return raw_loc

    tbm['location_id'] = tbm['location_id_raw'].apply(resolve_location)
    tbm['location_valid'] = tbm['location_id'].isin(location_set)
    valid_location = tbm['location_valid'] & has_location
    results['location_valid'] = valid_location.sum()
    results['pct_location_valid'] = round(valid_location.sum() / has_location.sum() * 100, 1) if has_location.sum() > 0 else 0

    # Track unmapped locations
    unmapped_locs = tbm[has_location & ~tbm['location_valid']]['location_id_raw'].dropna().unique()
    for loc in unmapped_locs:
        unmapped.append({'source': 'TBM', 'entity_type': 'location', 'value': loc})

    print(f"  Company field: {results['has_company_field']:,} ({results['pct_has_company_field']}%)")
    print(f"  Company mapped: {results['company_mapped']:,} ({results['pct_company_mapped']}% of records with company)")
    print(f"  Location field: {results['has_location_field']:,} ({results['pct_has_location_field']}%)")
    print(f"  Location valid: {results['location_valid']:,} ({results['pct_location_valid']}% of records with location)")

    return results, unmapped


def validate_quality(alias_lookup, location_set):
    """Validate Quality records coverage."""
    print("\n--- Validating Quality Records ---")

    quality_dir = Settings.PROCESSED_DATA_DIR / 'quality'
    results_list = []
    all_unmapped = []

    # Yates inspections
    yates_path = quality_dir / 'yates_all_inspections.csv'
    if yates_path.exists():
        yates = pd.read_csv(yates_path, low_memory=False)
        total = len(yates)
        print(f"\n  Yates records: {total:,}")

        results = {'source': 'QUALITY_YATES', 'total_records': total}
        unmapped = []

        # Company coverage
        yates['contractor'] = yates['Contractor_Normalized'].fillna(yates['Contractor'])
        has_company = yates['contractor'].notna()
        results['has_company_field'] = has_company.sum()
        results['pct_has_company_field'] = round(has_company.sum() / total * 100, 1)

        # Map company
        quality_lookup = alias_lookup.get('QUALITY', {})
        p6_lookup = alias_lookup.get('P6', {})

        yates['company_upper'] = yates['contractor'].fillna('').str.upper().str.strip()
        yates['company_id'] = yates['company_upper'].map(quality_lookup)
        unmapped_mask = yates['company_id'].isna()
        yates.loc[unmapped_mask, 'company_id'] = yates.loc[unmapped_mask, 'company_upper'].map(p6_lookup)

        mapped_company = yates['company_id'].notna() & has_company
        results['company_mapped'] = mapped_company.sum()
        results['pct_company_mapped'] = round(mapped_company.sum() / has_company.sum() * 100, 1) if has_company.sum() > 0 else 0

        # Track unmapped
        unmapped_companies = yates[has_company & ~mapped_company]['contractor'].dropna().unique()
        for co in unmapped_companies:
            unmapped.append({'source': 'QUALITY_YATES', 'entity_type': 'company', 'value': co})

        # Location coverage - check Location field
        has_location = yates['Location'].notna()
        results['has_location_field'] = has_location.sum()
        results['pct_has_location_field'] = round(has_location.sum() / total * 100, 1)

        # For simplicity, just report location field presence (parsing is complex)
        results['location_valid'] = 'N/A'
        results['pct_location_valid'] = 'N/A'

        print(f"    Company field: {results['has_company_field']:,} ({results['pct_has_company_field']}%)")
        print(f"    Company mapped: {results['company_mapped']:,} ({results['pct_company_mapped']}%)")
        print(f"    Location field: {results['has_location_field']:,} ({results['pct_has_location_field']}%)")

        results_list.append(results)
        all_unmapped.extend(unmapped)

    # SECAI inspections
    secai_path = quality_dir / 'secai_ir_master.csv'
    if secai_path.exists():
        secai = pd.read_csv(secai_path, low_memory=False)
        total = len(secai)
        print(f"\n  SECAI records: {total:,}")

        results = {'source': 'QUALITY_SECAI', 'total_records': total}
        unmapped = []

        # Check what columns are available
        contractor_col = None
        for col in ['contractor', 'Contractor', 'responsible_contractor']:
            if col in secai.columns:
                contractor_col = col
                break

        if contractor_col:
            has_company = secai[contractor_col].notna()
            results['has_company_field'] = has_company.sum()
            results['pct_has_company_field'] = round(has_company.sum() / total * 100, 1)

            # Map company
            quality_lookup = alias_lookup.get('QUALITY', {})
            p6_lookup = alias_lookup.get('P6', {})

            secai['company_upper'] = secai[contractor_col].fillna('').str.upper().str.strip()
            secai['company_id'] = secai['company_upper'].map(quality_lookup)
            unmapped_mask = secai['company_id'].isna()
            secai.loc[unmapped_mask, 'company_id'] = secai.loc[unmapped_mask, 'company_upper'].map(p6_lookup)

            mapped_company = secai['company_id'].notna() & has_company
            results['company_mapped'] = mapped_company.sum()
            results['pct_company_mapped'] = round(mapped_company.sum() / has_company.sum() * 100, 1) if has_company.sum() > 0 else 0

            # Track unmapped
            unmapped_companies = secai[has_company & ~mapped_company][contractor_col].dropna().unique()
            for co in unmapped_companies:
                unmapped.append({'source': 'QUALITY_SECAI', 'entity_type': 'company', 'value': co})
        else:
            results['has_company_field'] = 0
            results['pct_has_company_field'] = 0
            results['company_mapped'] = 0
            results['pct_company_mapped'] = 0

        # Location coverage
        location_col = None
        for col in ['location', 'Location', 'building']:
            if col in secai.columns:
                location_col = col
                break

        if location_col:
            has_location = secai[location_col].notna()
            results['has_location_field'] = has_location.sum()
            results['pct_has_location_field'] = round(has_location.sum() / total * 100, 1)
        else:
            results['has_location_field'] = 0
            results['pct_has_location_field'] = 0

        results['location_valid'] = 'N/A'
        results['pct_location_valid'] = 'N/A'

        print(f"    Company field: {results['has_company_field']:,} ({results['pct_has_company_field']}%)")
        print(f"    Company mapped: {results['company_mapped']:,} ({results['pct_company_mapped']}%)")
        print(f"    Location field: {results['has_location_field']:,} ({results['pct_has_location_field']}%)")

        results_list.append(results)
        all_unmapped.extend(unmapped)

    return results_list, all_unmapped


def validate_projectsight(alias_lookup):
    """Validate ProjectSight labor records coverage."""
    print("\n--- Validating ProjectSight Labor ---")

    ps_path = Settings.PROJECTSIGHT_PROCESSED_DIR / 'daily_reports_labor.csv'
    if not ps_path.exists():
        print("  WARNING: daily_reports_labor.csv not found")
        return None

    ps = pd.read_csv(ps_path, low_memory=False, nrows=100000)  # Sample for speed
    total = len(ps)
    print(f"  Sample records: {total:,}")

    results = {'source': 'PROJECTSIGHT_LABOR', 'total_records': total}
    unmapped = []

    # Find company column
    company_col = None
    for col in ['Company', 'company', 'contractor', 'Contractor']:
        if col in ps.columns:
            company_col = col
            break

    if company_col:
        has_company = ps[company_col].notna()
        results['has_company_field'] = has_company.sum()
        results['pct_has_company_field'] = round(has_company.sum() / total * 100, 1)

        # Map company
        ps_lookup = alias_lookup.get('PS', {})
        p6_lookup = alias_lookup.get('P6', {})

        ps['company_upper'] = ps[company_col].fillna('').str.upper().str.strip()
        ps['company_id'] = ps['company_upper'].map(ps_lookup)
        unmapped_mask = ps['company_id'].isna()
        ps.loc[unmapped_mask, 'company_id'] = ps.loc[unmapped_mask, 'company_upper'].map(p6_lookup)

        mapped_company = ps['company_id'].notna() & has_company
        results['company_mapped'] = mapped_company.sum()
        results['pct_company_mapped'] = round(mapped_company.sum() / has_company.sum() * 100, 1) if has_company.sum() > 0 else 0

        # Track unmapped (sample)
        unmapped_companies = ps[has_company & ~mapped_company][company_col].dropna().unique()[:20]
        for co in unmapped_companies:
            unmapped.append({'source': 'PROJECTSIGHT', 'entity_type': 'company', 'value': co})
    else:
        results['has_company_field'] = 0
        results['pct_has_company_field'] = 0
        results['company_mapped'] = 0
        results['pct_company_mapped'] = 0

    # ProjectSight has no location
    results['has_location_field'] = 0
    results['pct_has_location_field'] = 0
    results['location_valid'] = 'N/A'
    results['pct_location_valid'] = 'N/A'

    print(f"  Company field: {results['has_company_field']:,} ({results['pct_has_company_field']}%)")
    print(f"  Company mapped: {results['company_mapped']:,} ({results['pct_company_mapped']}%)")
    print(f"  Location field: N/A (not available in source)")

    return results, unmapped


def validate_weekly_labor(alias_lookup):
    """Validate Weekly Labor records coverage."""
    print("\n--- Validating Weekly Labor ---")

    labor_path = Settings.PROCESSED_DATA_DIR / 'weekly_reports' / 'weekly_labor_detail.csv'
    if not labor_path.exists():
        print("  WARNING: weekly_labor_detail.csv not found")
        return None

    labor = pd.read_csv(labor_path, low_memory=False)
    total = len(labor)
    print(f"  Total records: {total:,}")

    results = {'source': 'WEEKLY_LABOR', 'total_records': total}
    unmapped = []

    # Find company column
    company_col = None
    for col in ['company', 'Company', 'contractor', 'Contractor', 'subcontractor']:
        if col in labor.columns:
            company_col = col
            break

    if company_col:
        has_company = labor[company_col].notna()
        results['has_company_field'] = has_company.sum()
        results['pct_has_company_field'] = round(has_company.sum() / total * 100, 1)

        # Map company
        labor_lookup = alias_lookup.get('LABOR', {})
        p6_lookup = alias_lookup.get('P6', {})

        labor['company_upper'] = labor[company_col].fillna('').str.upper().str.strip()
        labor['company_id'] = labor['company_upper'].map(labor_lookup)
        unmapped_mask = labor['company_id'].isna()
        labor.loc[unmapped_mask, 'company_id'] = labor.loc[unmapped_mask, 'company_upper'].map(p6_lookup)

        mapped_company = labor['company_id'].notna() & has_company
        results['company_mapped'] = mapped_company.sum()
        results['pct_company_mapped'] = round(mapped_company.sum() / has_company.sum() * 100, 1) if has_company.sum() > 0 else 0

        # Track unmapped
        unmapped_companies = labor[has_company & ~mapped_company][company_col].dropna().unique()
        for co in unmapped_companies:
            unmapped.append({'source': 'WEEKLY_LABOR', 'entity_type': 'company', 'value': co})
    else:
        results['has_company_field'] = 0
        results['pct_has_company_field'] = 0
        results['company_mapped'] = 0
        results['pct_company_mapped'] = 0

    # Weekly Labor has no location
    results['has_location_field'] = 0
    results['pct_has_location_field'] = 0
    results['location_valid'] = 'N/A'
    results['pct_location_valid'] = 'N/A'

    print(f"  Company field: {results['has_company_field']:,} ({results['pct_has_company_field']}%)")
    print(f"  Company mapped: {results['company_mapped']:,} ({results['pct_company_mapped']}%)")
    print(f"  Location field: N/A (not available in source)")

    return results, unmapped


def main():
    print("=" * 70)
    print("VALIDATING DIMENSION AND MAPPING TABLE COVERAGE")
    print("=" * 70)

    # Load dimension tables
    tables = load_dimension_tables()

    # Build lookup structures
    alias_lookup = build_alias_lookup(tables['map_company_aliases'])
    location_set = set(tables['dim_location']['location_id'].tolist())

    # Build location code map for TBM special formats
    tbm_loc_codes = tables['map_location_codes'][tables['map_location_codes']['source'] == 'TBM']
    location_code_map = dict(zip(tbm_loc_codes['source_code'], tbm_loc_codes['location_id']))

    print(f"\nAlias lookups built for: {list(alias_lookup.keys())}")
    print(f"Valid locations: {len(location_set)}")
    print(f"TBM location code mappings: {len(location_code_map)}")

    # Collect results
    all_results = []
    all_unmapped = []

    # Validate each source
    result = validate_p6_taxonomy(alias_lookup, location_set)
    if result:
        all_results.append(result[0])
        all_unmapped.extend(result[1])

    result = validate_tbm(alias_lookup, location_set, location_code_map)
    if result:
        all_results.append(result[0])
        all_unmapped.extend(result[1])

    result = validate_quality(alias_lookup, location_set)
    if result:
        all_results.extend(result[0])
        all_unmapped.extend(result[1])

    result = validate_projectsight(alias_lookup)
    if result:
        all_results.append(result[0])
        all_unmapped.extend(result[1])

    result = validate_weekly_labor(alias_lookup)
    if result:
        all_results.append(result[0])
        all_unmapped.extend(result[1])

    # Create output directory
    output_dir = project_root / 'scripts/integrated_analysis/validation'
    output_dir.mkdir(exist_ok=True)

    # Save coverage report
    coverage_df = pd.DataFrame(all_results)
    coverage_path = output_dir / 'coverage_report.csv'
    coverage_df.to_csv(coverage_path, index=False)
    print(f"\n\nSaved coverage report to: {coverage_path}")

    # Save unmapped entities
    unmapped_df = pd.DataFrame(all_unmapped).drop_duplicates()
    unmapped_path = output_dir / 'unmapped_entities.csv'
    unmapped_df.to_csv(unmapped_path, index=False)
    print(f"Saved unmapped entities to: {unmapped_path}")
    print(f"  {len(unmapped_df)} unique unmapped entities")

    # Summary
    print("\n" + "=" * 70)
    print("COVERAGE SUMMARY")
    print("=" * 70)
    print("\n" + coverage_df.to_string(index=False))

    # Check success criteria
    print("\n" + "=" * 70)
    print("SUCCESS CRITERIA CHECK")
    print("=" * 70)

    # Company resolution: ≥95% target
    company_sources = coverage_df[coverage_df['pct_company_mapped'].notna() & (coverage_df['pct_company_mapped'] != 'N/A')]
    if len(company_sources) > 0:
        avg_company = company_sources['pct_company_mapped'].astype(float).mean()
        print(f"Average company resolution: {avg_company:.1f}% (target: ≥95%)")

    # Location coverage: ≥80% target
    location_sources = coverage_df[
        coverage_df['pct_location_valid'].notna() &
        (coverage_df['pct_location_valid'] != 'N/A')
    ]
    if len(location_sources) > 0:
        avg_location = location_sources['pct_location_valid'].astype(float).mean()
        print(f"Average location validity: {avg_location:.1f}% (target: ≥80%)")


if __name__ == "__main__":
    main()
