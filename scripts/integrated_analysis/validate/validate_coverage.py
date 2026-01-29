#!/usr/bin/env python3
"""
Validate coverage of dimension tables across main datasets.

Tests how well the dimension tables (dim_company, dim_location, dim_trade)
and mapping tables cover the actual data in P6, Quality, Labor Hours, and TBM.

Outputs:
    - Coverage statistics per source
    - Lists of unmapped values for review
    - Summary report
"""

import pandas as pd
import sys
from pathlib import Path
from collections import defaultdict

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import Settings

# Paths
DIMS_DIR = PROJECT_ROOT / "scripts" / "integrated_analysis" / "dimensions"
MAPS_DIR = PROJECT_ROOT / "scripts" / "integrated_analysis" / "mappings"
OUTPUT_DIR = PROJECT_ROOT / "scripts" / "integrated_analysis" / "validate"


def load_dimension_tables():
    """Load all dimension and mapping tables."""
    tables = {}

    # Dimensions
    tables['dim_company'] = pd.read_csv(DIMS_DIR / "dim_company.csv")
    tables['dim_location'] = pd.read_csv(DIMS_DIR / "dim_location.csv")
    tables['dim_trade'] = pd.read_csv(DIMS_DIR / "dim_trade.csv")
    tables['map_trade_codes'] = pd.read_csv(DIMS_DIR / "map_trade_codes.csv")

    # Mappings
    tables['map_company_aliases'] = pd.read_csv(MAPS_DIR / "map_company_aliases.csv")
    tables['map_location_codes'] = pd.read_csv(MAPS_DIR / "map_location_codes.csv")

    return tables


def build_lookup_sets(tables):
    """Build lookup sets for quick matching."""
    lookups = {}

    # Company aliases by source
    lookups['company_aliases'] = defaultdict(set)
    for _, row in tables['map_company_aliases'].iterrows():
        source = row['source']
        alias = str(row['alias']).upper().strip()
        lookups['company_aliases'][source].add(alias)
        lookups['company_aliases']['ALL'].add(alias)

    # Trade codes by source
    lookups['trade_codes'] = defaultdict(set)
    for _, row in tables['map_trade_codes'].iterrows():
        source = row['source']
        code = str(row['source_code']).upper().strip()
        lookups['trade_codes'][source].add(code)
        lookups['trade_codes']['ALL'].add(code)

    # Location codes by source
    lookups['location_codes'] = defaultdict(set)
    for _, row in tables['map_location_codes'].iterrows():
        source = row['source']
        code = str(row['source_code']).upper().strip()
        lookups['location_codes'][source].add(code)
        lookups['location_codes']['ALL'].add(code)

    # Valid location_ids
    lookups['valid_locations'] = set(tables['dim_location']['location_id'].str.upper())

    # Valid buildings
    lookups['valid_buildings'] = set(tables['dim_location']['building'].str.upper())

    return lookups


def validate_p6_coverage(lookups):
    """Validate coverage against P6 activity codes."""
    print("\n" + "="*70)
    print("P6 PRIMAVERA COVERAGE")
    print("="*70)

    p6_processed = Settings.PROCESSED_DATA_DIR / "primavera"

    results = {
        'source': 'P6',
        'contractor_total': 0,
        'contractor_matched': 0,
        'building_total': 0,
        'building_matched': 0,
        'trade_total': 0,
        'trade_matched': 0,
        'unmapped_contractors': set(),
        'unmapped_buildings': set(),
        'unmapped_trades': set(),
    }

    # Find activity code files
    actvcode_files = list(p6_processed.glob("**/ACTVCODE.csv"))

    if not actvcode_files:
        print(f"  No ACTVCODE.csv files found in {p6_processed}")
        return results

    print(f"  Found {len(actvcode_files)} ACTVCODE files")

    # Process a sample of files (latest ones)
    actvcode_files = sorted(actvcode_files, reverse=True)[:10]

    for actvcode_file in actvcode_files:
        try:
            df = pd.read_csv(actvcode_file, low_memory=False)

            # Check for Z-SUB CONTRACTOR
            if 'actvcode_type_id' in df.columns or 'actv_code_type' in df.columns:
                type_col = 'actvcode_type_id' if 'actvcode_type_id' in df.columns else 'actv_code_type'
                code_col = 'short_name' if 'short_name' in df.columns else 'actv_code_name'

                # Contractors
                contractors = df[df[type_col].str.contains('SUB', case=False, na=False)]
                if not contractors.empty and code_col in contractors.columns:
                    for val in contractors[code_col].dropna().unique():
                        val_upper = str(val).upper().strip()
                        results['contractor_total'] += 1
                        if val_upper in lookups['company_aliases']['P6'] or val_upper in lookups['company_aliases']['ALL']:
                            results['contractor_matched'] += 1
                        else:
                            results['unmapped_contractors'].add(val)

                # Buildings (Z-BLDG)
                buildings = df[df[type_col].str.contains('BLDG', case=False, na=False)]
                if not buildings.empty and code_col in buildings.columns:
                    for val in buildings[code_col].dropna().unique():
                        val_upper = str(val).upper().strip()
                        results['building_total'] += 1
                        if val_upper in lookups['valid_buildings'] or val_upper in lookups['location_codes']['P6']:
                            results['building_matched'] += 1
                        else:
                            results['unmapped_buildings'].add(val)

                # Trades (Z-TRADE)
                trades = df[df[type_col].str.contains('TRADE', case=False, na=False)]
                if not trades.empty and code_col in trades.columns:
                    for val in trades[code_col].dropna().unique():
                        val_upper = str(val).upper().strip()
                        results['trade_total'] += 1
                        if val_upper in lookups['trade_codes']['P6'] or val_upper in lookups['trade_codes']['ALL']:
                            results['trade_matched'] += 1
                        else:
                            results['unmapped_trades'].add(val)

        except Exception as e:
            print(f"  Error reading {actvcode_file.name}: {e}")

    # Print results
    print(f"\n  Contractor Coverage:")
    print(f"    Unique values: {results['contractor_total']}")
    print(f"    Matched: {results['contractor_matched']}")
    if results['contractor_total'] > 0:
        pct = results['contractor_matched'] / results['contractor_total'] * 100
        print(f"    Coverage: {pct:.1f}%")
    if results['unmapped_contractors']:
        print(f"    Unmapped ({len(results['unmapped_contractors'])}): {sorted(results['unmapped_contractors'])[:10]}")

    print(f"\n  Building Coverage:")
    print(f"    Unique values: {results['building_total']}")
    print(f"    Matched: {results['building_matched']}")
    if results['building_total'] > 0:
        pct = results['building_matched'] / results['building_total'] * 100
        print(f"    Coverage: {pct:.1f}%")
    if results['unmapped_buildings']:
        print(f"    Unmapped ({len(results['unmapped_buildings'])}): {sorted(results['unmapped_buildings'])[:10]}")

    print(f"\n  Trade Coverage:")
    print(f"    Unique values: {results['trade_total']}")
    print(f"    Matched: {results['trade_matched']}")
    if results['trade_total'] > 0:
        pct = results['trade_matched'] / results['trade_total'] * 100
        print(f"    Coverage: {pct:.1f}%")
    if results['unmapped_trades']:
        print(f"    Unmapped ({len(results['unmapped_trades'])}): {sorted(results['unmapped_trades'])[:10]}")

    return results


def validate_quality_coverage(lookups):
    """Validate coverage against Quality inspection records."""
    print("\n" + "="*70)
    print("QUALITY RECORDS COVERAGE")
    print("="*70)

    quality_processed = Settings.PROCESSED_DATA_DIR / "quality"

    results = {
        'source': 'QUALITY',
        'contractor_total': 0,
        'contractor_matched': 0,
        'building_total': 0,
        'building_matched': 0,
        'unmapped_contractors': set(),
        'unmapped_buildings': set(),
    }

    # Try to find quality files
    quality_files = list(quality_processed.glob("*.csv"))

    if not quality_files:
        print(f"  No CSV files found in {quality_processed}")
        return results

    print(f"  Found {len(quality_files)} quality files")

    for qfile in quality_files:
        try:
            df = pd.read_csv(qfile, low_memory=False)

            # Look for contractor columns
            contractor_cols = [c for c in df.columns if 'contractor' in c.lower() or 'company' in c.lower()]
            for col in contractor_cols:
                for val in df[col].dropna().unique():
                    val_upper = str(val).upper().strip()
                    if val_upper and val_upper not in ['NAN', 'NONE', '']:
                        results['contractor_total'] += 1
                        if val_upper in lookups['company_aliases']['QUALITY'] or val_upper in lookups['company_aliases']['ALL']:
                            results['contractor_matched'] += 1
                        else:
                            results['unmapped_contractors'].add(val)

            # Look for building/location columns
            location_cols = [c for c in df.columns if any(x in c.lower() for x in ['building', 'bldg', 'location'])]
            for col in location_cols:
                for val in df[col].dropna().unique():
                    val_upper = str(val).upper().strip()
                    if val_upper and val_upper not in ['NAN', 'NONE', '']:
                        results['building_total'] += 1
                        if val_upper in lookups['valid_buildings'] or val_upper in lookups['location_codes']['QUALITY']:
                            results['building_matched'] += 1
                        else:
                            results['unmapped_buildings'].add(val)

        except Exception as e:
            print(f"  Error reading {qfile.name}: {e}")

    # Print results
    print(f"\n  Contractor Coverage:")
    print(f"    Unique values: {results['contractor_total']}")
    print(f"    Matched: {results['contractor_matched']}")
    if results['contractor_total'] > 0:
        pct = results['contractor_matched'] / results['contractor_total'] * 100
        print(f"    Coverage: {pct:.1f}%")
    if results['unmapped_contractors']:
        print(f"    Unmapped ({len(results['unmapped_contractors'])}): {sorted(results['unmapped_contractors'])[:10]}")

    print(f"\n  Building Coverage:")
    print(f"    Unique values: {results['building_total']}")
    print(f"    Matched: {results['building_matched']}")
    if results['building_total'] > 0:
        pct = results['building_matched'] / results['building_total'] * 100
        print(f"    Coverage: {pct:.1f}%")
    if results['unmapped_buildings']:
        print(f"    Unmapped ({len(results['unmapped_buildings'])}): {sorted(results['unmapped_buildings'])[:10]}")

    return results


def validate_labor_coverage(lookups):
    """Validate coverage against labor hours data (Weekly Labor + ProjectSight)."""
    print("\n" + "="*70)
    print("LABOR HOURS COVERAGE")
    print("="*70)

    results = {
        'source': 'LABOR',
        'ps_company_total': 0,
        'ps_company_matched': 0,
        'unmapped_ps': set(),
    }

    # ProjectSight
    ps_processed = Settings.PROCESSED_DATA_DIR / "projectsight"
    ps_files = list(ps_processed.glob("*labor*.csv")) + list(ps_processed.glob("*daily*.csv"))

    print(f"\n  ProjectSight:")
    if ps_files:
        print(f"    Found {len(ps_files)} files")
        for psfile in ps_files[:5]:  # Sample first 5
            try:
                df = pd.read_csv(psfile, low_memory=False, nrows=10000)
                company_cols = [c for c in df.columns if 'company' in c.lower() or 'contractor' in c.lower()]
                for col in company_cols:
                    for val in df[col].dropna().unique():
                        val_upper = str(val).upper().strip()
                        if val_upper and val_upper not in ['NAN', 'NONE', '']:
                            results['ps_company_total'] += 1
                            if val_upper in lookups['company_aliases']['PS'] or val_upper in lookups['company_aliases']['ALL']:
                                results['ps_company_matched'] += 1
                            else:
                                results['unmapped_ps'].add(val)
            except Exception as e:
                print(f"    Error reading {psfile.name}: {e}")
    else:
        print(f"    No files found in {ps_processed}")

    print(f"    Unique companies: {results['ps_company_total']}")
    print(f"    Matched: {results['ps_company_matched']}")
    if results['ps_company_total'] > 0:
        pct = results['ps_company_matched'] / results['ps_company_total'] * 100
        print(f"    Coverage: {pct:.1f}%")
    if results['unmapped_ps']:
        print(f"    Unmapped ({len(results['unmapped_ps'])}): {sorted(results['unmapped_ps'])[:10]}")

    return results


def validate_tbm_coverage(lookups):
    """Validate coverage against TBM daily plans."""
    print("\n" + "="*70)
    print("TBM DAILY PLANS COVERAGE")
    print("="*70)

    tbm_processed = Settings.PROCESSED_DATA_DIR / "tbm"

    results = {
        'source': 'TBM',
        'company_total': 0,
        'company_matched': 0,
        'location_total': 0,
        'location_matched': 0,
        'unmapped_companies': set(),
        'unmapped_locations': set(),
    }

    tbm_files = list(tbm_processed.glob("*.csv"))

    if not tbm_files:
        print(f"  No CSV files found in {tbm_processed}")
        return results

    print(f"  Found {len(tbm_files)} TBM files")

    for tfile in tbm_files:
        try:
            df = pd.read_csv(tfile, low_memory=False)

            # Subcontractor
            sub_cols = [c for c in df.columns if 'subcontractor' in c.lower() or 'company' in c.lower()]
            for col in sub_cols:
                for val in df[col].dropna().unique():
                    val_str = str(val).strip()
                    val_upper = val_str.upper()
                    if val_upper and val_upper not in ['NAN', 'NONE', '']:
                        results['company_total'] += 1
                        if val_upper in lookups['company_aliases']['TBM'] or val_upper in lookups['company_aliases']['ALL']:
                            results['company_matched'] += 1
                        else:
                            results['unmapped_companies'].add(val_str)

            # Building/Level
            bldg_cols = [c for c in df.columns if 'building' in c.lower() or 'bldg' in c.lower()]
            level_cols = [c for c in df.columns if 'level' in c.lower() or 'floor' in c.lower()]

            # Check for combined building-level column
            for col in df.columns:
                if '-' in str(df[col].iloc[0] if len(df) > 0 else ''):
                    # Might be building-level format
                    for val in df[col].dropna().unique():
                        val_upper = str(val).upper().strip()
                        if '-' in val_upper and any(b in val_upper for b in ['FAB', 'SUP', 'CUB', 'FIZ', 'SUE', 'SUW']):
                            results['location_total'] += 1
                            if val_upper in lookups['location_codes']['TBM'] or val_upper in lookups['valid_locations']:
                                results['location_matched'] += 1
                            else:
                                results['unmapped_locations'].add(val_upper)

        except Exception as e:
            print(f"  Error reading {tfile.name}: {e}")

    # Print results
    print(f"\n  Company Coverage:")
    print(f"    Unique values: {results['company_total']}")
    print(f"    Matched: {results['company_matched']}")
    if results['company_total'] > 0:
        pct = results['company_matched'] / results['company_total'] * 100
        print(f"    Coverage: {pct:.1f}%")
    if results['unmapped_companies']:
        print(f"    Unmapped ({len(results['unmapped_companies'])}): {sorted(results['unmapped_companies'])[:10]}")

    print(f"\n  Location Coverage:")
    print(f"    Unique values: {results['location_total']}")
    print(f"    Matched: {results['location_matched']}")
    if results['location_total'] > 0:
        pct = results['location_matched'] / results['location_total'] * 100
        print(f"    Coverage: {pct:.1f}%")
    if results['unmapped_locations']:
        print(f"    Unmapped ({len(results['unmapped_locations'])}): {sorted(results['unmapped_locations'])[:10]}")

    return results


def save_unmapped_report(all_results):
    """Save detailed report of unmapped values."""
    report_lines = ["# Unmapped Values Report", ""]

    for result in all_results:
        source = result.get('source', 'Unknown')
        report_lines.append(f"## {source}")
        report_lines.append("")

        # Contractors
        unmapped = result.get('unmapped_contractors', set()) or result.get('unmapped_companies', set()) or set()
        unmapped.update(result.get('unmapped_weekly', set()))
        unmapped.update(result.get('unmapped_ps', set()))
        if unmapped:
            report_lines.append("### Unmapped Companies/Contractors")
            for val in sorted(unmapped):
                report_lines.append(f"- {val}")
            report_lines.append("")

        # Locations
        unmapped_loc = result.get('unmapped_buildings', set()) or result.get('unmapped_locations', set())
        if unmapped_loc:
            report_lines.append("### Unmapped Locations/Buildings")
            for val in sorted(unmapped_loc):
                report_lines.append(f"- {val}")
            report_lines.append("")

        # Trades
        unmapped_trades = result.get('unmapped_trades', set())
        if unmapped_trades:
            report_lines.append("### Unmapped Trades")
            for val in sorted(unmapped_trades):
                report_lines.append(f"- {val}")
            report_lines.append("")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = OUTPUT_DIR / "unmapped_values.md"
    with open(report_path, 'w') as f:
        f.write('\n'.join(report_lines))

    print(f"\nUnmapped values report saved to: {report_path}")


def main():
    print("="*70)
    print("DIMENSION TABLE COVERAGE VALIDATION")
    print("="*70)

    # Load tables
    print("\nLoading dimension and mapping tables...")
    tables = load_dimension_tables()

    print(f"  dim_company: {len(tables['dim_company'])} companies")
    print(f"  dim_location: {len(tables['dim_location'])} locations")
    print(f"  dim_trade: {len(tables['dim_trade'])} trades")
    print(f"  map_company_aliases: {len(tables['map_company_aliases'])} aliases")
    print(f"  map_trade_codes: {len(tables['map_trade_codes'])} trade mappings")
    print(f"  map_location_codes: {len(tables['map_location_codes'])} location mappings")

    # Build lookups
    lookups = build_lookup_sets(tables)

    # Validate each source
    all_results = []

    all_results.append(validate_p6_coverage(lookups))
    all_results.append(validate_quality_coverage(lookups))
    all_results.append(validate_labor_coverage(lookups))
    all_results.append(validate_tbm_coverage(lookups))

    # Save unmapped report
    save_unmapped_report(all_results)

    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print("\nNext steps:")
    print("  1. Review unmapped_values.md for gaps")
    print("  2. Add missing aliases to map_company_aliases.csv")
    print("  3. Add missing location codes to map_location_codes.csv")
    print("  4. Add missing trade codes to map_trade_codes.csv")


if __name__ == "__main__":
    main()
