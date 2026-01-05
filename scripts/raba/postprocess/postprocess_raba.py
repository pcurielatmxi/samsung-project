#!/usr/bin/env python3
"""
Postprocess RABA quality inspection JSON files to normalized CSV tables.

Input: {WINDOWS_DATA_DIR}/processed/raba/v2/*.format.json (9,397 files)
Output:
    - raba_inspections.csv - Main inspection/test records
    - raba_parties.csv - Parties involved (with company matching)
    - raba_issues.csv - Test failures/issues found
    - processing_errors.csv - Error log

Usage:
    python postprocess_raba.py [--output-dir /path/to/output]
"""

import json
import pandas as pd
import argparse
import traceback
from pathlib import Path
from typing import List, Dict, Optional
import sys

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.config.settings import Settings
from scripts.quality.postprocess.shared_normalization import (
    normalize_date,
    normalize_role,
    normalize_inspection_type
)
from scripts.quality.postprocess.location_parser import parse_location
from scripts.quality.postprocess.company_matcher import CompanyMatcher


def load_json_files(input_dir: Path) -> List[Dict]:
    """Load all .format.json files from directory."""
    json_files = sorted(input_dir.glob('*.format.json'))
    print(f"Found {len(json_files)} JSON files to process")

    records = []
    errors = []

    for i, json_file in enumerate(json_files):
        if i % 1000 == 0:
            print(f"  Loading {i}/{len(json_files)}...")

        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
                records.append(data)
        except Exception as e:
            errors.append((json_file.name, str(e)))

    print(f"✓ Loaded {len(records)} records ({len(errors)} errors)")
    if errors:
        print(f"  First 5 errors: {errors[:5]}")

    return records, errors


def build_main_table(records: List[Dict]) -> pd.DataFrame:
    """Build main inspections table with normalization."""
    rows = []
    errors = []

    for record in records:
        try:
            metadata = record.get('metadata', {})
            content = record.get('content', {})

            # Extract inspection_id from filename (e.g., "A22-016104.pdf" → "A22-016104")
            relative_path = metadata.get('relative_path', '')
            inspection_id = Path(relative_path).stem  # Remove .pdf extension

            # Parse location
            location_parsed = parse_location(content.get('location'))

            row = {
                'inspection_id': inspection_id,
                'source_file': metadata.get('source_file'),
                'report_date': normalize_date(content.get('report_date')),
                'test_type': content.get('test_type'),
                'test_type_normalized': normalize_inspection_type(
                    content.get('test_type')
                ),
                'location_raw': content.get('location'),
                'building': location_parsed['building'],
                'level': location_parsed['level'],
                'area': location_parsed['area'],
                'grid': location_parsed['grid'],
                'location_id': location_parsed['location_id'],
                'summary': content.get('summary'),
                'outcome': content.get('outcome'),
                'failure_reason': content.get('failure_reason'),
                'reinspection_required': content.get('reinspection_required'),
                'corrective_action': content.get('corrective_action'),
                'test_counts': str(content.get('test_counts')) if content.get('test_counts') else None,
                'processed_at': metadata.get('processed_at'),
                'model': metadata.get('model')
            }

            rows.append(row)

        except Exception as e:
            errors.append({
                'type': 'main_table',
                'inspection_id': relative_path,
                'error': str(e),
                'traceback': traceback.format_exc()
            })

    df = pd.DataFrame(rows)
    print(f"✓ Built main table: {len(df)} rows ({len(errors)} errors)")
    return df, errors


def build_parties_table(records: List[Dict], company_matcher: CompanyMatcher) -> pd.DataFrame:
    """Build parties involved table with company fuzzy matching."""
    rows = []
    errors = []

    for record in records:
        try:
            metadata = record.get('metadata', {})
            content = record.get('content', {})

            inspection_id = Path(metadata.get('relative_path', '')).stem
            parties = content.get('parties_involved', [])

            for seq, party in enumerate(parties, 1):
                name_raw = party.get('name')
                role_raw = party.get('role')

                # Fuzzy match company
                company_id, canonical_name, confidence = company_matcher.match(name_raw)

                rows.append({
                    'inspection_id': inspection_id,
                    'party_seq': seq,
                    'name_raw': name_raw,
                    'name_normalized': canonical_name,
                    'company_id': company_id,
                    'role_raw': role_raw,
                    'role_normalized': normalize_role(role_raw),
                    'match_confidence': confidence
                })

        except Exception as e:
            errors.append({
                'type': 'parties_table',
                'inspection_id': metadata.get('relative_path'),
                'error': str(e),
                'traceback': traceback.format_exc()
            })

    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    print(f"✓ Built parties table: {len(df)} rows ({len(errors)} errors)")
    return df, errors


def build_issues_table(records: List[Dict]) -> pd.DataFrame:
    """Build issues table."""
    rows = []
    errors = []

    for record in records:
        try:
            metadata = record.get('metadata', {})
            content = record.get('content', {})

            inspection_id = Path(metadata.get('relative_path', '')).stem
            issues = content.get('issues', [])

            for seq, issue in enumerate(issues, 1):
                rows.append({
                    'inspection_id': inspection_id,
                    'issue_seq': seq,
                    'issue_description': issue.get('description'),
                    'issue_type': issue.get('type'),
                    'severity': issue.get('severity')
                })

        except Exception as e:
            errors.append({
                'type': 'issues_table',
                'inspection_id': metadata.get('relative_path'),
                'error': str(e),
                'traceback': traceback.format_exc()
            })

    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    print(f"✓ Built issues table: {len(df)} rows ({len(errors)} errors)")
    return df, errors


def validate_outputs(
    inspections_df: pd.DataFrame,
    parties_df: pd.DataFrame,
    issues_df: pd.DataFrame
) -> List[str]:
    """Run quality checks on output tables."""
    checks = []

    # Check 1: All inspection_ids are unique
    if len(inspections_df) != inspections_df['inspection_id'].nunique():
        checks.append("⚠ FAIL: Duplicate inspection_ids found")
    else:
        checks.append("✓ PASS: All inspection_ids unique")

    # Check 2: Date parsing coverage
    date_valid_count = inspections_df['report_date'].notna().sum()
    date_coverage = date_valid_count / len(inspections_df)
    checks.append(f"✓ Date parsing: {date_valid_count}/{len(inspections_df)} valid ({date_coverage:.1%})")
    if date_coverage < 0.95:
        checks.append("⚠ WARNING: Date coverage <95%")

    # Check 3: Location parsing coverage
    loc_coverage = inspections_df['location_id'].notna().sum() / len(inspections_df)
    checks.append(f"✓ Location coverage: {loc_coverage:.1%}")
    if loc_coverage < 0.80:
        checks.append("⚠ WARNING: Location coverage <80%")

    # Check 4: Company matching rate
    if len(parties_df) > 0:
        company_match_rate = parties_df['company_id'].notna().sum() / len(parties_df)
        checks.append(f"✓ Company match rate: {company_match_rate:.1%}")
        if company_match_rate < 0.70:
            checks.append("⚠ WARNING: Company match rate <70%")
    else:
        checks.append("✓ No parties data to check")

    # Check 5: FK integrity
    if len(parties_df) > 0:
        orphan_parties = parties_df[~parties_df['inspection_id'].isin(inspections_df['inspection_id'])]
        if len(orphan_parties) > 0:
            checks.append(f"⚠ FAIL: {len(orphan_parties)} orphan party records")
        else:
            checks.append("✓ PASS: FK integrity maintained (parties)")

    if len(issues_df) > 0:
        orphan_issues = issues_df[~issues_df['inspection_id'].isin(inspections_df['inspection_id'])]
        if len(orphan_issues) > 0:
            checks.append(f"⚠ FAIL: {len(orphan_issues)} orphan issue records")
        else:
            checks.append("✓ PASS: FK integrity maintained (issues)")

    return checks


def main():
    """Main processing function."""
    parser = argparse.ArgumentParser(
        description='Postprocess RABA quality inspection JSON files to normalized CSV'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        help='Output directory for CSV files (default: derived from settings)'
    )
    args = parser.parse_args()

    try:
        settings = Settings()
    except Exception as e:
        print(f"⚠ Could not load settings: {e}")
        settings = None

    # Paths
    if settings:
        input_dir = settings.RABA_PROCESSED_DIR / 'v2'
    else:
        input_dir = Path('/mnt/c/Users/pdcur/OneDrive - MXI/Desktop/Samsung Dashboard/Data/processed/raba/v2')

    if args.output_dir:
        output_dir = Path(args.output_dir)
    elif settings:
        output_dir = settings.RABA_PROCESSED_DIR / 'v3'
    else:
        output_dir = Path('/mnt/c/Users/pdcur/OneDrive - MXI/Desktop/Samsung Dashboard/Data/processed/raba/v3')

    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("RABA Quality Inspection Postprocessing")
    print("=" * 70)
    print(f"Input: {input_dir}")
    print(f"Output: {output_dir}")
    print()

    # Initialize company matcher
    print("Initializing company matcher...")
    company_matcher = CompanyMatcher()
    print()

    # Load JSON files
    print("Loading JSON files...")
    records, load_errors = load_json_files(input_dir)
    print()

    # Build tables
    print("Building main inspections table...")
    inspections_df, main_errors = build_main_table(records)
    print()

    print("Building parties table...")
    parties_df, parties_errors = build_parties_table(records, company_matcher)
    print()

    print("Building issues table...")
    issues_df, issues_errors = build_issues_table(records)
    print()

    # Collect all errors
    all_errors = []
    for filename, error in load_errors:
        all_errors.append({'stage': 'load', 'error': error, 'detail': filename})
    all_errors.extend(main_errors)
    all_errors.extend(parties_errors)
    all_errors.extend(issues_errors)

    # Save outputs
    print("Saving CSV files...")
    inspections_df.to_csv(output_dir / 'raba_inspections.csv', index=False)
    print(f"  ✓ raba_inspections.csv ({len(inspections_df)} rows)")

    if len(parties_df) > 0:
        parties_df.to_csv(output_dir / 'raba_parties.csv', index=False)
        print(f"  ✓ raba_parties.csv ({len(parties_df)} rows)")

    if len(issues_df) > 0:
        issues_df.to_csv(output_dir / 'raba_issues.csv', index=False)
        print(f"  ✓ raba_issues.csv ({len(issues_df)} rows)")

    if all_errors:
        errors_df = pd.DataFrame(all_errors)
        errors_df.to_csv(output_dir / 'processing_errors.csv', index=False)
        print(f"  ✓ processing_errors.csv ({len(errors_df)} rows)")

    print()

    # Print validation checks
    print("=" * 70)
    print("Validation Checks")
    print("=" * 70)
    checks = validate_outputs(inspections_df, parties_df, issues_df)
    for check in checks:
        print(check)
    print()

    # Print statistics
    print("=" * 70)
    print("Statistics")
    print("=" * 70)
    print(f"Total inspections: {len(inspections_df)}")
    if len(inspections_df) > 0:
        valid_dates = inspections_df['report_date'].notna().sum()
        if valid_dates > 0:
            valid_dates_df = inspections_df[inspections_df['report_date'].notna()]['report_date']
            print(f"Date range: {valid_dates_df.min()} to {valid_dates_df.max()}")

        outcomes = inspections_df['outcome'].value_counts().to_dict()
        print(f"Outcomes: {outcomes}")

        buildings = inspections_df['building'].value_counts().to_dict()
        print(f"Buildings: {buildings}")

    print(f"Total parties: {len(parties_df)}")
    print(f"Total issues: {len(issues_df)}")
    print()
    print("✓ Done!")


if __name__ == '__main__':
    main()
