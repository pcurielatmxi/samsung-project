#!/usr/bin/env python3
"""
Consolidate PSI cleaned JSON records into a single CSV.

Reads all *.clean.json files from the clean stage output and combines them
into a single CSV file with flattened structure for analysis.

Also generates a validation report flagging data quality issues.
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import csv

# Add project root to path for imports
_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings
from scripts.shared.company_standardization import (
    standardize_company,
    standardize_inspector,
    standardize_trade,
)


# Validation rules
PROJECT_START_DATE = "2022-05-01"
PROJECT_END_DATE = "2025-12-31"

VALID_OUTCOMES = {"PASS", "FAIL", "PARTIAL", "CANCELLED"}

VALID_ROLES = {
    "inspector", "contractor", "subcontractor", "trade", "client",
    "testing_company", "engineer", "other", "supplier", "requestor",
    "inspection company"
}


def load_clean_records(clean_dir: Path) -> List[Dict[str, Any]]:
    """Load all clean JSON files from directory."""
    records = []
    clean_files = sorted(clean_dir.glob("*.clean.json"))

    for filepath in clean_files:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                data['_source_file'] = filepath.name
                records.append(data)
        except Exception as e:
            print(f"Error loading {filepath}: {e}")

    return records


def validate_record(record: Dict[str, Any]) -> List[str]:
    """
    Validate a single record and return list of issues.

    Returns:
        List of validation issue strings (empty if valid)
    """
    issues = []
    # Handle nested content structure from pipeline
    content = record.get('content', {})
    if 'content' in content:
        content = content.get('content', {})

    # Check required fields
    if not content.get('inspection_id'):
        issues.append("missing:inspection_id")

    # Validate date
    date_norm = content.get('report_date_normalized')
    if not date_norm:
        issues.append("missing:report_date")
    elif date_norm < PROJECT_START_DATE or date_norm > PROJECT_END_DATE:
        issues.append(f"invalid:report_date_out_of_range:{date_norm}")

    # Validate outcome
    outcome = content.get('outcome')
    if not outcome:
        issues.append("missing:outcome")
    elif outcome not in VALID_OUTCOMES:
        issues.append(f"invalid:outcome:{outcome}")

    # Validate location
    if not content.get('building'):
        issues.append("missing:building")
    if not content.get('level'):
        issues.append("missing:level")

    # Validate inspection type
    if not content.get('inspection_type'):
        issues.append("missing:inspection_type")

    return issues


def flatten_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten a record for CSV output.

    Handles nested structures:
    - parties_involved: Extracts first inspector, contractor, subcontractor
    - issues: Concatenates descriptions into single field
    """
    # Handle nested content structure from pipeline
    content = record.get('content', {})
    if 'content' in content:
        content = content.get('content', {})
    metadata = record.get('metadata', {})

    # Extract parties by role
    parties = content.get('parties_involved', [])
    inspector = None
    contractor = None
    subcontractor = None
    trade = None

    for party in parties:
        if isinstance(party, dict):
            role = (party.get('role') or '').lower()
            name = party.get('name')
            if 'inspector' in role and not inspector:
                inspector = name
            elif role in ('contractor', 'contractor/subcontractor') and not contractor:
                contractor = name
            elif role == 'subcontractor' and not subcontractor:
                subcontractor = name
            elif role == 'trade' and not trade:
                trade = name

    # Flatten issues to single string
    issues_list = content.get('issues', [])
    issues_text = None
    if issues_list:
        descriptions = []
        for issue in issues_list:
            if isinstance(issue, dict):
                desc = issue.get('description', '')
                if desc:
                    descriptions.append(desc)
            elif isinstance(issue, str):
                descriptions.append(issue)
        if descriptions:
            issues_text = " | ".join(descriptions)

    # Apply company standardization
    inspector_std = standardize_inspector(inspector)
    contractor_std = standardize_company(contractor)
    subcontractor_std = standardize_company(subcontractor)
    trade_std = standardize_trade(trade)

    # Build flat record
    return {
        # Identification
        'inspection_id': content.get('inspection_id'),
        'source_file': record.get('_source_file'),

        # Dates
        'report_date': content.get('report_date'),
        'report_date_normalized': content.get('report_date_normalized'),

        # Inspection type
        'inspection_type': content.get('inspection_type'),
        'inspection_type_normalized': content.get('inspection_type_normalized'),

        # Location
        'location_raw': content.get('location_raw'),
        'building': content.get('building'),
        'level': content.get('level'),
        'area': content.get('area'),
        'grid': content.get('grid'),
        'location_id': content.get('location_id'),

        # Results
        'outcome': content.get('outcome'),
        'failure_reason': content.get('failure_reason'),
        'summary': content.get('summary'),

        # Follow-up
        'reinspection_required': content.get('reinspection_required'),
        'corrective_action': content.get('corrective_action'),
        'deficiency_count': content.get('deficiency_count'),

        # Parties (raw values)
        'inspector_raw': inspector,
        'contractor_raw': contractor,
        'subcontractor_raw': subcontractor,
        'trade_raw': trade,

        # Parties (standardized)
        'inspector': inspector_std,
        'contractor': contractor_std,
        'subcontractor': subcontractor_std,
        'trade': trade_std,

        # Issues (flattened)
        'issues': issues_text,
        'issue_count': len(issues_list) if issues_list else 0,
    }


def consolidate(clean_dir: Path, output_dir: Path) -> Dict[str, Any]:
    """
    Consolidate all clean records into CSV.

    Args:
        clean_dir: Path to 3.clean directory with *.clean.json files
        output_dir: Path to write consolidated.csv and validation_report.json

    Returns:
        Summary statistics
    """
    print(f"Loading records from: {clean_dir}")
    records = load_clean_records(clean_dir)
    print(f"Loaded {len(records)} records")

    if not records:
        print("No records found!")
        return {'total': 0, 'valid': 0, 'invalid': 0}

    # Validate and flatten records
    flat_records = []
    validation_issues = []
    valid_count = 0

    for record in records:
        issues = validate_record(record)
        flat = flatten_record(record)
        flat['_validation_issues'] = "|".join(issues) if issues else None
        flat_records.append(flat)

        if issues:
            validation_issues.append({
                'inspection_id': flat['inspection_id'],
                'source_file': flat['source_file'],
                'issues': issues,
            })
        else:
            valid_count += 1

    # Write CSV
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "psi_consolidated.csv"

    if flat_records:
        fieldnames = list(flat_records[0].keys())
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flat_records)
        print(f"Wrote {len(flat_records)} records to: {csv_path}")

    # Write validation report
    report_path = output_dir / "psi_validation_report.json"
    report = {
        'generated_at': datetime.now().isoformat(),
        'total_records': len(records),
        'valid_records': valid_count,
        'invalid_records': len(validation_issues),
        'validation_rate': f"{valid_count / len(records) * 100:.1f}%",
        'issues_by_type': _count_issues_by_type(validation_issues),
        'invalid_records_detail': validation_issues[:100],  # First 100 for review
    }

    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2)
    print(f"Wrote validation report to: {report_path}")

    # Print summary
    print("\n" + "=" * 60)
    print("CONSOLIDATION SUMMARY")
    print("=" * 60)
    print(f"Total records:   {len(records)}")
    print(f"Valid records:   {valid_count} ({valid_count / len(records) * 100:.1f}%)")
    print(f"Invalid records: {len(validation_issues)} ({len(validation_issues) / len(records) * 100:.1f}%)")

    if report['issues_by_type']:
        print("\nTop validation issues:")
        for issue_type, count in sorted(report['issues_by_type'].items(), key=lambda x: -x[1])[:10]:
            print(f"  {issue_type}: {count}")

    return report


def _count_issues_by_type(validation_issues: List[Dict]) -> Dict[str, int]:
    """Count occurrences of each issue type."""
    counts = {}
    for item in validation_issues:
        for issue in item.get('issues', []):
            # Extract issue type (before first colon)
            issue_type = issue.split(':')[0] if ':' in issue else issue
            counts[issue_type] = counts.get(issue_type, 0) + 1
    return counts


def main():
    """Main entry point."""
    # Use settings module which handles path conversion
    output_dir = settings.PSI_PROCESSED_DIR
    clean_dir = output_dir / "3.clean"

    if not clean_dir.exists():
        print(f"Clean directory not found: {clean_dir}")
        print("Run the pipeline first: ./run.sh run")
        sys.exit(1)

    consolidate(clean_dir, output_dir)


if __name__ == "__main__":
    main()
