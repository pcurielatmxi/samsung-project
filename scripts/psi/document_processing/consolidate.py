#!/usr/bin/env python3
"""
Consolidate PSI cleaned JSON records into a single CSV.

Reads all *.clean.json files from the clean stage output and combines them
into a single CSV file with flattened structure for analysis.

Uses the unified QC inspection schema (shared with RABA) to enable direct
append in Power BI without transformation.

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
    categorize_inspection_type,
    standardize_level,
    infer_level_from_location,
    categorize_failure_reason,
    infer_trade_from_inspection_type,
)
from scripts.shared.dimension_lookup import (
    get_location_id,
    get_building_level,
    get_company_id,
    get_trade_id,
    get_trade_code,
    get_affected_rooms,
    parse_grid_field,
    normalize_grid,
    get_company_primary_trade_id,
)
from scripts.shared.qc_inspection_schema import UNIFIED_COLUMNS, apply_unified_schema
from scripts.integrated_analysis.add_csi_to_raba import (
    infer_csi_section,
    CSI_SECTIONS,
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

    # Apply inspection type categorization
    inspection_type = content.get('inspection_type')
    inspection_category = categorize_inspection_type(inspection_type)

    # Infer trade from inspection type if not provided
    if not trade_std and inspection_type:
        trade_std = infer_trade_from_inspection_type(inspection_type)

    # Apply level standardization with fallback to location inference
    level_raw = content.get('level')
    location_raw = content.get('location_raw')
    level_std = standardize_level(level_raw)
    if not level_std and location_raw:
        level_std = infer_level_from_location(location_raw)

    # Apply failure reason categorization
    failure_reason = content.get('failure_reason')
    failure_category = categorize_failure_reason(failure_reason) if failure_reason else None

    # Dimension lookups for integration
    building = content.get('building')
    dim_location_id = get_location_id(building, level_std)
    building_level = get_building_level(building, level_std)
    # For company, prefer contractor (subcontractor often contains person names)
    # Try contractor first, then subcontractor if contractor lookup fails
    dim_company_id = get_company_id(contractor_std)
    if dim_company_id is None and subcontractor_std:
        dim_company_id = get_company_id(subcontractor_std)
    # For trade, use the inferred/standardized trade name
    dim_trade_id = get_trade_id(trade_std)

    # Fallback: if trade not found from trade field, use company's primary trade
    if dim_trade_id is None and dim_company_id is not None:
        dim_trade_id = get_company_primary_trade_id(dim_company_id)

    dim_trade_code = get_trade_code(dim_trade_id) if dim_trade_id else None

    # Parse and normalize grid coordinates
    grid_raw = content.get('grid')
    grid_normalized = normalize_grid(grid_raw)
    grid_parsed = parse_grid_field(grid_raw)

    # Compute affected_rooms based on grid overlap
    affected_rooms = None
    if building and level_std:
        has_row = grid_parsed['grid_row_min'] is not None
        has_col = grid_parsed['grid_col_min'] is not None
        if has_row or has_col:
            rooms = get_affected_rooms(
                building, level_std,
                grid_parsed['grid_row_min'] if has_row else None,
                grid_parsed['grid_row_max'] if has_row else None,
                grid_parsed['grid_col_min'] if has_col else None,
                grid_parsed['grid_col_max'] if has_col else None,
            )
            if rooms:
                affected_rooms = json.dumps(rooms)

    # Infer CSI section from inspection type and category
    csi_section_id, csi_section_code, csi_source = infer_csi_section(inspection_type, inspection_category)
    csi_title = CSI_SECTIONS[csi_section_id][1] if csi_section_id and csi_section_id in CSI_SECTIONS else None

    # Build flat record using UNIFIED column names
    return {
        # Identification
        'inspection_id': content.get('inspection_id'),
        'source_file': record.get('_source_file'),

        # Dates
        'report_date': content.get('report_date'),
        'report_date_normalized': content.get('report_date_normalized'),

        # Inspection type
        'inspection_type': inspection_type,
        'inspection_type_normalized': content.get('inspection_type_normalized'),
        'inspection_category': inspection_category,

        # Location
        'location_raw': location_raw,
        'building': content.get('building'),
        'level_raw': level_raw,
        'level': level_std,
        'area': content.get('area'),
        'grid': grid_normalized,
        'grid_row_min': grid_parsed['grid_row_min'],
        'grid_row_max': grid_parsed['grid_row_max'],
        'grid_col_min': grid_parsed['grid_col_min'],
        'grid_col_max': grid_parsed['grid_col_max'],
        'location_id': content.get('location_id'),

        # Results
        'outcome': content.get('outcome'),
        'failure_reason': failure_reason,
        'failure_category': failure_category,
        'summary': content.get('summary'),

        # Test counts (RABA-specific - always None for PSI)
        'tests_total': None,
        'tests_passed': None,
        'tests_failed': None,

        # Deficiency count (PSI-specific)
        'deficiency_count': content.get('deficiency_count'),

        # Follow-up
        'reinspection_required': content.get('reinspection_required'),
        'corrective_action': content.get('corrective_action'),

        # Parties (raw values)
        'inspector_raw': inspector,
        'contractor_raw': contractor,
        'testing_company_raw': None,  # RABA-specific
        'subcontractor_raw': subcontractor,
        'trade_raw': trade,
        'engineer': None,  # RABA-specific

        # Parties (standardized)
        'inspector': inspector_std,
        'contractor': contractor_std,
        'testing_company': None,  # RABA-specific
        'subcontractor': subcontractor_std,
        'trade': trade_std,

        # Issues (flattened)
        'issues': issues_text,
        'issue_count': len(issues_list) if issues_list else 0,

        # Dimension IDs (for integration)
        'dim_location_id': dim_location_id,
        'building_level': building_level,
        'dim_company_id': dim_company_id,
        'dim_trade_id': dim_trade_id,
        'dim_trade_code': dim_trade_code,

        # CSI Section (52-category classification)
        'dim_csi_section_id': csi_section_id,
        'csi_section': csi_section_code,
        'csi_inference_source': csi_source,
        'csi_title': csi_title,

        # Affected rooms (JSON array of rooms whose grid bounds overlap)
        'affected_rooms': affected_rooms,
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

    # Apply unified schema and write CSV to 4.consolidate folder
    consolidate_dir = output_dir / "4.consolidate"
    consolidate_dir.mkdir(parents=True, exist_ok=True)
    csv_path = consolidate_dir / "psi_qc_inspections.csv"

    if flat_records:
        # Apply unified schema to ensure consistent column order with RABA
        df = apply_unified_schema(flat_records, source='PSI')
        df.to_csv(csv_path, index=False)
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

    # Calculate dimension coverage
    import pandas as pd
    df = pd.DataFrame(flat_records)
    dim_coverage = {
        'location': {
            'mapped': int(df['dim_location_id'].notna().sum()),
            'total': len(df),
            'pct': df['dim_location_id'].notna().mean() * 100
        },
        'company': {
            'mapped': int(df['dim_company_id'].notna().sum()),
            'total': len(df),
            'pct': df['dim_company_id'].notna().mean() * 100
        },
        'trade': {
            'mapped': int(df['dim_trade_id'].notna().sum()),
            'total': len(df),
            'pct': df['dim_trade_id'].notna().mean() * 100
        },
        'csi_section': {
            'mapped': int(df['dim_csi_section_id'].notna().sum()),
            'total': len(df),
            'pct': df['dim_csi_section_id'].notna().mean() * 100
        }
    }
    report['dimension_coverage'] = dim_coverage

    # Print summary
    print("\n" + "=" * 60)
    print("CONSOLIDATION SUMMARY")
    print("=" * 60)
    print(f"Total records:   {len(records)}")
    print(f"Valid records:   {valid_count} ({valid_count / len(records) * 100:.1f}%)")
    print(f"Invalid records: {len(validation_issues)} ({len(validation_issues) / len(records) * 100:.1f}%)")

    print("\nDimension Coverage:")
    for dim_name, stats in dim_coverage.items():
        print(f"  {dim_name}: {stats['mapped']}/{stats['total']} ({stats['pct']:.1f}%)")

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
