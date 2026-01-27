#!/usr/bin/env python3
"""
Consolidate RABA cleaned JSON records into a single CSV.

Reads all *.clean.json files from the clean stage output and combines them
into a single CSV file with flattened structure for analysis.

Uses the unified QC inspection schema (shared with PSI) to enable direct
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
    categorize_inspection_type,
    standardize_level,
    infer_level_from_location,
    categorize_failure_reason,
)
from scripts.shared.dimension_lookup import (
    get_company_id,
    get_trade_id,
    get_trade_code,
    get_company_primary_trade_id,
    get_performing_company_id,
)
from scripts.integrated_analysis.location import enrich_location
from scripts.shared.qc_inspection_schema import UNIFIED_COLUMNS, apply_unified_schema
from schemas.validator import validated_df_to_csv
from scripts.integrated_analysis.add_csi_to_raba import (
    infer_csi_section,
    CSI_SECTIONS,
)


# Validation rules
PROJECT_START_DATE = "2022-05-01"
PROJECT_END_DATE = "2025-12-31"

VALID_OUTCOMES = {"PASS", "FAIL", "PARTIAL", "CANCELLED", "MEASUREMENT"}

VALID_ROLES = {
    "inspector", "contractor", "subcontractor", "trade", "client",
    "testing_company", "engineer", "other", "supplier", "testing_personnel"
}

# Patterns in summary text indicating no pass/fail criteria (measurement-only)
MEASUREMENT_SUMMARY_PATTERNS = [
    "does not specify pass/fail",
    "does not list pass/fail",
    "no pass/fail criteria",
    "does not state pass/fail",
    "does not explicitly state pass/fail",
    "no outcome was specified",
    "provides data but does not",
    "no explicit pass/fail",
]

# Inspection types that are inherently measurement-only (no pass/fail outcome)
MEASUREMENT_INSPECTION_TYPES = [
    "length change",  # Shrinkage testing - just measurements over time
]


def is_measurement_only(inspection_type: str, summary: str, outcome: str) -> bool:
    """
    Detect if a record is a measurement-only report without pass/fail criteria.

    These records were forced into PARTIAL by the LLM because the schema didn't
    allow null outcomes. We detect them based on:
    1. Summary text indicating no pass/fail criteria
    2. Inspection type that is inherently measurement-only

    Args:
        inspection_type: The test/inspection type
        summary: The LLM-generated summary
        outcome: The current outcome value

    Returns:
        True if this appears to be a measurement-only record
    """
    # Only reclassify PARTIAL outcomes (LLM's "least wrong" choice for no-outcome)
    if outcome != "PARTIAL":
        return False

    inspection_type_lower = (inspection_type or "").lower()
    summary_lower = (summary or "").lower()

    # Check if inspection type is inherently measurement-only
    for pattern in MEASUREMENT_INSPECTION_TYPES:
        if pattern in inspection_type_lower:
            return True

    # Check if summary indicates no pass/fail criteria
    for pattern in MEASUREMENT_SUMMARY_PATTERNS:
        if pattern in summary_lower:
            return True

    return False


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

    # Validate test type
    if not content.get('test_type'):
        issues.append("missing:test_type")

    return issues


def extract_companies_from_text(text: str) -> List[str]:
    """
    Extract company names from narrative text using pattern matching.

    Looks for patterns like:
    - "X with Company was notified"
    - "Company was present"
    - "Contractor/Subcontractor: Company1; Company2"

    Returns:
        List of company names found (excluding Samsung E&C)
    """
    import re

    if not text:
        return []

    companies = []

    # Pattern 1: "X with COMPANY" (e.g., "Fernando Urbina with YATES")
    pattern1 = r'(?:with|from)\s+([A-Z][A-Za-z\s&\.\-]+?)(?:\s+(?:was|were|represented)|\.|,)'
    matches1 = re.findall(pattern1, text)
    companies.extend(matches1)

    # Pattern 2: "Contractor/Subcontractor: ... ; COMPANY"
    pattern2 = r'Contractor/Subcontractor:.*?;\s*([A-Z][A-Za-z\s&\.\-]+?)(?:\.|;|$)'
    matches2 = re.findall(pattern2, text)
    companies.extend(matches2)

    # Pattern 3: "Contractor/Subcontractor: ... and COMPANY"
    pattern3 = r'Contractor/Subcontractor:.*?\s+and\s+([A-Z][A-Za-z\s&\.\-]+?)(?:\.|$)'
    matches3 = re.findall(pattern3, text)
    companies.extend(matches3)

    # Pattern 4: "Representatives from COMPANY & COMPANY"
    pattern4 = r'Representatives from\s+([A-Z][A-Za-z\s&\.\-]+?)(?:\s+&|\s+and|were|\.|$)'
    matches4 = re.findall(pattern4, text)
    companies.extend(matches4)

    # Pattern 5: "COMPANY was present" or "COMPANY were present"
    pattern5 = r'([A-Z][A-Z\-]+)\s+(?:was|were)\s+present'
    matches5 = re.findall(pattern5, text)
    companies.extend(matches5)

    # Clean up companies
    cleaned = []
    for company in companies:
        company = company.strip()

        # Skip Samsung E&C variations
        if 'samsung' in company.lower() and 'e&c' in company.lower():
            continue

        # Skip testing/inspection companies (they're not subcontractors doing work)
        testing_companies = ['raba kistner', 'raba-kistner']
        if any(tc in company.lower() for tc in testing_companies):
            continue

        # Truncate verbose captures (e.g., "Austin Global to discuss...")
        # Keep only the company name before common trailing phrases
        truncate_patterns = [' to ', ' and briefly', ' was ', ' were ', ' for ']
        for pattern in truncate_patterns:
            if pattern in company.lower():
                company = company[:company.lower().index(pattern)]
                break

        # Skip very short names (likely false positives)
        if len(company) < 3:
            continue

        # Skip common false positives
        if company.upper() in ['TO', 'INC', 'LLC', 'AMERICA', 'PROJECT', 'CWI']:
            continue

        cleaned.append(company)

    # Remove duplicates while preserving order
    seen = set()
    unique = []
    for company in cleaned:
        company_lower = company.lower()
        if company_lower not in seen:
            seen.add(company_lower)
            unique.append(company)

    return unique


def flatten_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten a record for CSV output.

    Handles nested structures:
    - parties_involved: Extracts first inspector, contractor, testing_company, subcontractor
    - test_counts: Flattens to individual columns
    - issues: Concatenates descriptions into single field
    - Postprocessing: Extracts companies from narrative text if not in structured parties
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
    testing_company = None
    subcontractor = None
    engineer = None

    for party in parties:
        if isinstance(party, dict):
            role = (party.get('role') or '').lower()
            name = party.get('name')
            if 'inspector' in role and not inspector:
                inspector = name
            elif role == 'contractor' and not contractor:
                contractor = name
            elif role == 'subcontractor' and not subcontractor:
                subcontractor = name
            elif role == 'testing_company' and not testing_company:
                testing_company = name
            elif role == 'engineer' and not engineer:
                engineer = name

    # Postprocessing: Extract companies from narrative if subcontractor not in structured parties
    # The extract stage captures company mentions in narrative text - parse them here
    narrative_companies = []
    is_multi_party = False

    if not subcontractor:
        # Look for extract content - try to load from extract.json file
        extract_content = None

        # Get the source file path from metadata
        source_file = record.get('_source_file', '')
        if source_file and source_file.endswith('.clean.json'):
            # The extract files are in processed/raba/1.extract/
            # Current working directory should be the repo root
            from pathlib import Path
            extract_file = source_file.replace('.clean.json', '.extract.json')
            extract_path = settings.RABA_PROCESSED_DIR / '1.extract' / extract_file

            if extract_path.exists():
                try:
                    with open(extract_path, 'r', encoding='utf-8') as f:
                        extract_data = json.load(f)
                        extract_content = extract_data.get('content', '')
                except Exception:
                    pass  # Silently fall back to summary

        # Fallback: use summary field from current content
        if not extract_content:
            extract_content = content.get('summary', '')

        # Extract companies from narrative
        if extract_content and isinstance(extract_content, str):
            narrative_companies = extract_companies_from_text(extract_content)

            if narrative_companies:
                # Use first company as subcontractor
                subcontractor = narrative_companies[0]

                # Flag if multiple companies found (multi-party inspection)
                if len(narrative_companies) > 1:
                    is_multi_party = True

    # Extract test counts
    test_counts = content.get('test_counts') or {}
    tests_total = test_counts.get('total') if isinstance(test_counts, dict) else None
    tests_passed = test_counts.get('passed') if isinstance(test_counts, dict) else None
    tests_failed = test_counts.get('failed') if isinstance(test_counts, dict) else None

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
    testing_company_std = standardize_company(testing_company)

    # Apply test type categorization (same as inspection type)
    test_type = content.get('test_type')
    test_category = categorize_inspection_type(test_type)

    # Apply level standardization with fallback to location inference
    level_raw = content.get('level')
    location_raw = content.get('location_raw')
    level_std = standardize_level(level_raw)
    if not level_std and location_raw:
        level_std = infer_level_from_location(location_raw)

    # Apply failure reason categorization
    failure_reason = content.get('failure_reason')
    failure_category = categorize_failure_reason(failure_reason) if failure_reason else None

    # Centralized location enrichment
    loc = enrich_location(
        building=content.get('building'),
        level=level_std,
        grid=content.get('grid'),
        source='RABA'
    )

    # Dimension lookups for integration (non-location)
    dim_company_id = get_company_id(contractor_std)
    dim_subcontractor_id = get_company_id(subcontractor_std)
    dim_trade_id = get_trade_id(test_category)

    # Fallback: if trade not found from inspection type, use company's primary trade
    if dim_trade_id is None and dim_company_id is not None:
        dim_trade_id = get_company_primary_trade_id(dim_company_id)

    dim_trade_code = get_trade_code(dim_trade_id) if dim_trade_id else None

    # Determine performing company (who actually did the work)
    performing_company_id = get_performing_company_id(dim_company_id, dim_subcontractor_id)

    # Detect and reclassify measurement-only records
    # These were forced into PARTIAL by LLM because schema didn't allow null
    outcome = content.get('outcome')
    summary = content.get('summary')
    if is_measurement_only(test_type, summary, outcome):
        outcome = "MEASUREMENT"

    # Infer CSI section from inspection type and category
    csi_section_id, csi_section_code, csi_source = infer_csi_section(test_type, test_category)
    csi_title = CSI_SECTIONS[csi_section_id][1] if csi_section_id and csi_section_id in CSI_SECTIONS else None

    # Build flat record using UNIFIED column names
    return {
        # Identification
        'inspection_id': content.get('inspection_id'),
        'source_file': record.get('_source_file'),

        # Dates
        'report_date': content.get('report_date'),
        'report_date_normalized': content.get('report_date_normalized'),

        # Inspection type (unified naming: test_type -> inspection_type)
        'inspection_type': test_type,
        'inspection_type_normalized': content.get('test_type_normalized'),
        'inspection_category': test_category,

        # Location
        'location_raw': location_raw,
        'building': content.get('building'),
        'level_raw': level_raw,
        'level': loc.level_normalized,
        'area': content.get('area'),
        'grid': loc.grid_normalized,
        'grid_row_min': loc.grid_row_min,
        'grid_row_max': loc.grid_row_max,
        'grid_col_min': loc.grid_col_min,
        'grid_col_max': loc.grid_col_max,
        'location_id': content.get('location_id'),

        # Results
        'outcome': outcome,
        'failure_reason': failure_reason,
        'failure_category': failure_category,
        'summary': content.get('summary'),

        # Test counts (RABA-specific)
        'tests_total': tests_total,
        'tests_passed': tests_passed,
        'tests_failed': tests_failed,

        # Deficiency count (PSI-specific - always None for RABA)
        'deficiency_count': None,

        # Follow-up
        'reinspection_required': content.get('reinspection_required'),
        'corrective_action': content.get('corrective_action'),

        # Parties (raw values)
        'inspector_raw': inspector,
        'contractor_raw': contractor,
        'testing_company_raw': testing_company,
        'subcontractor_raw': subcontractor,
        'trade_raw': None,  # PSI-specific
        'engineer': engineer,

        # Parties (standardized)
        'inspector': inspector_std,
        'contractor': contractor_std,
        'testing_company': testing_company_std,
        'subcontractor': subcontractor_std,
        'trade': None,  # PSI-specific

        # Issues (flattened)
        'issues': issues_text,
        'issue_count': len(issues_list) if issues_list else 0,

        # Multi-party inspection flag
        'is_multi_party': is_multi_party,
        'narrative_companies': '|'.join(narrative_companies) if narrative_companies else None,

        # Dimension IDs (for integration)
        'dim_location_id': loc.dim_location_id,
        'building_level': loc.building_level,
        'dim_company_id': dim_company_id,
        'dim_subcontractor_id': dim_subcontractor_id,
        'performing_company_id': performing_company_id,
        'dim_trade_id': dim_trade_id,
        'dim_trade_code': dim_trade_code,

        # CSI Section (52-category classification)
        'dim_csi_section_id': csi_section_id,
        'csi_section': csi_section_code,
        'csi_inference_source': csi_source,
        'csi_title': csi_title,

        # Affected rooms (JSON array of rooms whose grid bounds overlap)
        'affected_rooms': loc.affected_rooms,
        'affected_rooms_count': loc.affected_rooms_count,

        # Location quality diagnostics (for Power BI filtering)
        'grid_completeness': loc.grid_completeness,
        'match_quality': loc.match_quality,
        'location_review_flag': loc.location_review_flag,
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
    csv_path = consolidate_dir / "raba_qc_inspections.csv"

    if flat_records:
        # Apply unified schema to ensure consistent column order with PSI
        df = apply_unified_schema(flat_records, source='RABA')

        # Write to intermediate 4.consolidate folder (skip validation for intermediate)
        df.to_csv(csv_path, index=False)
        print(f"Wrote {len(flat_records)} records to: {csv_path}")

        # Write to root location with schema validation (final output)
        root_csv_path = output_dir / "raba_consolidated.csv"
        validated_df_to_csv(df, root_csv_path, index=False)
        print(f"Wrote {len(flat_records)} records to: {root_csv_path} (validated)")

    # Write validation report
    report_path = output_dir / "raba_validation_report.json"
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
            'mapped': df['dim_location_id'].notna().sum(),
            'total': len(df),
            'pct': df['dim_location_id'].notna().mean() * 100
        },
        'company': {
            'mapped': df['dim_company_id'].notna().sum(),
            'total': len(df),
            'pct': df['dim_company_id'].notna().mean() * 100
        },
        'trade': {
            'mapped': df['dim_trade_id'].notna().sum(),
            'total': len(df),
            'pct': df['dim_trade_id'].notna().mean() * 100
        },
        'csi_section': {
            'mapped': df['dim_csi_section_id'].notna().sum(),
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

    print("\n[!] Remember to regenerate bridge table after consolidation:")
    print("    cd scripts/integrated_analysis/dimensions && ./run.sh bridge")

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
    output_dir = settings.RABA_PROCESSED_DIR
    clean_dir = output_dir / "3.clean"

    if not clean_dir.exists():
        print(f"Clean directory not found: {clean_dir}")
        print("Run the pipeline first: ./run.sh run")
        sys.exit(1)

    consolidate(clean_dir, output_dir)


if __name__ == "__main__":
    main()
