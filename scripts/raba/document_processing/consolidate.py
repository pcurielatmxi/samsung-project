#!/usr/bin/env python3
"""
Consolidate RABA and PSI cleaned JSON records into a single CSV.

Reads all *.clean.json files from both RABA and PSI clean stage outputs
and combines them into a single CSV file with flattened structure for analysis.

Uses the unified QC inspection schema to enable direct use in Power BI.

Output: processed/raba/raba_psi_consolidated.csv
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime

# Add project root to path for imports
_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings
from scripts.shared.pipeline_utils import get_output_path, write_fact_and_quality
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
    get_company_id,
    get_performing_company_id,
)
from scripts.integrated_analysis.location import enrich_location
from scripts.shared.qc_inspection_schema import UNIFIED_COLUMNS, apply_unified_schema
from schemas.validator import validated_df_to_csv
from scripts.integrated_analysis.add_csi_to_raba import (
    infer_csi_section,
    CSI_SECTIONS,
)
# RABA-specific outcome detection
from scripts.raba.document_processing.fix_raba_outcomes import (
    detect_cancelled as raba_detect_cancelled,
    detect_partial_cancelled as raba_detect_partial_cancelled,
    detect_measurement as raba_detect_measurement,
    detect_implied_pass as raba_detect_implied_pass,
)
# PSI-specific outcome detection
from scripts.psi.document_processing.fix_psi_outcomes import (
    detect_cancelled as psi_detect_cancelled,
    detect_partial_cancelled as psi_detect_partial_cancelled,
    detect_implied_pass as psi_detect_implied_pass,
)


# Validation rules
PROJECT_START_DATE = "2022-05-01"
PROJECT_END_DATE = "2025-12-31"

VALID_OUTCOMES = {"PASS", "FAIL", "PARTIAL", "CANCELLED", "MEASUREMENT"}


# =============================================================================
# Data quality columns - moved to separate table for Power BI cleanliness
# =============================================================================

RABA_PSI_DATA_QUALITY_COLUMNS = [
    # Raw/unnormalized values
    'level_raw',
    'location_raw',
    'inspector_raw',
    'contractor_raw',
    'testing_company_raw',
    'subcontractor_raw',
    'trade_raw',
    # Grid bounds (technical)
    'grid_row_min',
    'grid_row_max',
    'grid_col_min',
    'grid_col_max',
    'grid_source',
    # Room matching
    'affected_rooms',
    'affected_rooms_count',
    # Location inference metadata
    'location_type',
    'location_code',
    'match_type',
    # CSI inference metadata
    'csi_inference_source',
    # Validation
    '_validation_issues',
    # Multi-party detection
    'is_multi_party',
    'narrative_companies',
]

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
    """Detect if a record is a measurement-only report without pass/fail criteria."""
    if outcome != "PARTIAL":
        return False

    inspection_type_lower = (inspection_type or "").lower()
    summary_lower = (summary or "").lower()

    for pattern in MEASUREMENT_INSPECTION_TYPES:
        if pattern in inspection_type_lower:
            return True

    for pattern in MEASUREMENT_SUMMARY_PATTERNS:
        if pattern in summary_lower:
            return True

    return False


def load_clean_records(clean_dir: Path) -> List[Dict[str, Any]]:
    """Load all clean JSON files from directory."""
    records = []
    if not clean_dir.exists():
        return records

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


def validate_record(record: Dict[str, Any], source: str) -> List[str]:
    """Validate a single record and return list of issues."""
    issues = []
    content = record.get('content', {})
    if 'content' in content:
        content = content.get('content', {})

    if not content.get('inspection_id'):
        issues.append("missing:inspection_id")

    date_norm = content.get('report_date_normalized')
    if not date_norm:
        issues.append("missing:report_date")
    elif date_norm < PROJECT_START_DATE or date_norm > PROJECT_END_DATE:
        issues.append(f"invalid:report_date_out_of_range:{date_norm}")

    outcome = content.get('outcome')
    if not outcome:
        issues.append("missing:outcome")
    elif outcome not in VALID_OUTCOMES:
        issues.append(f"invalid:outcome:{outcome}")

    if not content.get('building'):
        issues.append("missing:building")
    if not content.get('level'):
        issues.append("missing:level")

    # Field name differs by source
    type_field = 'test_type' if source == 'RABA' else 'inspection_type'
    if not content.get(type_field):
        issues.append(f"missing:{type_field}")

    return issues


def extract_companies_from_text(text: str) -> List[str]:
    """Extract company names from narrative text using pattern matching."""
    import re

    if not text:
        return []

    companies = []

    # Pattern 1: "X with COMPANY"
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

    # Pattern 5: "COMPANY was present"
    pattern5 = r'([A-Z][A-Z\-]+)\s+(?:was|were)\s+present'
    matches5 = re.findall(pattern5, text)
    companies.extend(matches5)

    # Clean up
    cleaned = []
    for company in companies:
        company = company.strip()

        if 'samsung' in company.lower() and 'e&c' in company.lower():
            continue

        testing_companies = ['raba kistner', 'raba-kistner']
        if any(tc in company.lower() for tc in testing_companies):
            continue

        truncate_patterns = [' to ', ' and briefly', ' was ', ' were ', ' for ']
        for pattern in truncate_patterns:
            if pattern in company.lower():
                company = company[:company.lower().index(pattern)]
                break

        if len(company) < 3:
            continue

        if company.upper() in ['TO', 'INC', 'LLC', 'AMERICA', 'PROJECT', 'CWI']:
            continue

        cleaned.append(company)

    seen = set()
    unique = []
    for company in cleaned:
        company_lower = company.lower()
        if company_lower not in seen:
            seen.add(company_lower)
            unique.append(company)

    return unique


def flatten_raba_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a RABA record for CSV output."""
    content = record.get('content', {})
    if 'content' in content:
        content = content.get('content', {})

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

    # Extract companies from narrative if subcontractor not found
    narrative_companies = []
    is_multi_party = False

    if not subcontractor:
        extract_content = None
        source_file = record.get('_source_file', '')
        if source_file and source_file.endswith('.clean.json'):
            extract_file = source_file.replace('.clean.json', '.extract.json')
            extract_path = settings.RABA_PROCESSED_DIR / '1.extract' / extract_file

            if extract_path.exists():
                try:
                    with open(extract_path, 'r', encoding='utf-8') as f:
                        extract_data = json.load(f)
                        extract_content = extract_data.get('content', '')
                except Exception:
                    pass

        if not extract_content:
            extract_content = content.get('summary', '')

        if extract_content and isinstance(extract_content, str):
            narrative_companies = extract_companies_from_text(extract_content)
            if narrative_companies:
                subcontractor = narrative_companies[0]
                if len(narrative_companies) > 1:
                    is_multi_party = True

    # Extract test counts
    test_counts = content.get('test_counts') or {}
    tests_total = test_counts.get('total') if isinstance(test_counts, dict) else None
    tests_passed = test_counts.get('passed') if isinstance(test_counts, dict) else None
    tests_failed = test_counts.get('failed') if isinstance(test_counts, dict) else None

    # Flatten issues
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

    # Apply standardization
    inspector_std = standardize_inspector(inspector)
    contractor_std = standardize_company(contractor)
    subcontractor_std = standardize_company(subcontractor)
    testing_company_std = standardize_company(testing_company)

    test_type = content.get('test_type')
    test_category = categorize_inspection_type(test_type)

    level_raw = content.get('level')
    location_raw = content.get('location_raw')
    level_std = standardize_level(level_raw)
    if not level_std and location_raw:
        level_std = infer_level_from_location(location_raw)

    failure_reason = content.get('failure_reason')
    failure_category = categorize_failure_reason(failure_reason) if failure_reason else None

    # Location enrichment
    location_text_parts = [str(x) for x in [location_raw, content.get('summary')] if x]
    location_text = ' '.join(location_text_parts) if location_text_parts else None

    loc = enrich_location(
        building=content.get('building'),
        level=level_std,
        grid=content.get('grid'),
        location_text=location_text,
        source='RABA'
    )

    # Dimension lookups
    dim_company_id = get_company_id(contractor_std)
    dim_subcontractor_id = get_company_id(subcontractor_std)
    performing_company_id = get_performing_company_id(dim_company_id, dim_subcontractor_id)

    # Outcome correction
    outcome = content.get('outcome')
    summary = content.get('summary')

    row = {
        'outcome': outcome,
        'summary': summary,
        'failure_reason': failure_reason,
        'issues': issues_text,
        'inspection_type': test_type,
        'issue_count': len(issues_list) if issues_list else 0,
    }

    should_cancel, _ = raba_detect_cancelled(row)
    if should_cancel:
        outcome = "CANCELLED"
    else:
        should_partial_cancel, _ = raba_detect_partial_cancelled(row)
        if should_partial_cancel:
            outcome = "CANCELLED"
        else:
            should_measure, _ = raba_detect_measurement(row)
            if should_measure:
                outcome = "MEASUREMENT"
            else:
                should_pass, _ = raba_detect_implied_pass(row)
                if should_pass:
                    outcome = "PASS"
                elif is_measurement_only(test_type, summary, content.get('outcome')):
                    outcome = "MEASUREMENT"

    # CSI section inference
    csi_section_id, csi_section_code, csi_source = infer_csi_section(test_type, test_category)
    csi_title = CSI_SECTIONS[csi_section_id][1] if csi_section_id and csi_section_id in CSI_SECTIONS else None

    return {
        'inspection_id': content.get('inspection_id'),
        'source_file': record.get('_source_file'),
        'report_date': content.get('report_date'),
        'report_date_normalized': content.get('report_date_normalized'),
        'inspection_type': test_type,
        'inspection_type_normalized': content.get('test_type_normalized'),
        'inspection_category': test_category,
        'location_raw': location_raw,
        'building': content.get('building'),
        'level_raw': level_raw,
        'level': loc.level,
        'area': content.get('area'),
        'grid': content.get('grid'),
        'grid_row_min': loc.grid_row_min,
        'grid_row_max': loc.grid_row_max,
        'grid_col_min': loc.grid_col_min,
        'grid_col_max': loc.grid_col_max,
        'grid_source': loc.grid_source,
        'location_id': content.get('location_id'),
        'outcome': outcome,
        'failure_reason': failure_reason,
        'failure_category': failure_category,
        'summary': content.get('summary'),
        'tests_total': tests_total,
        'tests_passed': tests_passed,
        'tests_failed': tests_failed,
        'deficiency_count': None,
        'reinspection_required': content.get('reinspection_required'),
        'corrective_action': content.get('corrective_action'),
        'inspector_raw': inspector,
        'contractor_raw': contractor,
        'testing_company_raw': testing_company,
        'subcontractor_raw': subcontractor,
        'trade_raw': None,
        'engineer': engineer,
        'inspector': inspector_std,
        'contractor': contractor_std,
        'testing_company': testing_company_std,
        'subcontractor': subcontractor_std,
        'trade': None,
        'issues': issues_text,
        'issue_count': len(issues_list) if issues_list else 0,
        'is_multi_party': is_multi_party,
        'narrative_companies': '|'.join(narrative_companies) if narrative_companies else None,
        'dim_location_id': loc.dim_location_id,
        'location_type': loc.location_type,
        'location_code': loc.location_code,
        'dim_company_id': dim_company_id,
        'dim_subcontractor_id': dim_subcontractor_id,
        'performing_company_id': performing_company_id,
        'dim_csi_section_id': csi_section_id,
        'csi_section': csi_section_code,
        'csi_inference_source': csi_source,
        'csi_title': csi_title,
        'affected_rooms': loc.affected_rooms,
        'affected_rooms_count': loc.affected_rooms_count,
        'match_type': loc.match_type,
    }


def flatten_psi_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a PSI record for CSV output."""
    content = record.get('content', {})
    if 'content' in content:
        content = content.get('content', {})

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

    # Flatten issues
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

    # Apply standardization
    inspector_std = standardize_inspector(inspector)
    contractor_std = standardize_company(contractor)
    subcontractor_std = standardize_company(subcontractor)
    trade_std = standardize_trade(trade)

    inspection_type = content.get('inspection_type')
    inspection_category = categorize_inspection_type(inspection_type)

    if not trade_std and inspection_type:
        trade_std = infer_trade_from_inspection_type(inspection_type)

    level_raw = content.get('level')
    location_raw = content.get('location_raw')
    level_std = standardize_level(level_raw)
    if not level_std and location_raw:
        level_std = infer_level_from_location(location_raw)

    failure_reason = content.get('failure_reason')
    failure_category = categorize_failure_reason(failure_reason) if failure_reason else None

    # Location enrichment
    location_text_parts = [str(x) for x in [location_raw, content.get('summary')] if x]
    location_text = ' '.join(location_text_parts) if location_text_parts else None

    loc = enrich_location(
        building=content.get('building'),
        level=level_std,
        grid=content.get('grid'),
        location_text=location_text,
        source='PSI'
    )

    # Dimension lookups
    dim_company_id = get_company_id(contractor_std)
    if dim_company_id is None and subcontractor_std:
        dim_company_id = get_company_id(subcontractor_std)
    dim_subcontractor_id = get_company_id(subcontractor_std)
    performing_company_id = get_performing_company_id(dim_company_id, dim_subcontractor_id)

    # CSI section inference
    csi_section_id, csi_section_code, csi_source = infer_csi_section(inspection_type, inspection_category)
    csi_title = CSI_SECTIONS[csi_section_id][1] if csi_section_id and csi_section_id in CSI_SECTIONS else None

    # Outcome correction
    outcome = content.get('outcome')
    summary = content.get('summary')

    row = {
        'outcome': outcome,
        'summary': summary,
        'failure_reason': failure_reason,
        'issues': issues_text,
        'inspection_type': inspection_type,
        'issue_count': len(issues_list) if issues_list else 0,
    }

    should_cancel, _ = psi_detect_cancelled(row)
    if should_cancel:
        outcome = "CANCELLED"
    else:
        should_partial_cancel, _ = psi_detect_partial_cancelled(row)
        if should_partial_cancel:
            outcome = "CANCELLED"
        else:
            should_pass, _ = psi_detect_implied_pass(row)
            if should_pass:
                outcome = "PASS"

    return {
        'inspection_id': content.get('inspection_id'),
        'source_file': record.get('_source_file'),
        'report_date': content.get('report_date'),
        'report_date_normalized': content.get('report_date_normalized'),
        'inspection_type': inspection_type,
        'inspection_type_normalized': content.get('inspection_type_normalized'),
        'inspection_category': inspection_category,
        'location_raw': location_raw,
        'building': content.get('building'),
        'level_raw': level_raw,
        'level': loc.level,
        'area': content.get('area'),
        'grid': content.get('grid'),
        'grid_row_min': loc.grid_row_min,
        'grid_row_max': loc.grid_row_max,
        'grid_col_min': loc.grid_col_min,
        'grid_col_max': loc.grid_col_max,
        'grid_source': loc.grid_source,
        'location_id': content.get('location_id'),
        'outcome': outcome,
        'failure_reason': failure_reason,
        'failure_category': failure_category,
        'summary': content.get('summary'),
        'tests_total': None,
        'tests_passed': None,
        'tests_failed': None,
        'deficiency_count': content.get('deficiency_count'),
        'reinspection_required': content.get('reinspection_required'),
        'corrective_action': content.get('corrective_action'),
        'inspector_raw': inspector,
        'contractor_raw': contractor,
        'testing_company_raw': None,
        'subcontractor_raw': subcontractor,
        'trade_raw': trade,
        'engineer': None,
        'inspector': inspector_std,
        'contractor': contractor_std,
        'testing_company': None,
        'subcontractor': subcontractor_std,
        'trade': trade_std,
        'issues': issues_text,
        'issue_count': len(issues_list) if issues_list else 0,
        'is_multi_party': None,
        'narrative_companies': None,
        'dim_location_id': loc.dim_location_id,
        'location_type': loc.location_type,
        'location_code': loc.location_code,
        'match_type': loc.match_type,
        'dim_company_id': dim_company_id,
        'dim_subcontractor_id': dim_subcontractor_id,
        'performing_company_id': performing_company_id,
        'dim_csi_section_id': csi_section_id,
        'csi_section': csi_section_code,
        'csi_inference_source': csi_source,
        'csi_title': csi_title,
        'affected_rooms': loc.affected_rooms,
        'affected_rooms_count': loc.affected_rooms_count,
    }


def consolidate(staging_dir: Path = None) -> Dict[str, Any]:
    """
    Consolidate RABA and PSI records into a single CSV.

    Args:
        staging_dir: If provided, write outputs to staging directory

    Returns:
        Summary statistics
    """
    import pandas as pd
    from pathlib import Path

    raba_clean_dir = settings.RABA_PROCESSED_DIR / "3.clean"
    psi_clean_dir = settings.PSI_PROCESSED_DIR / "3.clean"

    # Output paths (staging or final)
    fact_path = get_output_path('raba/raba_psi_consolidated.csv', staging_dir)
    quality_path = get_output_path('raba/raba_psi_data_quality.csv', staging_dir)
    report_path = get_output_path('raba/raba_psi_validation_report.json', staging_dir)

    # Load records from both sources
    print(f"Loading RABA records from: {raba_clean_dir}")
    raba_records = load_clean_records(raba_clean_dir)
    print(f"Loaded {len(raba_records)} RABA records")

    print(f"Loading PSI records from: {psi_clean_dir}")
    psi_records = load_clean_records(psi_clean_dir)
    print(f"Loaded {len(psi_records)} PSI records")

    total_records = len(raba_records) + len(psi_records)
    if total_records == 0:
        print("No records found!")
        return {'total': 0, 'valid': 0, 'invalid': 0}

    # Process RABA records
    raba_flat = []
    raba_issues = []
    for record in raba_records:
        issues = validate_record(record, 'RABA')
        flat = flatten_raba_record(record)
        flat['_validation_issues'] = "|".join(issues) if issues else None
        raba_flat.append(flat)
        if issues:
            raba_issues.append({
                'inspection_id': flat['inspection_id'],
                'source_file': flat['source_file'],
                'source': 'RABA',
                'issues': issues,
            })

    # Process PSI records
    psi_flat = []
    psi_issues = []
    for record in psi_records:
        issues = validate_record(record, 'PSI')
        flat = flatten_psi_record(record)
        flat['_validation_issues'] = "|".join(issues) if issues else None
        psi_flat.append(flat)
        if issues:
            psi_issues.append({
                'inspection_id': flat['inspection_id'],
                'source_file': flat['source_file'],
                'source': 'PSI',
                'issues': issues,
            })

    # Apply unified schema to each source
    raba_df = apply_unified_schema(raba_flat, source='RABA')
    psi_df = apply_unified_schema(psi_flat, source='PSI')

    # Combine into single DataFrame
    combined_df = pd.concat([raba_df, psi_df], ignore_index=True)
    print(f"Combined: {len(combined_df)} total records ({len(raba_df)} RABA + {len(psi_df)} PSI)")

    # Write fact and data quality tables
    print(f"Writing fact table to: {fact_path}")
    print(f"Writing data quality table to: {quality_path}")
    fact_rows, quality_cols = write_fact_and_quality(
        df=combined_df,
        primary_key='inspection_id',
        quality_columns=RABA_PSI_DATA_QUALITY_COLUMNS,
        fact_path=fact_path,
        quality_path=quality_path,
    )
    print(f"Wrote {fact_rows:,} rows, moved {quality_cols} columns to data quality table")

    # Write validation report
    all_issues = raba_issues + psi_issues
    valid_count = total_records - len(all_issues)
    report = {
        'generated_at': datetime.now().isoformat(),
        'total_records': total_records,
        'raba_records': len(raba_records),
        'psi_records': len(psi_records),
        'valid_records': valid_count,
        'invalid_records': len(all_issues),
        'validation_rate': f"{valid_count / total_records * 100:.1f}%",
        'issues_by_type': _count_issues_by_type(all_issues),
        'issues_by_source': {
            'RABA': len(raba_issues),
            'PSI': len(psi_issues),
        },
        'invalid_records_detail': all_issues[:100],
    }

    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2)
    print(f"Wrote validation report to: {report_path}")

    # Calculate dimension coverage
    dim_coverage = {
        'location': {
            'mapped': int(combined_df['dim_location_id'].notna().sum()),
            'total': len(combined_df),
            'pct': combined_df['dim_location_id'].notna().mean() * 100
        },
        'company': {
            'mapped': int(combined_df['dim_company_id'].notna().sum()),
            'total': len(combined_df),
            'pct': combined_df['dim_company_id'].notna().mean() * 100
        },
        'csi_section': {
            'mapped': int(combined_df['dim_csi_section_id'].notna().sum()),
            'total': len(combined_df),
            'pct': combined_df['dim_csi_section_id'].notna().mean() * 100
        }
    }
    report['dimension_coverage'] = dim_coverage

    # Print summary
    print("\n" + "=" * 60)
    print("CONSOLIDATION SUMMARY")
    print("=" * 60)
    print(f"Total records:   {total_records}")
    print(f"  RABA:          {len(raba_records)}")
    print(f"  PSI:           {len(psi_records)}")
    print(f"Valid records:   {valid_count} ({valid_count / total_records * 100:.1f}%)")
    print(f"Invalid records: {len(all_issues)} ({len(all_issues) / total_records * 100:.1f}%)")

    print("\nDimension Coverage:")
    for dim_name, stats in dim_coverage.items():
        print(f"  {dim_name}: {stats['mapped']}/{stats['total']} ({stats['pct']:.1f}%)")

    if report['issues_by_type']:
        print("\nTop validation issues:")
        for issue_type, count in sorted(report['issues_by_type'].items(), key=lambda x: -x[1])[:10]:
            print(f"  {issue_type}: {count}")

    print(f"\nOutput:")
    print(f"  Fact table: {fact_path}")
    print(f"  Data quality: {quality_path}")

    return report


def _count_issues_by_type(validation_issues: List[Dict]) -> Dict[str, int]:
    """Count occurrences of each issue type."""
    counts = {}
    for item in validation_issues:
        for issue in item.get('issues', []):
            issue_type = issue.split(':')[0] if ':' in issue else issue
            counts[issue_type] = counts.get(issue_type, 0) + 1
    return counts


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Consolidate RABA + PSI quality inspections')
    parser.add_argument('--staging-dir', type=Path, default=None,
                        help='Write outputs to staging directory instead of final location')
    args = parser.parse_args()

    consolidate(staging_dir=args.staging_dir)


if __name__ == "__main__":
    main()
