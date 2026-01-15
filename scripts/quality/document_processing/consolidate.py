#!/usr/bin/env python3
"""
Consolidate Yates and SECAI QC inspection data with dimension IDs.

Reads parsed CSV files from processed/quality/ and enriches them with:
- dim_location_id (from building + level)
- dim_company_id (from contractor/author company)
- dim_trade_id (from template/inspection type)

Column Strategy:
- Common columns: Unified naming for data that exists in both sources
- Source-specific columns: Prefixed with yates_ or secai_ to indicate availability

Follows the same pattern as RABA/PSI consolidation scripts.

Input:
    - processed/quality/yates_all_inspections.csv
    - processed/quality/secai_inspection_log.csv

Output:
    - processed/quality/enriched/yates_qc_inspections.csv
    - processed/quality/enriched/secai_qc_inspections.csv
    - processed/quality/enriched/combined_qc_inspections.csv
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import json

import pandas as pd

# Add project root to path for imports
_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings
from scripts.shared.company_standardization import (
    standardize_company,
    categorize_inspection_type,
    standardize_level,
    infer_level_from_location,
)
from scripts.shared.dimension_lookup import (
    get_location_id,
    get_building_level,
    get_company_id,
    get_trade_id,
    get_trade_code,
    get_company_primary_trade_id,
)
from scripts.quality.postprocess.location_parser import parse_location

# Import CSI inference from quality workbook script
from scripts.integrated_analysis.add_csi_to_quality_workbook import (
    infer_csi_from_keywords,
    YATES_KEYWORD_TO_CSI,
    SECAI_KEYWORD_TO_CSI,
)
from scripts.integrated_analysis.add_csi_to_raba import CSI_SECTIONS


# =============================================================================
# Column Definitions
# =============================================================================

# Common columns - data exists in both sources (unified naming)
COMMON_COLUMNS = [
    'source',                    # 'YATES' or 'SECAI'
    'inspection_id',             # Yates: WIR #, SECAI: IR Number
    'inspection_date',           # Yates: Date, SECAI: Inspection Request Date
    'year',
    'month',
    'week',
    'day_of_week',               # Day name (Monday, Tuesday, etc.)
    'template',                  # Yates: Inspection Description, SECAI: Template
    'inspection_category',       # Categorized inspection type
    'status',                    # Yates: Inspection Status, SECAI: Status
    'status_normalized',         # Normalized (ACCEPTED, FAILURE, etc.)
    'location_raw',              # Yates: Location, SECAI: System / Equip/ Location
    'building',                  # Parsed: FAB, SUE, SUW, etc.
    'level',                     # Parsed: 1F, 2F, B1, ROOF
    'area',                      # Parsed: area within building
    'grid',                      # Parsed: grid coordinate
    'contractor_raw',            # Yates: Contractor, SECAI: Author Company
    'contractor',                # Standardized contractor name
    'failure_reason',            # Yates: Remarks (on failure), SECAI: Reasons for failure
    # Dimension IDs
    'dim_location_id',
    'building_level',
    'dim_company_id',
    'dim_trade_id',
    'dim_trade_code',
    # CSI Section IDs
    'dim_csi_section_id',
    'csi_section',
    'csi_title',
]

# Yates-only columns (prefixed)
YATES_COLUMNS = [
    'yates_time',                # Time of inspection
    'yates_wir_number',          # WIR # (Work Inspection Request number)
    'yates_rep',                 # Yates Rep
    'yates_3rd_party',           # 3rd Party
    'yates_secai_cm',            # SECAI CM
    'yates_inspection_comment',  # Inspection Comment
    'yates_category',            # INTERNAL or OFFICIAL
]

# SECAI-only columns (prefixed)
SECAI_COLUMNS = [
    'secai_discipline',          # ARCH, MECH, ELEC
    'secai_number',              # Sequential number
    'secai_request_date',        # Request Date (separate from inspection date)
    'secai_revision',            # Revision number
    'secai_building_type',       # Building Type field
    'secai_module',              # Module
]

# Full column order for output
OUTPUT_COLUMNS = COMMON_COLUMNS + YATES_COLUMNS + SECAI_COLUMNS


def _safe_str(val) -> str:
    """Convert value to string, handling NaN/None."""
    if pd.isna(val):
        return ''
    return str(val)


def _safe_val(val):
    """Return value or None if NaN."""
    if pd.isna(val):
        return None
    return val


def enrich_yates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich Yates inspection data with dimension IDs.

    Maps Yates columns to unified schema + yates_ prefixed columns.
    """
    enriched = []

    for _, row in df.iterrows():
        # Parse location
        location_raw = _safe_str(row.get('Location', ''))
        loc_parsed = parse_location(location_raw)

        # Standardize level
        level = loc_parsed.get('level')
        if not level:
            level = infer_level_from_location(location_raw)
        level = standardize_level(level) if level else None

        building = loc_parsed.get('building')

        # Default to FAB when level exists but building is not specified
        if not building and level:
            building = 'FAB'

        # Standardize contractor
        contractor_raw = _safe_str(row.get('Contractor', ''))
        contractor_std = standardize_company(contractor_raw)

        # Get inspection category from description
        inspection_desc = _safe_str(row.get('Inspection Description', ''))
        inspection_category = categorize_inspection_type(inspection_desc)

        # Dimension lookups
        dim_location_id = get_location_id(building, level)
        building_level = get_building_level(building, level)
        dim_company_id = get_company_id(contractor_std)
        dim_trade_id = get_trade_id(inspection_category)

        # Fallback: use company's primary trade if trade not found
        if dim_trade_id is None and dim_company_id is not None:
            dim_trade_id = get_company_primary_trade_id(dim_company_id)

        dim_trade_code = get_trade_code(dim_trade_id) if dim_trade_id else None

        # CSI Section inference from inspection description
        csi_section_id, csi_section_code = infer_csi_from_keywords(inspection_desc, YATES_KEYWORD_TO_CSI)
        csi_title = CSI_SECTIONS[csi_section_id][1] if csi_section_id and csi_section_id in CSI_SECTIONS else None

        # Get failure reason only on failure
        status_norm = _safe_str(row.get('Status_Normalized', ''))
        failure_reason = _safe_val(row.get('Remarks')) if 'FAIL' in status_norm.upper() else None

        # Build enriched record
        record = {
            # Common columns
            'source': 'YATES',
            'inspection_id': _safe_val(row.get('WIR #')) or f"YT-{row.name}",
            'inspection_date': _safe_val(row.get('Date')),
            'year': _safe_val(row.get('Year')),
            'month': _safe_val(row.get('Month')),
            'week': _safe_val(row.get('Week')),
            'day_of_week': _safe_val(row.get('DayOfWeek')),
            'template': inspection_desc if inspection_desc else None,
            'inspection_category': inspection_category,
            'status': _safe_val(row.get('Inspection Status')),
            'status_normalized': _safe_val(row.get('Status_Normalized')),
            'location_raw': location_raw if location_raw else None,
            'building': building,
            'level': level,
            'area': loc_parsed.get('area'),
            'grid': loc_parsed.get('grid'),
            'contractor_raw': contractor_raw if contractor_raw else None,
            'contractor': contractor_std,
            'failure_reason': failure_reason,
            'dim_location_id': dim_location_id,
            'building_level': building_level,
            'dim_company_id': dim_company_id,
            'dim_trade_id': dim_trade_id,
            'dim_trade_code': dim_trade_code,
            'dim_csi_section_id': csi_section_id,
            'csi_section': csi_section_code,
            'csi_title': csi_title,

            # Yates-specific columns
            'yates_time': _safe_val(row.get('Time')),
            'yates_wir_number': _safe_val(row.get('WIR #')),
            'yates_rep': _safe_val(row.get('Yates Rep')),
            'yates_3rd_party': _safe_val(row.get('3rd\nParty')),
            'yates_secai_cm': _safe_val(row.get('SECAI\nCM')),
            'yates_inspection_comment': _safe_val(row.get('Inspection Comment')),
            'yates_category': _safe_val(row.get('Category')),

            # SECAI columns (null for Yates)
            'secai_discipline': None,
            'secai_number': None,
            'secai_request_date': None,
            'secai_revision': None,
            'secai_building_type': None,
            'secai_module': None,
        }
        enriched.append(record)

    return pd.DataFrame(enriched)


def enrich_secai(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich SECAI inspection data with dimension IDs.

    Maps SECAI columns to unified schema + secai_ prefixed columns.
    """
    enriched = []

    for _, row in df.iterrows():
        # Parse location
        location_raw = _safe_str(row.get('System / Equip/ Location', ''))
        loc_parsed = parse_location(location_raw)

        # Standardize level
        level = loc_parsed.get('level')
        if not level:
            level = infer_level_from_location(location_raw)
        level = standardize_level(level) if level else None

        building = loc_parsed.get('building')

        # Default to FAB when level exists but building is not specified
        if not building and level:
            building = 'FAB'

        # Standardize contractor (Author Company in SECAI)
        contractor_raw = _safe_str(row.get('Author Company', ''))
        contractor_std = standardize_company(contractor_raw)

        # Get inspection category from template
        template = _safe_str(row.get('Template', ''))
        inspection_category = categorize_inspection_type(template)

        # Dimension lookups
        dim_location_id = get_location_id(building, level)
        building_level = get_building_level(building, level)
        dim_company_id = get_company_id(contractor_std)
        dim_trade_id = get_trade_id(inspection_category)

        # Fallback: use company's primary trade if trade not found
        if dim_trade_id is None and dim_company_id is not None:
            dim_trade_id = get_company_primary_trade_id(dim_company_id)

        dim_trade_code = get_trade_code(dim_trade_id) if dim_trade_id else None

        # CSI Section inference from template
        csi_section_id, csi_section_code = infer_csi_from_keywords(template, SECAI_KEYWORD_TO_CSI)
        csi_title = CSI_SECTIONS[csi_section_id][1] if csi_section_id and csi_section_id in CSI_SECTIONS else None

        # Get failure reason only on failure
        status_norm = _safe_str(row.get('Status_Normalized', ''))
        failure_reason = _safe_val(row.get('Reasons for failure')) if 'FAIL' in status_norm.upper() else None

        # Build enriched record
        record = {
            # Common columns
            'source': 'SECAI',
            'inspection_id': _safe_val(row.get('IR Number')) or f"SECAI-{row.name}",
            'inspection_date': _safe_val(row.get('Inspection Request Date')),
            'year': _safe_val(row.get('Year')),
            'month': _safe_val(row.get('Month')),
            'week': _safe_val(row.get('Week')),
            'day_of_week': _safe_val(row.get('DayOfWeek')),
            'template': template if template else None,
            'inspection_category': inspection_category,
            'status': _safe_val(row.get('Status')),
            'status_normalized': _safe_val(row.get('Status_Normalized')),
            'location_raw': location_raw if location_raw else None,
            'building': building,
            'level': level,
            'area': loc_parsed.get('area'),
            'grid': loc_parsed.get('grid'),
            'contractor_raw': contractor_raw if contractor_raw else None,
            'contractor': contractor_std,
            'failure_reason': failure_reason,
            'dim_location_id': dim_location_id,
            'building_level': building_level,
            'dim_company_id': dim_company_id,
            'dim_trade_id': dim_trade_id,
            'dim_trade_code': dim_trade_code,
            'dim_csi_section_id': csi_section_id,
            'csi_section': csi_section_code,
            'csi_title': csi_title,

            # Yates columns (null for SECAI)
            'yates_time': None,
            'yates_wir_number': None,
            'yates_rep': None,
            'yates_3rd_party': None,
            'yates_secai_cm': None,
            'yates_inspection_comment': None,
            'yates_category': None,

            # SECAI-specific columns
            'secai_discipline': _safe_val(row.get('Discipline')),
            'secai_number': _safe_val(row.get('Number')),
            'secai_request_date': _safe_val(row.get('Request Date')),
            'secai_revision': _safe_val(row.get('Revision')),
            'secai_building_type': _safe_val(row.get('Building Type')),
            'secai_module': _safe_val(row.get('Module')),
        }
        enriched.append(record)

    return pd.DataFrame(enriched)


def calculate_coverage(df: pd.DataFrame, source: str) -> Dict[str, Dict]:
    """Calculate dimension ID coverage statistics."""
    return {
        'source': source,
        'total_records': len(df),
        'location': {
            'mapped': int(df['dim_location_id'].notna().sum()),
            'total': len(df),
            'pct': round(df['dim_location_id'].notna().mean() * 100, 1)
        },
        'company': {
            'mapped': int(df['dim_company_id'].notna().sum()),
            'total': len(df),
            'pct': round(df['dim_company_id'].notna().mean() * 100, 1)
        },
        'trade': {
            'mapped': int(df['dim_trade_id'].notna().sum()),
            'total': len(df),
            'pct': round(df['dim_trade_id'].notna().mean() * 100, 1)
        },
        'csi_section': {
            'mapped': int(df['dim_csi_section_id'].notna().sum()),
            'total': len(df),
            'pct': round(df['dim_csi_section_id'].notna().mean() * 100, 1)
        },
        'building': {
            'mapped': int(df['building'].notna().sum()),
            'total': len(df),
            'pct': round(df['building'].notna().mean() * 100, 1)
        }
    }


def print_coverage(coverage: Dict):
    """Print coverage statistics."""
    print(f"\n{coverage['source']} Coverage ({coverage['total_records']} records):")
    print(f"  Building:     {coverage['building']['mapped']}/{coverage['building']['total']} ({coverage['building']['pct']}%)")
    print(f"  Location:     {coverage['location']['mapped']}/{coverage['location']['total']} ({coverage['location']['pct']}%)")
    print(f"  Company:      {coverage['company']['mapped']}/{coverage['company']['total']} ({coverage['company']['pct']}%)")
    print(f"  Trade:        {coverage['trade']['mapped']}/{coverage['trade']['total']} ({coverage['trade']['pct']}%)")
    print(f"  CSI Section:  {coverage['csi_section']['mapped']}/{coverage['csi_section']['total']} ({coverage['csi_section']['pct']}%)")


def consolidate():
    """Main consolidation function."""
    input_dir = settings.PROCESSED_DATA_DIR / 'quality'
    output_dir = input_dir / 'enriched'
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Quality Inspection Data Consolidation")
    print("=" * 60)

    results = []

    # Process Yates
    yates_path = input_dir / 'yates_all_inspections.csv'
    if yates_path.exists():
        print(f"\nLoading Yates: {yates_path}")
        yates_df = pd.read_csv(yates_path)
        print(f"  Loaded {len(yates_df)} records")

        yates_enriched = enrich_yates(yates_df)

        # Reorder columns to match output schema
        yates_enriched = yates_enriched[OUTPUT_COLUMNS]

        yates_output = output_dir / 'yates_qc_inspections.csv'
        yates_enriched.to_csv(yates_output, index=False)
        print(f"  Wrote: {yates_output}")

        coverage = calculate_coverage(yates_enriched, 'YATES')
        print_coverage(coverage)
        results.append(('YATES', yates_enriched, coverage))
    else:
        print(f"\nWARNING: Yates file not found: {yates_path}")

    # Process SECAI
    secai_path = input_dir / 'secai_inspection_log.csv'
    if secai_path.exists():
        print(f"\nLoading SECAI: {secai_path}")
        secai_df = pd.read_csv(secai_path)
        print(f"  Loaded {len(secai_df)} records")

        secai_enriched = enrich_secai(secai_df)

        # Reorder columns to match output schema
        secai_enriched = secai_enriched[OUTPUT_COLUMNS]

        secai_output = output_dir / 'secai_qc_inspections.csv'
        secai_enriched.to_csv(secai_output, index=False)
        print(f"  Wrote: {secai_output}")

        coverage = calculate_coverage(secai_enriched, 'SECAI')
        print_coverage(coverage)
        results.append(('SECAI', secai_enriched, coverage))
    else:
        print(f"\nWARNING: SECAI file not found: {secai_path}")

    # Combine into single file
    if len(results) > 0:
        combined = pd.concat([r[1] for r in results], ignore_index=True)
        combined_output = output_dir / 'combined_qc_inspections.csv'
        combined.to_csv(combined_output, index=False)
        print(f"\nCombined output: {combined_output}")
        print(f"  Total records: {len(combined)}")
        print(f"  Columns: {len(combined.columns)}")

        # Overall coverage
        overall_coverage = calculate_coverage(combined, 'COMBINED')
        print_coverage(overall_coverage)

        # Write consolidation report
        report = {
            'generated_at': datetime.now().isoformat(),
            'sources': [r[2] for r in results],
            'combined': overall_coverage,
            'columns': {
                'common': COMMON_COLUMNS,
                'yates_only': YATES_COLUMNS,
                'secai_only': SECAI_COLUMNS,
                'total': len(OUTPUT_COLUMNS),
            }
        }
        report_path = output_dir / 'consolidation_report.json'
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"\nReport: {report_path}")

    print("\n" + "=" * 60)
    print("Consolidation Complete")
    print("=" * 60)

    # Print column summary
    print(f"\nColumn Summary:")
    print(f"  Common columns:     {len(COMMON_COLUMNS)}")
    print(f"  Yates-only (yates_*): {len(YATES_COLUMNS)}")
    print(f"  SECAI-only (secai_*): {len(SECAI_COLUMNS)}")
    print(f"  Total columns:      {len(OUTPUT_COLUMNS)}")


def main():
    """Entry point."""
    consolidate()


if __name__ == "__main__":
    main()
