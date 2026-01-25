"""
Unified schema for third-party QC inspections (RABA + PSI).

Both sources output CSV files with IDENTICAL column order and names,
allowing direct append in Power BI without transformation.

Usage:
    from scripts.shared.qc_inspection_schema import UNIFIED_COLUMNS, apply_unified_schema
"""

from typing import Dict, Any, List
import pandas as pd


# Unified column order - both RABA and PSI will output these exact columns
# Source-specific columns will be blank/null for the non-applicable source
UNIFIED_COLUMNS = [
    # Source identification
    'source',                    # 'RABA' or 'PSI'
    'inspection_id',             # Primary key
    'source_file',               # Original file name

    # Dates
    'report_date',               # Raw date string
    'report_date_normalized',    # YYYY-MM-DD format

    # Inspection type (unified naming)
    'inspection_type',           # RABA: test_type, PSI: inspection_type
    'inspection_type_normalized',# RABA: test_type_normalized, PSI: inspection_type_normalized
    'inspection_category',       # RABA: test_category, PSI: inspection_category

    # Location
    'location_raw',              # Original location text
    'building',                  # FAB, SUE, SUW, FIZ
    'level_raw',                 # Raw level value
    'level',                     # Standardized level (1F, 2F, B1, ROOF)
    'area',                      # Area within building
    'grid',                      # Grid coordinate (normalized)
    'grid_row_min',              # Grid row minimum (A-Z)
    'grid_row_max',              # Grid row maximum (A-Z)
    'grid_col_min',              # Grid column minimum (numeric)
    'grid_col_max',              # Grid column maximum (numeric)
    'location_id',               # Location reference

    # Results
    'outcome',                   # PASS, FAIL, PARTIAL, CANCELLED, MEASUREMENT
    'failure_reason',            # Quoted reason from document
    'failure_category',          # Categorized failure reason
    'summary',                   # Report summary text

    # Test metrics (RABA only - blank for PSI)
    'tests_total',               # Total tests performed
    'tests_passed',              # Tests that passed
    'tests_failed',              # Tests that failed

    # Deficiency metrics (PSI only - blank for RABA)
    'deficiency_count',          # Number of deficiencies found

    # Follow-up
    'reinspection_required',     # Boolean
    'corrective_action',         # Corrective action text

    # Parties - raw values
    'inspector_raw',             # Raw inspector name
    'contractor_raw',            # Raw contractor name
    'testing_company_raw',       # RABA only: testing lab
    'subcontractor_raw',         # PSI only: subcontractor
    'trade_raw',                 # PSI only: trade/crew
    'engineer',                  # RABA only: engineer name

    # Parties - standardized
    'inspector',                 # Standardized inspector
    'contractor',                # Standardized contractor
    'testing_company',           # RABA only: standardized testing co
    'subcontractor',             # PSI only: standardized subcontractor
    'trade',                     # PSI only: standardized trade

    # Issues
    'issues',                    # Pipe-delimited issue descriptions
    'issue_count',               # Count of issues

    # Dimension keys (for integration)
    'dim_location_id',           # Location dimension ID (integer FK)
    'building_level',            # Building-level string for display (e.g., "FAB-1F")
    'dim_company_id',            # Company dimension ID
    'dim_trade_id',              # Trade dimension ID
    'dim_trade_code',            # Trade code

    # CSI Section (52-category classification)
    'dim_csi_section_id',        # CSI section dimension ID
    'csi_section',               # CSI code (e.g., "03 30 00")
    'csi_inference_source',      # How CSI was inferred (keyword, category)
    'csi_title',                 # CSI section title

    # Room matching (JSON array of rooms whose grid bounds overlap)
    'affected_rooms',            # JSON: [{"location_code": "FAB1xxx", "room_name": "...", "match_type": "FULL|PARTIAL"}, ...]
    'affected_rooms_count',      # Count of rooms (1=single match, >1=multiple)

    # Location quality diagnostics (for Power BI filtering)
    'grid_completeness',         # FULL, ROW_ONLY, COL_ONLY, LEVEL_ONLY, NONE - what grid info was available
    'match_quality',             # PRECISE, MIXED, PARTIAL, NONE - summary of match types
    'location_review_flag',      # Boolean - True if location needs human investigation

    # Validation
    '_validation_issues',        # Pipe-delimited validation issues
]


def apply_unified_schema(records: List[Dict[str, Any]], source: str) -> pd.DataFrame:
    """
    Apply unified schema to a list of flattened records.

    Ensures all columns are present in the correct order, with missing columns
    filled with None.

    Args:
        records: List of flattened record dicts
        source: Source identifier ('RABA' or 'PSI')

    Returns:
        DataFrame with unified schema
    """
    if not records:
        # Return empty DataFrame with correct schema
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    df = pd.DataFrame(records)

    # Add source column
    df['source'] = source

    # Add any missing columns with None
    for col in UNIFIED_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Reorder to match unified schema exactly
    df = df[UNIFIED_COLUMNS]

    return df


def get_raba_column_mapping() -> Dict[str, str]:
    """
    Get column mapping from RABA-specific names to unified names.

    Returns:
        Dict mapping old column names to unified names
    """
    return {
        'test_type': 'inspection_type',
        'test_type_normalized': 'inspection_type_normalized',
        'test_category': 'inspection_category',
    }


def get_psi_column_mapping() -> Dict[str, str]:
    """
    Get column mapping from PSI-specific names to unified names.

    PSI columns are already aligned with unified schema, so this is empty.

    Returns:
        Dict mapping old column names to unified names
    """
    return {
        # PSI columns already match unified names
    }
