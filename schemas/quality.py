"""
Quality inspection data schemas.

These schemas define the structure for RABA and PSI quality inspection
output files. Both use the unified QC inspection schema for Power BI
compatibility.

Output Location: {WINDOWS_DATA_DIR}/processed/{raba|psi}/
"""

from typing import Optional
from pydantic import BaseModel, Field


class QCInspectionConsolidated(BaseModel):
    """
    Unified QC inspection schema for RABA and PSI consolidated files.

    Files:
      - raba_consolidated.csv
      - psi_consolidated.csv

    Both files use IDENTICAL column order and names for direct append
    in Power BI without transformation. Source-specific columns are
    blank/null for the non-applicable source.

    Based on: scripts/shared/qc_inspection_schema.py UNIFIED_COLUMNS
    """

    model_config = {'populate_by_name': True}

    # Source identification
    source: str = Field(description="Source identifier: 'RABA' or 'PSI'")
    inspection_id: str = Field(description="Primary key (assignment number or report ID)")
    source_file: str = Field(description="Original source file name")

    # Dates
    report_date: Optional[str] = Field(default=None, description="Raw date string from document")
    report_date_normalized: Optional[str] = Field(default=None, description="Normalized date (YYYY-MM-DD)")

    # Inspection type (unified naming)
    inspection_type: Optional[str] = Field(default=None, description="Raw inspection/test type")
    inspection_type_normalized: Optional[str] = Field(default=None, description="Normalized inspection type")
    inspection_category: Optional[str] = Field(default=None, description="Inspection category")

    # Location
    location_raw: Optional[str] = Field(default=None, description="Original location text from document")
    building: Optional[str] = Field(default=None, description="Building code: FAB, SUE, SUW, FIZ")
    level_raw: Optional[str] = Field(default=None, description="Raw level value from document")
    level: Optional[str] = Field(default=None, description="Standardized level (1F, 2F, B1, ROOF)")
    area: Optional[str] = Field(default=None, description="Area within building")
    grid: Optional[str] = Field(default=None, description="Grid coordinate (normalized)")
    grid_row_min: Optional[str] = Field(default=None, description="Grid row minimum (A-Z)")
    grid_row_max: Optional[str] = Field(default=None, description="Grid row maximum (A-Z)")
    grid_col_min: Optional[float] = Field(default=None, description="Grid column minimum (numeric)")
    grid_col_max: Optional[float] = Field(default=None, description="Grid column maximum (numeric)")
    location_id: Optional[str] = Field(default=None, description="Location reference")

    # Results
    outcome: Optional[str] = Field(default=None, description="PASS, FAIL, PARTIAL, CANCELLED, MEASUREMENT")
    failure_reason: Optional[str] = Field(default=None, description="Quoted failure reason from document")
    failure_category: Optional[str] = Field(default=None, description="Categorized failure reason")
    summary: Optional[str] = Field(default=None, description="Report summary text")

    # Test metrics (RABA only - blank for PSI)
    tests_total: Optional[float] = Field(default=None, description="Total tests performed")
    tests_passed: Optional[float] = Field(default=None, description="Tests that passed")
    tests_failed: Optional[float] = Field(default=None, description="Tests that failed")

    # Deficiency metrics (PSI only - blank for RABA)
    deficiency_count: Optional[float] = Field(default=None, description="Number of deficiencies found")

    # Follow-up
    reinspection_required: Optional[str] = Field(default=None, description="Whether reinspection is required")
    corrective_action: Optional[str] = Field(default=None, description="Corrective action text")

    # Parties - raw values
    inspector_raw: Optional[str] = Field(default=None, description="Raw inspector name")
    contractor_raw: Optional[str] = Field(default=None, description="Raw contractor name")
    testing_company_raw: Optional[str] = Field(default=None, description="RABA only: testing lab")
    subcontractor_raw: Optional[str] = Field(default=None, description="PSI only: subcontractor")
    trade_raw: Optional[str] = Field(default=None, description="PSI only: trade/crew")
    engineer: Optional[str] = Field(default=None, description="RABA only: engineer name")

    # Parties - standardized
    inspector: Optional[str] = Field(default=None, description="Standardized inspector name")
    contractor: Optional[str] = Field(default=None, description="Standardized contractor name")
    testing_company: Optional[str] = Field(default=None, description="RABA only: standardized testing company")
    subcontractor: Optional[str] = Field(default=None, description="PSI only: standardized subcontractor")
    trade: Optional[str] = Field(default=None, description="PSI only: standardized trade")

    # Issues
    issues: Optional[str] = Field(default=None, description="Pipe-delimited issue descriptions")
    issue_count: int = Field(description="Count of issues")

    # Dimension keys (for integration)
    dim_location_id: Optional[float] = Field(default=None, description="FK to dim_location")
    building_level: Optional[str] = Field(default=None, description="Building-level string (e.g., 'FAB-1F')")
    dim_company_id: Optional[float] = Field(default=None, description="FK to dim_company")
    dim_trade_id: Optional[float] = Field(default=None, description="FK to dim_trade")
    dim_trade_code: Optional[str] = Field(default=None, description="Trade code from dim_trade")

    # CSI Section (52-category classification)
    dim_csi_section_id: Optional[float] = Field(default=None, description="FK to dim_csi_section")
    csi_section: Optional[str] = Field(default=None, description="CSI code (e.g., '03 30 00')")
    csi_inference_source: Optional[str] = Field(default=None, description="How CSI was inferred")
    csi_title: Optional[str] = Field(default=None, description="CSI section title")

    # Room matching (JSON array)
    affected_rooms: Optional[str] = Field(
        default=None,
        description="JSON array of rooms whose grid bounds overlap"
    )
    affected_rooms_count: Optional[int] = Field(
        default=None,
        description="Count of rooms in affected_rooms (1=single match, >1=multiple)"
    )

    # Location quality diagnostics (for Power BI filtering)
    grid_completeness: Optional[str] = Field(
        default=None,
        description="What grid info was available: FULL, ROW_ONLY, COL_ONLY, LEVEL_ONLY, NONE"
    )
    match_quality: Optional[str] = Field(
        default=None,
        description="Summary of match types: PRECISE, MIXED, PARTIAL, NONE"
    )
    location_review_flag: Optional[bool] = Field(
        default=None,
        description="True if location needs human investigation"
    )

    # Validation (note: field uses alias for CSV column name with underscore prefix)
    validation_issues: Optional[str] = Field(
        default=None,
        alias='_validation_issues',
        description="Pipe-delimited validation issues"
    )


# RABA-specific schema (full columns including CSI)
RabaConsolidated = QCInspectionConsolidated


class PsiConsolidated(BaseModel):
    """
    PSI-specific schema (without CSI columns that haven't been added yet).

    File: psi_consolidated.csv

    Note: PSI hasn't been enriched with CSI columns yet, so they are not
    included in this schema. Once PSI is enriched, migrate to QCInspectionConsolidated.
    """

    model_config = {'populate_by_name': True}

    # Source identification
    source: str = Field(description="Source identifier: 'PSI'")
    inspection_id: str = Field(description="Primary key (report ID)")
    source_file: str = Field(description="Original source file name")

    # Dates
    report_date: Optional[str] = Field(default=None, description="Raw date string from document")
    report_date_normalized: Optional[str] = Field(default=None, description="Normalized date (YYYY-MM-DD)")

    # Inspection type (unified naming)
    inspection_type: Optional[str] = Field(default=None, description="Raw inspection type")
    inspection_type_normalized: Optional[str] = Field(default=None, description="Normalized inspection type")
    inspection_category: Optional[str] = Field(default=None, description="Inspection category")

    # Location
    location_raw: Optional[str] = Field(default=None, description="Original location text from document")
    building: Optional[str] = Field(default=None, description="Building code: FAB, SUE, SUW, FIZ")
    level_raw: Optional[str] = Field(default=None, description="Raw level value from document")
    level: Optional[str] = Field(default=None, description="Standardized level (1F, 2F, B1, ROOF)")
    area: Optional[str] = Field(default=None, description="Area within building")
    grid: Optional[str] = Field(default=None, description="Grid coordinate (normalized)")
    grid_row_min: Optional[str] = Field(default=None, description="Grid row minimum (A-Z)")
    grid_row_max: Optional[str] = Field(default=None, description="Grid row maximum (A-Z)")
    grid_col_min: Optional[float] = Field(default=None, description="Grid column minimum (numeric)")
    grid_col_max: Optional[float] = Field(default=None, description="Grid column maximum (numeric)")
    location_id: Optional[str] = Field(default=None, description="Location reference")

    # Results
    outcome: Optional[str] = Field(default=None, description="PASS, FAIL, PARTIAL, CANCELLED, MEASUREMENT")
    failure_reason: Optional[str] = Field(default=None, description="Quoted failure reason from document")
    failure_category: Optional[str] = Field(default=None, description="Categorized failure reason")
    summary: Optional[str] = Field(default=None, description="Report summary text")

    # Test metrics
    tests_total: Optional[float] = Field(default=None, description="Total tests performed")
    tests_passed: Optional[float] = Field(default=None, description="Tests that passed")
    tests_failed: Optional[float] = Field(default=None, description="Tests that failed")

    # Deficiency metrics
    deficiency_count: Optional[float] = Field(default=None, description="Number of deficiencies found")

    # Follow-up
    reinspection_required: Optional[str] = Field(default=None, description="Whether reinspection is required")
    corrective_action: Optional[str] = Field(default=None, description="Corrective action text")

    # Parties - raw values
    inspector_raw: Optional[str] = Field(default=None, description="Raw inspector name")
    contractor_raw: Optional[str] = Field(default=None, description="Raw contractor name")
    testing_company_raw: Optional[str] = Field(default=None, description="Testing lab")
    subcontractor_raw: Optional[str] = Field(default=None, description="Subcontractor")
    trade_raw: Optional[str] = Field(default=None, description="Trade/crew")
    engineer: Optional[str] = Field(default=None, description="Engineer name")

    # Parties - standardized
    inspector: Optional[str] = Field(default=None, description="Standardized inspector name")
    contractor: Optional[str] = Field(default=None, description="Standardized contractor name")
    testing_company: Optional[str] = Field(default=None, description="Standardized testing company")
    subcontractor: Optional[str] = Field(default=None, description="Standardized subcontractor")
    trade: Optional[str] = Field(default=None, description="Standardized trade")

    # Issues
    issues: Optional[str] = Field(default=None, description="Pipe-delimited issue descriptions")
    issue_count: int = Field(description="Count of issues")

    # Dimension keys (for integration)
    dim_location_id: Optional[float] = Field(default=None, description="FK to dim_location")
    building_level: Optional[str] = Field(default=None, description="Building-level string (e.g., 'FAB-1F')")
    dim_company_id: Optional[float] = Field(default=None, description="FK to dim_company")
    dim_trade_id: Optional[float] = Field(default=None, description="FK to dim_trade")
    dim_trade_code: Optional[str] = Field(default=None, description="Trade code from dim_trade")

    # Room matching (JSON array)
    affected_rooms: Optional[str] = Field(
        default=None,
        description="JSON array of rooms whose grid bounds overlap"
    )
    affected_rooms_count: Optional[int] = Field(
        default=None,
        description="Count of rooms in affected_rooms (1=single match, >1=multiple)"
    )

    # Location quality diagnostics (for Power BI filtering)
    grid_completeness: Optional[str] = Field(
        default=None,
        description="What grid info was available: FULL, ROW_ONLY, COL_ONLY, LEVEL_ONLY, NONE"
    )
    match_quality: Optional[str] = Field(
        default=None,
        description="Summary of match types: PRECISE, MIXED, PARTIAL, NONE"
    )
    location_review_flag: Optional[bool] = Field(
        default=None,
        description="True if location needs human investigation"
    )

    # Validation
    validation_issues: Optional[str] = Field(
        default=None,
        alias='_validation_issues',
        description="Pipe-delimited validation issues"
    )
