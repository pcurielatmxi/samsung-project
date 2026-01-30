"""
RABA and PSI quality inspection table schemas.

Output Location: {WINDOWS_DATA_DIR}/processed/raba/
"""

from datetime import date
from typing import Optional
from pydantic import BaseModel, Field


class RabaPsiConsolidated(BaseModel):
    """
    Consolidated RABA + PSI quality inspection data.

    File: raba_psi_consolidated.csv
    Records: ~15.7K inspections
    """
    source: str = Field(description="Source system (RABA or PSI)")
    inspection_id: str = Field(description="Unique inspection identifier")
    source_file: Optional[str] = Field(default=None, description="Source file name")
    report_date: Optional[date] = Field(default=None, description="Report date (YYYY-MM-DD)")
    report_date_normalized: Optional[date] = Field(default=None, description="Normalized report date")
    inspection_type: Optional[str] = Field(default=None, description="Type of inspection")
    inspection_type_normalized: Optional[str] = Field(default=None, description="Normalized inspection type")
    inspection_category: Optional[str] = Field(default=None, description="Inspection category")
    building: Optional[str] = Field(default=None, description="Building code")
    level: Optional[str] = Field(default=None, description="Level code")
    area: Optional[str] = Field(default=None, description="Area within building")
    grid: Optional[str] = Field(default=None, description="Grid coordinates")
    location_id: Optional[str] = Field(default=None, description="Location ID (legacy)")
    outcome: Optional[str] = Field(default=None, description="Inspection outcome (PASS/FAIL)")
    failure_reason: Optional[str] = Field(default=None, description="Reason for failure")
    failure_category: Optional[str] = Field(default=None, description="Category of failure")
    summary: Optional[str] = Field(default=None, description="Inspection summary")
    tests_total: Optional[int] = Field(default=None, description="Total number of tests")
    tests_passed: Optional[int] = Field(default=None, description="Number of tests passed")
    tests_failed: Optional[int] = Field(default=None, description="Number of tests failed")
    deficiency_count: Optional[int] = Field(default=None, description="Count of deficiencies")
    reinspection_required: Optional[bool] = Field(default=None, description="Whether reinspection required")
    corrective_action: Optional[str] = Field(default=None, description="Corrective action required")
    engineer: Optional[str] = Field(default=None, description="Engineer name")
    inspector: Optional[str] = Field(default=None, description="Inspector name")
    contractor: Optional[str] = Field(default=None, description="Contractor name")
    testing_company: Optional[str] = Field(default=None, description="Testing company name")
    subcontractor: Optional[str] = Field(default=None, description="Subcontractor name")
    trade: Optional[str] = Field(default=None, description="Trade")
    issues: Optional[str] = Field(default=None, description="Issues JSON")
    issue_count: Optional[int] = Field(default=None, description="Number of issues")
    dim_location_id: Optional[int] = Field(default=None, description="FK to dim_location")
    building_level: Optional[str] = Field(default=None, description="Combined building-level")
    dim_company_id: Optional[int] = Field(default=None, description="FK to dim_company (contractor)")
    dim_subcontractor_id: Optional[int] = Field(default=None, description="FK to dim_company (subcontractor)")
    performing_company_id: Optional[int] = Field(default=None, description="FK to dim_company (performing)")
    dim_csi_section_id: Optional[int] = Field(default=None, description="FK to dim_csi_section")
    csi_section: Optional[str] = Field(default=None, description="CSI section code")
    csi_title: Optional[str] = Field(default=None, description="CSI section title")


class RabaPsiDataQuality(BaseModel):
    """
    RABA + PSI data quality tracking.

    File: raba_psi_data_quality.csv
    """
    inspection_id: str = Field(description="Unique inspection identifier")
    level_raw: Optional[str] = Field(default=None, description="Raw level value")
    location_raw: Optional[str] = Field(default=None, description="Raw location string")
    inspector_raw: Optional[str] = Field(default=None, description="Raw inspector name")
    contractor_raw: Optional[str] = Field(default=None, description="Raw contractor name")
    testing_company_raw: Optional[str] = Field(default=None, description="Raw testing company name")
    subcontractor_raw: Optional[str] = Field(default=None, description="Raw subcontractor name")
    trade_raw: Optional[str] = Field(default=None, description="Raw trade value")
    grid_row_min: Optional[str] = Field(default=None, description="Grid row minimum")
    grid_row_max: Optional[str] = Field(default=None, description="Grid row maximum")
    grid_col_min: Optional[float] = Field(default=None, description="Grid column minimum")
    grid_col_max: Optional[float] = Field(default=None, description="Grid column maximum")
    grid_source: Optional[str] = Field(default=None, description="Source of grid extraction")
    affected_rooms: Optional[str] = Field(default=None, description="JSON array of affected rooms")
    affected_rooms_count: Optional[int] = Field(default=None, description="Count of affected rooms")
    location_type: Optional[str] = Field(default=None, description="Location type")
    location_code: Optional[str] = Field(default=None, description="Location code")
    match_type: Optional[str] = Field(default=None, description="Location match type")
    csi_inference_source: Optional[str] = Field(default=None, description="Source of CSI inference")
    validation_issues: Optional[str] = Field(default=None, alias="_validation_issues", description="Validation issues")
    is_multi_party: Optional[bool] = Field(default=None, description="Whether multiple parties involved")
    narrative_companies: Optional[str] = Field(default=None, description="Companies from narrative")
