"""
Quality inspection data schemas.

These schemas define the structure for Yates/SECAI QC inspection
output files from the quality workbook processing pipeline.

Output Location: {WINDOWS_DATA_DIR}/processed/quality/
"""

from datetime import date
from typing import Optional
from pydantic import BaseModel, Field


class QCInspectionsEnriched(BaseModel):
    """
    QC inspections enriched with dimension lookups.

    File: quality/qc_inspections_enriched.csv
    Records: Combined Yates + SECAI inspections
    """
    source: str = Field(description="Source: 'YATES' or 'SECAI'")
    inspection_id: int = Field(description="Primary key")
    inspection_date: Optional[date] = Field(default=None, description="Inspection date (YYYY-MM-DD)")
    year: Optional[int] = Field(default=None, description="Year")
    month: Optional[int] = Field(default=None, description="Month (1-12)")
    week: Optional[int] = Field(default=None, description="ISO week number")
    day_of_week: Optional[str] = Field(default=None, description="Day of week")
    template: Optional[str] = Field(default=None, description="Inspection template/type")
    inspection_category: Optional[str] = Field(default=None, description="Inspection category")
    status: Optional[str] = Field(default=None, description="Raw status")
    status_normalized: Optional[str] = Field(default=None, description="Normalized status (PASS/FAIL)")
    location_raw: Optional[str] = Field(default=None, description="Original location text")
    building: Optional[str] = Field(default=None, description="Building code")
    level: Optional[str] = Field(default=None, description="Level (1F, 2F, etc.)")
    area: Optional[str] = Field(default=None, description="Area within building")
    grid: Optional[str] = Field(default=None, description="Grid coordinate")
    contractor_raw: Optional[str] = Field(default=None, description="Raw contractor name")
    contractor: Optional[str] = Field(default=None, description="Standardized contractor")
    failure_reason: Optional[str] = Field(default=None, description="Failure reason if failed")
    dim_location_id: Optional[int] = Field(default=None, description="FK to dim_location")
    building_level: Optional[str] = Field(default=None, description="Building-level (e.g., FAB-1F)")
    dim_company_id: Optional[int] = Field(default=None, description="FK to dim_company")
    dim_csi_section_id: Optional[int] = Field(default=None, description="FK to dim_csi_section")
    csi_section: Optional[str] = Field(default=None, description="CSI code")
    csi_title: Optional[str] = Field(default=None, description="CSI section title")
    csi_division: Optional[int] = Field(default=None, description="CSI division")
    location_type: Optional[str] = Field(default=None, description="Location type")
    location_code: Optional[str] = Field(default=None, description="Location code")
    affected_rooms: Optional[str] = Field(default=None, description="JSON array of affected rooms")
    affected_rooms_count: Optional[int] = Field(default=None, description="Count of affected rooms")


class QCInspectionsCombined(BaseModel):
    """
    Combined QC inspections with source-specific columns.

    Files:
      - quality/enriched/combined_qc_inspections.csv
      - quality/enriched/yates_qc_inspections.csv
      - quality/enriched/secai_qc_inspections.csv
    """
    source: str = Field(description="Source: 'YATES' or 'SECAI'")
    inspection_id: int = Field(description="Primary key")
    inspection_date: Optional[date] = Field(default=None, description="Inspection date (YYYY-MM-DD)")
    year: Optional[int] = Field(default=None, description="Year")
    month: Optional[int] = Field(default=None, description="Month (1-12)")
    week: Optional[int] = Field(default=None, description="ISO week number")
    day_of_week: Optional[str] = Field(default=None, description="Day of week")
    template: Optional[str] = Field(default=None, description="Inspection template/type")
    inspection_category: Optional[str] = Field(default=None, description="Inspection category")
    status: Optional[str] = Field(default=None, description="Raw status")
    status_normalized: Optional[str] = Field(default=None, description="Normalized status (PASS/FAIL)")
    location_raw: Optional[str] = Field(default=None, description="Original location text")
    building: Optional[str] = Field(default=None, description="Building code")
    level: Optional[str] = Field(default=None, description="Level (1F, 2F, etc.)")
    area: Optional[str] = Field(default=None, description="Area within building")
    grid: Optional[str] = Field(default=None, description="Grid coordinate")
    contractor_raw: Optional[str] = Field(default=None, description="Raw contractor name")
    contractor: Optional[str] = Field(default=None, description="Standardized contractor")
    failure_reason: Optional[str] = Field(default=None, description="Failure reason if failed")
    dim_location_id: Optional[int] = Field(default=None, description="FK to dim_location")
    building_level: Optional[str] = Field(default=None, description="Building-level (e.g., FAB-1F)")
    dim_company_id: Optional[int] = Field(default=None, description="FK to dim_company")
    dim_csi_section_id: Optional[int] = Field(default=None, description="FK to dim_csi_section")
    csi_section: Optional[str] = Field(default=None, description="CSI code")
    csi_title: Optional[str] = Field(default=None, description="CSI section title")
    # Yates-specific columns (blank for SECAI)
    yates_time: Optional[str] = Field(default=None, description="Inspection time")
    yates_wir_number: Optional[int] = Field(default=None, description="WIR number")
    yates_rep: Optional[float] = Field(default=None, description="Yates representative")
    yates_3rd_party: Optional[str] = Field(default=None, description="Third party inspector")
    yates_secai_cm: Optional[str] = Field(default=None, description="SECAI CM")
    yates_inspection_comment: Optional[str] = Field(default=None, description="Inspection comment")
    yates_category: Optional[str] = Field(default=None, description="Yates category")
    # SECAI-specific columns (blank for Yates)
    secai_discipline: Optional[float] = Field(default=None, description="SECAI discipline")
    secai_number: Optional[float] = Field(default=None, description="SECAI IR number")
    secai_request_date: Optional[float] = Field(default=None, description="Request date")
    secai_revision: Optional[float] = Field(default=None, description="Revision number")
    secai_building_type: Optional[float] = Field(default=None, description="Building type")
    secai_module: Optional[float] = Field(default=None, description="Module")


class QCInspectionsDataQuality(BaseModel):
    """
    QC inspections data quality tracking.

    File: quality/qc_inspections_data_quality.csv
    1:1 with qc_inspections_enriched.csv via inspection_id
    """
    inspection_id: int = Field(description="FK to qc_inspections_enriched")
    grid_row_min: Optional[str] = Field(default=None, description="Grid row minimum")
    grid_row_max: Optional[str] = Field(default=None, description="Grid row maximum")
    grid_col_min: Optional[float] = Field(default=None, description="Grid column minimum")
    grid_col_max: Optional[float] = Field(default=None, description="Grid column maximum")
    grid_source: Optional[str] = Field(default=None, description="Source of grid")
    match_type: Optional[str] = Field(default=None, description="Location match type")
    # Yates-specific columns
    yates_time: Optional[str] = Field(default=None, description="Inspection time")
    yates_wir_number: Optional[int] = Field(default=None, description="WIR number")
    yates_rep: Optional[float] = Field(default=None, description="Yates representative")
    yates_3rd_party: Optional[str] = Field(default=None, description="Third party inspector")
    yates_secai_cm: Optional[str] = Field(default=None, description="SECAI CM")
    yates_inspection_comment: Optional[str] = Field(default=None, description="Inspection comment")
    yates_category: Optional[str] = Field(default=None, description="Yates category")
    # SECAI-specific columns
    secai_discipline: Optional[float] = Field(default=None, description="SECAI discipline")
    secai_number: Optional[float] = Field(default=None, description="SECAI IR number")
    secai_request_date: Optional[float] = Field(default=None, description="Request date")
    secai_revision: Optional[float] = Field(default=None, description="Revision number")
    secai_building_type: Optional[float] = Field(default=None, description="Building type")
    secai_module: Optional[float] = Field(default=None, description="Module")


# Legacy aliases for backward compatibility with existing code
QCInspectionConsolidated = QCInspectionsEnriched
RabaConsolidated = QCInspectionsEnriched
PsiConsolidated = QCInspectionsEnriched
RabaPsiDataQuality = QCInspectionsDataQuality
