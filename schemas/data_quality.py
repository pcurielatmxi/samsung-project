"""
Data quality tracking table schemas.

These tables contain metadata about data lineage, inference sources,
and validation issues. They are 1:1 with their corresponding fact tables.

Output Location: {WINDOWS_DATA_DIR}/processed/{source}/
"""

from typing import Optional
from pydantic import BaseModel, Field


class TbmWorkEntriesDataQuality(BaseModel):
    """
    TBM work entries data quality tracking.

    File: tbm/work_entries_data_quality.csv
    1:1 with tbm/work_entries.csv
    """
    tbm_work_entry_id: str = Field(description="FK to work_entries")
    grid_row_min: Optional[str] = Field(default=None, description="Grid row minimum")
    grid_row_max: Optional[str] = Field(default=None, description="Grid row maximum")
    grid_col_min: Optional[float] = Field(default=None, description="Grid column minimum")
    grid_col_max: Optional[float] = Field(default=None, description="Grid column maximum")
    grid_raw: Optional[str] = Field(default=None, description="Raw grid string from source")
    grid_type: Optional[str] = Field(default=None, description="Grid type (POINT, RANGE)")
    affected_rooms: Optional[str] = Field(default=None, description="JSON array of affected rooms")
    affected_rooms_count: Optional[int] = Field(default=None, description="Count of affected rooms")
    location_source: Optional[str] = Field(default=None, description="Source of location")
    grid_completeness: Optional[str] = Field(default=None, description="Grid completeness (FULL, PARTIAL)")
    match_quality: Optional[str] = Field(default=None, description="Match quality (PRECISE, FUZZY)")
    location_review_flag: Optional[bool] = Field(default=None, description="Whether location needs review")
    csi_inference_source: Optional[str] = Field(default=None, description="Source of CSI inference")
    building_normalized: Optional[str] = Field(default=None, description="Normalized building code")
    level_normalized: Optional[str] = Field(default=None, description="Normalized level code")
    room_code_extracted: Optional[float] = Field(default=None, description="Extracted room code")
    trade_inferred: Optional[str] = Field(default=None, description="Inferred trade")
    start_time_raw: Optional[str] = Field(default=None, description="Raw start time from source")
    end_time_raw: Optional[str] = Field(default=None, description="Raw end time from source")


class ProjectSightLaborEntriesDataQuality(BaseModel):
    """
    ProjectSight labor entries data quality tracking.

    File: projectsight/labor_entries_data_quality.csv
    1:1 with projectsight/labor_entries.csv
    """
    labor_entry_id: str = Field(description="FK to labor_entries")
    csi_inference_source: Optional[str] = Field(default=None, description="Source of CSI inference")
    company_primary_trade_id: Optional[float] = Field(default=None, description="Company primary trade ID")


class ProjectSightNcrDataQuality(BaseModel):
    """
    ProjectSight NCR data quality tracking.

    File: projectsight/ncr_data_quality.csv
    1:1 with projectsight/ncr_consolidated.csv
    """
    ncr_id: str = Field(description="FK to ncr_consolidated")
    csi_inference_source: Optional[str] = Field(default=None, description="Source of CSI inference")
    validation_issues: Optional[float] = Field(default=None, alias="_validation_issues", description="Validation issues")
    data_quality_flags: Optional[float] = Field(default=None, description="Data quality flags")
