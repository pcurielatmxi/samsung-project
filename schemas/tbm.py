"""
TBM (Toolbox Meeting) daily work plan schemas.

These schemas define the structure for TBM work entry output files.

Output Location: {WINDOWS_DATA_DIR}/processed/tbm/
"""

from typing import Optional
from pydantic import BaseModel, Field


class TbmFiles(BaseModel):
    """
    TBM file metadata schema.

    File: tbm_files.csv
    Records: File-level metadata for TBM Excel workbooks.
    Purpose: Track source files and report dates.
    """

    file_id: int = Field(description="Primary key (auto-generated)")
    filename: str = Field(description="Source Excel filename")
    report_date: Optional[str] = Field(default=None, description="Report date (YYYY-MM-DD)")
    subcontractor_file: Optional[str] = Field(default=None, description="Subcontractor name from filename")


class TbmWorkEntries(BaseModel):
    """
    TBM work entries (base) schema.

    File: work_entries.csv
    Records: ~13,539 individual work activities
    Purpose: Daily work plan data by crew/company.
    """

    file_id: int = Field(description="FK to tbm_files")
    report_date: Optional[str] = Field(default=None, description="Report date (YYYY-MM-DD)")
    subcontractor_file: Optional[str] = Field(default=None, description="Subcontractor name from filename")
    row_num: int = Field(description="Row number within file")
    division: Optional[str] = Field(default=None, description="Division code (string)")
    tier1_gc: Optional[str] = Field(default=None, description="Tier 1 GC (usually Yates)")
    tier2_sc: Optional[str] = Field(default=None, description="Tier 2 subcontractor")
    foreman: Optional[str] = Field(default=None, description="Foreman/crew leader name")
    contact_number: Optional[str] = Field(default=None, description="Contact phone number")
    num_employees: Optional[float] = Field(default=None, description="Number of employees/workers")
    work_activities: Optional[str] = Field(default=None, description="Work activity description")
    location_building: Optional[str] = Field(default=None, description="Building (raw)")
    location_level: Optional[str] = Field(default=None, description="Level (raw)")
    location_row: Optional[str] = Field(default=None, description="Grid row/area (raw)")
    start_time: Optional[str] = Field(default=None, description="Start time")
    end_time: Optional[str] = Field(default=None, description="End time")


class TbmWorkEntriesEnriched(BaseModel):
    """
    TBM work entries (enriched) schema with dimension IDs.

    File: work_entries_enriched.csv
    Purpose: TBM data enriched with dimension lookups and grid bounds.
    """

    # Base columns from work_entries
    file_id: int = Field(description="FK to tbm_files")
    report_date: Optional[str] = Field(default=None, description="Report date (YYYY-MM-DD)")
    subcontractor_file: Optional[str] = Field(default=None, description="Subcontractor name from filename")
    row_num: int = Field(description="Row number within file")
    division: Optional[str] = Field(default=None, description="Division code (string)")
    tier1_gc: Optional[str] = Field(default=None, description="Tier 1 GC (usually Yates)")
    tier2_sc: Optional[str] = Field(default=None, description="Tier 2 subcontractor")
    foreman: Optional[str] = Field(default=None, description="Foreman/crew leader name")
    contact_number: Optional[str] = Field(default=None, description="Contact phone number")
    num_employees: Optional[float] = Field(default=None, description="Number of employees/workers")
    work_activities: Optional[str] = Field(default=None, description="Work activity description")
    location_building: Optional[str] = Field(default=None, description="Building (raw)")
    location_level: Optional[str] = Field(default=None, description="Level (raw)")
    location_row: Optional[str] = Field(default=None, description="Grid row/area (raw)")
    start_time: Optional[str] = Field(default=None, description="Start time")
    end_time: Optional[str] = Field(default=None, description="End time")

    # Enrichment columns
    building_normalized: Optional[str] = Field(default=None, description="Normalized building code")
    level_normalized: Optional[str] = Field(default=None, description="Normalized level (1F, 2F, etc.)")
    dim_location_id: Optional[float] = Field(default=None, description="FK to dim_location")
    dim_company_id: int = Field(description="FK to dim_company")
    trade_inferred: Optional[str] = Field(default=None, description="Inferred trade from work description")
    dim_trade_id: Optional[float] = Field(default=None, description="FK to dim_trade")
    dim_trade_code: Optional[str] = Field(default=None, description="Trade code from dim_trade")
    trade_source: Optional[str] = Field(default=None, description="How trade was determined")

    # CSI Section
    dim_csi_section_id: Optional[float] = Field(default=None, description="FK to dim_csi_section")
    csi_section: Optional[str] = Field(default=None, description="CSI code (e.g., '03 30 00')")
    csi_inference_source: Optional[str] = Field(default=None, description="How CSI was inferred")
    csi_title: Optional[str] = Field(default=None, description="CSI section title")

    # Grid bounds
    grid_row_min: Optional[str] = Field(default=None, description="Grid row minimum")
    grid_row_max: Optional[str] = Field(default=None, description="Grid row maximum")
    grid_col_min: Optional[float] = Field(default=None, description="Grid column minimum")
    grid_col_max: Optional[float] = Field(default=None, description="Grid column maximum")
    grid_raw: Optional[str] = Field(default=None, description="Raw grid string")
    grid_type: Optional[str] = Field(default=None, description="Grid type")

    # Room matching
    affected_rooms: Optional[str] = Field(default=None, description="JSON array of affected rooms")
    affected_rooms_count: Optional[int] = Field(default=None, description="Count of rooms (1=single match, >1=multiple)")

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


# Alias for the enriched version with CSI
TbmWithCSI = TbmWorkEntriesEnriched
