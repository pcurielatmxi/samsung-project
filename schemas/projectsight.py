"""
ProjectSight labor hours schemas.

These schemas define the structure for ProjectSight output files.

Output Location: {WINDOWS_DATA_DIR}/processed/projectsight/
"""

from typing import Optional
from pydantic import BaseModel, Field


class LaborEntriesBase(BaseModel):
    """
    ProjectSight labor entries (base) schema from parse stage.

    File: labor_entries.csv (before enrichment)
    Records: ~857K individual labor entries
    Purpose: Labor hours by worker/company from daily reports.
    """

    # Date fields
    report_date: str = Field(description="Report date (MM/DD/YYYY)")
    year: Optional[int] = Field(default=None, description="Year from report_date")
    month: Optional[int] = Field(default=None, description="Month from report_date (1-12)")
    week_number: Optional[int] = Field(default=None, description="ISO week number")
    day_of_week: Optional[str] = Field(default=None, description="Day name (Monday-Sunday)")

    # Action
    action: Optional[str] = Field(default=None, description="Action type (Added, Modified, Deleted)")

    # Worker info
    person_name: Optional[str] = Field(default=None, description="Worker's name")
    company: Optional[str] = Field(default=None, description="Company name")
    activity: Optional[str] = Field(default=None, description="Activity description")

    # Trade info
    trade_code: Optional[str] = Field(default=None, description="Trade code (e.g., '05')")
    trade_name: Optional[str] = Field(default=None, description="Trade name (e.g., 'Metals')")
    trade_full: Optional[str] = Field(default=None, description="Full trade string (e.g., '05 - Metals')")
    classification: Optional[str] = Field(default=None, description="Worker classification (Journeyman, etc.)")

    # Hours
    hours_old: Optional[float] = Field(default=None, description="Previous hours value")
    hours_new: Optional[float] = Field(default=None, description="New hours value")
    hours_delta: Optional[float] = Field(default=None, description="Change in hours (hours_new - hours_old)")
    is_overtime: Optional[bool] = Field(default=None, description="True if hours_new > 8")

    # Time tracking
    start_time: Optional[str] = Field(default=None, description="Start time")
    end_time: Optional[str] = Field(default=None, description="End time")
    break_hours: Optional[float] = Field(default=None, description="Break hours")

    # Audit trail
    modifier_name: Optional[str] = Field(default=None, description="Name of person who made the change")
    modifier_email: Optional[str] = Field(default=None, description="Email of modifier")
    modifier_company: Optional[str] = Field(default=None, description="Company of modifier")
    modify_timestamp: Optional[str] = Field(default=None, description="Timestamp of modification")


class LaborEntriesEnriched(LaborEntriesBase):
    """
    ProjectSight labor entries (enriched) schema with dimension IDs.

    File: labor_entries.csv (after consolidation)
    Purpose: Labor data enriched with company and CSI dimension lookups.

    Note: ProjectSight has NO location data - only company and trade information.
    Note: company_primary_trade_id and csi_inference_source are in data quality table.
    """

    # Primary key
    labor_entry_id: str = Field(description="Primary key (PS-{row_number})")

    # Dimension IDs
    dim_location_id: Optional[float] = Field(
        default=None,
        description="FK to dim_location (always None - no location in source)"
    )
    dim_company_id: Optional[int] = Field(default=None, description="FK to dim_company")

    # CSI Section (csi_inference_source is in data quality table)
    dim_csi_section_id: Optional[float] = Field(default=None, description="FK to dim_csi_section")
    csi_section: Optional[str] = Field(default=None, description="CSI code (e.g., '03 30 00')")
    csi_title: Optional[str] = Field(default=None, description="CSI section title")


# Alias for the enriched version (used by registry)
ProjectSightLaborEntries = LaborEntriesEnriched
