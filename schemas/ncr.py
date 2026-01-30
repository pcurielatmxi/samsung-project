"""
NCR (Non-Conformance Report) data schemas.

These schemas define the structure for ProjectSight NCR output files.

Output Location: {WINDOWS_DATA_DIR}/processed/projectsight/
"""

from typing import Optional
from pydantic import BaseModel, Field


class NcrConsolidated(BaseModel):
    """
    NCR consolidated data schema.

    File: ncr_consolidated.csv
    Records: ~2000 non-conformance reports
    Source: ProjectSight exports
    Purpose: Track quality issues and non-conformances.

    Note: csi_inference_source, _validation_issues, data_quality_flags
    are in the data quality table (ncr_data_quality.csv).
    """

    model_config = {'populate_by_name': True}

    # Primary key
    ncr_id: str = Field(description="Primary key (NCR-{number})")

    number: int = Field(description="NCR number/ID")
    type: Optional[str] = Field(default=None, description="NCR type (NCR, QOR, SOR, SWN, VR)")
    status: Optional[str] = Field(default=None, description="Current status")
    company: Optional[str] = Field(default=None, description="Company responsible")
    discipline: Optional[str] = Field(default=None, description="Discipline/trade category")
    description: Optional[str] = Field(default=None, description="Issue description")
    created_on: Optional[str] = Field(default=None, description="Date reported (YYYY-MM-DD)")
    cause_of_issue: Optional[str] = Field(default=None, description="Root cause of issue")
    date_resolved: Optional[str] = Field(default=None, description="Resolution date")
    resolution: Optional[str] = Field(default=None, description="Resolution description")

    # Dimension keys
    dim_company_id: Optional[float] = Field(default=None, description="FK to dim_company")

    # CSI Section (csi_inference_source is in data quality table)
    dim_csi_section_id: Optional[float] = Field(default=None, description="FK to dim_csi_section")
    csi_section: Optional[str] = Field(default=None, description="CSI code (e.g., '03 30 00')")
    csi_title: Optional[str] = Field(default=None, description="CSI section title")
