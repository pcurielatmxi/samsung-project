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
    Records: ~800+ non-conformance reports
    Source: ProjectSight exports
    Purpose: Track quality issues and non-conformances.
    """

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
    data_quality_flags: Optional[str] = Field(default=None, description="Data quality issues")

    # Dimension keys
    dim_company_id: Optional[float] = Field(default=None, description="FK to dim_company")
    dim_trade_id: Optional[float] = Field(default=None, description="FK to dim_trade")
    dim_trade_code: Optional[str] = Field(default=None, description="Trade code from dim_trade")

    # CSI Section
    dim_csi_section_id: Optional[float] = Field(default=None, description="FK to dim_csi_section")
    csi_section: Optional[str] = Field(default=None, description="CSI code (e.g., '03 30 00')")
    csi_inference_source: Optional[str] = Field(default=None, description="How CSI was inferred")
    csi_title: Optional[str] = Field(default=None, description="CSI section title")
