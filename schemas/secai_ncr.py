"""
SECAI NCR/QOR log table schemas.

Output Location: {WINDOWS_DATA_DIR}/processed/secai_ncr_log/
"""

from typing import Optional
from pydantic import BaseModel, Field


class SecaiNcrConsolidated(BaseModel):
    """
    SECAI NCR/QOR consolidated records.

    File: secai_ncr_consolidated.csv
    """
    secai_ncr_id: str = Field(description="Unique SECAI NCR identifier")
    record_type: Optional[str] = Field(default=None, description="Record type (NCR, QOR)")
    source_type: Optional[str] = Field(default=None, description="Source type (INTERNAL, EXTERNAL)")
    ncr_number: Optional[str] = Field(default=None, description="NCR number")
    seq_number: Optional[float] = Field(default=None, description="Sequence number")
    description: Optional[str] = Field(default=None, description="NCR description")
    building: Optional[str] = Field(default=None, description="Building code")
    location: Optional[str] = Field(default=None, description="Location string")
    contractor: Optional[str] = Field(default=None, description="Contractor name")
    discipline: Optional[str] = Field(default=None, description="Discipline code")
    work_type: Optional[str] = Field(default=None, description="Work type")
    issue_date: Optional[str] = Field(default=None, description="Issue date")
    receipt_date: Optional[str] = Field(default=None, description="Receipt date")
    requested_close_date: Optional[str] = Field(default=None, description="Requested close date")
    issued_by: Optional[float] = Field(default=None, description="Issued by")
    issuing_org: Optional[float] = Field(default=None, description="Issuing organization")
    action_description: Optional[str] = Field(default=None, description="Action description")
    actual_close_date: Optional[str] = Field(default=None, description="Actual close date")
    status: Optional[float] = Field(default=None, description="Status")
    dim_company_id: Optional[float] = Field(default=None, description="FK to dim_company")
    dim_location_id: Optional[int] = Field(default=None, description="FK to dim_location")
    dim_csi_section_id: Optional[float] = Field(default=None, description="FK to dim_csi_section")
    csi_section: Optional[str] = Field(default=None, description="CSI section code")
    csi_title: Optional[str] = Field(default=None, description="CSI section title")


class SecaiNcrDataQuality(BaseModel):
    """
    SECAI NCR data quality tracking.

    File: secai_ncr_data_quality.csv
    """
    secai_ncr_id: str = Field(description="Unique SECAI NCR identifier")
    csi_inference_source: Optional[str] = Field(default=None, description="Source of CSI inference")
    source_sheet: Optional[str] = Field(default=None, description="Source Excel sheet name")
    row_number: Optional[float] = Field(default=None, description="Row number in source")
    validation_issues: Optional[float] = Field(default=None, alias="_validation_issues", description="Validation issues")
    data_quality_flags: Optional[float] = Field(default=None, description="Data quality flags")
