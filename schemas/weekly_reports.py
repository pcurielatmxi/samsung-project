"""
Weekly report data schemas.

These schemas define the structure for parsed weekly report output files.

Output Location: {WINDOWS_DATA_DIR}/processed/weekly_reports/
"""

from typing import Optional
from pydantic import BaseModel, Field


class WeeklyReports(BaseModel):
    """
    Weekly report file metadata schema.

    File: weekly_reports.csv
    Records: 37 PDF reports (Aug 2022 - Jun 2023)
    Purpose: File-level metadata and summary counts.
    """

    file_id: int = Field(description="Primary key (auto-generated)")
    filename: str = Field(description="Source PDF filename")
    report_date: Optional[str] = Field(default=None, description="Report date (YYYY-MM-DD)")
    author_name: Optional[str] = Field(default=None, description="Report author name")
    author_role: Optional[str] = Field(default=None, description="Author role/title")
    page_count: int = Field(description="Number of pages in PDF")
    narrative_length: int = Field(description="Character count of narrative text")
    work_items_count: int = Field(description="Number of work progressing items")
    issues_count: int = Field(description="Number of key issues")
    procurement_count: int = Field(description="Number of procurement items")


class KeyIssues(BaseModel):
    """
    Key issues from weekly reports schema.

    File: key_issues.csv
    Records: ~1,108 documented issues
    Purpose: Track reported issues and concerns.
    """

    file_id: int = Field(description="FK to weekly_reports")
    report_date: Optional[str] = Field(default=None, description="Report date (YYYY-MM-DD)")
    item_number: int = Field(description="Issue number within report")
    content: str = Field(description="Issue description text")


class WorkProgressing(BaseModel):
    """
    Work progressing items from weekly reports schema.

    File: work_progressing.csv
    Purpose: Track reported work progress items.
    """

    file_id: int = Field(description="FK to weekly_reports")
    report_date: Optional[str] = Field(default=None, description="Report date (YYYY-MM-DD)")
    item_number: int = Field(description="Item number within report")
    content: str = Field(description="Work progress description")


class Procurement(BaseModel):
    """
    Procurement items from weekly reports schema.

    File: procurement.csv
    Purpose: Track procurement status items.
    """

    file_id: int = Field(description="FK to weekly_reports")
    report_date: Optional[str] = Field(default=None, description="Report date (YYYY-MM-DD)")
    item_number: int = Field(description="Item number within report")
    content: str = Field(description="Procurement item description")


class LaborDetail(BaseModel):
    """
    Labor detail from weekly reports schema.

    File: labor_detail.csv
    Purpose: Individual labor hours by worker/classification.
    """

    file_id: int = Field(description="FK to weekly_reports")
    source_section: Optional[str] = Field(default=None, description="Section in report where data was found")
    report_date: Optional[str] = Field(default=None, description="Report date (YYYY-MM-DD)")
    company: Optional[str] = Field(default=None, description="Company name")
    status: Optional[str] = Field(default=None, description="Employment status")
    name: Optional[str] = Field(default=None, description="Worker name")
    classification: Optional[str] = Field(default=None, description="Job classification")
    trade: Optional[str] = Field(default=None, description="Trade/work type")
    hours: float = Field(description="Hours worked")


class LaborDetailByCompany(BaseModel):
    """
    Labor detail aggregated by company schema.

    File: labor_detail_by_company.csv
    Purpose: Summary labor hours by company.
    """

    company: str = Field(description="Company name")
    hours: float = Field(description="Total hours")
    unique_workers: int = Field(description="Unique worker count")


# Addendum schemas
class AddendumFiles(BaseModel):
    """
    Addendum file metadata schema.

    File: addendum_files.csv
    Purpose: Track addendum files and their content counts.
    """

    file_id: int = Field(description="Primary key")
    filename: str = Field(description="Source filename")
    report_date: Optional[str] = Field(default=None, description="Report date (YYYY-MM-DD)")
    rfi_count: int = Field(description="Number of RFIs")
    submittal_count: int = Field(description="Number of submittals")
    manpower_count: int = Field(description="Number of manpower entries")


class AddendumManpower(BaseModel):
    """
    Addendum manpower data schema.

    File: addendum_manpower.csv
    Purpose: Labor deployment data from addendum reports.
    """

    file_id: int = Field(description="FK to addendum_files")
    source_section: Optional[str] = Field(default=None, description="Section in addendum")
    report_date: Optional[str] = Field(default=None, description="Report date (YYYY-MM-DD)")
    company: Optional[str] = Field(default=None, description="Company name")
    total_workers: int = Field(description="Total worker count")
    total_hours: float = Field(description="Total hours")


class AddendumRfiLog(BaseModel):
    """
    Addendum RFI log schema.

    File: addendum_rfi_log.csv
    Purpose: RFI (Request for Information) tracking.
    """

    file_id: int = Field(description="FK to addendum_files")
    source_section: Optional[str] = Field(default=None, description="Section in addendum")
    rfi_number: int = Field(description="RFI number")
    subject: Optional[str] = Field(default=None, description="RFI subject")
    created_date: Optional[str] = Field(default=None, description="RFI creation date")
    due_date: Optional[str] = Field(default=None, description="RFI due date")


class AddendumSubmittalLog(BaseModel):
    """
    Addendum submittal log schema.

    File: addendum_submittal_log.csv
    Purpose: Submittal tracking.
    """

    file_id: int = Field(description="FK to addendum_files")
    source_section: Optional[str] = Field(default=None, description="Section in addendum")
    submittal_number: int = Field(description="Submittal number")
    content: Optional[str] = Field(default=None, description="Submittal content/description")
    created_date: Optional[str] = Field(default=None, description="Submittal creation date")
    due_date: Optional[str] = Field(default=None, description="Submittal due date")
