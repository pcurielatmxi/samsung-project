"""
Fieldwire table schemas.

Output Location: {WINDOWS_DATA_DIR}/processed/fieldwire/
"""

from typing import Optional
from pydantic import BaseModel, Field


class FieldwireChecklists(BaseModel):
    """
    Fieldwire checklist responses.

    File: fieldwire_checklists.csv
    """
    ID: str = Field(description="Task ID (e.g., TBM-6887)")
    Seq: int = Field(description="Checklist item sequence number")
    Response: Optional[str] = Field(default=None, description="Response value (Yes/No/NA)")
    Checklist_Item: Optional[str] = Field(default=None, alias="Checklist Item", description="Checklist item text")
    Username: Optional[str] = Field(default=None, description="User who responded")
    Date: Optional[str] = Field(default=None, description="Response date (YYYY-MM-DD)")
    Source: Optional[str] = Field(default=None, description="Data source identifier")


class FieldwireCombined(BaseModel):
    """
    Fieldwire combined task data (TBM, QC, Progress tracking).

    File: fieldwire_combined.csv
    """
    ID: str = Field(description="Task ID (e.g., TBM-6887)")
    Title: Optional[str] = Field(default=None, description="Task title")
    Status: Optional[str] = Field(default=None, description="Task status")
    Category: Optional[str] = Field(default=None, description="Task category")
    Assignee: Optional[str] = Field(default=None, description="Assigned user email")
    Start_date: Optional[str] = Field(default=None, alias="Start date", description="Start date")
    End_date: Optional[str] = Field(default=None, alias="End date", description="End date")
    Plan: Optional[str] = Field(default=None, description="Floor plan name")
    X_pos_pct: Optional[float] = Field(default=None, alias="X pos (%)", description="X position percentage")
    Y_pos_pct: Optional[float] = Field(default=None, alias="Y pos (%)", description="Y position percentage")
    Tier_1: Optional[str] = Field(default=None, alias="Tier 1", description="Tier 1 classification")
    Tier_2: Optional[float] = Field(default=None, alias="Tier 2", description="Tier 2 classification")
    Tier_3: Optional[float] = Field(default=None, alias="Tier 3", description="Tier 3 classification")
    Tier_4: Optional[float] = Field(default=None, alias="Tier 4", description="Tier 4 classification")
    Tier_5: Optional[float] = Field(default=None, alias="Tier 5", description="Tier 5 classification")
    Activity_Name: Optional[str] = Field(default=None, alias="Activity Name", description="Activity name")
    Activity_ID: Optional[float] = Field(default=None, alias="Activity ID", description="P6 Activity ID")
    WBS_Code: Optional[float] = Field(default=None, alias="WBS Code", description="WBS code")
    Phase: Optional[float] = Field(default=None, description="Construction phase")
    Scope_Category: Optional[float] = Field(default=None, alias="Scope Category", description="Scope category")
    Building: Optional[str] = Field(default=None, description="Building code")
    Level: Optional[int] = Field(default=None, description="Floor level number")
    Company: Optional[str] = Field(default=None, description="Company name")
    Location_ID: Optional[str] = Field(default=None, alias="Location ID", description="Location identifier")
    WIR_Number: Optional[float] = Field(default=None, alias="WIR Number", description="Work Inspection Request number")
    Inspector: Optional[float] = Field(default=None, description="Inspector ID")
    Inspection_Status: Optional[float] = Field(default=None, alias="Inspection Status", description="Inspection status")
    TBM_Manpower: Optional[float] = Field(default=None, alias="TBM Manpower", description="TBM reported manpower")
    Direct_Manpower: Optional[float] = Field(default=None, alias="Direct Manpower", description="Direct manpower count")
    Indirect_Manpower: Optional[float] = Field(default=None, alias="Indirect Manpower", description="Indirect manpower count")
    Total_Idle_Hours: Optional[float] = Field(default=None, alias="Total Idle Hours", description="Total idle hours")
    Unit: Optional[float] = Field(default=None, description="Unit of measure")
    Quantity: Optional[float] = Field(default=None, description="Quantity")
    Plan_folder: Optional[float] = Field(default=None, alias="Plan folder", description="Plan folder")
    Plan_Link: Optional[float] = Field(default=None, alias="Plan Link", description="Plan link")
    Created: Optional[str] = Field(default=None, description="Created timestamp")
    Completed: Optional[float] = Field(default=None, description="Completed timestamp")
    Verified: Optional[float] = Field(default=None, description="Verified timestamp")
    Deleted: Optional[float] = Field(default=None, description="Deleted timestamp")
    Last_Updated: Optional[str] = Field(default=None, alias="Last Updated", description="Last updated timestamp")
    Tag_1: Optional[str] = Field(default=None, alias="Tag 1", description="Tag 1")
    Tag_2: Optional[str] = Field(default=None, alias="Tag 2", description="Tag 2")
    Tag_3: Optional[float] = Field(default=None, alias="Tag 3", description="Tag 3")
    Tag_4: Optional[float] = Field(default=None, alias="Tag 4", description="Tag 4")
    Tag_5: Optional[float] = Field(default=None, alias="Tag 5", description="Tag 5")
    Source: Optional[str] = Field(default=None, description="Data source identifier")
    DataSource: Optional[str] = Field(default=None, description="Data source name")
    Plan_URL: Optional[float] = Field(default=None, alias="Plan URL", description="Plan URL")
    Total_Manpower: Optional[float] = Field(default=None, alias="Total Manpower", description="Total manpower")
    Location_Type: Optional[float] = Field(default=None, alias="Location Type", description="Location type")
    Scaffold_Tag: Optional[float] = Field(default=None, alias="Scaffold Tag #", description="Scaffold tag number")
    Obstruction_Cause: Optional[float] = Field(default=None, alias="Obstruction - Cause", description="Obstruction cause")


class FieldwireComments(BaseModel):
    """
    Fieldwire task comments and activity log.

    File: fieldwire_comments.csv
    """
    ID: str = Field(description="Task ID")
    Seq: int = Field(description="Comment sequence number")
    Value: Optional[str] = Field(default=None, description="Comment text")
    User: Optional[str] = Field(default=None, description="User name")
    Action: Optional[str] = Field(default=None, description="Action type")
    Update: Optional[str] = Field(default=None, description="Update description")
    IsPhoto: Optional[bool] = Field(default=None, description="Whether comment has photo")
    PhotoURL: Optional[str] = Field(default=None, description="Photo URL if applicable")
    Deleted: Optional[bool] = Field(default=None, description="Whether comment is deleted")
    Source: Optional[str] = Field(default=None, description="Data source identifier")


class FieldwireRelatedTasks(BaseModel):
    """
    Fieldwire related task links.

    File: fieldwire_related_tasks.csv
    """
    ID: str = Field(description="Task ID")
    Seq: int = Field(description="Related task sequence number")
    Related_Task_ID: Optional[str] = Field(default=None, alias="Related Task ID", description="Related task identifier")
    Source: Optional[str] = Field(default=None, description="Data source identifier")
