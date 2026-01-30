"""
Primavera P6 schedule table schemas.

Output Location: {WINDOWS_DATA_DIR}/processed/primavera/
"""

from typing import Optional
from pydantic import BaseModel, Field


class P6TaskTaxonomy(BaseModel):
    """
    P6 task taxonomy with WBS classification and dimension IDs.

    File: p6_task_taxonomy.csv
    """
    task_id: str = Field(description="Unique task identifier (snapshot_id + task_id)")
    sub_trade: Optional[str] = Field(default=None, description="Sub-trade code")
    sub_trade_desc: Optional[str] = Field(default=None, description="Sub-trade description")
    scope: Optional[str] = Field(default=None, description="Scope code")
    scope_desc: Optional[str] = Field(default=None, description="Scope description")
    building: Optional[str] = Field(default=None, description="Building code (FAB, SUE, SUW, FIZ)")
    building_desc: Optional[str] = Field(default=None, description="Building description")
    level: Optional[str] = Field(default=None, description="Level code (1F, 2F, B1, etc.)")
    level_desc: Optional[str] = Field(default=None, description="Level description")
    area: Optional[str] = Field(default=None, description="Area code")
    room: Optional[str] = Field(default=None, description="Room code")
    sub_contractor: Optional[str] = Field(default=None, description="Subcontractor")
    phase: Optional[str] = Field(default=None, description="Phase code")
    phase_desc: Optional[str] = Field(default=None, description="Phase description")
    work_phase: Optional[str] = Field(default=None, description="Work phase")
    location_type: Optional[str] = Field(default=None, description="Location type (ROOM, BUILDING, LEVEL, etc.)")
    location_code: Optional[str] = Field(default=None, description="Location code")
    Building_Code_Desc: Optional[str] = Field(default=None, alias="Building Code Desc", description="Building code with description")
    location: Optional[str] = Field(default=None, description="Location description")
    label: Optional[str] = Field(default=None, description="Classification label")
    impact_code: Optional[str] = Field(default=None, description="Impact code")
    impact_type: Optional[str] = Field(default=None, description="Impact type")
    impact_type_desc: Optional[str] = Field(default=None, description="Impact type description")
    attributed_to: Optional[str] = Field(default=None, description="Attributed to")
    attributed_to_desc: Optional[str] = Field(default=None, description="Attributed to description")
    root_cause: Optional[str] = Field(default=None, description="Root cause")
    root_cause_desc: Optional[str] = Field(default=None, description="Root cause description")
    dim_csi_section_id: Optional[int] = Field(default=None, description="FK to dim_csi_section")
    csi_section: Optional[str] = Field(default=None, description="CSI section code")
    csi_title: Optional[str] = Field(default=None, description="CSI section title")
    dim_location_id: Optional[int] = Field(default=None, description="FK to dim_location")
    csi_inference_source: Optional[str] = Field(default=None, description="Source of CSI inference")


class P6TaskTaxonomyDataQuality(BaseModel):
    """
    P6 task taxonomy data quality tracking.

    File: p6_task_taxonomy_data_quality.csv
    """
    task_id: str = Field(description="Unique task identifier")
    sub_trade_source: Optional[str] = Field(default=None, description="Source of sub_trade")
    building_source: Optional[str] = Field(default=None, description="Source of building")
    level_source: Optional[str] = Field(default=None, description="Source of level")
    area_source: Optional[str] = Field(default=None, description="Source of area")
    room_source: Optional[str] = Field(default=None, description="Source of room")
    sub_source: Optional[str] = Field(default=None, description="Source of subcontractor")
    phase_source: Optional[str] = Field(default=None, description="Source of phase")
    work_phase_source: Optional[str] = Field(default=None, description="Source of work_phase")
    impact_source: Optional[str] = Field(default=None, description="Source of impact")
    csi_inference_source: Optional[str] = Field(default=None, description="Source of CSI inference")
    grid_source: Optional[str] = Field(default=None, description="Source of grid")
    grid_row_min: Optional[str] = Field(default=None, description="Grid row minimum")
    grid_row_max: Optional[str] = Field(default=None, description="Grid row maximum")
    grid_col_min: Optional[float] = Field(default=None, description="Grid column minimum")
    grid_col_max: Optional[float] = Field(default=None, description="Grid column maximum")
    loc_type: Optional[str] = Field(default=None, description="Location type code")
    loc_type_desc: Optional[str] = Field(default=None, description="Location type description")
    loc_id: Optional[str] = Field(default=None, description="Location ID")
