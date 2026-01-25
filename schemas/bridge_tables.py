"""
Schemas for bridge/junction tables used in Power BI.

Bridge tables enable many-to-many relationships between fact tables
and dimension tables.
"""

from typing import Optional
from pydantic import BaseModel, Field


class AffectedRoomsBridge(BaseModel):
    """
    Bridge table linking quality inspections and TBM work entries to rooms.

    Each row represents one room affected by an event (inspection or work entry).
    Events affecting multiple rooms have multiple rows with the same source_id.
    """

    source: str = Field(description="Data source: RABA, PSI, or TBM")
    source_id: str = Field(description="Primary key in source table")
    event_date: Optional[str] = Field(description="Date of the event (YYYY-MM-DD)")
    location_id: Optional[int] = Field(description="FK to dim_location")
    location_code: Optional[str] = Field(description="Room code (e.g., FAB116101)")
    building: Optional[str] = Field(description="Building code (FAB, SUE, SUW, FIZ)")
    room_name: Optional[str] = Field(description="Human-readable room name")
    match_type: Optional[str] = Field(description="Grid match type: FULL or PARTIAL")
    source_room_count: Optional[int] = Field(
        description="Total rooms affected by this event (for context)"
    )
