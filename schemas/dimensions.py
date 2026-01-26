"""
Dimension table schemas.

These schemas define the structure for integration dimension tables used
to link data across all sources.

Output Location: {WINDOWS_DATA_DIR}/processed/integrated_analysis/dimensions/
"""

from typing import Optional
from pydantic import BaseModel, Field


class DimLocation(BaseModel):
    """
    Location dimension table schema.

    File: dim_location.csv
    Records: ~505 location codes (rooms, elevators, stairs, gridlines, etc.)
    Purpose: Master location reference with grid bounds for spatial joins.

    Used by: RABA, PSI, TBM, P6 for location normalization and room matching.

    Note: Nullable string columns may appear as float64 in pandas when all values
    are NaN. The schema uses Optional[float] for columns that are often empty.

    Grid bounds source:
    - Direct: From Samsung_FAB_Codes_by_Gridline_3.xlsx mapping file
    - Inferred: From sibling rooms on other floors (same room_num in FAB1[FLOOR][ROOM_NUM])
    """

    location_id: int = Field(description="Primary key (auto-generated)")
    location_code: str = Field(description="Location code (FAB112345, FAB1-ST05, FAB1-EL02)")
    p6_alias: Optional[str] = Field(default=None, description="Original P6 code if converted (e.g., STR-05 when location_code is FAB1-ST05)")
    location_type: str = Field(description="Type: ROOM, ELEVATOR, STAIR, GRIDLINE, LEVEL, BUILDING, AREA, SITE, UNDEFINED")
    room_name: Optional[float] = Field(default=None, description="Human-readable room name (often empty)")
    building: Optional[str] = Field(default=None, description="Building code: FAB, SUE, SUW, FIZ, SITE")
    level: Optional[str] = Field(default=None, description="Level: 1F, 2F, B1, ROOF, etc.")
    grid_row_min: Optional[str] = Field(default=None, description="Grid row minimum (A-Z)")
    grid_row_max: Optional[str] = Field(default=None, description="Grid row maximum (A-Z)")
    grid_col_min: Optional[float] = Field(default=None, description="Grid column minimum (1-34)")
    grid_col_max: Optional[float] = Field(default=None, description="Grid column maximum (1-34)")
    grid_inferred_from: Optional[str] = Field(default=None, description="Source location_code if grid was inferred from sibling room")
    status: Optional[str] = Field(default=None, description="Location status")
    task_count: Optional[int] = Field(default=None, description="Number of P6 tasks at this location")
    building_level: Optional[str] = Field(default=None, description="Combined building-level (FAB-1F)")
    in_drawings: Optional[bool] = Field(default=None, description="Whether location was found in PDF floor drawings")


class DimCompany(BaseModel):
    """
    Company dimension table schema.

    File: dim_company.csv
    Records: ~80+ companies (owner, GC, T1/T2 subcontractors)
    Purpose: Master company reference with trade/CSI defaults and hierarchy.

    Used by: All sources for company name normalization via alias lookup.
    """

    company_id: int = Field(description="Primary key (auto-generated)")
    canonical_name: str = Field(description="Canonical company name")
    short_code: Optional[str] = Field(default=None, description="Short code (3-5 chars)")
    tier: Optional[str] = Field(default=None, description="Tier: OWNER, GC, T1, T2")
    primary_trade_id: Optional[float] = Field(default=None, description="FK to dim_trade")
    other_trade_ids: Optional[str] = Field(default=None, description="Pipe-delimited additional trade IDs")
    default_csi_section_id: Optional[float] = Field(default=None, description="FK to dim_csi_section")
    notes: Optional[str] = Field(default=None, description="Notes about company")
    parent_company_id: Optional[float] = Field(default=None, description="FK to parent company")
    parent_confidence: Optional[str] = Field(default=None, description="Confidence in parent relationship (string)")
    company_type: Optional[str] = Field(default=None, description="Company type classification")
    is_yates_sub: Optional[bool] = Field(default=None, description="Whether company is a Yates subcontractor")
    full_name: Optional[str] = Field(default=None, description="Full legal company name")


class DimTrade(BaseModel):
    """
    Trade dimension table schema.

    File: dim_trade.csv
    Records: 13 trade categories
    Purpose: Work type classification for labor and quality analysis.

    Trades: CONCRETE, STEEL, ROOFING, DRYWALL, FINISHES, FIREPROOF,
            MEP, INSULATION, EARTHWORK, PRECAST, PANELS, GENERAL, MASONRY
    """

    trade_id: int = Field(description="Primary key")
    trade_code: str = Field(description="Trade code (CONCRETE, STEEL, etc.)")
    trade_name: str = Field(description="Human-readable trade name")
    phase: Optional[str] = Field(default=None, description="Construction phase")
    csi_division: Optional[str] = Field(default=None, description="Primary CSI division (may contain ranges)")
    description: Optional[str] = Field(default=None, description="Trade description")


class DimCSISection(BaseModel):
    """
    CSI Section dimension table schema.

    File: dim_csi_section.csv
    Records: 52 CSI sections
    Purpose: MasterFormat construction classification for work categorization.

    Based on CSI MasterFormat 2020.
    """

    csi_section_id: int = Field(description="Primary key")
    csi_section: str = Field(description="CSI section code (e.g., '03 30 00')")
    csi_title: str = Field(description="Section title (e.g., 'Cast-in-Place Concrete')")
    csi_division: int = Field(description="Division number (1-49)")
    division_name: str = Field(description="Division name (e.g., 'Concrete')")
    notes: Optional[str] = Field(default=None, description="Additional notes")
