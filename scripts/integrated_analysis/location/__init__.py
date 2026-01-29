"""
Location Processing Module

Centralized location processing for all data sources. Consolidates grid parsing,
spatial matching, and location enrichment logic.

Usage (Fact tables - TBM/RABA/PSI/QC Workbooks):
    from scripts.integrated_analysis.location import enrich_location, enrich_dataframe

    # Single record
    result = enrich_location(
        building='FAB',
        level='2F',
        grid='G/10-12',
        room_code='FAB126401',  # Optional - if provided, grid inferred from room
        source='RABA'
    )

    # Access results
    result.dim_location_id      # Integer FK to dim_location
    result.location_type        # ROOM, STAIR, ELEVATOR, GRIDLINE, LEVEL, BUILDING, UNDEFINED
    result.location_code        # FAB126401, STR-21, ELV-01, etc.
    result.level                # 1F, 2F, ROOF, B1, etc.
    result.grid_row_min/max     # Grid row bounds (A-N)
    result.grid_col_min/max     # Grid column bounds (numeric)
    result.affected_rooms       # JSON array of rooms with grid overlap
    result.affected_rooms_count # Integer count
    result.match_type           # ROOM_DIRECT, ROOM_FROM_GRID, GRID_MULTI, GRIDLINE, LEVEL, BUILDING, UNDEFINED

    # Entire DataFrame
    df = enrich_dataframe(df, building_col='building', level_col='level', grid_col='grid', source='RABA')

Usage (P6 schedule data):
    from scripts.integrated_analysis.location import extract_p6_location, P6LocationResult

    result = extract_p6_location(
        task_name='INSTALL DRYWALL - FAB116406',
        task_code='CN.SWA5.1234',
        wbs_name='Room FAB116406',
        tier_3='LEVEL 1',
        tier_4='L1 FAB',
        tier_5='FAB116406',
    )

Module Structure:
    location/
    ├── __init__.py              # Public API (this file)
    ├── core/                    # Core location logic
    │   ├── extractors.py        # P6 extraction patterns
    │   └── normalizers.py       # Building/level normalization
    ├── enrichment/              # Location enrichment
    │   ├── location_enricher.py # Fact table enrichment (TBM/RABA/PSI/QC)
    │   └── p6_location.py       # P6 schedule location extraction
    └── validation/              # Coverage and quality checks
"""

from scripts.integrated_analysis.location.enrichment.location_enricher import (
    enrich_location,
    enrich_dataframe,
    enrich_location_row,
    LocationEnrichmentResult,
)
from scripts.integrated_analysis.location.enrichment.p6_location import (
    extract_p6_location,
    P6LocationResult,
)
from scripts.integrated_analysis.location.core.grid_parser import (
    parse_grid,
    parse_grid_to_dict,
    GridParseResult,
)
from scripts.integrated_analysis.location.core.tbm_grid_parser import (
    parse_tbm_grid,
)

__all__ = [
    # Fact table enrichment (TBM/RABA/PSI/QC Workbooks)
    'enrich_location',
    'enrich_dataframe',
    'enrich_location_row',
    'LocationEnrichmentResult',
    # P6 schedule location extraction
    'extract_p6_location',
    'P6LocationResult',
    # Grid parsing (used by sources before calling enrich_location)
    'parse_grid',
    'parse_grid_to_dict',
    'GridParseResult',
    # TBM-specific grid parsing (103+ patterns for TBM location_row field)
    'parse_tbm_grid',
]
