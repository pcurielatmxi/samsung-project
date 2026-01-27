"""
Location Processing Module

Centralized location processing for all data sources. Consolidates grid parsing,
spatial matching, and location enrichment logic that was previously duplicated
across RABA, PSI, TBM, and other consolidation scripts.

Usage (Quality data - RABA/PSI/TBM):
    from scripts.integrated_analysis.location import enrich_location, LocationEnrichmentResult

    result = enrich_location(
        building='FAB',
        level='2F',
        grid='G/10-12',
        source='RABA'
    )

    # Access results
    result.dim_location_id      # Integer FK or None
    result.affected_rooms       # JSON string of overlapping rooms
    result.grid_completeness    # FULL, ROW_ONLY, COL_ONLY, LEVEL_ONLY, NONE
    result.match_quality        # PRECISE, MIXED, PARTIAL, NONE

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

    # Access results
    result.location_type        # ROOM, STAIR, ELEVATOR, GRIDLINE, LEVEL, BUILDING
    result.location_code        # FAB116406, STR-10, ELV-01, GL-5, FAB-2F, FAB
    result.building             # FAB, SUE, SUW, FIZ
    result.level                # 1F, 2F, ROOF, B1

Module Structure:
    location/
    ├── __init__.py              # Public API (this file)
    ├── core/                    # Core location logic
    │   ├── extractors.py        # P6 extraction patterns (room, stair, elevator, etc.)
    │   └── normalizers.py       # Building/level normalization
    ├── enrichment/              # Location enrichment
    │   ├── location_enricher.py # Quality data enrichment (RABA/PSI/TBM)
    │   └── p6_location.py       # P6 schedule location extraction
    └── validation/              # Coverage and quality checks
"""

from scripts.integrated_analysis.location.enrichment.location_enricher import (
    enrich_location,
    LocationEnrichmentResult,
)
from scripts.integrated_analysis.location.enrichment.p6_location import (
    extract_p6_location,
    P6LocationResult,
)

__all__ = [
    # Quality data enrichment (RABA/PSI/TBM)
    'enrich_location',
    'LocationEnrichmentResult',
    # P6 schedule location extraction
    'extract_p6_location',
    'P6LocationResult',
]
