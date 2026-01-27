"""
Location Processing Module

Centralized location processing for all data sources. Consolidates grid parsing,
spatial matching, and location enrichment logic that was previously duplicated
across RABA, PSI, TBM, and other consolidation scripts.

Usage:
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

Module Structure:
    location/
    ├── __init__.py              # Public API (this file)
    ├── core/                    # Core location logic
    │   ├── grid_parser.py       # Grid coordinate parsing
    │   └── normalizers.py       # Building/level normalization
    ├── enrichment/              # Location enrichment
    │   └── location_enricher.py # Main enrichment function
    └── validation/              # Coverage and quality checks
"""

from scripts.integrated_analysis.location.enrichment.location_enricher import (
    enrich_location,
    LocationEnrichmentResult,
)

__all__ = [
    'enrich_location',
    'LocationEnrichmentResult',
]
