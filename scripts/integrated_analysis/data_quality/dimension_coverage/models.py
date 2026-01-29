"""
Data models for dimension coverage analysis.

This module defines the data classes used to represent:
- Dimension table statistics
- Source coverage metrics
"""

from dataclasses import dataclass, field
from typing import Dict, Set


@dataclass
class DimensionStats:
    """
    Statistics about a dimension table.

    Attributes:
        name: Dimension table name (e.g., 'dim_location')
        total_records: Total number of records in the dimension
        breakdown: Distribution by category (e.g., location_type counts)

    Example:
        >>> stats = DimensionStats(
        ...     name='dim_location',
        ...     total_records=540,
        ...     breakdown={'ROOM': 374, 'STAIR': 66, 'GRIDLINE': 62}
        ... )
    """
    name: str
    total_records: int
    breakdown: Dict[str, int] = field(default_factory=dict)


@dataclass
class SourceCoverage:
    """
    Coverage metrics for a single data source across all dimensions.

    This captures how well a fact table is linked to dimension tables,
    providing both counts and percentages for each dimension.

    Attributes:
        source_name: Name of the source (e.g., 'P6', 'RABA')
        total_records: Total rows in the source
        location_id_count: Rows with dim_location_id populated
        location_id_pct: Percentage of rows with location
        company_id_count: Rows with dim_company_id populated
        company_id_pct: Percentage of rows with company
        csi_section_count: Rows with csi_section populated
        csi_section_pct: Percentage of rows with CSI
        location_type_distribution: Count by location type (ROOM, GRIDLINE, etc.)
        unresolved_companies: Company names that couldn't be mapped
        csi_distribution: Count by CSI section code

    Location Type Distribution:
        The location_type_distribution shows the granularity of location data:
        - ROOM/STAIR/ELEVATOR: Most specific (can identify exact room)
        - GRIDLINE: Grid coordinates only (room inferred via spatial join)
        - LEVEL: Building + floor only
        - BUILDING: Building-wide
        - UNDEFINED: No location could be determined

    Example:
        >>> coverage = SourceCoverage(
        ...     source_name='RABA',
        ...     total_records=9391,
        ...     location_id_count=9391,
        ...     location_id_pct=100.0,
        ...     company_id_count=8358,
        ...     company_id_pct=89.0,
        ...     csi_section_count=9385,
        ...     csi_section_pct=99.9,
        ...     location_type_distribution={'ROOM': 2482, 'GRIDLINE': 2811},
        ...     unresolved_companies={'Unknown Inc'},
        ...     csi_distribution={'03 30 00': 3306},
        ... )
    """
    # Required fields (no defaults) must come first
    source_name: str
    total_records: int
    location_id_count: int
    location_id_pct: float
    company_id_count: int
    company_id_pct: float
    csi_section_count: int
    csi_section_pct: float

    # Optional fields with defaults must come after required fields
    location_type_distribution: Dict[str, int] = field(default_factory=dict)
    unresolved_companies: Set[str] = field(default_factory=set)
    csi_distribution: Dict[str, int] = field(default_factory=dict)

    def get_granularity_summary(self) -> Dict[str, float]:
        """
        Calculate location granularity summary.

        Returns percentages for three granularity levels:
        - room_level: ROOM + STAIR + ELEVATOR (most specific)
        - grid_level: GRIDLINE (medium specificity)
        - coarse_level: LEVEL + BUILDING + AREA + SITE + UNDEFINED

        Returns:
            Dict with 'room_level', 'grid_level', 'coarse_level' percentages
        """
        dist = self.location_type_distribution
        total = sum(dist.values())

        if total == 0:
            return {'room_level': 0.0, 'grid_level': 0.0, 'coarse_level': 0.0}

        room = dist.get('ROOM', 0) + dist.get('STAIR', 0) + dist.get('ELEVATOR', 0)
        grid = dist.get('GRIDLINE', 0)
        coarse = (
            dist.get('LEVEL', 0) +
            dist.get('BUILDING', 0) +
            dist.get('AREA', 0) +
            dist.get('SITE', 0) +
            dist.get('UNDEFINED', 0)
        )

        return {
            'room_level': room / total * 100,
            'grid_level': grid / total * 100,
            'coarse_level': coarse / total * 100,
        }
