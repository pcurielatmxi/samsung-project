"""Core location processing modules."""

from scripts.integrated_analysis.location.core.normalizers import (
    normalize_level,
    normalize_building,
)
from scripts.integrated_analysis.location.core.extractors import (
    # Room extraction
    extract_room,
    infer_building_from_room,
    infer_level_from_room,
    # Stair extraction
    extract_stair,
    normalize_stair_code,
    # Elevator extraction
    extract_elevator,
    normalize_elevator_code,
    # Gridline extraction
    extract_gridline,
    # Building extraction
    extract_building_from_wbs,
    extract_building_from_task_code,
    extract_building_from_z_area,
    extract_building_from_z_level,
    # Level extraction
    extract_level_from_wbs,
    extract_level_from_z_level,
)

__all__ = [
    # Normalizers
    'normalize_level',
    'normalize_building',
    # Room
    'extract_room',
    'infer_building_from_room',
    'infer_level_from_room',
    # Stair
    'extract_stair',
    'normalize_stair_code',
    # Elevator
    'extract_elevator',
    'normalize_elevator_code',
    # Gridline
    'extract_gridline',
    # Building
    'extract_building_from_wbs',
    'extract_building_from_task_code',
    'extract_building_from_z_area',
    'extract_building_from_z_level',
    # Level
    'extract_level_from_wbs',
    'extract_level_from_z_level',
]
