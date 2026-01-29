"""
Task Taxonomy Module

Generates task taxonomy lookup table mapping task_id to classifications.

Note: dim_trade has been superseded by dim_csi_section. Trade-related exports
(infer_trade, Z_TRADE_TO_DIM_TRADE, etc.) have been removed.

Usage:
    from task_taxonomy import build_task_context, infer_all_fields, generate_taxonomy

    # Build combined context
    context = build_task_context(tasks_df, wbs_df, taskactv_df, actvcode_df, actvtype_df)

    # Generate taxonomy
    taxonomy = generate_taxonomy(context)
"""

from .context import build_task_context, build_activity_code_lookup
from .inference import (
    infer_building,
    infer_level,
    infer_area,
    infer_room,
    infer_subcontractor,
    infer_sub_trade,
    infer_phase,
    infer_location_type,
    infer_impact,
    infer_csi_section,
    infer_all_fields,
)
from .mappings import (
    Z_BLDG_TO_CODE,
)
from .extractors import (
    extract_building_from_wbs,
    extract_level_from_wbs,
    extract_area_from_wbs,
    extract_room_from_wbs,
    extract_level_from_z_level,
    extract_building_from_z_level,
    extract_building_from_task_code,
    extract_building_from_z_area,
    extract_elevator_from_task_name,
    extract_stair_from_task_name,
    extract_gridline_from_task_name_and_area,
    normalize_level,
)
# Gridline mapping is now in scripts/shared/ for cross-source usage
import sys
from pathlib import Path
_shared_dir = Path(__file__).parent.parent.parent.parent / 'shared'
if str(_shared_dir) not in sys.path:
    sys.path.insert(0, str(_shared_dir))

from gridline_mapping import (
    GridlineMapping,
    get_default_mapping,
    get_gridline_bounds,
    get_row_range_for_building,
    normalize_elevator_code,
    normalize_stair_code,
    normalize_room_code,
)

__all__ = [
    # Context building
    'build_task_context',
    'build_activity_code_lookup',
    # Inference functions (one per field)
    'infer_building',
    'infer_level',
    'infer_area',
    'infer_room',
    'infer_subcontractor',
    'infer_sub_trade',
    'infer_phase',
    'infer_location_type',
    'infer_impact',
    'infer_csi_section',
    'infer_all_fields',
    # Mappings
    'Z_BLDG_TO_CODE',
    # Extractors
    'extract_building_from_wbs',
    'extract_level_from_wbs',
    'extract_area_from_wbs',
    'extract_room_from_wbs',
    'extract_level_from_z_level',
    'extract_building_from_z_level',
    'extract_building_from_task_code',
    'extract_building_from_z_area',
    'extract_elevator_from_task_name',
    'extract_stair_from_task_name',
    'extract_gridline_from_task_name_and_area',
    'normalize_level',
    # Gridline mapping
    'GridlineMapping',
    'get_default_mapping',
    'get_gridline_bounds',
    'get_row_range_for_building',
    'normalize_elevator_code',
    'normalize_stair_code',
    'normalize_room_code',
]
