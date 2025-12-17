"""
Task Taxonomy Module

Generates task taxonomy lookup table mapping task_id to classifications.

Usage:
    from task_taxonomy import build_task_context, infer_all_fields, generate_taxonomy

    # Build combined context
    context = build_task_context(tasks_df, wbs_df, taskactv_df, actvcode_df, actvtype_df)

    # Generate taxonomy
    taxonomy = generate_taxonomy(context)
"""

from .context import build_task_context, build_activity_code_lookup
from .inference import (
    infer_trade,
    infer_building,
    infer_level,
    infer_area,
    infer_room,
    infer_subcontractor,
    infer_sub_trade,
    infer_phase,
    infer_location_type,
    infer_impact,
    infer_all_fields,
)
from .mappings import (
    Z_TRADE_TO_DIM_TRADE,
    DIM_TRADE,
    SCOPE_TO_TRADE_ID,
    Z_BLDG_TO_CODE,
    get_trade_details,
)
from .extractors import (
    extract_building_from_wbs,
    extract_level_from_wbs,
    extract_trade_from_wbs,
    extract_area_from_wbs,
    extract_room_from_wbs,
    extract_level_from_z_level,
    extract_building_from_task_code,
    extract_building_from_z_area,
    extract_trade_from_task_name,
    extract_elevator_from_task_name,
    extract_stair_from_task_name,
    extract_gridline_from_task_name_and_area,
)

__all__ = [
    # Context building
    'build_task_context',
    'build_activity_code_lookup',
    # Inference functions (one per field)
    'infer_trade',
    'infer_building',
    'infer_level',
    'infer_area',
    'infer_room',
    'infer_subcontractor',
    'infer_sub_trade',
    'infer_phase',
    'infer_location_type',
    'infer_impact',
    'infer_all_fields',
    # Mappings
    'Z_TRADE_TO_DIM_TRADE',
    'DIM_TRADE',
    'SCOPE_TO_TRADE_ID',
    'Z_BLDG_TO_CODE',
    'get_trade_details',
    # Extractors
    'extract_building_from_wbs',
    'extract_level_from_wbs',
    'extract_trade_from_wbs',
    'extract_area_from_wbs',
    'extract_room_from_wbs',
    'extract_level_from_z_level',
    'extract_building_from_task_code',
    'extract_building_from_z_area',
    'extract_trade_from_task_name',
    'extract_elevator_from_task_name',
    'extract_stair_from_task_name',
    'extract_gridline_from_task_name_and_area',
]
