"""
Building Mapping Tables for Task Taxonomy

Note: Trade mappings (Z_TRADE_TO_DIM_TRADE, DIM_TRADE, SCOPE_TO_TRADE_ID,
WBS_TRADE_PATTERNS, TASK_NAME_TRADE_PATTERNS) have been removed.
dim_trade has been superseded by dim_csi_section for work type classification.

Use dim_csi_section_id instead of trade_id for work type classification.
CSI inference is handled by add_csi_to_p6_tasks.py.
"""

# =============================================================================
# Building Mappings
# =============================================================================

# Map Z-BLDG activity code values to building code
Z_BLDG_TO_CODE = {
    'fab building': 'FAB',
    'east support building': 'SUE',
    'west support building': 'SUW',
    'support building overall': 'SUP',
    'bim / vdc overall': None,
    'data center (gridlines 1-3)': 'FAB',
    'drilled piers': None,
    'key milestones': None,
    'north areas (team b -gridlines 17-33)': 'FAB',
    'south areas (team a - gridlines 3-17)': 'FAB',
}
