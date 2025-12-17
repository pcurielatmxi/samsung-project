"""
Field Inference Functions for Task Taxonomy

Each function takes a row from the combined task context and returns:
    (value, source) where source is 'activity_code', 'wbs', 'inferred', or None

Data source priority:
1. Activity codes (Z-TRADE, Z-BLDG, Z-LEVEL, Z-SUB) - highest priority
2. WBS hierarchy context - intermediate fallback
3. Task name inference (TaskClassifier) - last resort
"""

import pandas as pd

from .mappings import (
    Z_TRADE_TO_DIM_TRADE,
    Z_BLDG_TO_CODE,
    SCOPE_TO_TRADE_ID,
    DIM_TRADE,
    get_trade_details,
)
from .extractors import (
    extract_building_from_wbs,
    extract_level_from_wbs,
    extract_trade_from_wbs,
    extract_area_from_wbs,
    extract_room_from_wbs,
    extract_level_from_z_level,
)


def infer_trade(row: pd.Series) -> tuple[int | None, str | None, str | None, str | None]:
    """
    Infer trade classification from activity code, WBS, or task name.

    Args:
        row: Combined task context row with columns:
            - z_trade: Z-TRADE activity code value
            - tier_4: WBS tier_4 value
            - scope: Inferred scope from TaskClassifier

    Returns:
        Tuple of (trade_id, trade_code, trade_name, source)
    """
    # Priority 1: Activity code
    z_trade = row.get('z_trade')
    if z_trade and pd.notna(z_trade):
        key = str(z_trade).lower().strip()
        trade_id = Z_TRADE_TO_DIM_TRADE.get(key)
        if trade_id:
            details = get_trade_details(trade_id)
            return (trade_id, details['trade_code'], details['trade_name'], 'activity_code')

    # Priority 2: WBS context
    tier_4 = row.get('tier_4')
    if tier_4 and pd.notna(tier_4):
        trade_id = extract_trade_from_wbs(tier_4)
        if trade_id:
            details = get_trade_details(trade_id)
            return (trade_id, details['trade_code'], details['trade_name'], 'wbs')

    # Priority 3: Task name inference
    scope = row.get('scope')
    if scope and pd.notna(scope):
        trade_id = SCOPE_TO_TRADE_ID.get(scope)
        if trade_id:
            details = get_trade_details(trade_id)
            return (trade_id, details['trade_code'], details['trade_name'], 'inferred')

    return (None, None, None, None)


def infer_building(row: pd.Series) -> tuple[str | None, str | None]:
    """
    Infer building code from activity code, WBS, or task name.

    Args:
        row: Combined task context row with columns:
            - z_bldg: Z-BLDG activity code value
            - tier_3, tier_4: WBS tier values
            - building: Inferred building from TaskClassifier

    Returns:
        Tuple of (building, source)
    """
    # Priority 1: Activity code
    z_bldg = row.get('z_bldg')
    if z_bldg and pd.notna(z_bldg):
        key = str(z_bldg).lower().strip()
        building = Z_BLDG_TO_CODE.get(key)
        if building:
            return (building, 'activity_code')

    # Priority 2: WBS context
    tier_3 = row.get('tier_3')
    tier_4 = row.get('tier_4')
    building = extract_building_from_wbs(tier_3, tier_4)
    if building:
        return (building, 'wbs')

    # Priority 3: Task name inference
    inferred_building = row.get('building_inferred')
    if inferred_building and pd.notna(inferred_building):
        if inferred_building not in ('UNK', 'GEN'):
            return (inferred_building, 'inferred')

    return (None, None)


def infer_level(row: pd.Series) -> tuple[str | None, str | None]:
    """
    Infer floor level from activity code, WBS, or task name.

    Note: Z-LEVEL is often misused (contains trade names), so WBS is often
    more reliable. Activity code still takes priority when it's a valid level.

    Args:
        row: Combined task context row with columns:
            - z_level: Z-LEVEL activity code value
            - tier_3, tier_4, tier_5, wbs_name: WBS tier values
            - level: Inferred level from TaskClassifier

    Returns:
        Tuple of (level, source)
    """
    # Priority 1: Activity code (when valid level pattern)
    z_level = row.get('z_level')
    if z_level and pd.notna(z_level):
        level = extract_level_from_z_level(z_level)
        if level:
            return (level, 'activity_code')

    # Priority 2: WBS context
    tier_3 = row.get('tier_3')
    tier_4 = row.get('tier_4')
    tier_5 = row.get('tier_5')
    wbs_name = row.get('wbs_name')
    level = extract_level_from_wbs(tier_3, tier_4, tier_5, wbs_name)
    if level:
        return (level, 'wbs')

    # Priority 3: Task name inference
    inferred_level = row.get('level_inferred')
    if inferred_level and pd.notna(inferred_level):
        if inferred_level not in ('UNK', 'GEN'):
            return (inferred_level, 'inferred')

    return (None, None)


def infer_area(row: pd.Series) -> tuple[str | None, str | None]:
    """
    Infer grid area from WBS tier_4.

    Area is only available from WBS context (not in activity codes or inference).

    Args:
        row: Combined task context row with tier_4 column

    Returns:
        Tuple of (area, source)
    """
    tier_4 = row.get('tier_4')
    if tier_4 and pd.notna(tier_4):
        area = extract_area_from_wbs(tier_4)
        if area:
            return (area, 'wbs')

    return (None, None)


def infer_room(row: pd.Series) -> tuple[str | None, str | None]:
    """
    Infer room code from WBS tier_5 or wbs_name.

    Room is only available from WBS context (not in activity codes or inference).

    Args:
        row: Combined task context row with tier_5, wbs_name columns

    Returns:
        Tuple of (room, source)
    """
    tier_5 = row.get('tier_5')
    wbs_name = row.get('wbs_name')
    room = extract_room_from_wbs(tier_5, wbs_name)
    if room:
        return (room, 'wbs')

    return (None, None)


def infer_subcontractor(row: pd.Series) -> tuple[str | None, str | None]:
    """
    Infer subcontractor from Z-SUB CONTRACTOR activity code.

    Subcontractor is only available from activity codes.

    Args:
        row: Combined task context row with z_sub_contractor column

    Returns:
        Tuple of (sub_contractor, source)
    """
    z_sub = row.get('z_sub_contractor')
    if z_sub and pd.notna(z_sub):
        return (str(z_sub), 'activity_code')

    return (None, None)


def infer_sub_trade(row: pd.Series) -> tuple[str | None, str | None, str | None]:
    """
    Infer sub-trade (detailed scope) from task name.

    Sub-trade provides finer granularity than trade (e.g., CIP vs CTG for concrete).
    Only available from task name inference.

    Args:
        row: Combined task context row with scope, scope_desc columns

    Returns:
        Tuple of (sub_trade, sub_trade_desc, source)
    """
    scope = row.get('scope')
    scope_desc = row.get('scope_desc')

    if scope and pd.notna(scope):
        return (scope, scope_desc, 'inferred')

    return (None, None, None)


def infer_phase(row: pd.Series) -> tuple[str | None, str | None, str | None]:
    """
    Infer construction phase from task name.

    Phase indicates project stage: PRE (preconstruction), CON (construction),
    COM (commissioning), etc.
    Only available from task name inference.

    Args:
        row: Combined task context row with phase, phase_desc columns

    Returns:
        Tuple of (phase, phase_desc, source)
    """
    phase = row.get('phase')
    phase_desc = row.get('phase_desc')

    if phase and pd.notna(phase):
        return (phase, phase_desc, 'inferred')

    return (None, None, None)


def infer_location_type(row: pd.Series) -> tuple[str | None, str | None, str | None, str | None]:
    """
    Infer location type from task name.

    Location type categorizes where work occurs: BLD (building), GRD (grid),
    ROM (room), etc.
    Only available from task name inference.

    Args:
        row: Combined task context row with loc_type, loc_type_desc, loc_id columns

    Returns:
        Tuple of (loc_type, loc_type_desc, loc_id, source)
    """
    loc_type = row.get('loc_type')
    loc_type_desc = row.get('loc_type_desc')
    loc_id = row.get('loc_id')

    if loc_type and pd.notna(loc_type):
        return (loc_type, loc_type_desc, loc_id, 'inferred')

    return (None, None, None, None)


def infer_impact(row: pd.Series) -> dict:
    """
    Infer impact classification from task name.

    Impact fields are only populated for tasks with "IMPACT" in the name.
    Tracks delay attribution, root cause, and impact type.
    Only available from task name inference.

    Args:
        row: Combined task context row with impact columns

    Returns:
        Dict with impact fields and source:
        - impact_code, impact_type, impact_type_desc
        - attributed_to, attributed_to_desc
        - root_cause, root_cause_desc
        - impact_source
    """
    impact_code = row.get('impact_code')

    if impact_code and pd.notna(impact_code):
        return {
            'impact_code': impact_code,
            'impact_type': row.get('impact_type'),
            'impact_type_desc': row.get('impact_type_desc'),
            'attributed_to': row.get('attributed_to'),
            'attributed_to_desc': row.get('attributed_to_desc'),
            'root_cause': row.get('root_cause'),
            'root_cause_desc': row.get('root_cause_desc'),
            'impact_source': 'inferred',
        }

    return {
        'impact_code': None,
        'impact_type': None,
        'impact_type_desc': None,
        'attributed_to': None,
        'attributed_to_desc': None,
        'root_cause': None,
        'root_cause_desc': None,
        'impact_source': None,
    }


def infer_all_fields(row: pd.Series) -> dict:
    """
    Infer all taxonomy fields for a single task row.

    Each field has a corresponding _source column showing how it was derived:
    - 'activity_code': From P6 activity codes
    - 'wbs': From WBS hierarchy tiers
    - 'inferred': From task name pattern matching
    - None: Could not be determined

    Args:
        row: Combined task context row with all required columns

    Returns:
        Dict with all taxonomy fields and their sources
    """
    # Infer each field using dedicated functions
    trade_id, trade_code, trade_name, trade_source = infer_trade(row)
    building, building_source = infer_building(row)
    level, level_source = infer_level(row)
    area, area_source = infer_area(row)
    room, room_source = infer_room(row)
    sub_contractor, sub_source = infer_subcontractor(row)
    sub_trade, sub_trade_desc, sub_trade_source = infer_sub_trade(row)
    phase, phase_desc, phase_source = infer_phase(row)
    loc_type, loc_type_desc, loc_id, loc_type_source = infer_location_type(row)
    impact = infer_impact(row)

    return {
        'task_id': row.get('task_id'),
        # Trade (from activity_code > wbs > inferred)
        'trade_id': trade_id,
        'trade_code': trade_code,
        'trade_name': trade_name,
        'trade_source': trade_source,
        # Sub-trade / detailed scope (inferred only)
        'sub_trade': sub_trade,
        'sub_trade_desc': sub_trade_desc,
        'sub_trade_source': sub_trade_source,
        # Building (from activity_code > wbs > inferred)
        'building': building,
        'building_source': building_source,
        # Level (from activity_code > wbs > inferred)
        'level': level,
        'level_source': level_source,
        # Area (wbs only)
        'area': area,
        'area_source': area_source,
        # Room (wbs only)
        'room': room,
        'room_source': room_source,
        # Subcontractor (activity_code only)
        'sub_contractor': sub_contractor,
        'sub_source': sub_source,
        # Phase (inferred only)
        'phase': phase,
        'phase_desc': phase_desc,
        'phase_source': phase_source,
        # Location type (inferred only)
        'loc_type': loc_type,
        'loc_type_desc': loc_type_desc,
        'loc_id': loc_id,
        'loc_type_source': loc_type_source,
        # Label (combined classification)
        'label': row.get('label'),
        # Impact tracking (inferred only, sparse)
        'impact_code': impact['impact_code'],
        'impact_type': impact['impact_type'],
        'impact_type_desc': impact['impact_type_desc'],
        'attributed_to': impact['attributed_to'],
        'attributed_to_desc': impact['attributed_to_desc'],
        'root_cause': impact['root_cause'],
        'root_cause_desc': impact['root_cause_desc'],
        'impact_source': impact['impact_source'],
    }
