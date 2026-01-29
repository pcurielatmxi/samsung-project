"""
Field Inference Functions for Task Taxonomy

Each function takes a row from the combined task context and returns:
    (value, source) where source is 'activity_code', 'wbs', 'inferred', or None

Data source priority:
1. Activity codes (Z-TRADE, Z-BLDG, Z-LEVEL, Z-SUB) - highest priority
2. WBS hierarchy context - intermediate fallback
3. Task name inference (TaskClassifier) - last resort

Location extraction now uses the centralized module at:
    scripts/integrated_analysis/location/
"""

import sys
from pathlib import Path

import pandas as pd

# Gridline mapping is now in scripts/shared/ for cross-source usage
_shared_dir = Path(__file__).parent.parent.parent.parent / 'shared'
if str(_shared_dir) not in sys.path:
    sys.path.insert(0, str(_shared_dir))

from gridline_mapping import get_gridline_bounds, get_default_mapping
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
    extract_building_from_z_level,
    extract_building_from_task_code,
    extract_building_from_z_area,
    extract_trade_from_task_name,
    extract_elevator_from_task_name,
    extract_stair_from_task_name,
    extract_gridline_from_task_name_and_area,
    normalize_level,
)

# Import centralized P6 location extraction
from scripts.integrated_analysis.location import extract_p6_location

# Import CSI section inference (reuse from quality data processing)
from scripts.integrated_analysis.add_csi_to_raba import (
    infer_csi_section as _infer_csi_section_quality,
    KEYWORD_TO_CSI,
    CSI_SECTIONS,
)


def infer_trade(row: pd.Series) -> tuple[int | None, str | None, str | None, str | None]:
    """
    Infer trade classification from activity code, WBS, or task name.

    Priority order:
    1. Z-TRADE activity code (highest priority)
    2. WBS tier_4 context
    3. TaskClassifier scope code
    4. Task name pattern matching (fallback)

    Args:
        row: Combined task context row with columns:
            - z_trade: Z-TRADE activity code value
            - tier_4: WBS tier_4 value
            - scope: Inferred scope from TaskClassifier
            - task_name: Task name for pattern matching

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

    # Priority 3: TaskClassifier scope
    scope = row.get('scope')
    if scope and pd.notna(scope):
        trade_id = SCOPE_TO_TRADE_ID.get(scope)
        if trade_id:
            details = get_trade_details(trade_id)
            return (trade_id, details['trade_code'], details['trade_name'], 'inferred')

    # Priority 4: Task name pattern matching (fallback)
    task_name = row.get('task_name')
    if task_name and pd.notna(task_name):
        trade_id = extract_trade_from_task_name(task_name)
        if trade_id:
            details = get_trade_details(trade_id)
            return (trade_id, details['trade_code'], details['trade_name'], 'inferred')

    return (None, None, None, None)


def infer_building(row: pd.Series) -> tuple[str | None, str | None]:
    """
    Infer building code from activity codes, task_code, WBS, or task name.

    Priority order:
    1. Z-BLDG activity code (highest priority, explicit building assignment)
    2. Z-AREA activity code (structured area with building prefix)
    3. task_code segment (e.g., CN.SWA5.1234 -> SUW)
    4. WBS tier hierarchy
    5. TaskClassifier inference (lowest priority)

    Args:
        row: Combined task context row with columns:
            - z_bldg: Z-BLDG activity code value
            - z_area: Z-AREA activity code value
            - task_code: P6 task code (PREFIX.AREA.NUMBER format)
            - tier_3, tier_4: WBS tier values
            - building_inferred: Inferred building from TaskClassifier

    Returns:
        Tuple of (building, source)
    """
    # Priority 1: Z-BLDG activity code (direct building assignment)
    z_bldg = row.get('z_bldg')
    if z_bldg and pd.notna(z_bldg):
        key = str(z_bldg).lower().strip()
        building = Z_BLDG_TO_CODE.get(key)
        if building:
            return (building, 'activity_code')

    # Priority 2: Z-AREA activity code (structured area with building)
    z_area = row.get('z_area')
    if z_area and pd.notna(z_area):
        building = extract_building_from_z_area(z_area)
        if building:
            return (building, 'activity_code')

    # Priority 2.5: Z-LEVEL activity code (cardinal directions like "STAIR 5 (WEST)")
    z_level = row.get('z_level')
    if z_level and pd.notna(z_level):
        building = extract_building_from_z_level(z_level)
        if building:
            return (building, 'activity_code')

    # Priority 3: task_code segment (e.g., CN.SWA5.1234 -> SUW)
    task_code = row.get('task_code')
    if task_code and pd.notna(task_code):
        building = extract_building_from_task_code(task_code)
        if building:
            return (building, 'task_code')

    # Priority 4: WBS context
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

    All returned levels are normalized to dim_location format:
    '1' -> '1F', '2' -> '2F', etc.

    Args:
        row: Combined task context row with columns:
            - z_level: Z-LEVEL activity code value
            - tier_3, tier_4, tier_5, wbs_name: WBS tier values
            - level: Inferred level from TaskClassifier

    Returns:
        Tuple of (level, source) where level is in dim_location format
    """
    # Priority 1: Activity code (when valid level pattern)
    z_level = row.get('z_level')
    if z_level and pd.notna(z_level):
        level = extract_level_from_z_level(z_level)
        if level:
            return (normalize_level(level), 'activity_code')

    # Priority 2: WBS context
    tier_3 = row.get('tier_3')
    tier_4 = row.get('tier_4')
    tier_5 = row.get('tier_5')
    wbs_name = row.get('wbs_name')
    level = extract_level_from_wbs(tier_3, tier_4, tier_5, wbs_name)
    if level:
        return (normalize_level(level), 'wbs')

    # Priority 3: Task name inference
    inferred_level = row.get('level_inferred')
    if inferred_level and pd.notna(inferred_level):
        if inferred_level not in ('UNK', 'GEN'):
            return (normalize_level(inferred_level), 'inferred')

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


def infer_location_type(row: pd.Series, building: str | None = None, level: str | None = None, area: str | None = None, room: str | None = None) -> tuple[str | None, str | None, str | None, str | None]:
    """
    Infer generalized location type and code from task context.

    Uses centralized extract_p6_location() with priority:
    ROOM > STAIR > ELEVATOR > GRIDLINE > LEVEL > BUILDING

    Args:
        row: Combined task context row
        building: Inferred building code (optional, passed for efficiency)
        level: Inferred level (optional, passed for efficiency)
        area: Inferred area (optional, passed for efficiency)
        room: Inferred room (optional, passed for efficiency)

    Returns:
        Tuple of (location_type, location_code, row_min, row_max)
        row_min/row_max are only populated for GRIDLINE type when bounds are
        extracted from task names (e.g., "GL 33 - D-F" -> row_min=D, row_max=F)
    """
    # If room is already known, return ROOM type directly (optimization)
    if room and pd.notna(room):
        return ('ROOM', str(room), None, None)

    # Use centralized extraction for full location type determination
    result = extract_p6_location(
        task_name=row.get('task_name'),
        task_code=row.get('task_code'),
        wbs_name=row.get('wbs_name'),
        tier_3=row.get('tier_3'),
        tier_4=row.get('tier_4'),
        tier_5=row.get('tier_5'),
        z_bldg=row.get('z_bldg'),
        z_level=row.get('z_level'),
        z_area=row.get('z_area'),
        area=area,
    )

    return (result.location_type, result.location_code, result.row_min, result.row_max)


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


def infer_work_phase(row: pd.Series) -> tuple[str | None, str | None]:
    """
    Infer work phase from task name keywords.

    Phases are ordered by construction sequence:
    - DESIGN: Design, engineering, RFI responses, shop drawings
    - FABRICATION: Manufacturing/fabrication of components
    - DELIVERY: Transportation and delivery to site
    - ERECTION: Installation, erection, setting of components

    Args:
        row: Task context row with task_name column

    Returns:
        Tuple of (work_phase, work_phase_source)
        work_phase: DESIGN, FABRICATION, DELIVERY, ERECTION, or None
        work_phase_source: 'inferred' if matched, None otherwise
    """
    import re

    task_name = row.get('task_name', '')
    if not task_name or pd.isna(task_name):
        return (None, None)

    name_upper = str(task_name).upper()

    # Design phase patterns
    design_patterns = [
        r'\bDESIGN\b',
        r'\bREDESIGN',
        r'\bEOR\b',           # Engineer of Record
        r'\bSHOP\s*DWG',
        r'\bSHOP\s*DRAWING',
        r'\bIFC\b',           # Issued for Construction
        r'\bRFI\b',           # Request for Information
        r'\bRFA\b',           # Request for Approval
        r'\bSUBMITTAL',
        r'\bSUBMIT\b',
        r'\bAPPROVAL',
        r'\bAPPROVE\b',
        r'\bENGINEER',
        r'\bCOORDINAT',       # Coordination
        r'\bBID\b',           # Bidding
        r'\bBIDDING',
        r'\bLEVELING\b',      # Bid leveling
        r'\bBUYOUT',          # Buyout
        r'\bREVIEW\b',        # Review
        r'\bRESPONSE',        # Responses
        r'\bIOCC\b',          # Internal Owner Change Condition
        r'\bCCD\b',           # Contract Change Directive
    ]

    # Fabrication phase patterns
    # Note: Avoid \bFAB\b alone - matches "FAB" building name and "fab side"
    fab_patterns = [
        r'\bFABRICAT',
        r'\bREFABRICAT',
        r'\bRE-FABRICAT',
        r'\bCASTING\b',
        r'\bCAST\b(?!.*IN.*PLACE)',  # CAST but not CAST-IN-PLACE
        r'\bMANUFACTUR',
        r'\bPROCURE',
        r'\bPURCHASE',
    ]

    # Delivery phase patterns
    delivery_patterns = [
        r'\bDELIVER',
        r'\bSHIP\b',
        r'\bTRANSPORT',
        r'\bRECEIV',  # Receive materials
        r'\bOFFLOAD',
        r'\bARRIV',
    ]

    # Erection/Installation phase patterns
    erection_patterns = [
        # Core installation verbs
        r'\bERECT',
        r'\bINSTALL',
        r'\bSET\b',
        r'\bSETTING\b',
        r'\bPLACE\b',
        r'\bPLACEMENT\b',
        r'\bPOUR\b',
        r'\bMOUNT',
        r'\bHANG\b',
        r'\bHANGING\b',
        r'\bASSEMBL',
        r'\bCONSTRUCT',
        r'\bBUILD\b',
        r'\bLAY\b',           # Laying (tile, flooring)
        r'\bLAYING\b',
        # Connection/finishing work
        r'\bGROUT',           # Grouting after setting precast/steel
        r'\bWELD',            # Welding connections
        r'\bFRAMING\b',       # Framing installation
        r'\bBOLT\b',          # Bolting connections
        r'\bTORQUE',          # Torquing bolts
        r'\bANCHOR',          # Anchor bolts
        r'\bCONNECT',         # Connections
        r'\bTIE\b',           # Tie-ins
        # Finishing trades
        r'\bTAPE\b',          # Drywall tape
        r'\bPAINT',           # Painting
        r'\bCOAT\b',          # Coating (base coat, top coat)
        r'\bCOATING',         # Coating application
        r'\bPATCH',           # Patching
        r'\bREPAIR',          # Repair work
        r'\bCAULK',           # Caulking
        r'\bSEAL\b',          # Sealing
        r'\bSEALANT',         # Sealant application
        r'\bDRYWALL',         # Drywall installation
        r'\bGLAZING',         # Glazing
        r'\bFLOORING',        # Flooring
        r'\bCEILING',         # Ceiling work
        r'\bROOFING',         # Roofing
        r'\bFLASHING',        # Flashing
        # Specialty trades
        r'\bFIREPROOF',       # Fireproofing
        r'\bFIRESTOP',        # Firestopping
        r'\bSFRM\b',          # Spray fireproofing
        r'\bINSULAT',         # Insulation
        r'\bWATERPROOF',      # Waterproofing
        r'\bFRP\b',           # Fiberglass reinforced plastic
        # Concrete/masonry work
        r'\bCURE\b',          # Curing concrete
        r'\bTOPPING',         # Topping slab
        r'\bDECKING\b',       # Decking installation
        r'\bSLAB\b',          # Slab work
        r'\bCURB',            # Curbs
        r'\bREBAR',           # Rebar placement
        r'\bFORM\b',          # Formwork
        r'\bFORMS\b',         # Formwork
        r'\bSTRIP\b',         # Strip forms
        # Doors/openings
        r'\bDOOR\b',          # Door installation
        r'\bDOORS\b',
        r'\bHARDWARE',        # Hardware installation
        # Site work
        r'\bDEMO\b',          # Demolition
        r'\bEXCAVAT',         # Excavation
        r'\bBACKFILL',        # Backfilling
        r'\bUNDERGROUND',     # Underground work
        r'\bTRENCH',          # Trenching
        # MEP
        r'\bROUGH\s*IN',      # Rough-in (MEP)
        r'\bROUGH-IN',        # Rough-in alternate
        r'\bMEP\b',           # MEP work
        # Complete/finish activities
        r'\bCOMPLETE\b',      # Complete work
        r'\bFINISH\b',        # Finish work
        r'\bREMEDY\b',        # Remedy work
        # Inspection (part of installation)
        r'\bINSPECT',         # Inspection
        # Additional construction activities
        r'\bWALL\b',          # Wall construction
        r'\bPARAPET',         # Parapet
        r'\bSCAFFOLD',        # Scaffolding
        r'\bDRILL',           # Drilling
        r'\bGRIND',           # Grinding
        r'\bSCRAP',           # Scraping
        r'\bPROTECT',         # Protection
        r'\bSTRIP',           # Striping/stripping
        r'\bLAYER',           # Layer (roof layer)
        r'\bBEAM\b',          # Beam work
        r'\bCOLUMN',          # Column work
        r'\bFLANGE',          # Flange work
        r'\bPIPE\b',          # Pipe work
        r'\bDUCT\b',          # Duct work
        r'\bCIP\b',           # Cast-in-place
        r'\bCAST.IN.PLACE',   # Cast-in-place
        r'\bSOG\b',           # Slab on grade
        r'\bPAD\b',           # Equipment pads
        r'\bPADS\b',
        r'\bSTAIR\b',         # Stair work
        r'\bSTAIRS\b',
        r'\bLANDING',         # Landings
        r'\bRAILING',         # Railings
        r'\bHANDRAIL',        # Handrails
        r'\bCLEAN\b',         # Cleaning
        r'\bCLEANING\b',
        r'\bPUNCH',           # Punch list
        # Additional patterns
        r'\bWATER\s*PROOF',   # Water proof (with space)
        r'\bFOAM\b',          # Spray foam
        r'\bCANOPY',          # Canopy
        r'\bSOFT?FIT',        # Soffit (with typo variation)
        r'\bTILE',            # Tile work
        r'\bCERAMIC',         # Ceramic tiles
        r'\bTERRAZZO',        # Terrazzo
        r'\bEPOXY',           # Epoxy
        r'\bINTUMESCENT',     # Intumescent coating
        r'\bCLADDING',        # Cladding
        r'\bSHEATH',          # Sheathing
        r'\bPLASTER',         # Plastering
        r'\bSTUCCO',          # Stucco
        r'\bDRAIN',           # Drainage
        r'\bFRENCH\s*DRAIN',  # French drain
        r'\bTURNOVER',        # Turnover activities
        r'\bMEASURE',         # Field measurement
        r'\bSURVEY',          # Survey
        r'\bLAYOUT',          # Layout
        r'\bMARK',            # Marking
        r'\bSPRAY\b',         # Spray application
        r'\bAPPLY\b',         # Apply (coatings, etc.)
        r'\bCOVER',           # Cover/covering
        r'\bWRAP',            # Wrapping
        r'\bTEST\b',          # Testing
        r'\bFLUSH',           # Flushing
        r'\bCHARGE',          # Charging (MEP)
        r'\bSTART\s*UP',      # Start-up
        r'\bCOMMISSION',      # Commissioning
        # More edge cases
        r'\bCRC\b',           # Chemical Resistant Coating
        r'\bSEALER',          # Sealer application
        r'\bGRATING',         # Grating installation
        r'\bDISMANTL',        # Dismantling
        r'\bSOMD\b',          # SOMD (material application)
        r'\bEDGE\s*METAL',    # Edge metal
        r'\bCOPING',          # Coping
        r'\bCRICKET',         # Roof cricket
        r'\bPRIMER',          # Primer application
        r'\bPRIME\b',         # Prime
        r'\bFRP\b',           # Fiberglass (if not already caught)
        r'\bSOG\b',           # Slab on grade
        r'\bEQUIPMENT',       # Equipment
        r'\bEQPT\b',          # Equipment abbrev
        r'\bPENETRAT',        # Penetrations
        r'\bOPENING',         # Openings
        r'\bLEAVE\s*OUT',     # Leave-outs
        r'\bBLOCK\s*OUT',     # Block-outs
        r'\bEMBED',           # Embeds
        r'\bSLEEVE',          # Sleeves
        r'\bCONDUIT',         # Conduit
        r'\bCONDENSATE',      # Condensate
        r'\bCHILLER',         # Chiller
        r'\bAHU\b',           # Air handling unit
        r'\bVAV\b',           # Variable air volume
        r'\bDIFFUSER',        # Diffusers
        r'\bGRILLE',          # Grilles
        r'\bLOUVER',          # Louvers
        r'\bDAMPER',          # Dampers
        r'\bVALVE',           # Valves
        r'\bPUMP\b',          # Pumps
        r'\bFAN\b',           # Fans
        r'\bMOTOR',           # Motors
        r'\bPANEL\b',         # Panels
        r'\bTRANSFORMER',     # Transformers
        r'\bSWITCH',          # Switches
        r'\bBREAKER',         # Breakers
        r'\bCONTROL',         # Controls
        r'\bSENSOR',          # Sensors
        r'\bDETECTOR',        # Detectors
        r'\bALARM',           # Alarms
        r'\bSPRINKLER',       # Sprinklers
        r'\bEXTINGUISH',      # Fire extinguishers
    ]

    # Check patterns in priority order
    # Priority: ERECTION > DELIVERY > FABRICATION > DESIGN
    # Rationale: If task mentions "INSTALL", that's the primary activity even if
    # it also mentions fabrication. "FABRICATE & DELIVER" is FABRICATION since
    # that's the earlier/driving activity.

    # Check for erection keywords first - these take priority
    has_erection = any(re.search(p, name_upper) for p in erection_patterns)
    has_delivery = any(re.search(p, name_upper) for p in delivery_patterns)
    has_fab = any(re.search(p, name_upper) for p in fab_patterns)
    has_design = any(re.search(p, name_upper) for p in design_patterns)

    # ERECTION takes priority - if it says INSTALL, it's an installation task
    if has_erection:
        return ('ERECTION', 'inferred')

    # DELIVERY next, unless it also has fabrication (then it's fab+deliver = fab)
    if has_delivery and not has_fab:
        return ('DELIVERY', 'inferred')

    # FABRICATION (includes fab+deliver tasks)
    if has_fab:
        return ('FABRICATION', 'inferred')

    # DESIGN last
    if has_design:
        return ('DESIGN', 'inferred')

    return (None, None)


def infer_csi_section(row: pd.Series, trade_code: str = None) -> tuple[int | None, str | None, str | None, str | None]:
    """
    Infer CSI section from task name and trade classification.

    Uses the same keyword patterns as quality data CSI inference for consistency.
    Falls back to trade_code → CSI mapping if no keyword match.

    Args:
        row: Task context row with task_name column
        trade_code: Inferred trade code (CONCRETE, STEEL, etc.)

    Returns:
        Tuple of (csi_section_id, csi_section_code, csi_title, csi_source)
        csi_source is 'keyword' or 'trade' or None
    """
    task_name = row.get('task_name', '')
    if not task_name or pd.isna(task_name):
        task_name = ''

    # Try keyword matching first (same patterns as quality data)
    name_lower = str(task_name).lower()

    for keywords, csi_id in KEYWORD_TO_CSI:
        for keyword in keywords:
            if keyword in name_lower:
                csi_code, csi_title = CSI_SECTIONS[csi_id]
                return (csi_id, csi_code, csi_title, 'keyword')

    # Fall back to trade_code → CSI mapping
    if trade_code:
        trade_to_csi = {
            'CONCRETE': (2, '03 30 00', 'Cast-in-Place Concrete'),
            'PRECAST': (3, '03 41 00', 'Structural Precast Concrete'),
            'STEEL': (6, '05 12 00', 'Structural Steel Framing'),
            'MASONRY': (5, '04 20 00', 'Unit Masonry'),
            'DRYWALL': (26, '09 21 16', 'Gypsum Board Assemblies'),
            'FINISHES': (29, '09 91 26', 'Painting - Building'),
            'FIREPROOF': (18, '07 81 00', 'Applied Fireproofing'),
            'ROOFING': (16, '07 52 00', 'Modified Bituminous Membrane Roofing'),
            'INSULATION': (13, '07 21 16', 'Blanket Insulation'),
            'PANELS': (15, '07 42 43', 'Composite Wall Panels'),
            'MEP': (44, '26 05 00', 'Common Work Results for Electrical'),
            'EARTHWORK': (51, '31 23 00', 'Excavation and Fill'),
            'GENERAL': (1, '01 10 00', 'Summary'),
        }
        if trade_code in trade_to_csi:
            csi_id, csi_code, csi_title = trade_to_csi[trade_code]
            return (csi_id, csi_code, csi_title, 'trade')

    return (None, None, None, None)


def infer_all_fields(row: pd.Series, gridline_mapping=None) -> dict:
    """
    Infer all taxonomy fields for a single task row.

    Each field has a corresponding _source column showing how it was derived:
    - 'activity_code': From P6 activity codes
    - 'wbs': From WBS hierarchy tiers
    - 'inferred': From task name pattern matching
    - None: Could not be determined

    Args:
        row: Combined task context row with all required columns
        gridline_mapping: Optional GridlineMapping instance for coordinate lookup

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
    work_phase, work_phase_source = infer_work_phase(row)
    # CSI section inference (uses task name keywords, falls back to trade)
    csi_section_id, csi_section_code, csi_title, csi_source = infer_csi_section(row, trade_code=trade_code)
    # Pass inferred location fields to location_type inference
    # Now also returns row_min/row_max for GRIDLINE types with specific row bounds
    location_type, location_code, row_min, row_max = infer_location_type(row, building=building, level=level, area=area, room=room)
    impact = infer_impact(row)

    # Get gridline bounds based on location type and building
    # For GRIDLINE type, row_min/row_max override the default A-N span if extracted from task name
    gridline_bounds = get_gridline_bounds(
        location_type=location_type,
        location_code=location_code,
        building=building,
        mapping=gridline_mapping,
        row_min_override=row_min,
        row_max_override=row_max,
    )

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
        # Scope (raw classification from TaskClassifier, for backward compatibility)
        'scope': row.get('scope'),
        'scope_desc': row.get('scope_desc'),
        # Building (from activity_code > wbs > inferred)
        'building': building,
        'building_source': building_source,
        'building_desc': row.get('building_desc'),  # Backward-compatible
        # Level (from activity_code > wbs > inferred)
        'level': level,
        'level_source': level_source,
        'level_desc': row.get('level_desc'),  # Backward-compatible
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
        # Work Phase - construction sequence (DESIGN, FABRICATION, DELIVERY, ERECTION)
        'work_phase': work_phase,
        'work_phase_source': work_phase_source,
        # Location (unified type and code system)
        'location_type': location_type,
        'location_code': location_code,
        # Original TaskClassifier location fields (for existing BI queries)
        'loc_type': row.get('loc_type'),
        'loc_type_desc': row.get('loc_type_desc'),
        'loc_id': row.get('loc_id'),
        # Computed fields for BI dashboard compatibility
        'Building Code Desc': f"{building} - {row.get('building_desc')}" if building and row.get('building_desc') else (building or row.get('building_desc')),
        'location': row.get('loc_type_desc'),
        # Gridline coordinates (from mapping or building inference)
        'grid_row_min': gridline_bounds['grid_row_min'],
        'grid_row_max': gridline_bounds['grid_row_max'],
        'grid_col_min': gridline_bounds['grid_col_min'],
        'grid_col_max': gridline_bounds['grid_col_max'],
        # Grid source: where grid bounds came from
        # RECORD = from task name parsing (gridlines, row bounds)
        # DIM_LOCATION = from dim_location lookup (rooms, stairs, elevators)
        # NONE = no grid bounds available
        'grid_source': (
            'RECORD' if location_type == 'GRIDLINE' and gridline_bounds['grid_col_min'] is not None
            else 'DIM_LOCATION' if location_type in ('ROOM', 'STAIR', 'ELEVATOR') and gridline_bounds['grid_row_min'] is not None
            else 'NONE'
        ),
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
        # CSI Section (52-category classification, same as quality data)
        'dim_csi_section_id': csi_section_id,
        'csi_section': csi_section_code,
        'csi_title': csi_title,
        'csi_inference_source': csi_source,
    }
