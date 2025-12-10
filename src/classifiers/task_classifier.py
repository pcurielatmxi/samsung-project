"""
Task Classifier for YATES Schedule Taxonomy

Classifies tasks into Phase, Scope Category, Location Type, and Location ID
based on task names and WBS hierarchy.

Usage:
    from src.classifiers import TaskClassifier

    classifier = TaskClassifier()
    result = classifier.classify_task(task_name, wbs_name)
    # Returns: {'phase': 'INT', 'scope': 'DRY', 'loc_type': 'RM', 'loc_id': 'FAB146103', 'label': 'INT-DRY|RM:FAB146103'}
"""

import re
from typing import Dict, Optional, Tuple


class TaskClassifier:
    """Classifier for YATES schedule tasks based on WBS taxonomy."""

    # Phase descriptions
    PHASES = {
        'PRE': 'Pre-Construction',
        'STR': 'Structure',
        'ENC': 'Enclosure',
        'INT': 'Interior',
        'COM': 'Commissioning',
        'ADM': 'Administrative',
        'UNK': 'Unknown'
    }

    # Scope descriptions by phase
    SCOPES = {
        'PRE': {'DES': 'Design', 'PRO': 'Procurement', 'SUB': 'Submittals', 'FAB': 'Fabrication'},
        'STR': {'PIR': 'Piers', 'FND': 'Foundations', 'UGD': 'Underground', 'CIP': 'Cast-in-Place',
                'CTG': 'Coating', 'STL': 'Structural Steel', 'PRC': 'Precast', 'MSC': 'Misc Steel'},
        'ENC': {'ROF': 'Roofing', 'PNL': 'Panels', 'WPF': 'Waterproofing', 'GLZ': 'Glazing',
                'CTG': 'Exterior Coating', 'MSC': 'Misc Enclosure'},
        'INT': {'FRM': 'Framing', 'DRY': 'Drywall', 'MEP': 'MEP Rough-in', 'FIR': 'Fire Protection',
                'FIN': 'Finishes', 'DOR': 'Doors & Hardware', 'SPE': 'Specialties', 'INS': 'Insulation',
                'ELV': 'Elevators', 'STR': 'Stairs', 'MSC': 'Misc Interior'},
        'COM': {'TST': 'Testing', 'TRN': 'Turnover'},
        'ADM': {'OWN': 'Owner Activities', 'IMP': 'Impacts/Delays', 'MIL': 'Milestones',
                'TRK': 'Tracking/Recovery', 'TMP': 'Temporary Works'},
        'UNK': {'UNK': 'Unknown'}
    }

    # Location type descriptions (precision level of location)
    LOC_TYPES = {
        'RM': 'Room',
        'EL': 'Elevator',
        'ST': 'Stair',
        'GL': 'Gridline',
        'AR': 'Area',
        'GEN': 'General/Project-Wide'
    }

    # Building codes and their full names
    BUILDINGS = {
        'FAB': 'Main FAB',
        'SUE': 'Support East',
        'SUW': 'Support West',
        'FIZ': 'FAB Integration Zone',
        'CUB': 'Central Utilities',
        'GCS': 'Gas/Chemical Supply',
        'GCSA': 'GCS Building A',
        'GCSB': 'GCS Building B',
        # Special codes for unresolved buildings
        'GEN': 'Project-Wide',
        'MULTI': 'Multiple Buildings',
        'UNK': 'Unknown'
    }

    # Level codes and their descriptions
    LEVELS = {
        '1': 'Level 1',
        '2': 'Level 2',
        '3': 'Level 3',
        '4': 'Level 4',
        '5': 'Level 5',
        '6': 'Level 6',
        'B1': 'Basement 1',
        # Special codes for unresolved levels
        'GEN': 'Project-Wide',
        'MULTI': 'Multiple Levels',
        'UNK': 'Unknown'
    }

    # FAB room code building digit mapping: FAB1{level}{building_digit}{room}
    # e.g., FAB146103 = Level 4, Building 6 (SUE), Room 103
    FAB_BUILDING_MAP = {
        '0': 'SUW',   # Support West
        '6': 'SUE',   # Support East
        '2': 'FAB',   # Main FAB interior (tentative)
        '4': 'FIZ',   # FAB Integration Zone (tentative)
    }

    # Area zone to building mapping
    AREA_BUILDING_MAP = {
        'SWA': 'SUW', 'SWB': 'SUW',  # Support West areas
        'SEA': 'SUE', 'SEB': 'SUE',  # Support East areas
    }

    def __init__(self):
        """Initialize the classifier."""
        pass

    def classify_phase_scope(self, task_name: str) -> Tuple[str, str]:
        """
        Classify task into Phase and Scope Category.

        Args:
            task_name: The task name string

        Returns:
            Tuple of (phase_code, scope_code)
        """
        t = str(task_name).upper()

        # ============ PRE-CONSTRUCTION ============
        if re.search(r'SUBCONTRACT|AWARD|BUYOUT|BID(?:DING)?\b|EXECUTE.*CONTRACT|CONTRACT AGREEMENT', t):
            return 'PRE', 'PRO'  # Procurement
        if re.search(r'SHOP DRAWING|SUBMITTAL|RFA\b|APPROV|REVIEW|JACOBS|RFI|FIELD MEASURE|RESPONSE|ARCHITECTURAL SET', t):
            return 'PRE', 'SUB'  # Submittals
        if re.search(r'FABRICAT|LEAD TIME|DELIVER(?!Y)|CONSOLIDATED SET|MOCKUP|FAB &|MATERIAL.*ORDER', t):
            return 'PRE', 'FAB'  # Fabrication
        if re.search(r'DESIGN|ENGINEER|RESOLVE.*ISSUE|RELEASE FOR FAB|IF[CR]\b|ISSUED FOR|DWG ISSUED|MATERIAL SCHEDULE', t):
            return 'PRE', 'DES'  # Design

        # ============ STRUCTURE ============
        if re.search(r'PIER|DRILL|CAISSON', t):
            return 'STR', 'PIR'  # Piers
        if re.search(r'FOUNDATION|FOOTING|GRADE BEAM', t):
            return 'STR', 'FND'  # Foundations
        if re.search(r'UNDERGROUND|U/G\b|FRENCH DRAIN|EXCAVATE|BACKFILL|LOADING DOCK.*FILL', t):
            return 'STR', 'UGD'  # Underground
        if re.search(r'SLAB|POUR\b|CURE\b|SOMD|FRP\b|CONCRETE|WAFFLE|CURB|EQPT PAD|F/R/P|CIP\b|KNEE WALL|GROUT|PATCH|OAC PAD', t):
            return 'STR', 'CIP'  # Cast-in-Place
        if re.search(r'COLUMN.*(?:PRIME|COAT|GRIND)|(?:PRIME|COAT|GRIND).*COLUMN|SEALER|DENSIFIER|CRC\b', t):
            return 'STR', 'CTG'  # Structural Coating
        if re.search(r'DECKING|DECK\b|DS/AD|DETAILING|DETAINING|GIRDER|TRUSS|ERECT.*STEEL|ANCHOR BOLT|GOAL POST|CLIP|EMBED|HEADER|BASE.?PLATE|MODIFY.*PATRIOT|FLANGE EXTENSION', t):
            return 'STR', 'STL'  # Structural Steel
        if re.search(r'STEEL', t) and not re.search(r'STUD|STAIR', t):
            return 'STR', 'STL'  # Structural Steel (catch-all)
        if re.search(r'PRECAST|PC\b.*ERECT', t):
            return 'STR', 'PRC'  # Precast
        if re.search(r'GRATING|PLATFORM(?!.*SCAFFOLD)|RAMP(?!.*CRANE)', t):
            return 'STR', 'MSC'  # Misc Steel

        # ============ ENCLOSURE ============
        if re.search(r'ROOF(?!.*DECK)|ROOFING|MEMBRANE|PARAPET', t):
            return 'ENC', 'ROF'  # Roofing
        if re.search(r'METAL PANEL|IMP\b|INSULATED PANEL|SHEATHING|CANOPY|ACM PANEL|CLADDING', t):
            return 'ENC', 'PNL'  # Panels
        if re.search(r'WATERPROOF|DAMPPROOF|DRAIN MAT|WEEP TUBE', t):
            return 'ENC', 'WPF'  # Waterproofing
        if re.search(r'WINDOW|GLAZING|CURTAIN WALL|STOREFRONT', t):
            return 'ENC', 'GLZ'  # Glazing
        if re.search(r'EXTERIOR.*(?:COAT|PAINT)|(?:COAT|PAINT).*EXTERIOR|PRIME.*COAT|COAT.*TOP', t):
            return 'ENC', 'CTG'  # Exterior Coating
        if re.search(r'LOUVER|PENTHOUSE|BREEZEWAY|AWNING', t):
            return 'ENC', 'MSC'  # Misc Enclosure

        # ============ INTERIOR ============
        if re.search(r'METAL STUD|FRAMING|STUD FRAME', t):
            return 'INT', 'FRM'  # Framing
        if re.search(r'DRYWALL|TAPE.*FINISH|GYPSUM|BOARD\b|SHEETROCK|TAPE\s*&\s*FLOAT', t):
            return 'INT', 'DRY'  # Drywall
        if re.search(r'MEP|ROUGH.?IN|CONDUIT|ELECTRICAL|PLUMB(?!ING.*UNDER)', t) and not re.search(r'FIRE|UNDERGROUND', t):
            return 'INT', 'MEP'  # MEP Rough-in
        if re.search(r'FIRE(?!PROOF)|SPRINKLER|CAULK|FIRESTOP|SMOKE', t):
            return 'INT', 'FIR'  # Fire Protection
        if re.search(r'PAINT(?!.*COLUMN)|TILE\b|FLOORING|CEILING(?!.*SYSTEM)|FINISH(?!.*DRYWALL)|CONTROL JOINT|EPOXY|VCT|ACCESS FLOOR|FLOOR STRIP|STRIPING|JOINT SEALANT|ALUMINIUM COVER|EMSEAL|EXPANSION JOINT', t):
            return 'INT', 'FIN'  # Finishes
        if re.search(r'DOOR|FRAME(?!.*STUD)|HARDWARE|HOLLOW METAL|DOCK LOCK|DOCK GUARDIAN', t):
            return 'INT', 'DOR'  # Doors & Hardware
        if re.search(r'WALL PROTECTION|CORNER GUARD|ACCESSORI|TOILET PARTITION|SIGNAGE|EXPANSION.*CONTROL|DOCK LEVELER|PEDESTAL.*LAM|DIV\s*10|SPECIALIT', t):
            return 'INT', 'SPE'  # Specialties
        if re.search(r'INSULATION|INSUL\b', t):
            return 'INT', 'INS'  # Insulation
        if re.search(r'ELEVATOR(?!.*STEEL)|ELEV\b', t):
            return 'INT', 'ELV'  # Elevators
        if re.search(r'STAIR(?!.*STEEL)', t):
            return 'INT', 'STR'  # Stairs
        if re.search(r'VESTIBULE|BATHROOM|TOILET|RESTROOM|CLEAN ROOM|DUCT SHAFT|AIR.?LOCK|BUNKER|I/?O\s*R[O]?M', t):
            if re.search(r'CONTROL JOINT|FINISH|INSPECT|TAPE|FLOAT', t):
                return 'INT', 'FIN'
            return 'INT', 'MSC'

        # ============ COMMISSIONING ============
        if re.search(r'TEST(?!ING.*IMPACT)|COMMISSION|ENERGIZE|START.?UP|PUNCH|INPSECTION|INSPECTION', t):
            return 'COM', 'TST'  # Testing
        if re.search(r'TURNOVER|SUBSTANTIAL|HANDOVER|BENEFICIAL', t):
            return 'COM', 'TRN'  # Turnover

        # ============ ADMINISTRATIVE ============
        if re.search(r'^OWNER|^SECAI|^GC\s|^YATES', t):
            return 'ADM', 'OWN'  # Owner Activities
        if re.search(r'IMPACT|DELAY|HOLD\b|WAITING|REWORK|REMEDIATION|REMIDIATION|PENDING', t):
            return 'ADM', 'IMP'  # Impacts/Delays
        if re.search(r'MILESTONE|COMPLETE$|TARGET', t):
            return 'ADM', 'MIL'  # Milestones
        if re.search(r'PRIORITY|REMOBIL|OUT.?OF.?SEQUENCE|FRAGNET|IOCC|CATCH.?UP', t):
            return 'ADM', 'TRK'  # Tracking/Recovery
        if re.search(r'SCAFFOLD|TEMP\b|TEMPORARY|PROTECTION|BARRICADE|HOIST|CRANE RAMP|TOWER CRANE', t):
            return 'ADM', 'TMP'  # Temporary Works

        # ============ CATCH-ALL ============
        if re.search(r'^INSTALL\b', t):
            return 'INT', 'MSC'

        return 'UNK', 'UNK'

    def extract_building_level(self, task_name: str, wbs_name: str = None,
                                loc_type: str = None, loc_id: str = None) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract building and level as separate values.

        Sources (in priority order):
        1. FAB room code (e.g., FAB146103 → SUE, Level 4)
        2. Area zone (e.g., SWA1 → SUW)
        3. Explicit in task/WBS text (e.g., "3F, SUE")

        Args:
            task_name: The task name string
            wbs_name: Optional WBS name
            loc_type: Already extracted location type
            loc_id: Already extracted location ID

        Returns:
            Tuple of (building, level) where either may be None
        """
        task_upper = str(task_name).upper()
        wbs_upper = str(wbs_name or '').upper()
        combined = f"{task_upper} {wbs_upper}"

        building = None
        level = None

        # 1. Extract from FAB room code: FAB1{level}{building_digit}{room}
        if loc_type == 'RM' and loc_id:
            fab_match = re.match(r'FAB1(\d)(\d)', loc_id)
            if fab_match:
                level = fab_match.group(1)
                building_digit = fab_match.group(2)
                building = self.FAB_BUILDING_MAP.get(building_digit)

        # 2. Extract from area zone prefix in loc_id (SWA, SEA, etc.)
        if loc_id:
            zone_prefix = loc_id[:3] if len(loc_id) >= 3 else None
            if zone_prefix in self.AREA_BUILDING_MAP:
                building = self.AREA_BUILDING_MAP[zone_prefix]

        # 3. Extract from area zone pattern in text (SEA1, SWA2, etc.) if not from loc_id
        if not building:
            zone_match = re.search(r'\b(S[EW][AB])\d', combined)
            if zone_match:
                zone_prefix = zone_match.group(1)
                building = self.AREA_BUILDING_MAP.get(zone_prefix)

        # 4. Extract building from text patterns (if not yet found)
        if not building:
            # Use word boundaries to avoid matching inside words like "SUPPORT"
            for bldg, pattern in [
                ('SUW', r'\bWSUP\b|\bW[\-\s]?SUP\b|\bSUW\b'),
                ('SUE', r'\bESUP\b|\bE[\-\s]?SUP\b|\bSUE\b'),
                ('FIZ', r'\bFIZ\b'),
                ('CUB', r'\bCUB\b'),
                ('FAB', r'\bFAB\b'),
                ('GCSA', r'\bGCS[\-\s]?A\b'),
                ('GCSB', r'\bGCS[\-\s]?B\b'),
                ('GCS', r'\bGCS\b'),
            ]:
                if re.search(pattern, combined):
                    building = bldg
                    break

        # 5. Extract level from text (if not from FAB code)
        if not level:
            # Patterns: L1, L2, 1F, 2F, B1, B1F, -4F-, etc.
            level_match = re.search(r'\bL(\d)\b|\b([B]?\d)F\b', combined)
            if level_match:
                level = level_match.group(1) or level_match.group(2)

        # 6. Apply fallbacks for missing building/level
        # Determine reason for missing values based on context
        if not building:
            if loc_type == 'GEN':
                building = 'GEN'  # General/Project-Wide - no specific building
            elif loc_type in ('GL', 'AR'):
                building = 'MULTI'  # Gridlines/Areas often span multiple buildings
            else:
                building = 'UNK'  # Unknown - should have building but couldn't extract

        if not level:
            if loc_type == 'GEN':
                level = 'GEN'  # General/Project-Wide - no specific level
            elif loc_type in ('GL', 'AR', 'EL', 'ST'):
                level = 'MULTI'  # These often span multiple levels
            else:
                level = 'UNK'  # Unknown - should have level but couldn't extract

        return building, level

    def extract_location(self, task_name: str, wbs_name: str = None) -> Tuple[str, Optional[str]]:
        """
        Extract location type and ID from task name and WBS.

        Priority: WBS FAB codes take precedence over task FAB codes to maintain
        consistency with the WBS structure familiar to the customer.

        Args:
            task_name: The task name string
            wbs_name: Optional WBS name for additional context

        Returns:
            Tuple of (loc_type, loc_id) where loc_id may be None
        """
        task_upper = str(task_name).upper()
        wbs_upper = str(wbs_name or '').upper()
        combined = f"{task_upper} {wbs_upper}"

        # 1. FAB Room Code (highest precision)
        # Priority: WBS FAB code > Task FAB code (WBS structure is customer-facing)
        wbs_fab_match = re.search(r'FAB1?(\d{5,6})', wbs_upper) if wbs_name else None
        task_fab_match = re.search(r'FAB1?(\d{5,6})', task_upper)

        fab_match = wbs_fab_match or task_fab_match
        if fab_match:
            return 'RM', f"FAB1{fab_match.group(1)}"

        # 2. Elevator code
        el_match = re.search(r'EL(?:EV(?:ATOR)?)?[\s\-]*(\d{1,2})', combined)
        if el_match:
            return 'EL', f"EL{el_match.group(1).zfill(2)}"

        # 3. Stair code
        st_match = re.search(r'ST(?:AIR)?[\s\-]*(\d{1,2})', combined)
        if st_match:
            return 'ST', f"ST{st_match.group(1).zfill(2)}"

        # 4. Gridline patterns
        # Range: "GL 14-17", "17-18"
        gl_match = re.search(r'GL[\s\-]*(\d+[\s\-]+\d+)', combined)
        if gl_match:
            gl_id = re.sub(r'\s+', '-', gl_match.group(1))
            return 'GL', f"GL{gl_id}"

        # Single: "GL 33", "GL5"
        gl_single = re.search(r'\bGL[\s\-]?(\d{1,2})\b', combined)
        if gl_single:
            return 'GL', f"GL{gl_single.group(1)}"

        # Letter line: "A LINE", "B LINE"
        line_match = re.search(r'\b([A-N])\s*LINE\b', combined)
        if line_match:
            return 'GL', f"GL-{line_match.group(1)}"

        # Numeric range: "17-18", "13-9"
        gridline_match = re.search(r'\b(\d{1,2})[\s\-]+(\d{1,2})\b', combined)
        if gridline_match:
            g1, g2 = gridline_match.groups()
            return 'GL', f"GL{g1}-{g2}"

        # 5. Area patterns
        # Penthouse: NE/NW/SE/SW PENTHOUSE
        pent_match = re.search(r'\b(N[EW]|S[EW])\s*PENTHOUSE\b', combined)
        if pent_match:
            return 'AR', f"PENT-{pent_match.group(1)}"

        # Milestone areas: A1, B2, etc.
        area_match = re.search(r'\b([AB][\-\s]?[1-5])\b', combined)
        if area_match:
            return 'AR', area_match.group(1).replace(' ', '').replace('-', '')

        # Support zones: SEA1, SWA1, SWB1, SEB1 (with optional hyphen)
        zone_match = re.search(r'\b(S[EW][AB])[\-]?(\d)\b', combined)
        if zone_match:
            return 'AR', f"{zone_match.group(1)}{zone_match.group(2)}"

        # Trade Impact Areas: TIA-1, E.TIA-1
        tia_match = re.search(r'TIA[\-]?(\d)', combined)
        if tia_match:
            return 'AR', f"TIA{tia_match.group(1)}"

        # 6. General/Project-Wide (no specific location identifier found)
        return 'GEN', None

    def classify_task(self, task_name: str, wbs_name: str = None) -> Dict[str, Optional[str]]:
        """
        Full classification of a task.

        Args:
            task_name: The task name string
            wbs_name: Optional WBS name for additional context

        Returns:
            Dictionary with phase, scope, loc_type, loc_id, building, level, and full label
        """
        phase, scope = self.classify_phase_scope(task_name)
        loc_type, loc_id = self.extract_location(task_name, wbs_name)
        building, level = self.extract_building_level(task_name, wbs_name, loc_type, loc_id)

        # Build label
        if loc_id:
            label = f"{phase}-{scope}|{loc_type}:{loc_id}"
        else:
            label = f"{phase}-{scope}|{loc_type}"

        return {
            'phase': phase,
            'scope': scope,
            'loc_type': loc_type,
            'loc_id': loc_id,
            'building': building,
            'level': level,
            'label': label,
            'phase_desc': self.PHASES.get(phase, 'Unknown'),
            'scope_desc': self.SCOPES.get(phase, {}).get(scope, 'Unknown'),
            'loc_type_desc': self.LOC_TYPES.get(loc_type, 'Unknown'),
            'building_desc': self.BUILDINGS.get(building, 'Unknown'),
            'level_desc': self.LEVELS.get(level, f'Level {level}' if level and level.isdigit() else 'Unknown')
        }

    def get_phase_description(self, phase: str) -> str:
        """Get description for a phase code."""
        return self.PHASES.get(phase, 'Unknown')

    def get_scope_description(self, phase: str, scope: str) -> str:
        """Get description for a scope code."""
        return self.SCOPES.get(phase, {}).get(scope, 'Unknown')

    def get_loc_type_description(self, loc_type: str) -> str:
        """Get description for a location type code."""
        return self.LOC_TYPES.get(loc_type, 'Unknown')


def main():
    """Test the classifier with sample tasks."""
    classifier = TaskClassifier()

    test_cases = [
        ("Complete Drywall & Inspect --Duct Shaft-4F-SUE-FAB146103", "Duct Shaft-4F-SUE-FAB146103"),
        ("PH1-ERECT STEEL TRUSSES - A1 - 17-18", None),
        ("INSTALL DOOR FRAMES - FAB146106A", None),
        ("METAL STUD FRAMING - SEA3 - GL 13-9 - L1", None),
        ("Execute Subcontract - Toilet Accessories", None),
        ("OWNER - TEMPORARY POWER FOR FAB 1F", None),
        ("POUR SLAB ON METAL DECK - AREA B2", None),
    ]

    print("Task Classifier Test Results")
    print("=" * 80)

    for task_name, wbs_name in test_cases:
        result = classifier.classify_task(task_name, wbs_name)
        print(f"\nTask: {task_name}")
        if wbs_name:
            print(f"WBS: {wbs_name}")
        print(f"  Phase: {result['phase']} ({result['phase_desc']})")
        print(f"  Scope: {result['scope']} ({result['scope_desc']})")
        print(f"  Location: {result['loc_type']} ({result['loc_type_desc']})")
        print(f"  Loc ID: {result['loc_id']}")
        print(f"  Label: {result['label']}")


if __name__ == "__main__":
    main()
