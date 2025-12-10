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

    # Location type descriptions
    LOC_TYPES = {
        'RM': 'Room',
        'EL': 'Elevator',
        'ST': 'Stair',
        'GL': 'Gridline Area',
        'AR': 'Area Zone',
        'BL': 'Building Level',
        'BD': 'Building',
        'NA': 'Not Applicable'
    }

    # Building codes
    BUILDINGS = {'FAB', 'SUE', 'SUW', 'FIZ', 'CUB', 'GCS', 'GCSA', 'GCSB'}

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

    def extract_location(self, task_name: str, wbs_name: str = None) -> Tuple[str, Optional[str]]:
        """
        Extract location type and ID from task name and WBS.

        Args:
            task_name: The task name string
            wbs_name: Optional WBS name for additional context

        Returns:
            Tuple of (loc_type, loc_id) where loc_id may be None
        """
        combined = f"{task_name} {wbs_name or ''}".upper()

        # 1. FAB Room Code (highest precision)
        fab_match = re.search(r'FAB1?(\d{5,6})', combined)
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

        # 4. Gridline range (e.g., "GL 14-17", "17-18", "A-C/4-5")
        gl_match = re.search(r'GL[\s\-]*(\d+[\s\-]+\d+)', combined)
        if gl_match:
            gl_id = re.sub(r'\s+', '-', gl_match.group(1))
            return 'GL', f"GL{gl_id}"

        # Pattern: A1, B2, etc. (area designations)
        area_match = re.search(r'\b([AB][\-\s]?[1-5])\b', combined)
        if area_match:
            return 'AR', area_match.group(1).replace(' ', '').replace('-', '')

        # Pattern: SEA1-5, SWB1-5 (support building zones)
        zone_match = re.search(r'\b(S[EW][AB]\d)\b', combined)
        if zone_match:
            return 'AR', zone_match.group(1)

        # Gridline range in task name (e.g., "17-18", "13-9")
        gridline_match = re.search(r'\b(\d{1,2})[\s\-]+(\d{1,2})\b', combined)
        if gridline_match:
            g1, g2 = gridline_match.groups()
            return 'GL', f"GL{g1}-{g2}"

        # 5. Building + Level
        building = None
        level = None

        # Extract building
        for bldg in ['SUE', 'SUW', 'FIZ', 'CUB', 'FAB', 'GCS']:
            if bldg in combined:
                building = bldg
                break

        # Extract level
        level_match = re.search(r'\b([B]?\d)[F]?\b', combined)
        if level_match:
            level = level_match.group(1)
            if not level.endswith('F'):
                level = f"{level}F"

        if building and level:
            return 'BL', f"{building}-L{level.replace('F', '')}"

        if building:
            return 'BD', building

        # 6. Check for procurement/design tasks (no location)
        phase, _ = self.classify_phase_scope(task_name)
        if phase == 'PRE':
            return 'NA', None

        return 'NA', None

    def classify_task(self, task_name: str, wbs_name: str = None) -> Dict[str, Optional[str]]:
        """
        Full classification of a task.

        Args:
            task_name: The task name string
            wbs_name: Optional WBS name for additional context

        Returns:
            Dictionary with phase, scope, loc_type, loc_id, and full label
        """
        phase, scope = self.classify_phase_scope(task_name)
        loc_type, loc_id = self.extract_location(task_name, wbs_name)

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
            'label': label,
            'phase_desc': self.PHASES.get(phase, 'Unknown'),
            'scope_desc': self.SCOPES.get(phase, {}).get(scope, 'Unknown'),
            'loc_type_desc': self.LOC_TYPES.get(loc_type, 'Unknown')
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
