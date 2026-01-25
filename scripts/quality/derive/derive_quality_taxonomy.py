"""
Derive taxonomy fields from quality inspection data.

Extracts Building, Level, Gridline, Location, and Scope from inspection records
to enable linking with P6 schedule tasks.

Inputs:
    - data/processed/quality/secai_inspection_log.csv
    - data/processed/quality/yates_all_inspections.csv

Outputs:
    - data/processed/quality/secai_taxonomy.csv
    - data/processed/quality/yates_taxonomy.csv
"""

import re
import sys
import pandas as pd
from pathlib import Path
from typing import Optional, Tuple, Dict

# Add project root to path for settings import
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings

# Paths - use settings for proper path resolution
PROCESSED_DIR = Settings.PROCESSED_DATA_DIR / "quality"
OUTPUT_DIR = PROCESSED_DIR  # Output to processed directory

# Building codes
BUILDINGS = ['FAB', 'SUE', 'SUW', 'FIZ', 'CUB', 'GCS', 'GCSA', 'GCSB', 'OB1', 'OB2']

# Scope patterns based on TaskClassifier
# Order matters - more specific patterns should come before general ones
SCOPE_PATTERNS = [
    # Interior - Clean Room / SCP (check first - specialized MEP)
    (r'CLEAN\s*ROOM|SCP\b|SCP\s*PANEL', 'MEP'),
    # Interior - MEP (check first to catch PANELBOARD before BOARD)
    (r'ELECTRICAL|CONDUIT|CABLE|RACEWAY|PANELBOARD|SWITCHGEAR|WIRING|LIGHTING|LOW VOLTAGE|GROUNDING|PLUMB|HVAC|DUCT|PIPE(?!.*RACK)', 'MEP'),
    # Interior - Drywall (GYPSUM BOARD, not PANELBOARD)
    (r'DRYWALL|GYPSUM|GYPSUM BOARD|SHEETROCK|1ST LAYER|2ND LAYER|3RD LAYER|TAPE.*FINISH|CLOSURE WALL', 'DRY'),
    # Interior - Framing
    (r'FRAMING|METAL STUD|STUD FRAME|BOTTOM PLATE', 'FRM'),
    # Interior - Fire Protection
    (r'FIRE|SPRINKLER|CAULK|FIRESTOP|SMOKE|SFRM|FIRE SPRAY|FIREPROOF', 'FIR'),
    # Structure - Waterproofing/Coating (before FIN to catch CRC before COATING)
    (r'WATERPROOF|CRC|DENSIFIER|SEALER|BLUESKIN', 'WPF'),
    # Interior - Finishes (after WPF so CRC doesn't match COATING)
    (r'PAINT|TILE|FLOORING|CEILING|COATING|EPOXY|VCT|FINISH(?!.*DRYWALL)', 'FIN'),
    # Interior - Doors
    (r'DOOR|FRAME|HARDWARE|HOLLOW METAL', 'DOR'),
    # Interior - Insulation
    (r'INSULATION|INSUL\b', 'INS'),
    # Interior - Elevators
    (r'ELEVATOR|ELEV\b', 'ELV'),
    # Interior - Stairs
    (r'STAIR', 'STR_INT'),
    # Structure - Concrete
    (r'CONCRETE|SLAB|POUR|REBAR|FORMWORK|CIP|PLACEMENT|TOPPING', 'CIP'),
    # Structure - Steel
    (r'STEEL|WELD|BOLT|DECK|ERECT|TRUSS|GIRDER|EMBED|ANCHOR', 'STL'),
    # Enclosure - Roofing
    (r'ROOF|MEMBRANE|PARAPET', 'ROF'),
    # Drill & Epoxy (common inspection type)
    (r'DRILL.*EPOXY|EPOXY.*DRILL', 'STL'),
]

# Failure reason categorization patterns
# Maps regex patterns to category names for classifying inspection failures
# Order matters - first match wins, so more specific patterns should come first
FAILURE_CATEGORIES = [
    # Time-based failures (check first - very specific)
    (r'72.?hour|72.?hr|72 hour', '72-Hour Deadline Exceeded'),

    # Process/administrative issues
    (r'process.*(noncompli|violation)|not follow.*(process|procedure)|inspection process|wir.*same day|not given.*hour|wir.*change|make a new wir|wir.*need.*create|wir.*place.*fail', 'Inspection Process Violation'),
    (r'submittal.*(not|reject)|not.*approv|approval.*not|shop drawing.*not|please provide.*submittal', 'Submittal/Approval Issues'),
    (r'no.*(seci|cm).*inspection|no cm\b|no documentation.*uploaded|no.*seci.*cm', 'Missing CM Inspection'),
    (r'document|photo|not provided|not uploaded|cover sheet|not signed|blank.*missing.*document|missing.*most.*document|missing required document|please provide.*picture|legible.*drawing|requesting.*drawing|see.*attached.*report', 'Documentation Issues'),
    (r'not.*attached|attachment.*missing|redline.*drawing|drawing.*upload|please see.*attached|attched', 'Documentation Issues'),
    (r'coversheet.*match|building.*name.*match|package.*name.*reflect', 'Documentation Issues'),

    # Work quality issues - GENERAL (broad, kept because high value count)
    (r'missing.*work|incomplete.*work|(?<!of\s)missing(?!\s*(document|inspection|cm))|not complete|not finish|installation.*not complete', 'Missing/Incomplete Work'),
    (r'contaminat|debris|dirt|dust|clean', 'Contamination/Debris'),

    # Work quality issues - MATERIAL DEFECTS
    (r'bare metal|bare bolt|rust|improper.*prep|surface.*not.*prep|not.*properly.*prep|primed.*substrate', 'Surface Prep/Coating'),
    (r'damage|crack|dent|broken|scratch|foreign material|defect.*found', 'Damage/Defect'),
    (r'runs|drips|sags|overspray|missed area|coating.*not.*accept', 'Coating/Paint Defect'),

    # Installation issues - Specific defects
    (r'screw|fastener|bolt|nail|torque|megger.*test', 'Screw/Fastener Issues'),
    (r'weld|undercut|arc mark|slag|bad weld', 'Installation Defect'),
    (r'improper.*spacing|wrong.*spacing|incorrect.*spacing|stacking|cables.*pressed|tray.*not.*support|conduit.*block', 'Installation Defect'),
    (r'wrong.*type|incorrect.*type|wrong.*box|wrong.*track|wrong.*depth|wrong.*detail', 'Installation Defect'),
    (r'not.*flush|incorrect.*detail|deviation|drilling|knocking.*hole', 'Installation Defect'),
    (r'wrong.*segment|wrong.*support|improper.*clamp|incorrect.*clamp', 'Installation Defect'),
    (r'cables.*being in.*raceway|cables.*installed.*before|cables pulled to', 'Installation Defect'),
    (r'frame.*not.*design|not doing.*job|not installed|installation.*NOT|panel.*NOT installed|nonconform', 'Installation Defect'),
    (r'caulk|seal|patch', 'Installation Defect'),
    (r'level|align|plumb|sagging|bowing|orientation|slope|pitch|backpitch', 'Alignment/Level Issues'),
    (r'framing.*incomplete|missing.*screw|missing.*bracket|missing.*stud|wrong.*stud', 'Framing Defect'),
    (r'\bbent\b|kinked|deform|damage(?!.*prep)', 'Installation Defect'),
    (r'loose.*hardware|hardware.*loose', 'Installation Defect'),
    (r'checklist.*not.*pass|did not pass|face beam|failed at.*lb', 'Installation Defect'),

    # Design/drawing issues (consolidated)
    (r'drawing.*not.*match|not per drawing|incorrect.*drawing|design.*change|design.*deviat|deviation.*design', 'Specification Non-Compliance'),
    (r'does not meet.*spec|not.*per.*spec|spec.*violation|wrong.*per.*spec|not.*per.*code|requirement', 'Specification Non-Compliance'),
    (r'required.*depth|depth.*less than|depth.*specification', 'Specification Non-Compliance'),
    (r'termination.*tag|term.*point|cable.*schedule|termination.*match', 'Specification Non-Compliance'),
    (r'ul.*detail|emergency.*light|red dot|work.*space.*clearance', 'Specification Non-Compliance'),
    (r'code|violation|safety|unrelated.*picture|legible', 'Code/Spec Violation'),
    (r'equipment.*not|panel.*not.*match|breaker.*not|device.*not|mismatch|wrong.*panel', 'Specification Non-Compliance'),

    # Test failures
    (r'pressure.*(test|loss|fail)|test.*pressure|leak|head test|hydro.*test|density.*test', 'Test Failure'),
    (r'meg.*report|witnessing.*point|initial.*setting', 'Test Failure'),

    # Access/No-show/Prerequisites
    (r'no scissor|no access|could not.*access|unable to see|ceiling.*not.*remov', 'Access/Visibility Issue'),
    (r'never showed|no.show|not available|no one from|left.*site|crew.*left|subcontractor.*left|not show|no one.*behaf|no.*subcontractor.*on site|no one showed up', 'No-Show/Crew Not Available'),
    (r'layer.*before.*inspect|before.*was inspect|sequence|not completed.*because|not finished.*because', 'Work Sequence Issue'),
    (r'not ready|pending|still needs|not acceptable|will not accept|too many.*wrong', 'Work Not Ready'),

    # Trade-specific issues (kept due to reasonable value count)
    (r'electrical.*metallic|emt(?!\s*approved)|cable.*not.*bond|cable.*spec|conduit.*spec|ground|grounding.*electrode|panelboard|pull.*box|fiber.*optic|conductor.*megger', 'Electrical/Cable Issue'),
    (r'fire caulk|firestop|fire.*proof', 'Fire Protection Issue'),
    (r'plumb|drainage|drain|water|hydro|hub.*drain', 'Plumbing/Drainage Issue'),
]


def extract_building(text: str) -> Optional[str]:
    """Extract building code from location text."""
    if not text or pd.isna(text):
        return None

    text_upper = str(text).upper()

    # Check for explicit building codes
    # SUE/SUW patterns (Support East/West)
    if re.search(r'\bSUE\b|E[\-\s]?SUP|ESUP|\bSUP.*EAST|SUPPORT.*EAST', text_upper):
        return 'SUE'
    if re.search(r'\bSUW\b|W[\-\s]?SUP|WSUP|\bSUP.*WEST|SUPPORT.*WEST', text_upper):
        return 'SUW'

    # GCS Building A/B
    if re.search(r'GCS[\-\s]?A|GCSA|GCS.*BLDG\s*A', text_upper):
        return 'GCSA'
    if re.search(r'GCS[\-\s]?B|GCSB|GCS.*BLDG\s*B', text_upper):
        return 'GCSB'

    # Other buildings
    for bldg in BUILDINGS:
        if re.search(rf'\b{bldg}\b', text_upper):
            return bldg

    # Infer from gridlines if no explicit building
    # A-D gridlines typically = SUE side
    # K-N gridlines typically = SUW side
    # E-J gridlines = FAB core
    if re.search(r'\b[A-D][/\-\s]', text_upper):
        return 'FAB'  # Could be SUE area but default to FAB
    if re.search(r'\b[K-N][/\-\s]', text_upper):
        return 'FAB'  # Could be SUW area but default to FAB

    return None


def extract_level(text: str) -> Optional[str]:
    """Extract level/floor from location text."""
    if not text or pd.isna(text):
        return None

    text_upper = str(text).upper()

    # Patterns: "Level 3", "L3", "3F", "LVL 3", "LEVEL 1-4" (take first)
    level_match = re.search(r'LEVEL\s*(\d)', text_upper)
    if level_match:
        return level_match.group(1)

    # Handle truncated "evel 3" (missing L at start)
    level_match = re.search(r'\bEVEL\s*(\d)', text_upper)
    if level_match:
        return level_match.group(1)

    # L1, L2, L3 etc. Also handle mezzanine: L1M, L2M -> extract base level
    level_match = re.search(r'\bL(\d)M?\b', text_upper)
    if level_match:
        return level_match.group(1)

    level_match = re.search(r'\b(\d)F\b', text_upper)
    if level_match:
        return level_match.group(1)

    level_match = re.search(r'LVL\s*(\d)', text_upper)
    if level_match:
        return level_match.group(1)

    # Basement
    if re.search(r'\bB1\b|BASEMENT', text_upper):
        return 'B1'

    return None


def extract_gridline(text: str) -> Optional[str]:
    """Extract gridline reference from location text."""
    if not text or pd.isna(text):
        return None

    text_upper = str(text).upper()

    # Pattern: "A-D/17-20", "K-L/30-33", "C/5-6"
    # Letter(s)/Number range with slash separator
    grid_match = re.search(r'([A-N](?:[\.\d]*)?(?:\s*-\s*[A-N](?:[\.\d]*)?)?)\s*/\s*(\d+(?:[\.\d]*)?(?:\s*-\s*\d+(?:[\.\d]*)?)?)', text_upper)
    if grid_match:
        letters = grid_match.group(1).replace(' ', '')
        numbers = grid_match.group(2).replace(' ', '')
        return f"{letters}/{numbers}"

    # Pattern: "A11-B14", "G13-J13", "J25-J26" (letter+number - letter+number, no slash)
    grid_match = re.search(r'\b([A-N])(\d+)\s*-\s*([A-N])(\d+)\b', text_upper)
    if grid_match:
        return f"{grid_match.group(1)}-{grid_match.group(3)}/{grid_match.group(2)}-{grid_match.group(4)}"

    # Pattern: "GL 14-17"
    gl_match = re.search(r'GL\s*(\d+(?:\s*-\s*\d+)?)', text_upper)
    if gl_match:
        return f"GL{gl_match.group(1).replace(' ', '')}"

    # Pattern: standalone gridline like "33 LINE", "A LINE"
    line_match = re.search(r'\b([A-N]|\d{1,2})\s*LINE\b', text_upper)
    if line_match:
        return f"GL-{line_match.group(1)}"

    return None


def extract_specific_location(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract specific location ID (room code, elevator, stair) and its type.

    Returns:
        Tuple of (location_id, location_type) where type is 'RM', 'EL', 'ST', etc.
    """
    if not text or pd.isna(text):
        return None, None

    text_upper = str(text).upper()

    # FAB room code: FAB1XXXXX or FAB XXXXXX
    fab_match = re.search(r'FAB1?(\d{5,6})', text_upper)
    if fab_match:
        return f"FAB1{fab_match.group(1)}", 'RM'

    # Elevator: "Elevator 3", "ELEV 19", "EL #2"
    elev_match = re.search(r'ELEV(?:ATOR)?\s*#?\s*(\d{1,2})', text_upper)
    if elev_match:
        return f"EL{elev_match.group(1).zfill(2)}", 'EL'

    # Stair: "Stair 3", "STAIR #46"
    stair_match = re.search(r'STAIR\s*#?\s*(\d{1,2})', text_upper)
    if stair_match:
        return f"ST{stair_match.group(1).zfill(2)}", 'ST'

    # Room references
    room_match = re.search(r'ROOM\s*#?\s*(\d+)', text_upper)
    if room_match:
        return f"RM{room_match.group(1)}", 'RM'

    # Sector references (GCS): "Sector 12", "SEC 4"
    sector_match = re.search(r'SEC(?:TOR)?\s*(\d+)', text_upper)
    if sector_match:
        return f"SEC{sector_match.group(1).zfill(2)}", 'SEC'

    # Penthouse
    if re.search(r'PENTHOUSE|PH\b', text_upper):
        # Try to get direction (NE, NW, SE, SW)
        dir_match = re.search(r'\b(N[EW]|S[EW])\b', text_upper)
        if dir_match:
            return f"PH-{dir_match.group(1)}", 'PH'
        return 'PH', 'PH'

    return None, None


def build_location_id(building: Optional[str], level: Optional[str],
                      gridline: Optional[str], specific_id: Optional[str],
                      specific_type: Optional[str]) -> Optional[str]:
    """
    Build a composite location_id from extracted components.

    Priority:
    1. Specific ID (elevator, stair, room) - most precise
    2. Composite: Building-Level-Gridline
    3. Composite: Building-Level (if no gridline)
    4. Building only (if no level)

    Returns:
        Composite location_id string or None
    """
    # Priority 1: Use specific ID if available
    if specific_id:
        # Prepend building context if we have it and ID doesn't include it
        if building and not specific_id.startswith(('FAB1', building)):
            if level:
                return f"{building}-L{level}-{specific_id}"
            return f"{building}-{specific_id}"
        return specific_id

    # Priority 2-4: Build composite from components
    parts = []

    if building:
        parts.append(building)

    if level:
        parts.append(f"L{level}")

    if gridline:
        # Normalize gridline format
        grid_normalized = gridline.replace(' ', '')
        parts.append(grid_normalized)

    if parts:
        return '-'.join(parts)

    return None


def extract_scope(text: str) -> Optional[str]:
    """Extract scope code from inspection description or template."""
    if not text or pd.isna(text):
        return None

    text_upper = str(text).upper()

    for pattern, scope in SCOPE_PATTERNS:
        if re.search(pattern, text_upper):
            return scope

    return None


def categorize_failure_reason(text: str) -> Optional[str]:
    """
    Categorize failure reason text into predefined categories.

    Uses keyword pattern matching to classify free-text failure descriptions
    into standardized categories for analysis.

    Returns:
        Category name string, or None if text is empty/null
    """
    if not text or pd.isna(text):
        return None

    text_lower = str(text).lower()

    # Strip common prefixes that hide the real reason
    # e.g., "MSR-FSR Fail" or "SECAI/FST Fail" or "SECAI Fail"
    text_lower = re.sub(r'^(msr-?fsr\s+fail|secai/?fst\s+fail|secai\s+fail|msr\s+fail|fst\s+fail|failed?[:\s]+)', '', text_lower)

    for pattern, category in FAILURE_CATEGORIES:
        if re.search(pattern, text_lower):
            return category

    return 'Other'


def process_yates_taxonomy():
    """Process Yates inspections and extract taxonomy."""
    print("Processing Yates taxonomy...")

    df = pd.read_csv(PROCESSED_DIR / "yates_all_inspections.csv")

    # Extract base fields
    buildings = df['Location'].apply(extract_building)
    levels = df['Location'].apply(extract_level)
    gridlines = df['Location'].apply(extract_gridline)
    scopes = df['Inspection Description'].apply(extract_scope)

    # Extract specific locations (elevator, stair, room)
    specific_results = df['Location'].apply(extract_specific_location)
    specific_ids = specific_results.apply(lambda x: x[0])
    specific_types = specific_results.apply(lambda x: x[1])

    # Also try combined text for missing values
    combined = df['Location'].fillna('') + ' ' + df['Inspection Description'].fillna('')
    buildings = buildings.combine_first(combined.apply(extract_building))
    scopes = scopes.combine_first(combined.apply(extract_scope))

    # Build composite location_ids
    location_ids = pd.Series([
        build_location_id(b, l, g, s_id, s_type)
        for b, l, g, s_id, s_type in zip(buildings, levels, gridlines, specific_ids, specific_types)
    ])

    # Determine location type
    location_types = specific_types.combine_first(
        pd.Series(['GL' if g else ('AR' if b else None)
                   for b, g in zip(buildings, gridlines)])
    )

    # Create taxonomy dataframe
    taxonomy = pd.DataFrame({
        'source': 'yates',
        'source_index': df.index,
        'wir_number': df['WIR #'],
        'building': buildings,
        'level': levels,
        'gridline': gridlines,
        'location_id': location_ids,
        'location_type': location_types,
        'scope': scopes,
        'location_raw': df['Location'],
        'description_raw': df['Inspection Description'],
    })

    return taxonomy


def process_secai_taxonomy():
    """Process SECAI inspections and extract taxonomy."""
    print("Processing SECAI taxonomy...")

    df = pd.read_csv(PROCESSED_DIR / "secai_inspection_log.csv")

    # Combine location fields for extraction
    location_text = df['Building Type'].fillna('') + ' ' + df['System / Equip/ Location'].fillna('')

    # Extract base fields
    buildings = location_text.apply(extract_building)
    levels = location_text.apply(extract_level)
    gridlines = location_text.apply(extract_gridline)
    scopes = df['Template'].apply(extract_scope)

    # Extract specific locations (elevator, stair, room, sector)
    specific_results = location_text.apply(extract_specific_location)
    specific_ids = specific_results.apply(lambda x: x[0])
    specific_types = specific_results.apply(lambda x: x[1])

    # Fallback: extract building from Building Type if not found
    building_type = df['Building Type'].fillna('')
    missing_building = buildings.isna()
    buildings.loc[missing_building] = building_type[missing_building].apply(
        lambda x: extract_building(x) or (x.split('>')[0].strip() if '>' in str(x) else None)
    )

    # Fallback: extract building from IR Number if still not found
    # IR Numbers often contain building codes like "OB1", "FAB", "GCS", etc.
    missing_building = buildings.isna()
    if missing_building.any():
        buildings.loc[missing_building] = df.loc[missing_building, 'IR Number'].apply(extract_building)

    # Build composite location_ids
    location_ids = pd.Series([
        build_location_id(b, l, g, s_id, s_type)
        for b, l, g, s_id, s_type in zip(buildings, levels, gridlines, specific_ids, specific_types)
    ])

    # Determine location type
    location_types = specific_types.combine_first(
        pd.Series(['GL' if g else ('AR' if b else None)
                   for b, g in zip(buildings, gridlines)])
    )

    # Categorize failure reasons
    failure_categories = df['Reasons for failure'].apply(categorize_failure_reason)

    # Create taxonomy dataframe
    taxonomy = pd.DataFrame({
        'source': 'secai',
        'source_index': df.index,
        'ir_number': df['IR Number'],
        'status': df['Status_Normalized'],
        'building': buildings,
        'level': levels,
        'gridline': gridlines,
        'location_id': location_ids,
        'location_type': location_types,
        'scope': scopes,
        'failure_category': failure_categories,
        'failure_reason': df['Reasons for failure'],
        'location_raw': df['System / Equip/ Location'],
        'description_raw': df['Template'],
    })

    return taxonomy


def main():
    """Main entry point."""
    print("=" * 60)
    print("Deriving Quality Inspection Taxonomy")
    print("=" * 60)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Process both sources
    yates_tax = process_yates_taxonomy()
    secai_tax = process_secai_taxonomy()

    # Save derived tables
    yates_file = OUTPUT_DIR / "yates_taxonomy.csv"
    secai_file = OUTPUT_DIR / "secai_taxonomy.csv"

    yates_tax.to_csv(yates_file, index=False)
    secai_tax.to_csv(secai_file, index=False)

    print(f"\nSaved: {yates_file}")
    print(f"Saved: {secai_file}")

    # Print coverage stats
    print("\n" + "=" * 60)
    print("EXTRACTION COVERAGE")
    print("=" * 60)

    for name, tax in [("YATES", yates_tax), ("SECAI", secai_tax)]:
        total = len(tax)
        print(f"\n{name} ({total} records):")
        print(f"  Building:    {tax['building'].notna().sum()} ({tax['building'].notna().sum()/total*100:.1f}%)")
        print(f"  Level:       {tax['level'].notna().sum()} ({tax['level'].notna().sum()/total*100:.1f}%)")
        print(f"  Gridline:    {tax['gridline'].notna().sum()} ({tax['gridline'].notna().sum()/total*100:.1f}%)")
        print(f"  Location ID: {tax['location_id'].notna().sum()} ({tax['location_id'].notna().sum()/total*100:.1f}%)")
        print(f"  Scope:       {tax['scope'].notna().sum()} ({tax['scope'].notna().sum()/total*100:.1f}%)")

        print(f"\n  Building distribution:")
        print(tax['building'].value_counts().head(10).to_string())

        print(f"\n  Location Type distribution:")
        print(tax['location_type'].value_counts().to_string())

        print(f"\n  Scope distribution:")
        print(tax['scope'].value_counts().head(10).to_string())

        # Show failure category distribution for SECAI
        if 'failure_category' in tax.columns:
            failures = tax[tax['status'] == 'FAILURE']
            print(f"\n  Failure Category distribution ({len(failures)} failures):")
            print(failures['failure_category'].value_counts().to_string())

        print(f"\n  Sample location_ids:")
        sample_ids = tax[tax['location_id'].notna()]['location_id'].sample(min(10, tax['location_id'].notna().sum()), random_state=42)
        for loc_id in sample_ids:
            print(f"    {loc_id}")


if __name__ == "__main__":
    main()
