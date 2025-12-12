"""
Derive taxonomy fields from quality inspection data.

Extracts Building, Level, Gridline, Location, and Scope from inspection records
to enable linking with P6 schedule tasks.

Inputs:
    - data/processed/quality/secai_inspection_log.csv
    - data/processed/quality/yates_all_inspections.csv

Outputs:
    - data/derived/quality/secai_taxonomy.csv
    - data/derived/quality/yates_taxonomy.csv
"""

import re
import pandas as pd
from pathlib import Path
from typing import Optional, Tuple, Dict

# Paths - Windows data folder (OneDrive)
DATA_ROOT = Path("/mnt/c/Users/pcuri/OneDrive - MXI/Desktop/Samsung Dashboard/Data")
PROCESSED_DIR = DATA_ROOT / "processed" / "quality"
DERIVED_DIR = DATA_ROOT / "derived" / "quality"

# Building codes
BUILDINGS = ['FAB', 'SUE', 'SUW', 'FIZ', 'CUB', 'GCS', 'GCSA', 'GCSB', 'OB1', 'OB2']

# Scope patterns based on TaskClassifier
SCOPE_PATTERNS = [
    # Interior - Drywall
    (r'DRYWALL|GYPSUM|BOARD|SHEETROCK|1ST LAYER|2ND LAYER|3RD LAYER|TAPE.*FINISH|CLOSURE WALL', 'DRY'),
    # Interior - Framing
    (r'FRAMING|METAL STUD|STUD FRAME|BOTTOM PLATE', 'FRM'),
    # Interior - Fire Protection
    (r'FIRE|SPRINKLER|CAULK|FIRESTOP|SMOKE|SFRM|FIRE SPRAY|FIREPROOF', 'FIR'),
    # Interior - MEP
    (r'ELECTRICAL|CONDUIT|CABLE|RACEWAY|PANELBOARD|WIRING|LIGHTING|LOW VOLTAGE|GROUNDING|PLUMB|HVAC|DUCT|PIPE(?!.*RACK)', 'MEP'),
    # Interior - Finishes
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
    # Structure - Waterproofing/Coating
    (r'WATERPROOF|CRC|DENSIFIER|SEALER', 'WPF'),
    # Enclosure - Roofing
    (r'ROOF|MEMBRANE|PARAPET', 'ROF'),
    # Drill & Epoxy (common inspection type)
    (r'DRILL.*EPOXY|EPOXY.*DRILL', 'STL'),
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

    level_match = re.search(r'\bL(\d)\b', text_upper)
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
    # Letter(s)/Number range
    grid_match = re.search(r'([A-N](?:[\.\d]*)?(?:\s*-\s*[A-N](?:[\.\d]*)?)?)\s*/\s*(\d+(?:[\.\d]*)?(?:\s*-\s*\d+(?:[\.\d]*)?)?)', text_upper)
    if grid_match:
        letters = grid_match.group(1).replace(' ', '')
        numbers = grid_match.group(2).replace(' ', '')
        return f"{letters}/{numbers}"

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
        'source': 'secai',
        'source_index': df.index,
        'ir_number': df['IR Number'],
        'building': buildings,
        'level': levels,
        'gridline': gridlines,
        'location_id': location_ids,
        'location_type': location_types,
        'scope': scopes,
        'location_raw': df['System / Equip/ Location'],
        'description_raw': df['Template'],
    })

    return taxonomy


def main():
    """Main entry point."""
    print("=" * 60)
    print("Deriving Quality Inspection Taxonomy")
    print("=" * 60)

    DERIVED_DIR.mkdir(parents=True, exist_ok=True)

    # Process both sources
    yates_tax = process_yates_taxonomy()
    secai_tax = process_secai_taxonomy()

    # Save derived tables
    yates_file = DERIVED_DIR / "yates_taxonomy.csv"
    secai_file = DERIVED_DIR / "secai_taxonomy.csv"

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

        print(f"\n  Sample location_ids:")
        sample_ids = tax[tax['location_id'].notna()]['location_id'].sample(min(10, tax['location_id'].notna().sum()), random_state=42)
        for loc_id in sample_ids:
            print(f"    {loc_id}")


if __name__ == "__main__":
    main()
