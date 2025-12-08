#!/usr/bin/env python3
"""
Extract location labels from WBS (Work Breakdown Structure) nodes.

Extracts building, level, and room labels from WBS names using pattern matching.
Tracks label source: 'parsed' (explicit in data), 'inferred' (derived), 'manual' (needs review).

Output: data/primavera/generated/wbs_labels.csv

Usage:
    python scripts/extract_wbs_labels.py
    python scripts/extract_wbs_labels.py --schedule-type YATES
    python scripts/extract_wbs_labels.py --schedule-type SECAI
"""

import argparse
import re
from pathlib import Path

import pandas as pd


def extract_building(name: str, short: str) -> tuple[str | None, str]:
    """
    Extract building code from WBS name.

    Returns: (building_code, source)
        source: 'parsed' if explicit in name, 'inferred' if derived, 'manual' if unknown
    """
    name_upper = name.upper()
    short_upper = short.upper()

    # PARSED: Explicit building codes in standard format "- SUE -" or "-SUE-"
    if '-SUE-' in name or '- SUE' in name or ' SUE-' in name or ' SUE' == name_upper[-4:]:
        return 'SUE', 'parsed'
    if '-SUW-' in name or '- SUW' in name or ' SUW-' in name:
        return 'SUW', 'parsed'
    if '-FIZ-' in name or '- FIZ' in name:
        return 'FIZ', 'parsed'
    if '-FAB-' in name and 'SAMSUNG' not in name_upper:
        return 'FAB', 'parsed'

    # PARSED: Building in WBS short name
    if 'SUE' in short_upper and len(short_upper) <= 10:
        return 'SUE', 'parsed'
    if 'SUW' in short_upper and len(short_upper) <= 10:
        return 'SUW', 'parsed'
    if short_upper.startswith('FIZ') or 'FIZ' in short_upper:
        return 'FIZ', 'parsed'

    # PARSED: Building code in WBS name (e.g., "L3 SUE", "SUW - CONCRETE", "L2 FIZ")
    if ' SUE' in name_upper or 'SUE ' in name_upper:
        return 'SUE', 'parsed'
    if ' SUW' in name_upper or 'SUW ' in name_upper:
        return 'SUW', 'parsed'
    if ' FIZ' in name_upper or 'FIZ ' in name_upper:
        return 'FIZ', 'parsed'

    # PARSED: Explicit building names
    if 'SUPPORT BUILDING - EAST' in name_upper or 'SUPPORT EAST' in name_upper:
        return 'SUE', 'parsed'
    if 'SUPPORT BUILDING - WEST' in name_upper or 'SUPPORT WEST' in name_upper:
        return 'SUW', 'parsed'
    if 'DATA CENTER' in name_upper:
        return 'FIZ', 'parsed'

    # INFERRED: Area codes that imply building
    if re.match(r'^[AB][1-5]$', short_upper):  # A1-A5, B1-B5 = Support West areas
        return 'SUW', 'inferred'
    if re.match(r'^SE[AB]', short_upper):  # SEA, SEB = Support East
        return 'SUE', 'inferred'
    if re.match(r'^SW[AB]', short_upper):  # SWA, SWB = Support West
        return 'SUW', 'inferred'

    # INFERRED: Keywords suggesting FAB building
    if any(x in name_upper for x in ['PRECAST', 'STEEL ERECT', 'ERECTOR', 'FABRICATOR',
                                      'PIER DRILL', 'FOUNDATION', 'SLAB ON', 'BUNKER']):
        return 'FAB', 'inferred'

    # INFERRED: FAB in name (not SAMSUNG-FAB project names)
    if 'FAB' in name_upper and 'SAMSUNG' not in name_upper and 'YATES' not in name_upper:
        return 'FAB', 'inferred'

    # INFERRED: Project-level nodes span all buildings
    if any(x in name_upper for x in ['EXECUTIVE SUMMARY', 'MILESTONE', 'PRE CONSTRUCTION',
                                      'LEVEL OF EFFORT', 'PROCUREMENT', 'BIM', 'VDC',
                                      'ROOFING', 'ENCLOSURE', 'PRIORITY', 'CONSTRUCTION']):
        return 'ALL', 'inferred'

    # INFERRED: SAMSUNG project root nodes
    if name_upper.startswith('SAMSUNG') or 'YATES T FAB1' in name_upper:
        return 'ALL', 'inferred'

    # INFERRED: Level-only nodes (LEVEL 4, etc.)
    if short_upper.startswith('LEVEL ') or name_upper.startswith('LEVEL '):
        return 'ALL', 'inferred'

    # INFERRED: Underground spans all
    if 'UNDERGROUND' in name_upper:
        return 'ALL', 'inferred'

    # INFERRED: Structural steel spans all
    if 'STRUCTURAL STEEL' in name_upper or short_upper == 'STEEL':
        return 'FAB', 'inferred'

    return None, 'manual'


def extract_level(name: str, short: str) -> tuple[str | None, str]:
    """
    Extract level/floor from WBS name.

    Returns: (level_code, source)
    """
    name_upper = name.upper()
    short_upper = short.upper()

    # PARSED: Standard format "- 2F -" or "- L2 -"
    match = re.search(r'[-\s](\d)F[-\s]', name)
    if match:
        return f"L{match.group(1)}", 'parsed'

    match = re.search(r'[-\s](L[1-6])[-\s]', name_upper)
    if match:
        return match.group(1), 'parsed'

    # PARSED: Level at end of name "- 1F" or "L3"
    match = re.search(r'[-\s](\d)F\s*$', name)
    if match:
        return f"L{match.group(1)}", 'parsed'

    match = re.search(r'[-\s](L[1-6])\s*$', name_upper)
    if match:
        return match.group(1), 'parsed'

    # PARSED: WBS short names that ARE levels
    level_map = {
        'LEVEL 4': 'L4', 'LEVEL 41': 'L3', 'LEVEL 411': 'L2', 'LEVEL 4111': 'L1'
    }
    if short_upper in level_map:
        return level_map[short_upper], 'parsed'

    # PARSED: Level in name like "L3 SUE", "LVL 1"
    match = re.search(r'\bL(\d)\b', name_upper)
    if match:
        return f"L{match.group(1)}", 'parsed'
    match = re.search(r'\bLVL\s*(\d)', name_upper)
    if match:
        return f"L{match.group(1)}", 'parsed'

    # PARSED: Roof/penthouse
    if 'PENTHOUSE' in name_upper:
        return 'ROOF', 'parsed'
    if 'ROOF' in name_upper and 'FIREPROOF' not in name_upper:
        return 'ROOF', 'parsed'

    # PARSED: Underground
    if 'UNDERGROUND' in name_upper or short_upper in ['UG', 'AREA A1UG', 'AREA B1UG']:
        return 'UG', 'parsed'

    # PARSED: Foundation
    if 'FOUNDATION' in name_upper or 'PIER DRILL' in name_upper:
        return 'FOUNDATION', 'parsed'

    # INFERRED: Area codes span multiple levels
    if re.match(r'^[AB][1-5]$', short_upper):
        return 'MULTI', 'inferred'
    if re.match(r'^SE[AB]', short_upper) or re.match(r'^SW[AB]', short_upper):
        return 'MULTI', 'inferred'
    if re.match(r'^FIZ\d', short_upper) or 'AREA FIZ' in name_upper:
        return 'MULTI', 'inferred'
    if 'AREA' in short_upper or ('AREA' in name_upper and 'DRYWALL AREA' not in name_upper):
        return 'MULTI', 'inferred'

    # INFERRED: Structure nodes span multiple levels
    if any(x in name_upper for x in ['PRECAST', 'STRUCTURAL STEEL', 'ERECTOR',
                                      'FABRICATOR', 'ENCLOSURE']):
        return 'MULTI', 'inferred'

    # INFERRED: Project-level nodes
    if any(x in name_upper for x in ['EXECUTIVE', 'MILESTONE', 'PRE CONSTRUCTION',
                                      'LEVEL OF EFFORT', 'PROCUREMENT', 'BIM', 'VDC',
                                      'PRIORITY', 'SAMSUNG', 'YATES T FAB1']):
        return 'ALL', 'inferred'

    # INFERRED: High-level category (short name = number)
    if short_upper in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']:
        return 'ALL', 'inferred'

    # INFERRED: Concrete work spans levels
    if 'CONCRETE' in name_upper:
        return 'MULTI', 'inferred'

    # INFERRED: Steel work spans levels
    if short_upper == 'STEEL' or 'STRUCTURAL STEEL' in name_upper:
        return 'MULTI', 'inferred'

    # INFERRED: Slab on void is foundation level
    if 'SLAB ON VOID' in name_upper:
        return 'FOUNDATION', 'inferred'

    # INFERRED: Building-level nodes
    if 'SUPPORT BUILDING' in name_upper or 'DATA CENTER BLDG' in name_upper:
        return 'MULTI', 'inferred'

    # INFERRED: Roofing
    if 'ROOFING' in name_upper:
        return 'ROOF', 'inferred'

    return None, 'manual'


def extract_room(name: str, short: str) -> tuple[str | None, str]:
    """
    Extract room type from WBS name.

    Returns: (room_type, source)
    """
    name_upper = name.upper()
    short_upper = short.upper()

    # PARSED: Room type patterns (explicit in name)
    room_patterns = {
        'ELEVATOR': (['ELEVATOR '], 'parsed'),
        'ELEVATOR_HALL': (['ELEVATOR HALL'], 'parsed'),
        'DUCT_SHAFT': (['DUCT SHAFT'], 'parsed'),
        'VESTIBULE': (['VESTIBULE'], 'parsed'),
        'AIR_LOCK': (['AIR LOCK', 'AIRLOCK'], 'parsed'),
        'FIRE_ALARM': (['FIRE ALARM'], 'parsed'),
        'FIRE_RISER': (['FIRE RISER'], 'parsed'),
        'MCC': (['MCC RM', 'MCC/IO', 'MCC ROOM', ' MCC '], 'parsed'),
        'BATTERY_RM': (['BATTERY RM', 'BATTERY ROOM'], 'parsed'),
        'IT_SECURITY': (['IT/SECURITY', 'IT SECURITY'], 'parsed'),
        'ELECTRICAL': (['ELECTRICAL RM', 'ELECTRICAL ROOM'], 'parsed'),
        'IO_ROOM': (['I/O RM', 'I/O ROOM'], 'parsed'),
        'STAIR': (['STAIR '], 'parsed'),
        'STORAGE': (['STORAGE'], 'parsed'),
        'MECH_ROOM': (['MECH RM', 'MECHANICAL RM'], 'parsed'),
        'GCS': (['GCS STORAGE', 'GCS TRAINING'], 'parsed'),
        'BATHROOM': (['BATHROOM', 'TOILET', 'RESTROOM'], 'parsed'),
        'WALKWAY': (['WALKWAY', 'PEDESTRIAN WALKWAY'], 'parsed'),
        'BREEZEWAY': (['BREEZEWAY', 'BREEZWAY'], 'parsed'),
        'ENTRANCE': (['ENTRANCE'], 'parsed'),
        'CHASE': (['RATED CHASE', 'CHASE -'], 'parsed'),
        'T_L_MAINT': (['T/L MAINT'], 'parsed'),
        'CLEAN_VESTIBULE': (['CLEAN VESTIBULE', 'CLEAN TOILET'], 'parsed'),
        'PR_LIFTER': (['PR LIFTER'], 'parsed'),
        'WAFER_IO': (['WAFER I/O'], 'parsed'),
        'THERMOHYGROSTAT': (['THERMOHYGROSTAT'], 'parsed'),
        'EXIT_PASSAGEWAY': (['EXIT PASSAGEWAY'], 'parsed'),
        'UTILITY': (['UTILITY RM', 'UTILITY ROOM'], 'parsed'),
        'CV_ROOM': (['CV ROOM'], 'parsed'),
        'JANITOR': (['JANITOR RM', 'JANITOR ROOM', 'JANITOR -'], 'parsed'),
        'MAINT_RM': (['MAINT RM', 'MAINT. RM', 'MAINTENANCE RM'], 'parsed'),
        'PR_ROOM': (['PR ROOM', 'PR OFFICE'], 'parsed'),
        'SERVER': (['SERVER RM', 'SERVER ROOM'], 'parsed'),
        'BUNKER': (['BUNKER RM', 'BUNKER ROOM'], 'parsed'),
        'PCS': (['PCS MONITORING', 'PCS CLEANING'], 'parsed'),
        'SOLVENT': (['SOLVENT RM'], 'parsed'),
        'H2_PURIFIER': (['H2 PURIFIER'], 'parsed'),
        'GAS_PURIFIER': (['GAS PURIFIER'], 'parsed'),
        'TGMS': (['TGMS RM'], 'parsed'),
        'EQUIP_PLATFORM': (['EQUIP PLATFORM', 'EQUIPMENT PLATFORM'], 'parsed'),
        'PIT': (['SOUTH/NORTH PIT', 'PIT -'], 'parsed'),
        'VIP_TOUR': (['VIP TOUR'], 'parsed'),
        'UNASSIGNED': (['UNASSIGNED'], 'parsed'),
        'VOC_CTR': (['VOC CTR'], 'parsed'),
    }

    for room_type, (patterns, source) in room_patterns.items():
        if any(p in name_upper for p in patterns):
            return room_type, source

    # INFERRED: Fire alarm without "RM" (still clearly a room)
    if 'FIRE ALARM' in name_upper and 'RM' not in name_upper and 'ROOM' not in name_upper:
        return 'FIRE_ALARM', 'inferred'

    # N/A: Area/structure nodes don't have room types
    if any(x in name_upper for x in ['EXECUTIVE', 'MILESTONE', 'CONSTRUCTION',
                                      'PRECAST', 'STRUCTURAL STEEL', 'FOUNDATION',
                                      'SWA -', 'SWB -', 'SEA -', 'SEB -',
                                      'ROOFING', 'SAMSUNG', 'ERECTOR', 'FABRICATOR',
                                      'ENCLOSURE', 'UNDERGROUND', 'PIER DRILL']):
        return 'N/A', 'inferred'

    # N/A: Area codes (short names)
    if re.match(r'^[AB][1-5]$', short_upper):
        return 'N/A', 'inferred'
    if re.match(r'^SE[AB]', short_upper) or re.match(r'^SW[AB]', short_upper):
        return 'N/A', 'inferred'
    if re.match(r'^FIZ\d', short_upper):
        return 'N/A', 'inferred'
    if 'AREA' in short_upper:
        return 'N/A', 'inferred'

    # N/A: Gridline reference nodes (D-G/33, G.3-J/33, etc.)
    if re.match(r'^[\dA-Z]+-[A-Z]/\d+', short_upper) or re.search(r'[A-Z]-[A-Z]/\d+', name_upper):
        return 'N/A', 'inferred'
    if re.search(r'[A-Z]\.\d+-[A-Z]/\d+', name_upper):  # G.3-J/33
        return 'N/A', 'inferred'
    if re.match(r'^\d+F-', short_upper):  # 4F-D-G/33
        return 'N/A', 'inferred'

    # N/A: Level of effort and high-level nodes
    if 'LEVEL OF EFFORT' in name_upper:
        return 'N/A', 'inferred'

    return None, 'manual'


def extract_area(name: str, short: str) -> tuple[str | None, str]:
    """
    Extract area code from WBS.

    Returns: (area_code, source)
    """
    short_upper = short.upper()
    name_upper = name.upper()

    # PARSED: Direct area codes in short name
    if re.match(r'^[AB][1-5]$', short_upper):
        # A1 -> SWA1, B3 -> SWB3
        return f"SW{short_upper}", 'parsed'

    if re.match(r'^SE[AB][- ]?\d$', short_upper.replace(' ', '')):
        # SEA-1, SEB 5 -> SEA1, SEB5
        clean = short_upper.replace(' ', '').replace('-', '')
        return clean, 'parsed'

    if re.match(r'^SW[AB][- ]?\d$', short_upper.replace(' ', '')):
        clean = short_upper.replace(' ', '').replace('-', '')
        return clean, 'parsed'

    if re.match(r'^FIZ[1-4]', short_upper):
        return short_upper[:4], 'parsed'

    # PARSED: Area in WBS name "SWA - 1", "Area FIZ1"
    match = re.search(r'(SWA|SWB|SEA|SEB)\s*-?\s*(\d)', name_upper)
    if match:
        return f"{match.group(1)}{match.group(2)}", 'parsed'

    match = re.search(r'AREA\s+(FIZ\d)', name_upper)
    if match:
        return match.group(1), 'parsed'

    # INFERRED: From building code patterns in name
    # e.g., FAB120304 -> could infer area from the code
    match = re.search(r'FAB1(\d)(\d)', name)
    if match:
        level = match.group(1)
        area_num = match.group(2)
        # This is a room code, not directly an area - skip for now
        pass

    return None, 'manual'


def build_wbs_paths(wbs_df: pd.DataFrame) -> dict[str, str]:
    """
    Build full hierarchical paths for each WBS node.

    Returns dict mapping wbs_id -> full path like "Root.Tier1.Tier2.Leaf"
    """
    id_to_row = {row['wbs_id']: row for _, row in wbs_df.iterrows()}

    def get_path(wbs_id, max_depth=15):
        path = []
        current_id = wbs_id
        depth = 0

        while depth < max_depth and current_id in id_to_row:
            row = id_to_row[current_id]
            path.append(str(row['wbs_short_name']))
            parent_id = row['parent_wbs_id']
            if pd.isna(parent_id) or parent_id not in id_to_row:
                break
            current_id = parent_id
            depth += 1

        # Reverse to get root->leaf order
        return '.'.join(reversed(path))

    return {wbs_id: get_path(wbs_id) for wbs_id in id_to_row}


def inherit_from_hierarchy(wbs_df: pd.DataFrame, labels_df: pd.DataFrame) -> pd.DataFrame:
    """
    Propagate labels from parent WBS nodes to children that lack their own labels.

    For each dimension (building, level, area), if a node has 'manual' or 'inferred' source,
    walk up the hierarchy to find a parent with a valid label and inherit it.

    Args:
        wbs_df: WBS data with wbs_id, parent_wbs_id, wbs_short_name
        labels_df: Labels extracted from node names (must have wbs_id column)

    Returns:
        Updated labels_df with inherited labels (source='inherited')
    """
    # Build lookup: wbs_id -> parent_wbs_id
    id_to_parent = dict(zip(wbs_df['wbs_id'], wbs_df['parent_wbs_id']))

    # Build lookup: wbs_id -> labels row (now using wbs_id as key)
    labels_lookup = labels_df.set_index('wbs_id').to_dict('index')

    def get_parent_label(wbs_id, dimension):
        """Walk up hierarchy to find first valid label for dimension.

        Returns: (label_value, source, depth) where source is 'parsed' or 'inferred'
        """
        current_id = wbs_id
        depth = 0
        max_depth = 15

        while depth < max_depth:
            # Get parent
            parent_id = id_to_parent.get(current_id)
            if pd.isna(parent_id) or parent_id not in labels_lookup:
                break

            # Check parent's label
            parent_row = labels_lookup[parent_id]
            parent_label = parent_row.get(dimension)
            parent_source = parent_row.get(f'{dimension}_source')

            # Only inherit if parent has a real label (not ALL, not manual)
            if parent_label and parent_label not in ['ALL', 'N/A', None] and parent_source != 'manual':
                return parent_label, parent_source, depth + 1

            current_id = parent_id
            depth += 1

        return None, None, 0

    # Apply inheritance
    # Priority: parsed > inherited(from parsed) > inherited(from inferred) > inferred > manual
    # Key rule: If parent has a PARSED label, it ALWAYS overrides child's inferred label
    inheritance_stats = {'building': 0, 'level': 0, 'area': 0}
    replaced_inferred = {'building': 0, 'level': 0, 'area': 0}
    result = labels_df.copy()

    for i, row in result.iterrows():
        wbs_id = row['wbs_id']

        for dim in ['building', 'level', 'area']:
            source_col = f'{dim}_source'
            current_source = row[source_col]
            current_val = row[dim]

            # Skip if source is 'parsed' (highest priority - never override)
            if current_source == 'parsed':
                continue

            inherited_val, inherited_source, depth = get_parent_label(wbs_id, dim)
            if not inherited_val:
                continue

            # Decision logic:
            # 1. If current is 'manual' -> always inherit
            # 2. If current is 'inferred' AND parent is 'parsed' -> inherit (parsed beats inferred)
            # 3. If current is 'inferred' with generic value (ALL/N/A/MULTI) -> inherit
            # 4. If current is 'inferred' with specific value AND parent is also 'inferred' -> keep current

            should_inherit = False

            if current_source == 'manual':
                should_inherit = True
            elif current_source == 'inferred':
                if inherited_source == 'parsed':
                    # Parent is parsed = ground truth, always override inferred
                    should_inherit = True
                elif current_val in ['ALL', 'N/A', 'MULTI', None]:
                    # Current is generic, prefer parent's specific value
                    should_inherit = True
                # else: current has specific inferred, parent also inferred -> keep current

            if should_inherit:
                result.at[i, dim] = inherited_val
                result.at[i, source_col] = 'inherited'
                inheritance_stats[dim] += 1
                if current_source == 'inferred':
                    replaced_inferred[dim] += 1

    print(f"\nHierarchy inheritance applied (priority: parsed > inherited > inferred > manual):")
    for dim, count in inheritance_stats.items():
        replaced = replaced_inferred[dim]
        print(f"  {dim}: {count} inherited ({replaced} replaced inferred, {count - replaced} replaced manual)")

    return result


def main():
    parser = argparse.ArgumentParser(description='Extract WBS labels')
    parser.add_argument('--processed-dir', type=Path,
                        default=Path('data/primavera/processed'))
    parser.add_argument('--output-dir', type=Path,
                        default=Path('data/primavera/generated'))
    parser.add_argument('--schedule-type', choices=['YATES', 'SECAI', 'ALL'],
                        default='YATES', help='Schedule type to process')
    parser.add_argument('--no-hierarchy', action='store_true',
                        help='Disable hierarchy inheritance')
    args = parser.parse_args()

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    print("Loading data...")
    xer_files = pd.read_csv(args.processed_dir / 'xer_files.csv')
    wbs = pd.read_csv(args.processed_dir / 'projwbs.csv', low_memory=False)

    # Filter by schedule type and get latest file
    if args.schedule_type != 'ALL':
        file_ids = xer_files[xer_files['schedule_type'] == args.schedule_type]['file_id'].tolist()
        wbs = wbs[wbs['file_id'].isin(file_ids)]
        print(f"Filtered to {args.schedule_type}: {len(file_ids)} files")

    # Use latest file for hierarchy (paths are file-specific)
    latest_file = xer_files[xer_files['schedule_type'] == args.schedule_type].sort_values('date').iloc[-1]
    latest_wbs = wbs[wbs['file_id'] == latest_file['file_id']]
    print(f"Using hierarchy from: {latest_file['filename']}")
    print(f"WBS nodes in latest file: {len(latest_wbs)}")

    # Build full WBS paths for unique identification
    wbs_paths = build_wbs_paths(latest_wbs)
    print(f"Built {len(wbs_paths)} unique WBS paths")

    # Extract labels for each WBS node (by unique path)
    results = []
    for _, row in latest_wbs.iterrows():
        wbs_id = row['wbs_id']
        short = str(row['wbs_short_name'])
        name = str(row['wbs_name']) if pd.notna(row['wbs_name']) else short
        path = wbs_paths.get(wbs_id, short)

        building, building_source = extract_building(name, short)
        level, level_source = extract_level(name, short)
        room, room_source = extract_room(name, short)
        area, area_source = extract_area(name, short)

        results.append({
            'wbs_id': wbs_id,
            'wbs_path': path,
            'wbs_short_name': short,
            'wbs_name': name,
            'parent_wbs_id': row['parent_wbs_id'] if pd.notna(row['parent_wbs_id']) else None,
            'building': building,
            'building_source': building_source,
            'level': level,
            'level_source': level_source,
            'room': room,
            'room_source': room_source,
            'area': area,
            'area_source': area_source,
        })

    df = pd.DataFrame(results)
    print(f"Processing {len(df)} WBS nodes...")

    # Apply hierarchy inheritance
    if not args.no_hierarchy:
        df = inherit_from_hierarchy(latest_wbs, df)

    # Summary statistics
    print("\n" + "="*60)
    print("EXTRACTION SUMMARY")
    print("="*60)

    for dim in ['building', 'level', 'room', 'area']:
        source_col = f'{dim}_source'
        parsed = (df[source_col] == 'parsed').sum()
        inferred = (df[source_col] == 'inferred').sum()
        inherited = (df[source_col] == 'inherited').sum()
        manual = (df[source_col] == 'manual').sum()
        total = len(df)

        print(f"\n{dim.upper()}:")
        print(f"  Parsed (explicit):    {parsed:3d} ({parsed/total*100:5.1f}%)")
        print(f"  Inferred (derived):   {inferred:3d} ({inferred/total*100:5.1f}%)")
        print(f"  Inherited (from parent): {inherited:3d} ({inherited/total*100:5.1f}%)")
        print(f"  Manual (unknown):     {manual:3d} ({manual/total*100:5.1f}%)")

    # Value distribution
    print("\n" + "="*60)
    print("VALUE DISTRIBUTION")
    print("="*60)

    for dim in ['building', 'level', 'room']:
        print(f"\n{dim.upper()}:")
        for val, cnt in df[dim].value_counts().head(10).items():
            if val is not None:
                print(f"  {str(val):15s}: {cnt:3d}")

    # Save output
    output_path = args.output_dir / 'wbs_labels.csv'
    df.to_csv(output_path, index=False)
    print(f"\n{'='*60}")
    print(f"Output saved to: {output_path}")
    print(f"Total WBS nodes: {len(df)}")

    # List items needing manual review
    manual_any = df[
        (df['building_source'] == 'manual') |
        (df['level_source'] == 'manual') |
        (df['room_source'] == 'manual')
    ]

    if len(manual_any) > 0:
        print(f"\nWBS needing manual review: {len(manual_any)}")
        manual_path = args.output_dir / 'wbs_labels_manual_review.csv'
        manual_any.to_csv(manual_path, index=False)
        print(f"Saved to: {manual_path}")


if __name__ == '__main__':
    main()
