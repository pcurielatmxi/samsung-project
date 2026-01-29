#!/usr/bin/env python3
"""
Consolidate TBM Work Entries with Dimension IDs and CSI Sections.

Enriches the parsed TBM work_entries.csv with:
- dim_location_id (via centralized location enrichment)
- dim_company_id (company lookup)
- dim_csi_section_id (CSI section inference from work activities)
- Grid coordinates and affected rooms

This script replaces the separate enrich + csi stages by integrating
all enrichment into the TBM pipeline.

Input:  processed/tbm/work_entries.csv (from parse stage)
Output: processed/tbm/work_entries.csv (enriched, overwrites input)

Usage:
    python -m scripts.tbm.process.consolidate_tbm
    python -m scripts.tbm.process.consolidate_tbm --dry-run
"""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

import pandas as pd

# Add project root to path
_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings
from schemas.validator import validated_df_to_csv
from scripts.shared.dimension_lookup import (
    get_company_id,
    get_affected_rooms,
    reset_cache,
)
from scripts.integrated_analysis.location import enrich_location, parse_tbm_grid

# Import CSI definitions from add_csi_to_raba (shared with add_csi_to_tbm)
from scripts.integrated_analysis.add_csi_to_raba import CSI_SECTIONS


# =============================================================================
# TBM-specific keyword patterns to CSI section ID mapping
# Moved from add_csi_to_tbm.py for consolidated enrichment
# =============================================================================

ACTIVITY_TO_CSI = [
    # Fireproofing vs Firestopping
    (["sfrm", "ifrm", "fireproofing", "fire proofing"], 18),  # 07 81 00 Applied Fireproofing
    (["firestop", "fire stop", "fire caulk", "penetration seal"], 19),  # 07 84 00 Firestopping

    # Concrete specifics
    (["precast", "waffle", "double t", "double-t", "spandrel", "tilt-up", "pc panel"], 3),  # 03 41 00 Structural Precast
    (["grout", "grouting"], 4),  # 03 60 00 Grouting
    (["delamination", "patching", "patch", "repair concrete", "concrete repair"], 2),  # 03 30 00 Cast-in-Place
    (["pour", "placement", "concrete placement", "slab", "topping", "sog", "mat foundation"], 2),  # 03 30 00 Cast-in-Place
    (["form", "strip", "rebar", "reinforcing", "cure", "finishing concrete"], 2),  # 03 30 00 Cast-in-Place

    # Steel specifics
    (["steel deck", "decking", "floor deck", "roof deck", "deck install"], 7),  # 05 31 00 Steel Decking
    (["steel erection", "erect steel", "structural steel", "steel column", "steel beam"], 6),  # 05 12 00 Structural Steel
    (["stud", "metal stud", "zee metal", "z-metal", "framing"], 8),  # 05 40 00 Cold-Formed Metal Framing
    (["weld", "welding", "connection"], 6),  # 05 12 00 Structural Steel
    (["misc steel", "stair", "railing", "handrail", "ladder", "grating", "platform"], 9),  # 05 50 00 Metal Fabrications
    (["clip", "elevator clip", "elevator front clip", "angle", "corbel"], 9),  # 05 50 00 Metal Fabrications
    (["anchor", "bolt", "dowel", "embed"], 6),  # 05 12 00 Structural Steel

    # Drywall/Gypsum - "densglass" must come BEFORE "glass" patterns in Openings
    (["densglass", "dens glass"], 26),  # 09 21 16 Gypsum Board (exterior sheathing)
    (["drywall", "gypsum", "gyp board", "sheetrock", "layer", "hanging"], 26),  # 09 21 16 Gypsum Board
    (["shaft wall", "shaft liner", "shaftliner", "shaft"], 26),  # 09 21 16 Gypsum Board
    (["tape", "taping", "float", "finish drywall", "mud", "joint compound"], 26),  # 09 21 16 Gypsum finishing
    (["screw", "fastener"], 26),  # 09 21 16 - drywall screws
    (["ceiling grid", "ceiling tile", "acoustical ceiling", "act ceiling"], 27),  # 09 51 00 Acoustical Ceilings
    (["track", "sliptrack", "slip track", "bottom track", "top track"], 8),  # 05 40 00 Cold-Formed Metal Framing

    # Waterproofing/Roofing
    (["waterproofing", "below grade", "sikaproof"], 11),  # 07 13 00 Sheet Waterproofing
    (["dampproofing", "damp proofing"], 10),  # 07 11 00 Dampproofing
    (["roofing", "tpo", "membrane", "roof membrane"], 16),  # 07 52 00 Membrane Roofing
    (["coping", "flashing", "expansion joint", "roof edge", "cricket"], 17),  # 07 71 00 Roof Specialties

    # Insulation
    (["insulation", "mineral wool", "batt", "blanket insul"], 13),  # 07 21 16 Blanket Insulation
    (["rigid insulation", "foam board", "board insul"], 12),  # 07 21 13 Board Insulation
    (["vapor barrier", "vapor retarder", "vb"], 14),  # 07 26 00 Vapor Retarders
    (["air barrier", "sheet applied"], 14),  # 07 26 00 Air Barriers -> Vapor Retarders
    (["urethane", "spray foam"], 13),  # 07 21 16 - spray insulation

    # Panels/Cladding - use "imp panel" to avoid matching "IMPROPER", "improvement"
    (["imp panel", "insulated metal panel", "metal panel", "panel install", "wall panel"], 15),  # 07 42 43 Composite Wall Panels
    (["clad", "cladding", "skin", "enclosure", "facade"], 15),  # 07 42 43 Composite Wall Panels

    # Masonry - avoid bare "block" (matches "blocking", "blocked")
    (["masonry", "cmu", "cmu block", "concrete block", "brick", "mortar"], 5),  # 04 20 00 Unit Masonry

    # Finishes
    (["paint", "painting", "primer", "topcoat", "coating", "touch up"], 29),  # 09 91 26 Painting - Building
    (["epoxy", "chemical resistant", "floor coating"], 25),  # 09 06 65 Chemical-Resistant Coatings
    (["flooring", "resilient", "vct", "tile"], 28),  # 09 65 00 Resilient Flooring
    (["sealant", "caulk", "joint seal", "control joint", "control joints", "cj", "expansion joint"], 20),  # 07 90 00 Joint Protection
    (["t/f wall", "t/f walls", "tenant finish", "penthouse wall"], 26),  # 09 21 16 T/F Walls -> Gypsum Board

    # Openings
    (["door frame", "hollow metal", "hm frame", "hm door"], 21),  # 08 11 13 Hollow Metal Doors
    (["coiling door", "roll-up", "overhead door", "rollup"], 22),  # 08 33 23 Overhead Coiling Doors
    (["hardware", "lockset", "hinge", "closer"], 23),  # 08 71 00 Door Hardware
    (["glazing", "glass", "curtain wall", "storefront", "window"], 24),  # 08 80 00 Glazing

    # MEP - Fire Suppression
    (["sprinkler", "fire suppression", "fire protection pip"], 34),  # 21 10 00 Fire Suppression
    (["fire pump"], 35),  # 21 30 00 Fire Pumps

    # MEP - Plumbing
    (["plumbing", "domestic water"], 37),  # 22 11 00 Water Distribution
    (["sanitary", "waste pip", "sewer"], 38),  # 22 13 00 Sanitary Sewerage
    (["storm drain"], 39),  # 22 14 00 Storm Drainage

    # MEP - HVAC
    (["duct", "ductwork", "hvac duct"], 41),  # 23 31 00 HVAC Ducts
    (["diffuser", "vav", "air terminal"], 42),  # 23 36 00 Air Terminal Units
    (["ahu", "air handling", "air handler"], 43),  # 23 73 00 AHUs
    (["hvac", "mechanical"], 40),  # 23 05 00 Common Work Results for HVAC

    # MEP - Electrical
    (["conduit", "raceway", "junction box", "j-box", "cable tray"], 46),  # 26 05 33 Raceway and Boxes
    (["wire", "cable", "conductor", "pull wire"], 45),  # 26 05 19 Conductors and Cables
    (["panel", "switchboard", "panelboard", "electrical panel"], 47),  # 26 24 00 Switchboards
    (["receptacle", "outlet", "switch", "device"], 48),  # 26 27 26 Wiring Devices
    (["lighting", "light fixture", "luminaire"], 49),  # 26 51 00 Interior Lighting
    (["electrical", "elec"], 44),  # 26 05 00 Common Work Results for Electrical

    # Earthwork
    (["pier", "drilled pier", "caisson", "pile", "auger"], 52),  # 31 63 00 Bored Piles
    (["excavation", "excavat", "backfill", "compaction", "grade", "grading", "dig"], 51),  # 31 23 00 Excavation and Fill
    (["clearing", "demolition", "demo", "site clearing", "site prep"], 50),  # 31 10 00 Site Clearing

    # Special/General
    (["elevator", "escalator", "conveyor", "lift"], 33),  # 14 21 00 Elevators
    (["vibration", "acoustic", "sound control"], 32),  # 13 48 00 Sound and Vibration Control
    (["dock", "loading dock", "dock leveler"], 31),  # 11 13 19 Loading Dock Equipment
    (["scaffold", "trestle", "trest"], 1),  # 01 10 00 Scaffolding -> General Requirements
    (["labor work", "cleaning", "clean up", "sweeping", "punch list"], 1),  # 01 10 00 General labor
    (["rough in", "busway", "termination"], 44),  # 26 05 00 Electrical rough-in
    (["supervising", "supervisor", "foreman", "superintendent", "managers", "pes"], 1),  # 01 10 00 Supervision -> General
    (["qc", "qa-qc", "qaqc", "quality control", "logistics"], 1),  # 01 10 00 QC -> General
    (["ej", "expansion joint plate", "interior expansion"], 20),  # 07 90 00 Expansion Joints -> Joint Protection
    (["pulling branch", "secondary elec", "elec room"], 44),  # 26 05 00 Electrical
    (["imp setting", "awning"], 15),  # 07 42 43 IMP/Awnings -> Composite Panels
    (["plywood", "blocking", "backing"], 8),  # 05 40 00 Backing/blocking -> Framing
    (["support", "install support"], 6),  # 05 12 00 Supports -> Structural Steel
    (["ncr", "change request", "catch up"], 1),  # 01 10 00 NCR/Change work -> General
]

# Fallback: trade_inferred to CSI section
TRADE_TO_CSI = {
    "Concrete": 2,  # 03 30 00 Cast-in-Place Concrete
    "Structural Steel": 6,  # 05 12 00 Structural Steel Framing
    "Roofing": 16,  # 07 52 00 Membrane Roofing
    "Drywall": 26,  # 09 21 16 Gypsum Board Assemblies
    "Finishes": 29,  # 09 91 26 Painting - Building (most common finish)
    "Fire Protection": 19,  # 07 84 00 Firestopping (most common fire prot)
    "MEP": 44,  # 26 05 00 Common Work Results for Electrical
    "Insulation": 13,  # 07 21 16 Blanket Insulation
    "Earthwork": 51,  # 31 23 00 Excavation and Fill
    "Precast": 3,  # 03 41 00 Structural Precast
    "Panels": 15,  # 07 42 43 Composite Wall Panels
    "General": 1,  # 01 10 00 Summary
    "Masonry": 5,  # 04 20 00 Unit Masonry
}


def infer_csi_from_activity(work_activities: str, trade_inferred: str) -> Tuple[Optional[int], Optional[str], str]:
    """
    Infer CSI section from work activities and inferred trade.

    Args:
        work_activities: Free text description of work activities
        trade_inferred: Already-inferred trade from enrichment

    Returns:
        Tuple of (csi_section_id, csi_section_code, inference_source)
        inference_source is 'activity' or 'trade'
    """
    if pd.isna(work_activities) and pd.isna(trade_inferred):
        return None, None, "none"

    # Try activity keyword matching first (more specific)
    if pd.notna(work_activities):
        activity_lower = str(work_activities).lower()

        for keywords, csi_id in ACTIVITY_TO_CSI:
            for keyword in keywords:
                if keyword in activity_lower:
                    csi_code, _ = CSI_SECTIONS[csi_id]
                    return csi_id, csi_code, "activity"

    # Fall back to trade mapping
    if pd.notna(trade_inferred):
        trade = str(trade_inferred)
        if trade in TRADE_TO_CSI:
            csi_id = TRADE_TO_CSI[trade]
            csi_code, _ = CSI_SECTIONS[csi_id]
            return csi_id, csi_code, "trade"

    return None, None, "none"


def infer_trade_from_activity(activity: str) -> Optional[str]:
    """
    Infer trade from work activities (enhanced mapping).
    Used as intermediate step for CSI inference fallback.
    """
    if pd.isna(activity):
        return None
    activity_lower = str(activity).lower()

    # General conditions (check first to skip non-trade activities)
    if any(x in activity_lower for x in ['laydown', 'yard', 'mobilization', 'demobilization', 'safety', 'cleanup']):
        return 'General'
    # Concrete (trade_id=1)
    if any(x in activity_lower for x in ['concrete', 'pour', 'slab', 'form', 'strip', 'rebar', 'topping', 'placement', 'cure', 'finishing', 'delamination', 'patching', 'chipping', 'patch', 'repair']):
        return 'Concrete'
    # Structural Steel (trade_id=2)
    if any(x in activity_lower for x in ['steel', 'erect', 'deck', 'weld', 'bolt', 'connection', 'joist', 'truss', 'iron', 'elevator front clip', 'elevator clip', 'clip']):
        return 'Structural Steel'
    # Roofing (trade_id=3)
    if any(x in activity_lower for x in ['roof', 'membrane', 'waterproof', 'eifs']):
        return 'Roofing'
    # Drywall (trade_id=4)
    if any(x in activity_lower for x in ['drywall', 'frame', 'stud', 'gyp', 'gypsum', 'framing', 'shaft', 'ceiling grid', 'metal track', 'sheathing']):
        return 'Drywall'
    # Finishes (trade_id=5)
    if any(x in activity_lower for x in ['paint', 'coat', 'finish', 'tile', 'floor', 'ceiling', 'door', 'hardware', 'casework', 'glazing', 'window']):
        return 'Finishes'
    # Fire Protection (trade_id=6)
    if any(x in activity_lower for x in ['fireproof', 'firestop', 'sfrm', 'fire caulk', 'intumescent', 'fire rating', 'fire barrier']):
        return 'Fire Protection'
    # MEP (trade_id=7)
    if any(x in activity_lower for x in ['mep', 'hvac', 'plumb', 'elec', 'pipe', 'conduit', 'duct', 'wire', 'electrical', 'mechanical', 'sprinkler']):
        return 'MEP'
    # Insulation (trade_id=8)
    if any(x in activity_lower for x in ['insul', 'thermal', 'urethane', 'wrap']):
        return 'Insulation'
    # Earthwork (trade_id=9)
    if any(x in activity_lower for x in ['excavat', 'backfill', 'grade', 'foundation', 'pier', 'pile', 'earth']):
        return 'Earthwork'
    # Precast (trade_id=10)
    if any(x in activity_lower for x in ['precast', 'tilt', 'pc panel']):
        return 'Precast'
    # Panels (trade_id=11)
    if any(x in activity_lower for x in ['panel', 'clad', 'skin', 'enclosure', 'metal wall', 'zee metal', 'corbel']):
        return 'Panels'
    # Masonry (trade_id=13)
    if any(x in activity_lower for x in ['masonry', 'cmu', 'block', 'brick', 'grout']):
        return 'Masonry'
    return None


def normalize_level_value(level: str) -> Optional[str]:
    """
    Normalize level values across all sources for consistent spatial filtering.

    Standardizes to format: B2, B1, 1F, 2F, 3F, 4F, 5F, 6F, 7F, ROOF, OUTSIDE

    Args:
        level: Raw level value from any source

    Returns:
        Normalized level string or None if invalid
    """
    if pd.isna(level):
        return None

    level = str(level).upper().strip()

    # Remove common prefixes/suffixes
    level = level.replace('LEVEL ', '').replace('LVL ', '').replace('L-', '')

    # Handle basement variations
    if level in ('B1', 'B1F', 'BASEMENT', 'BASEMENT 1', '1B'):
        return 'B1'
    if level in ('B2', 'B2F', 'BASEMENT 2', '2B'):
        return 'B2'
    if level in ('UG', 'UNDERGROUND'):
        return 'B1'  # Map underground to B1

    # Handle roof variations
    if level in ('ROOF', 'RF', 'ROOFTOP', 'R', 'RTF'):
        return 'ROOF'

    # Handle outside/ground variations
    if any(x in level for x in ['OUTSIDE', 'EXTERIOR', 'GROUND']):
        return 'OUTSIDE'

    # Handle floor number variations (1F, 01F, 1ST, FIRST, etc.)
    m = re.match(r'^0?(\d+)[F]?$', level)
    if m:
        return f"{int(m.group(1))}F"

    # Handle ordinal formats (1ST, 2ND, 3RD, etc.)
    m = re.match(r'^(\d+)(ST|ND|RD|TH)?\s*(FLOOR)?$', level)
    if m:
        return f"{int(m.group(1))}F"

    # Return as-is if no pattern matched (with F suffix if numeric)
    if level.isdigit():
        return f"{level}F"

    return level


def extract_room_code(row) -> Optional[str]:
    """Extract room code from location_row field if present."""
    location_row = row.get('location_row')
    if pd.isna(location_row):
        return None

    val = str(location_row).strip().upper()

    # Room code patterns: FAB1XXXXX (6+ digits after FAB1)
    m = re.match(r'^(FAB1\d{5,})', val)
    if m:
        return m.group(1)

    # Stair patterns: STR-XX, FAB1-STXX
    m = re.match(r'^(?:FAB1-)?ST[R]?[-]?(\d+)', val)
    if m:
        return f"STR-{m.group(1).zfill(2)}"

    # Elevator patterns: ELV-XX, FAB1-ELXX
    m = re.match(r'^(?:FAB1-)?EL[V]?[-]?(\d+)', val)
    if m:
        return f"ELV-{m.group(1).zfill(2)}"

    return None


def consolidate_tbm(dry_run: bool = False) -> Dict[str, Any]:
    """
    Consolidate TBM work_entries.csv with dimension IDs.

    This replaces the enrich_with_dimensions.py:enrich_tbm() + add_csi_to_tbm.py
    by integrating all enrichment directly into the TBM pipeline.
    """
    input_path = Settings.PROCESSED_DATA_DIR / 'tbm' / 'work_entries.csv'
    output_path = Settings.PROCESSED_DATA_DIR / 'tbm' / 'work_entries.csv'

    if not input_path.exists():
        return {'status': 'skipped', 'reason': 'file not found'}

    print(f"Loading data from: {input_path}")
    df = pd.read_csv(input_path)
    original_count = len(df)
    print(f"Loaded {original_count:,} records")

    # Create composite key ID for joining with bridge table
    df['tbm_work_entry_id'] = 'TBM-' + df['file_id'].astype(str) + '-' + df['row_num'].astype(str)

    # Normalize building codes
    building_map = {'FAB': 'FAB', 'SUP': 'SUE', 'Fab': 'FAB', 'OFFICE': None, 'Laydown': None}
    df['building_normalized'] = df['location_building'].map(
        lambda x: building_map.get(x, x) if pd.notna(x) else None
    )

    # Normalize level codes
    print("  Normalizing levels...")
    df['level_normalized'] = df['location_level'].apply(normalize_level_value)

    # Extract room codes from location_row
    print("  Extracting room codes...")
    df['room_code_extracted'] = df.apply(extract_room_code, axis=1)

    # Company lookup - try tier2_sc first, then tier1_gc, then subcontractor_file
    print("  Looking up companies...")
    def get_company_from_row(row):
        # Try tier2_sc (subcontractor) first
        company_id = get_company_id(row.get('tier2_sc'))
        if company_id:
            return company_id
        # Fall back to tier1_gc (some files put company name here)
        company_id = get_company_id(row.get('tier1_gc'))
        if company_id:
            return company_id
        # Final fallback to subcontractor_file (parsed from filename)
        company_id = get_company_id(row.get('subcontractor_file'))
        return company_id

    df['dim_company_id'] = df.apply(get_company_from_row, axis=1)

    # Infer trade from activity (used for CSI fallback)
    print("  Inferring trades from activities...")
    df['trade_inferred'] = df['work_activities'].apply(infer_trade_from_activity)

    # Infer CSI section from work activities and inferred trade
    print("  Inferring CSI sections...")
    csi_results = df.apply(
        lambda row: infer_csi_from_activity(row.get('work_activities'), row.get('trade_inferred')),
        axis=1
    )
    df['dim_csi_section_id'] = csi_results.apply(lambda x: x[0])
    df['csi_section'] = csi_results.apply(lambda x: x[1])
    df['csi_inference_source'] = csi_results.apply(lambda x: x[2])
    df['csi_title'] = df['dim_csi_section_id'].apply(
        lambda x: CSI_SECTIONS[x][1] if pd.notna(x) and x in CSI_SECTIONS else None
    )

    # Parse grid from location_row using TBM-specific parser
    print("  Parsing grid coordinates...")
    grid_parsed = df['location_row'].apply(parse_tbm_grid).apply(pd.Series)
    df['grid_row_min'] = grid_parsed['grid_row_min']
    df['grid_row_max'] = grid_parsed['grid_row_max']
    df['grid_col_min'] = grid_parsed['grid_col_min']
    df['grid_col_max'] = grid_parsed['grid_col_max']
    df['grid_raw'] = grid_parsed['grid_raw']
    df['grid_type'] = grid_parsed['grid_type']

    # Use centralized location enrichment with pre-parsed grid bounds
    print("  Enriching locations (centralized module)...")

    def enrich_location_row(row):
        """Enrich a row using centralized location module with pre-parsed grid."""
        result = enrich_location(
            building=row.get('building_normalized'),
            level=row.get('level_normalized'),
            room_code=row.get('room_code_extracted'),
            location_text=row.get('location_row'),  # For room/stair/elevator extraction
            source='TBM',
            # Pre-parsed grid bounds from TBM parser
            grid_row_min=row.get('grid_row_min'),
            grid_row_max=row.get('grid_row_max'),
            grid_col_min=row.get('grid_col_min'),
            grid_col_max=row.get('grid_col_max'),
            grid_type=row.get('grid_type'),
        )
        return result.to_dict()

    # Apply enrichment to all rows
    location_enriched = df.apply(enrich_location_row, axis=1).apply(pd.Series)

    # Copy enriched columns to dataframe
    df['dim_location_id'] = location_enriched['dim_location_id']
    df['affected_rooms'] = location_enriched['affected_rooms']
    df['affected_rooms_count'] = location_enriched['affected_rooms_count']
    df['location_source'] = location_enriched['match_type']  # map to existing column name

    # Add grid_completeness - describes what location info was available in source
    def get_grid_completeness(row):
        """Determine what grid info was available in the source record."""
        has_level = pd.notna(row.get('level_normalized'))
        has_row = pd.notna(row.get('grid_row_min'))
        has_col = pd.notna(row.get('grid_col_min'))

        if has_row and has_col:
            return 'FULL'
        elif has_row:
            return 'ROW_ONLY'
        elif has_col:
            return 'COL_ONLY'
        elif has_level:
            return 'LEVEL_ONLY'
        else:
            return 'NONE'

    df['grid_completeness'] = df.apply(get_grid_completeness, axis=1)

    # Add match_quality - summary of how rooms were matched
    def get_match_quality(row):
        """Summarize the quality of room matches."""
        json_str = row.get('affected_rooms')
        if pd.isna(json_str):
            return 'NONE'

        try:
            rooms = json.loads(json_str)
            if not rooms:
                return 'NONE'

            match_types = [r.get('match_type') for r in rooms]
            full_count = sum(1 for m in match_types if m == 'FULL')
            partial_count = sum(1 for m in match_types if m == 'PARTIAL')

            if partial_count == 0:
                return 'PRECISE'
            elif full_count == 0:
                return 'PARTIAL'
            else:
                return 'MIXED'
        except (json.JSONDecodeError, TypeError):
            return 'NONE'

    df['match_quality'] = df.apply(get_match_quality, axis=1)

    # Add location_review_flag - suggests whether human review is needed
    def needs_location_review(row):
        """Determine if record needs location investigation."""
        grid_completeness = row.get('grid_completeness')
        match_quality = row.get('match_quality')
        room_count = row.get('affected_rooms_count')

        # No rooms matched - nothing to review
        if pd.isna(room_count) or room_count == 0:
            return False

        # High room count with imprecise matching
        if room_count > 10 and match_quality != 'PRECISE':
            return True

        # Partial matches with moderate room count
        if match_quality == 'PARTIAL' and room_count > 5:
            return True

        # Mixed matches with high room count
        if match_quality == 'MIXED' and room_count > 8:
            return True

        # Level-only records that matched rooms (unusual)
        if grid_completeness == 'LEVEL_ONLY' and room_count > 0:
            return True

        return False

    df['location_review_flag'] = df.apply(needs_location_review, axis=1)

    # Calculate coverage
    has_grid_row = df['grid_row_min'].notna()
    has_grid_col = df['grid_col_min'].notna()
    has_affected_rooms = df['affected_rooms'].notna()
    coverage = {
        'location': df['dim_location_id'].notna().mean() * 100,
        'company': df['dim_company_id'].notna().mean() * 100,
        'csi_section': df['dim_csi_section_id'].notna().mean() * 100,
        'grid_row': has_grid_row.mean() * 100,
        'grid_col': has_grid_col.mean() * 100,
        'affected_rooms': has_affected_rooms.mean() * 100,
    }

    # Location source distribution
    location_source_dist = df['location_source'].value_counts().to_dict()

    # Grid type distribution for reporting
    grid_type_dist = df['grid_type'].value_counts().to_dict()

    # CSI inference source distribution
    csi_source_dist = df['csi_inference_source'].value_counts().to_dict()

    if not dry_run:
        print(f"  Writing output to: {output_path}")
        validated_df_to_csv(df, output_path, index=False)

    return {
        'status': 'success',
        'records': original_count,
        'coverage': coverage,
        'location_sources': location_source_dist,
        'grid_types': grid_type_dist,
        'csi_sources': csi_source_dist,
        'output': str(output_path) if not dry_run else 'DRY RUN (validated)',
    }


def main():
    parser = argparse.ArgumentParser(description='Consolidate TBM work entries with dimension IDs')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without writing files')
    args = parser.parse_args()

    # Reset dimension cache to ensure fresh data
    reset_cache()

    print("=" * 70)
    print("TBM CONSOLIDATION")
    print("=" * 70)

    result = consolidate_tbm(dry_run=args.dry_run)

    if result['status'] == 'success':
        print(f"\nRecords: {result['records']:,}")
        print(f"\nCoverage:")
        for dim, pct in result['coverage'].items():
            print(f"  {dim}: {pct:.1f}%")

        print(f"\nLocation sources:")
        for source, count in sorted(result['location_sources'].items(), key=lambda x: -x[1]):
            pct = count / result['records'] * 100
            print(f"  {source}: {count:,} ({pct:.1f}%)")

        print(f"\nGrid types:")
        for gtype, count in sorted(result['grid_types'].items(), key=lambda x: -x[1]):
            pct = count / result['records'] * 100
            print(f"  {gtype}: {count:,} ({pct:.1f}%)")

        print(f"\nCSI inference sources:")
        for source, count in sorted(result['csi_sources'].items(), key=lambda x: -x[1]):
            pct = count / result['records'] * 100
            print(f"  {source}: {count:,} ({pct:.1f}%)")

        print(f"\nOutput: {result['output']}")
    else:
        print(f"\nStatus: {result['status']}")
        print(f"Reason: {result.get('reason', 'unknown')}")


if __name__ == '__main__':
    main()
