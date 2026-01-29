#!/usr/bin/env python3
"""
Add CSI Section IDs to P6 Task Taxonomy.

Maps task taxonomy to specific CSI MasterFormat sections using hierarchical inference:
1. task_name keywords (most specific) - distinguishes fire-related work types
2. sub_trade codes (e.g., CIP, STL, DRY)
3. scope codes (broader category)
4. trade_id fallback

The keyword-based inference is critical for fire-related work where the FIR sub_trade
is used as a catch-all but actually contains:
- Firestopping (07 84 00) - penetration seals, fire caulk
- Fire Suppression (21 10 00) - sprinklers
- Fireproofing (07 81 00) - SFRM, IFRM, intumescent coatings

Appends CSI columns to the original taxonomy file (does not create separate file).
New columns added: dim_csi_section_id, csi_section, csi_inference_source, csi_title

Input/Output:
    {WINDOWS_DATA_DIR}/processed/primavera/p6_task_taxonomy.csv

Usage:
    python -m scripts.integrated_analysis.add_csi_to_p6_tasks
    python -m scripts.integrated_analysis.add_csi_to_p6_tasks --dry-run
"""

import argparse
import sys
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

# Add project root to path
_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings

# Import shared CSI definitions
from scripts.integrated_analysis.add_csi_to_raba import CSI_SECTIONS

# Keyword patterns for task_name-based CSI inference
# Order matters - more specific patterns checked first
# Format: (keywords_list, csi_section_id, description)
TASK_NAME_KEYWORDS = [
    # Firestopping vs Fireproofing (both Division 07, commonly confused)
    # Check firestopping FIRST since it's more specific
    (["firestop", "fire stop", "fire-stop", "firestopping", "penetration seal", "fire caulk"],
     19, "Firestopping"),  # 07 84 00

    # Fireproofing (SFRM/IFRM) - only if explicit keywords
    (["sfrm", "ifrm", "intumescent", "fireproofing", "spray fire", "applied fire"],
     18, "Fireproofing"),  # 07 81 00

    # Fire Suppression (sprinklers) - distinguish from firestopping
    (["sprinkler", "fire suppression", "fire protection piping", "fire line"],
     34, "Fire Suppression"),  # 21 10 00

    # Fire Pump
    (["fire pump"], 35, "Fire Pump"),  # 21 30 00

    # Grouting - often missed
    (["grout", "grouting", "non-shrink grout", "grout tube"], 4, "Grouting"),  # 03 60 00

    # Masonry - CMU, block work
    (["cmu", "masonry", "block wall", "concrete block", "parapet"], 5, "Masonry"),  # 04 20 00

    # Steel Decking - specific
    (["steel deck", "metal deck", "floor deck", "roof deck", "decking"], 7, "Steel Decking"),  # 05 31 00

    # Roof Specialties - copings, flashings
    (["coping", "flashing", "roof edge", "parapet cap"], 17, "Roof Specialties"),  # 07 71 00

    # Joint Protection / Sealants
    (["sealant", "caulk", "joint seal", "expansion joint", "control joint"],
     20, "Joint Protection"),  # 07 90 00

    # Door Hardware
    (["hardware", "lockset", "door closer", "hinge"], 23, "Door Hardware"),  # 08 71 00

    # Chemical-Resistant Coatings (epoxy floor coatings)
    (["epoxy coating", "chemical resistant", "floor coating", "coating inspection", "floor coat"],
     25, "Chemical-Resistant Coatings"),  # 09 06 65

    # Acoustical Ceilings
    (["acoustical ceiling", "ceiling tile", "ceiling grid", "act ceiling", "drop ceiling"],
     27, "Acoustical Ceilings"),  # 09 51 00

    # Resilient Flooring
    (["vct", "resilient floor", "vinyl tile", "rubber floor", "vinyl composition"],
     28, "Resilient Flooring"),  # 09 65 00

    # Sound/Vibration Control
    (["vibration", "acoustic", "sound control", "noise control"],
     32, "Sound and Vibration Control"),  # 13 48 00

    # HVAC Ducts
    (["ductwork", "hvac duct", "duct install", "spiral duct"], 41, "HVAC Ducts"),  # 23 31 00

    # Electrical Conductors/Cables
    (["wire pull", "cable pull", "conductor"], 45, "Electrical Conductors"),  # 26 05 19

    # Site Clearing/Demolition
    (["demolition", "demo ", "site clearing"], 50, "Site Clearing"),  # 31 10 00
]

# P6 sub_trade/scope codes to CSI section ID mapping
# These codes come from task_taxonomy extraction (e.g., CIP, STL, DRY)
SUB_TRADE_TO_CSI = {
    # Concrete work
    'CIP': 2,       # 03 30 00 Cast-in-Place Concrete
    'CTG': 2,       # 03 30 00 Cast-in-Place Concrete (topping)
    'GRT': 4,       # 03 60 00 Grouting

    # Precast concrete
    'PRC': 3,       # 03 41 00 Structural Precast Concrete
    'PCE': 3,       # 03 41 00 Structural Precast Erection
    'PCF': 3,       # 03 41 00 Structural Precast Fabrication

    # Masonry
    'CMU': 5,       # 04 20 00 Unit Masonry

    # Steel work
    'STL': 6,       # 05 12 00 Structural Steel Framing
    'DCK': 7,       # 05 31 00 Steel Decking
    'FRM': 8,       # 05 40 00 Cold-Formed Metal Framing
    'MSC': 9,       # 05 50 00 Metal Fabrications
    'STA': 9,       # 05 50 00 Metal Fabrications (stairs)
    'STR': 9,       # 05 50 00 Metal Fabrications (stairs - alternate code)

    # Thermal/moisture protection
    'DMP': 10,      # 07 11 00 Dampproofing
    'WPF': 11,      # 07 13 00 Sheet Waterproofing
    'BRD': 12,      # 07 21 13 Board Insulation
    'INS': 13,      # 07 21 16 Blanket Insulation
    'VB': 14,       # 07 26 00 Vapor Retarders
    'PNL': 15,      # 07 42 43 Composite Wall Panels
    'IMP': 15,      # 07 42 43 Insulated Metal Panels
    'SKN': 15,      # 07 42 43 Metal Panels (skin)
    'ROF': 16,      # 07 52 00 Membrane Roofing
    'CPG': 17,      # 07 71 00 Roof Specialties (coping)
    'FPR': 18,      # 07 81 00 Applied Fireproofing (SFRM/IFRM)
    'FIR': 18,      # 07 81 00 Applied Fireproofing
    'FST': 19,      # 07 84 00 Firestopping
    'JSL': 20,      # 07 90 00 Joint Protection (sealant)

    # Openings
    'DOR': 21,      # 08 11 13 Hollow Metal Doors and Frames
    'OHD': 22,      # 08 33 23 Overhead Coiling Doors
    'HDW': 23,      # 08 71 00 Door Hardware
    'GLZ': 24,      # 08 80 00 Glazing
    'CW': 24,       # 08 80 00 Curtain Wall

    # Finishes
    'CHR': 25,      # 09 06 65 Chemical-Resistant Coatings (epoxy)
    'DRY': 26,      # 09 21 16 Gypsum Board Assemblies
    'GYP': 26,      # 09 21 16 Gypsum Board
    'CLG': 27,      # 09 51 00 Acoustical Ceilings
    'ACT': 27,      # 09 51 00 Acoustical Ceiling Tile
    'FLR': 28,      # 09 65 00 Resilient Flooring
    'VCT': 28,      # 09 65 00 VCT Flooring
    'PNT': 29,      # 09 91 26 Painting - Building
    'COT': 29,      # 09 91 26 Coatings
    'SPE': 30,      # 10 00 00 Specialties

    # Equipment
    'DCE': 31,      # 11 13 19 Loading Dock Equipment

    # Special construction
    'SVC': 32,      # 13 48 00 Sound and Vibration Control

    # Conveying equipment
    'ELV': 33,      # 14 21 00 Electric Traction Elevators

    # Fire suppression
    'SPK': 34,      # 21 10 00 Fire Suppression (sprinklers)
    'FSP': 34,      # 21 10 00 Fire Suppression
    'FPP': 35,      # 21 30 00 Fire Pumps

    # Plumbing
    'PLB': 36,      # 22 05 00 Common Work Results for Plumbing
    'DWT': 37,      # 22 11 00 Water Distribution
    'SAN': 38,      # 22 13 00 Sanitary Sewerage
    'STD': 39,      # 22 14 00 Storm Drainage

    # HVAC
    'HVC': 40,      # 23 05 00 Common Work Results for HVAC
    'MCH': 40,      # 23 05 00 Mechanical
    'DCT': 41,      # 23 31 00 HVAC Ducts
    'VAV': 42,      # 23 36 00 Air Terminal Units
    'AHU': 43,      # 23 73 00 Air Handling Units

    # Electrical
    'ELC': 44,      # 26 05 00 Common Work Results for Electrical
    'WIR': 45,      # 26 05 19 Conductors and Cables
    'CDT': 46,      # 26 05 33 Raceway and Boxes
    'SWB': 47,      # 26 24 00 Switchboards
    'DEV': 48,      # 26 27 26 Wiring Devices
    'LTG': 49,      # 26 51 00 Interior Lighting

    # Earthwork
    'CLR': 50,      # 31 10 00 Site Clearing
    'DEM': 50,      # 31 10 00 Demolition
    'EXC': 51,      # 31 23 00 Excavation and Fill
    'BKF': 51,      # 31 23 00 Backfill
    'GRD': 51,      # 31 23 00 Grading
    'PIR': 52,      # 31 63 00 Bored Piles (piers)
    'FND': 52,      # 31 63 00 Foundations
}

# Note: TRADE_ID_TO_DEFAULT_CSI removed - dim_trade has been superseded by dim_csi_section
# CSI inference now relies solely on task_name keywords, sub_trade, and scope


def infer_csi_from_keywords(task_name: str) -> Tuple[Optional[int], Optional[str], str]:
    """
    Infer CSI section from task name keywords.

    This catches specific work types that sub_trade codes miss, especially
    fire-related work where FIR is used as a catch-all.

    Args:
        task_name: Raw task name string

    Returns:
        Tuple of (csi_section_id, csi_section_code, inference_source)
    """
    if pd.isna(task_name):
        return None, None, "none"

    name_lower = str(task_name).lower()

    for keywords, csi_id, description in TASK_NAME_KEYWORDS:
        for keyword in keywords:
            if keyword in name_lower:
                csi_code, _ = CSI_SECTIONS[csi_id]
                return csi_id, csi_code, "keyword"

    return None, None, "none"


def infer_csi_from_taxonomy(
    task_name: str,
    sub_trade: str,
    scope: str,
) -> Tuple[Optional[int], Optional[str], str]:
    """
    Infer CSI section from task taxonomy fields.

    Priority: task_name keywords → sub_trade → scope

    The keyword check is critical for fire-related work where FIR sub_trade
    is used as a catch-all but contains firestopping, fire suppression, etc.

    Note: dim_trade has been superseded by dim_csi_section. Trade-based
    fallback has been removed.

    Args:
        task_name: Raw task name for keyword matching
        sub_trade: Detailed scope code (e.g., 'CIP', 'STL', 'DRY')
        scope: Broader scope code

    Returns:
        Tuple of (csi_section_id, csi_section_code, inference_source)
    """
    # Try task_name keywords first (most specific for ambiguous sub_trades)
    if pd.notna(task_name):
        csi_id, csi_code, source = infer_csi_from_keywords(task_name)
        if csi_id is not None:
            return csi_id, csi_code, source

    # Try sub_trade codes
    if pd.notna(sub_trade):
        sub_trade_upper = str(sub_trade).strip().upper()
        if sub_trade_upper in SUB_TRADE_TO_CSI:
            csi_id = SUB_TRADE_TO_CSI[sub_trade_upper]
            csi_code, _ = CSI_SECTIONS[csi_id]
            return csi_id, csi_code, "sub_trade"

    # Try scope (broader category)
    if pd.notna(scope):
        scope_upper = str(scope).strip().upper()
        if scope_upper in SUB_TRADE_TO_CSI:
            csi_id = SUB_TRADE_TO_CSI[scope_upper]
            csi_code, _ = CSI_SECTIONS[csi_id]
            return csi_id, csi_code, "scope"

    return None, None, "none"


def add_csi_to_p6_tasks(dry_run: bool = False):
    """Add CSI section IDs to P6 task taxonomy (appends to original file)."""

    input_path = settings.PRIMAVERA_PROCESSED_DIR / "p6_task_taxonomy.csv"
    task_path = settings.PRIMAVERA_PROCESSED_DIR / "task.csv"
    # Write back to the same file (append columns to original)
    output_path = input_path

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return

    print(f"Loading task taxonomy from: {input_path}")
    df = pd.read_csv(input_path, low_memory=False)
    print(f"Loaded {len(df):,} records")

    # Load task names for keyword-based inference
    task_names = None
    if task_path.exists():
        print(f"Loading task names from: {task_path}")
        tasks_df = pd.read_csv(task_path, low_memory=False, usecols=['task_id', 'task_name'])
        task_names = tasks_df.set_index('task_id')['task_name'].to_dict()
        print(f"Loaded {len(task_names):,} task names")
    else:
        print("Warning: task.csv not found, keyword-based inference disabled")

    # Apply CSI inference with task_name lookup
    print("Inferring CSI sections (with keyword matching)...")

    def get_task_name(task_id):
        """Look up task name from task.csv."""
        if task_names is None:
            return None
        return task_names.get(task_id)

    results = df.apply(
        lambda row: infer_csi_from_taxonomy(
            get_task_name(row.get('task_id')),
            row.get('sub_trade'),
            row.get('scope'),
        ),
        axis=1
    )

    df['dim_csi_section_id'] = results.apply(lambda x: x[0])
    df['csi_section'] = results.apply(lambda x: x[1])
    df['csi_inference_source'] = results.apply(lambda x: x[2])

    # Add CSI title for reference
    df['csi_title'] = df['dim_csi_section_id'].apply(
        lambda x: CSI_SECTIONS[x][1] if pd.notna(x) and x in CSI_SECTIONS else None
    )

    # Calculate coverage
    coverage = df['dim_csi_section_id'].notna().mean() * 100
    keyword_count = (df['csi_inference_source'] == 'keyword').sum()
    sub_trade_count = (df['csi_inference_source'] == 'sub_trade').sum()
    scope_count = (df['csi_inference_source'] == 'scope').sum()
    none_count = (df['csi_inference_source'] == 'none').sum()

    print(f"\nCSI Section Coverage: {coverage:.1f}%")
    print(f"  From keyword:   {keyword_count:,} ({keyword_count/len(df)*100:.1f}%)")
    print(f"  From sub_trade: {sub_trade_count:,} ({sub_trade_count/len(df)*100:.1f}%)")
    print(f"  From scope:     {scope_count:,} ({scope_count/len(df)*100:.1f}%)")
    print(f"  No match:       {none_count:,} ({none_count/len(df)*100:.1f}%)")

    # Show keyword matches by CSI section
    if keyword_count > 0:
        print("\nKeyword-matched CSI sections:")
        keyword_df = df[df['csi_inference_source'] == 'keyword']
        keyword_dist = keyword_df.groupby(['csi_section', 'csi_title']).size().sort_values(ascending=False)
        for (section, title), count in keyword_dist.items():
            print(f"  {section} {title}: {count:,}")

    # Show distribution by CSI section
    print("\nTop 15 CSI Sections:")
    csi_dist = df[df['dim_csi_section_id'].notna()].groupby(['csi_section', 'csi_title']).size().sort_values(ascending=False).head(15)
    for (section, title), count in csi_dist.items():
        print(f"  {section} {title}: {count:,}")

    if not dry_run:
        df.to_csv(output_path, index=False)
        print(f"\nCSI columns appended to: {output_path}")
    else:
        print("\nDRY RUN - no changes written")

    return df


def main():
    parser = argparse.ArgumentParser(description='Add CSI section IDs to P6 task taxonomy')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing output')
    args = parser.parse_args()

    add_csi_to_p6_tasks(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
