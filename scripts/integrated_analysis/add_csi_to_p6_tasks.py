#!/usr/bin/env python3
"""
Add CSI Section IDs to P6 Task Taxonomy.

Maps task taxonomy sub_trade/scope codes to specific CSI MasterFormat sections.
Uses hierarchical inference: sub_trade → scope → trade_id fallback.

Input:
    {WINDOWS_DATA_DIR}/derived/primavera/task_taxonomy.csv

Output:
    {WINDOWS_DATA_DIR}/derived/primavera/task_taxonomy_with_csi.csv

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

# Trade ID fallback mapping (when sub_trade is not specific enough)
# Maps dim_trade trade_id to a default CSI section
TRADE_ID_TO_DEFAULT_CSI = {
    1: 2,    # Concrete → 03 30 00 Cast-in-Place Concrete
    2: 6,    # Steel → 05 12 00 Structural Steel Framing
    3: 16,   # Roofing → 07 52 00 Membrane Roofing
    4: 26,   # Drywall → 09 21 16 Gypsum Board Assemblies
    5: 29,   # Finishes → 09 91 26 Painting (most common finish)
    6: 18,   # Fireproof → 07 81 00 Applied Fireproofing
    7: 44,   # MEP → 26 05 00 Common Work Results for Electrical
    8: 13,   # Insulation → 07 21 16 Blanket Insulation
    9: 51,   # Earthwork → 31 23 00 Excavation and Fill
    10: 3,   # Precast → 03 41 00 Structural Precast
    11: 15,  # Panels → 07 42 43 Composite Wall Panels
    12: 1,   # General → 01 10 00 Summary
    13: 5,   # Masonry → 04 20 00 Unit Masonry
}


def infer_csi_from_taxonomy(sub_trade: str, scope: str, trade_id) -> Tuple[Optional[int], Optional[str], str]:
    """
    Infer CSI section from task taxonomy fields.

    Priority: sub_trade → scope → trade_id fallback

    Args:
        sub_trade: Detailed scope code (e.g., 'CIP', 'STL', 'DRY')
        scope: Broader scope code
        trade_id: dim_trade trade_id (1-13)

    Returns:
        Tuple of (csi_section_id, csi_section_code, inference_source)
    """
    # Try sub_trade first (most specific)
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

    # Fall back to trade_id
    if pd.notna(trade_id):
        try:
            tid = int(trade_id)
            if tid in TRADE_ID_TO_DEFAULT_CSI:
                csi_id = TRADE_ID_TO_DEFAULT_CSI[tid]
                csi_code, _ = CSI_SECTIONS[csi_id]
                return csi_id, csi_code, "trade_id"
        except (ValueError, TypeError):
            pass

    return None, None, "none"


def add_csi_to_p6_tasks(dry_run: bool = False):
    """Add CSI section IDs to P6 task taxonomy."""

    input_path = settings.PRIMAVERA_DERIVED_DIR / "task_taxonomy.csv"
    output_path = settings.PRIMAVERA_DERIVED_DIR / "task_taxonomy_with_csi.csv"

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return

    print(f"Loading task taxonomy from: {input_path}")
    df = pd.read_csv(input_path, low_memory=False)
    print(f"Loaded {len(df):,} records")

    # Apply CSI inference
    print("Inferring CSI sections...")
    results = df.apply(
        lambda row: infer_csi_from_taxonomy(
            row.get('sub_trade'),
            row.get('scope'),
            row.get('trade_id')
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
    sub_trade_count = (df['csi_inference_source'] == 'sub_trade').sum()
    scope_count = (df['csi_inference_source'] == 'scope').sum()
    trade_id_count = (df['csi_inference_source'] == 'trade_id').sum()
    none_count = (df['csi_inference_source'] == 'none').sum()

    print(f"\nCSI Section Coverage: {coverage:.1f}%")
    print(f"  From sub_trade: {sub_trade_count:,} ({sub_trade_count/len(df)*100:.1f}%)")
    print(f"  From scope: {scope_count:,} ({scope_count/len(df)*100:.1f}%)")
    print(f"  From trade_id: {trade_id_count:,} ({trade_id_count/len(df)*100:.1f}%)")
    print(f"  No match: {none_count:,} ({none_count/len(df)*100:.1f}%)")

    # Show distribution by CSI section
    print("\nTop 15 CSI Sections:")
    csi_dist = df[df['dim_csi_section_id'].notna()].groupby(['csi_section', 'csi_title']).size().sort_values(ascending=False).head(15)
    for (section, title), count in csi_dist.items():
        print(f"  {section} {title}: {count:,}")

    if not dry_run:
        df.to_csv(output_path, index=False)
        print(f"\nOutput written to: {output_path}")
    else:
        print("\nDRY RUN - no output written")

    return df


def main():
    parser = argparse.ArgumentParser(description='Add CSI section IDs to P6 task taxonomy')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing output')
    args = parser.parse_args()

    add_csi_to_p6_tasks(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
