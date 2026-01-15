#!/usr/bin/env python3
"""
Add CSI Section IDs to TBM Daily Work Entries.

Parses work_activities field to determine the most specific CSI section code.
TBM data has free-text activity descriptions that need keyword parsing.

Appends CSI columns to the original enriched file (does not create separate file).
New columns added: dim_csi_section_id, csi_section, csi_inference_source, csi_title

Input/Output:
    {WINDOWS_DATA_DIR}/processed/tbm/work_entries_enriched.csv

Usage:
    python -m scripts.integrated_analysis.add_csi_to_tbm
    python -m scripts.integrated_analysis.add_csi_to_tbm --dry-run
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

# TBM-specific keyword patterns to CSI section ID mapping
# work_activities field uses different terminology than inspection types
# Order matters - more specific patterns should come first
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
    (["air barrier", "sheet applied"], 14),  # 07 26 00 Air Barriers → Vapor Retarders
    # Removed "wrap" - too generic, matches cable wrapping which is electrical work
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
    (["t/f wall", "t/f walls", "tenant finish", "penthouse wall"], 26),  # 09 21 16 T/F Walls → Gypsum Board

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
    (["scaffold", "trestle", "trest"], 1),  # 01 10 00 Scaffolding → General Requirements
    (["labor work", "cleaning", "clean up", "sweeping", "punch list"], 1),  # 01 10 00 General labor
    (["rough in", "busway", "termination"], 44),  # 26 05 00 Electrical rough-in
    (["supervising", "supervisor", "foreman", "superintendent", "managers", "pes"], 1),  # 01 10 00 Supervision → General
    (["qc", "qa-qc", "qaqc", "quality control", "logistics"], 1),  # 01 10 00 QC → General
    (["ej", "expansion joint plate", "interior expansion"], 20),  # 07 90 00 Expansion Joints → Joint Protection
    (["pulling branch", "secondary elec", "elec room"], 44),  # 26 05 00 Electrical
    (["imp setting", "awning"], 15),  # 07 42 43 IMP/Awnings → Composite Panels
    (["plywood", "blocking", "backing"], 8),  # 05 40 00 Backing/blocking → Framing
    (["support", "install support"], 6),  # 05 12 00 Supports → Structural Steel
    (["ncr", "change request", "catch up"], 1),  # 01 10 00 NCR/Change work → General
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


def add_csi_to_tbm(dry_run: bool = False):
    """Add CSI section IDs to TBM work entries (appends to original file)."""

    input_path = settings.PROCESSED_DATA_DIR / "tbm" / "work_entries_enriched.csv"
    # Write back to the same file (append columns to original)
    output_path = input_path

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return

    print(f"Loading TBM data from: {input_path}")
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df):,} records")

    # Apply CSI inference
    print("Inferring CSI sections from work activities...")
    results = df.apply(
        lambda row: infer_csi_from_activity(row.get('work_activities'), row.get('trade_inferred')),
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
    activity_count = (df['csi_inference_source'] == 'activity').sum()
    trade_count = (df['csi_inference_source'] == 'trade').sum()

    print(f"\nCSI Section Coverage: {coverage:.1f}%")
    print(f"  From activities: {activity_count:,} ({activity_count/len(df)*100:.1f}%)")
    print(f"  From trade: {trade_count:,} ({trade_count/len(df)*100:.1f}%)")
    print(f"  No match: {len(df) - activity_count - trade_count:,}")

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
    parser = argparse.ArgumentParser(description='Add CSI section IDs to TBM data')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing output')
    args = parser.parse_args()

    add_csi_to_tbm(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
