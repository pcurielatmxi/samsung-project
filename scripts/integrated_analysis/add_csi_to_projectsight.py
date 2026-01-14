#!/usr/bin/env python3
"""
Add CSI Section IDs to ProjectSight Labor Entries.

ProjectSight has CSI division codes (2-digit) in trade_full field.
This script infers more specific CSI sections (6-digit) by:
1. Parsing activity field for specific keywords
2. Falling back to division-to-section mapping

Input:
    {WINDOWS_DATA_DIR}/processed/projectsight/labor_entries_enriched.csv

Output:
    {WINDOWS_DATA_DIR}/processed/projectsight/projectsight_with_csi.csv

Usage:
    python -m scripts.integrated_analysis.add_csi_to_projectsight
    python -m scripts.integrated_analysis.add_csi_to_projectsight --dry-run
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

# CSI Division to default Section mapping
# When we only have division info, use the "common work results" section
DIVISION_TO_DEFAULT_CSI = {
    "01": 1,   # 01 10 00 Summary (General Requirements)
    "02": 50,  # 31 10 00 Site Clearing (Existing Conditions → Demo/Clearing)
    "03": 2,   # 03 30 00 Cast-in-Place Concrete
    "04": 5,   # 04 20 00 Unit Masonry
    "05": 6,   # 05 12 00 Structural Steel Framing
    "06": 8,   # 05 40 00 Cold-Formed Metal Framing (Wood/Plastics → Framing)
    "07": 13,  # 07 21 16 Blanket Insulation (Thermal/Moisture default)
    "08": 21,  # 08 11 13 Hollow Metal Doors (Openings default)
    "09": 29,  # 09 91 26 Painting - Building (Finishes default)
    "10": 1,   # 01 10 00 Summary (Specialties → General)
    "11": 31,  # 11 13 19 Loading Dock Equipment
    "12": 1,   # 01 10 00 Summary (Furnishings → General)
    "13": 32,  # 13 48 00 Sound and Vibration Control
    "14": 33,  # 14 21 00 Electric Traction Elevators
    "21": 34,  # 21 10 00 Fire Suppression
    "22": 36,  # 22 05 00 Common Work Results for Plumbing
    "23": 40,  # 23 05 00 Common Work Results for HVAC
    "25": 44,  # 26 05 00 Electrical (Integrated Automation → Electrical)
    "26": 44,  # 26 05 00 Common Work Results for Electrical
    "27": 44,  # 26 05 00 Electrical (Communications → Electrical)
    "28": 44,  # 26 05 00 Electrical (Security → Electrical)
    "31": 51,  # 31 23 00 Excavation and Fill
    "32": 51,  # 31 23 00 Excavation (Exterior Improvements)
    "33": 51,  # 31 23 00 Excavation (Utilities)
}

# Activity keyword patterns for more specific CSI inference
# Similar to TBM but tailored for ProjectSight activity descriptions
ACTIVITY_TO_CSI = [
    # Fireproofing vs Firestopping
    (["sfrm", "ifrm", "fireproofing", "fire proof"], 18),  # 07 81 00 Applied Fireproofing
    (["firestop", "fire stop", "fire caulk"], 19),  # 07 84 00 Firestopping

    # Concrete specifics
    (["precast", "waffle", "double t", "spandrel", "tilt"], 3),  # 03 41 00 Structural Precast
    (["grout", "grouting"], 4),  # 03 60 00 Grouting
    (["voidform", "void form"], 2),  # 03 30 00 Cast-in-Place Concrete (forms)
    (["pour", "placement", "slab", "topping", "sog", "mat found", "concrete"], 2),  # 03 30 00 Cast-in-Place

    # Steel specifics
    (["steel deck", "decking", "roof deck", "floor deck"], 7),  # 05 31 00 Steel Decking
    (["structural steel", "steel erect", "steel beam", "steel column"], 6),  # 05 12 00 Structural Steel
    (["metal stud", "framing", "zee metal", "z-metal"], 8),  # 05 40 00 Cold-Formed Metal Framing
    (["misc steel", "stair", "railing", "handrail", "grating"], 9),  # 05 50 00 Metal Fabrications
    (["weld"], 6),  # 05 12 00 Structural Steel

    # Drywall/Gypsum - "densglass" must come BEFORE "glass" patterns
    (["densglass", "dens glass"], 26),  # 09 21 16 Gypsum Board (exterior sheathing)
    (["drywall", "gypsum", "gyp board", "sheetrock"], 26),  # 09 21 16 Gypsum Board
    (["shaft wall", "shaft liner"], 26),  # 09 21 16 Gypsum Board
    (["tape", "taping", "float", "mud", "finish drywall"], 26),  # 09 21 16 Gypsum finishing
    (["ceiling grid", "ceiling tile", "acoustical"], 27),  # 09 51 00 Acoustical Ceilings

    # Waterproofing/Roofing
    (["waterproof", "below grade", "sikaproof"], 11),  # 07 13 00 Sheet Waterproofing
    (["dampproof"], 10),  # 07 11 00 Dampproofing
    (["roofing", "tpo", "membrane roof"], 16),  # 07 52 00 Membrane Roofing
    (["coping", "flashing", "roof edge"], 17),  # 07 71 00 Roof Specialties

    # Insulation
    (["insulation", "mineral wool", "batt"], 13),  # 07 21 16 Blanket Insulation
    (["rigid insul", "foam board"], 12),  # 07 21 13 Board Insulation
    (["vapor barrier", "vapor retard"], 14),  # 07 26 00 Vapor Retarders

    # Panels/Cladding
    # Panels - use "imp panel" to avoid matching "IMPROPER", "improvement"
    (["imp panel", "metal panel", "wall panel", "cladding"], 15),  # 07 42 43 Composite Wall Panels

    # Masonry - avoid bare "block" (matches "blocking", "blocked")
    (["masonry", "cmu", "cmu block", "concrete block", "brick"], 5),  # 04 20 00 Unit Masonry

    # Finishes
    (["paint", "primer", "coating"], 29),  # 09 91 26 Painting - Building
    (["epoxy", "chemical resist"], 25),  # 09 06 65 Chemical-Resistant Coatings
    (["flooring", "resilient", "vct", "tile"], 28),  # 09 65 00 Resilient Flooring
    (["sealant", "caulk", "joint seal"], 20),  # 07 90 00 Joint Protection

    # Openings
    (["door frame", "hollow metal", "hm door"], 21),  # 08 11 13 Hollow Metal Doors
    (["coiling door", "roll-up", "overhead door"], 22),  # 08 33 23 Overhead Coiling Doors
    (["hardware", "lockset", "hinge", "closer"], 23),  # 08 71 00 Door Hardware
    (["glazing", "glass", "curtain wall", "storefront"], 24),  # 08 80 00 Glazing

    # MEP - Fire Suppression
    (["sprinkler", "fire suppression"], 34),  # 21 10 00 Fire Suppression
    (["fire pump"], 35),  # 21 30 00 Fire Pumps

    # MEP - Plumbing
    (["plumbing", "domestic water"], 37),  # 22 11 00 Water Distribution
    (["sanitary", "waste", "sewer"], 38),  # 22 13 00 Sanitary Sewerage
    (["storm drain"], 39),  # 22 14 00 Storm Drainage

    # MEP - HVAC
    (["duct", "ductwork"], 41),  # 23 31 00 HVAC Ducts
    (["diffuser", "vav", "air terminal"], 42),  # 23 36 00 Air Terminal Units
    (["ahu", "air handling"], 43),  # 23 73 00 AHUs
    (["hvac", "mechanical"], 40),  # 23 05 00 Common Work Results for HVAC

    # MEP - Electrical
    (["conduit", "raceway", "junction box", "cable tray"], 46),  # 26 05 33 Raceway and Boxes
    (["wire", "cable", "conductor"], 45),  # 26 05 19 Conductors and Cables
    # Use specific panel terms to avoid matching "wall panel", "metal panel"
    (["switchboard", "panelboard", "electrical panel"], 47),  # 26 24 00 Switchboards
    (["receptacle", "outlet", "switch"], 48),  # 26 27 26 Wiring Devices
    (["lighting", "light fixture"], 49),  # 26 51 00 Interior Lighting
    (["electrical"], 44),  # 26 05 00 Common Work Results for Electrical

    # Earthwork
    (["pier", "drilled pier", "caisson", "pile"], 52),  # 31 63 00 Bored Piles
    (["excavat", "backfill", "compaction", "grade"], 51),  # 31 23 00 Excavation and Fill
    (["clearing", "demo", "site clear"], 50),  # 31 10 00 Site Clearing

    # Special/General
    (["elevator", "escalator"], 33),  # 14 21 00 Elevators
]


def extract_division(trade_full: str) -> Optional[str]:
    """Extract 2-digit division code from trade_full like '03 - Concrete'."""
    if pd.isna(trade_full):
        return None
    trade_full = str(trade_full).strip()
    if len(trade_full) >= 2 and trade_full[:2].isdigit():
        return trade_full[:2]
    return None


def infer_csi_from_projectsight(activity: str, trade_full: str) -> Tuple[Optional[int], Optional[str], str]:
    """
    Infer CSI section from ProjectSight activity and trade_full fields.

    Args:
        activity: Activity description
        trade_full: CSI division string like '03 - Concrete'

    Returns:
        Tuple of (csi_section_id, csi_section_code, inference_source)
    """
    # Try activity keyword matching first (more specific)
    if pd.notna(activity):
        activity_lower = str(activity).lower()

        for keywords, csi_id in ACTIVITY_TO_CSI:
            for keyword in keywords:
                if keyword in activity_lower:
                    csi_code, _ = CSI_SECTIONS[csi_id]
                    return csi_id, csi_code, "activity"

    # Fall back to division mapping
    division = extract_division(trade_full)
    if division and division in DIVISION_TO_DEFAULT_CSI:
        csi_id = DIVISION_TO_DEFAULT_CSI[division]
        csi_code, _ = CSI_SECTIONS[csi_id]
        return csi_id, csi_code, "division"

    return None, None, "none"


def add_csi_to_projectsight(dry_run: bool = False):
    """Add CSI section IDs to ProjectSight labor entries."""

    input_path = settings.PROCESSED_DATA_DIR / "projectsight" / "labor_entries_enriched.csv"
    output_path = settings.PROCESSED_DATA_DIR / "projectsight" / "projectsight_with_csi.csv"

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return

    print(f"Loading ProjectSight data from: {input_path}")
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df):,} records")

    # Apply CSI inference
    print("Inferring CSI sections...")
    results = df.apply(
        lambda row: infer_csi_from_projectsight(row.get('activity'), row.get('trade_full')),
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
    division_count = (df['csi_inference_source'] == 'division').sum()

    print(f"\nCSI Section Coverage: {coverage:.1f}%")
    print(f"  From activity: {activity_count:,} ({activity_count/len(df)*100:.1f}%)")
    print(f"  From division: {division_count:,} ({division_count/len(df)*100:.1f}%)")
    print(f"  No match: {len(df) - activity_count - division_count:,}")

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
    parser = argparse.ArgumentParser(description='Add CSI section IDs to ProjectSight data')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing output')
    args = parser.parse_args()

    add_csi_to_projectsight(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
