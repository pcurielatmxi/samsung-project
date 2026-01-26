#!/usr/bin/env python3
"""
Add CSI Section IDs to RABA Quality Inspections.

Parses inspection_type field to determine the most specific CSI section code.
More granular than the existing dim_trade_id (13 trades) by using 52 CSI sections.

Inference Logic:
- Parse inspection_type for specific keywords (e.g., "drywall", "firestop", "SFRM")
- Match to most specific CSI section based on keyword patterns
- Fall back to inspection_category → CSI section mapping if no specific match

Appends CSI columns to the original consolidated file (does not create separate file).
New columns added: dim_csi_section_id, csi_section, csi_inference_source, csi_title

Input/Output:
    {WINDOWS_DATA_DIR}/processed/raba/4.consolidate/raba_qc_inspections.csv

Usage:
    python -m scripts.integrated_analysis.add_csi_to_raba
    python -m scripts.integrated_analysis.add_csi_to_raba --dry-run
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

# CSI Section definitions from dim_csi_section.csv
# Format: csi_section_id, csi_section, csi_title
CSI_SECTIONS = {
    1: ("01 10 00", "Summary"),
    2: ("03 30 00", "Cast-in-Place Concrete"),
    3: ("03 41 00", "Structural Precast Concrete"),
    4: ("03 60 00", "Grouting"),
    5: ("04 20 00", "Unit Masonry"),
    6: ("05 12 00", "Structural Steel Framing"),
    7: ("05 31 00", "Steel Decking"),
    8: ("05 40 00", "Cold-Formed Metal Framing"),
    9: ("05 50 00", "Metal Fabrications"),
    10: ("07 11 00", "Dampproofing"),
    11: ("07 13 00", "Sheet Waterproofing"),
    12: ("07 21 13", "Board Insulation"),
    13: ("07 21 16", "Blanket Insulation"),
    14: ("07 26 00", "Vapor Retarders"),
    15: ("07 42 43", "Composite Wall Panels"),
    16: ("07 52 00", "Modified Bituminous Membrane Roofing"),
    17: ("07 71 00", "Roof Specialties"),
    18: ("07 81 00", "Applied Fireproofing"),
    19: ("07 84 00", "Firestopping"),
    20: ("07 90 00", "Joint Protection"),
    21: ("08 11 13", "Hollow Metal Doors and Frames"),
    22: ("08 33 23", "Overhead Coiling Doors"),
    23: ("08 71 00", "Door Hardware"),
    24: ("08 80 00", "Glazing"),
    25: ("09 06 65", "Chemical-Resistant Coatings"),
    26: ("09 21 16", "Gypsum Board Assemblies"),
    27: ("09 51 00", "Acoustical Ceilings"),
    28: ("09 65 00", "Resilient Flooring"),
    29: ("09 91 26", "Painting - Building"),
    30: ("09 91 29", "Painting - Equipment and Piping"),
    31: ("11 13 19", "Loading Dock Equipment"),
    32: ("13 48 00", "Sound and Vibration Control"),
    33: ("14 21 00", "Electric Traction Elevators"),
    34: ("21 10 00", "Water-Based Fire-Suppression Systems"),
    35: ("21 30 00", "Fire Pumps"),
    36: ("22 05 00", "Common Work Results for Plumbing"),
    37: ("22 11 00", "Facility Water Distribution"),
    38: ("22 13 00", "Facility Sanitary Sewerage"),
    39: ("22 14 00", "Facility Storm Drainage"),
    40: ("23 05 00", "Common Work Results for HVAC"),
    41: ("23 31 00", "HVAC Ducts and Casings"),
    42: ("23 36 00", "Air Terminal Units"),
    43: ("23 73 00", "Indoor Central-Station Air-Handling Units"),
    44: ("26 05 00", "Common Work Results for Electrical"),
    45: ("26 05 19", "Low-Voltage Electrical Power Conductors and Cables"),
    46: ("26 05 33", "Raceway and Boxes for Electrical Systems"),
    47: ("26 24 00", "Switchboards and Panelboards"),
    48: ("26 27 26", "Wiring Devices"),
    49: ("26 51 00", "Interior Lighting"),
    50: ("31 10 00", "Site Clearing"),
    51: ("31 23 00", "Excavation and Fill"),
    52: ("31 63 00", "Bored Piles"),
}

# Keyword patterns to CSI section ID mapping
# Order matters - more specific patterns should come first
# Format: (keywords_list, csi_section_id)
KEYWORD_TO_CSI = [
    # Fireproofing vs Firestopping (both in Division 07)
    (["sfrm", "ifrm", "intumescent", "fireproofing", "applied fire"], 18),  # 07 81 00 Applied Fireproofing
    (["firestop", "fire stop", "penetration seal", "fire caulk", "firestopping"], 19),  # 07 84 00 Firestopping

    # Concrete specifics
    (["precast", "waffle", "double t", "double-t", "spandrel", "precast column"], 3),  # 03 41 00 Structural Precast
    (["grout", "grouting", "non-shrink"], 4),  # 03 60 00 Grouting
    (["voidform", "void form", "cardboard form"], 2),  # 03 30 00 Cast-in-Place Concrete (forms)
    (["chloride", "concrete test", "cylinder"], 2),  # 03 30 00 Cast-in-Place Concrete (testing)
    (["concrete", "pour", "placement", "slab", "topping", "mat foundation", "sog"], 2),  # 03 30 00 Cast-in-Place

    # Steel specifics
    (["steel deck", "decking", "floor deck", "roof deck"], 7),  # 05 31 00 Steel Decking
    (["structural steel", "steel erection", "steel connection", "high strength bolt"], 6),  # 05 12 00 Structural Steel
    (["metal stud", "cold-formed", "cold formed", "light gauge"], 8),  # 05 40 00 Cold-Formed Metal Framing
    (["misc steel", "miscellaneous steel", "stair", "railing", "handrail", "ladder", "grating"], 9),  # 05 50 00 Metal Fabrications
    (["weld", "welding", "vt inspection", "aws"], 6),  # 05 12 00 - welding is structural steel
    (["anchor", "post-installed", "epoxy dowel", "dowel", "embed", "coupler"], 6),  # 05 12 00 - anchors are steel

    # Drywall/Framing
    (["drywall", "gypsum", "gyp board", "sheetrock", "layer inspection", "1st layer", "2nd layer", "3rd layer"], 26),  # 09 21 16 Gypsum Board
    (["framing", "frame inspection", "bottom plate", "top plate", "sliptrack", "t-bar", "tee bar"], 8),  # 05 40 00 Cold-Formed Metal Framing
    (["shaft wall", "shaft liner", "shaftliner"], 26),  # 09 21 16 Gypsum Board
    (["control joint", "cj inspection"], 26),  # 09 21 16 - control joints in drywall
    (["ceiling", "acoustical ceiling", "ceiling grid", "ceiling tile"], 27),  # 09 51 00 Acoustical Ceilings
    (["screw inspection", "fastener"], 26),  # 09 21 16 - drywall screws

    # Waterproofing/Roofing
    (["waterproofing", "below grade", "sikaproof"], 11),  # 07 13 00 Sheet Waterproofing
    (["dampproofing", "damp proofing"], 10),  # 07 11 00 Dampproofing
    (["roofing", "tpo", "membrane roofing", "roof membrane"], 16),  # 07 52 00 Membrane Roofing
    (["coping", "flashing", "expansion joint", "roof edge"], 17),  # 07 71 00 Roof Specialties

    # Insulation
    (["insulation", "mineral wool", "batt insulation", "blanket"], 13),  # 07 21 16 Blanket Insulation
    (["rigid insulation", "board insulation", "foam board"], 12),  # 07 21 13 Board Insulation
    (["vapor barrier", "vapor retarder"], 14),  # 07 26 00 Vapor Retarders

    # Panels - use "imp panel" to avoid matching "IMPROPER", "improvement"
    (["imp panel", "insulated metal panel", "metal panel", "composite panel", "wall panel"], 15),  # 07 42 43 Composite Wall Panels

    # Masonry - avoid bare "block" (matches "blocking", "blocked")
    (["masonry", "cmu", "cmu block", "concrete block", "brick", "mortar"], 5),  # 04 20 00 Unit Masonry

    # Finishes - Chemical-Resistant Coatings must come BEFORE both painting and flooring
    # All epoxy/coating work on floors is 09 06 65, not painting or resilient flooring
    (["flooring surface prep", "flooring csp", "flooring coat", "flooring intermediate", "flooring top coat", "flooring final coat",
      "coating inspection – flooring", "coating visual inspection", "coatings inspection", "surface prep", "surface preparation",
      "epoxy coating", "chemical resistant", "floor coating", "csp2", "csp3", "csp-", "csp/", "nace",
      "fiberglass coat", "sealer inspection"], 25),  # 09 06 65 Chemical-Resistant Coatings
    (["paint", "painting", "primer", "topcoat"], 29),  # 09 91 26 Painting - Building
    (["flooring", "resilient", "vct", "tile floor", "vinyl tile", "rubber floor"], 28),  # 09 65 00 Resilient Flooring
    (["sealant", "caulk", "joint seal"], 20),  # 07 90 00 Joint Protection

    # Openings - use "door frame" instead of bare "frame" (matches framing)
    (["door", "hollow metal", "hm door", "door frame"], 21),  # 08 11 13 Hollow Metal Doors
    (["coiling door", "roll-up", "overhead door"], 22),  # 08 33 23 Overhead Coiling Doors
    (["hardware", "lockset", "hinge", "closer"], 23),  # 08 71 00 Door Hardware
    (["glazing", "glass", "curtain wall", "storefront"], 24),  # 08 80 00 Glazing

    # Specialties - check BEFORE electrical wire/cable patterns
    (["wire mesh partition", "wire mesh wall", "acorn wire mesh"], 9),  # 05 50 00 Metal Fabrications (security partitions)

    # MEP - Fire Suppression
    (["sprinkler", "fire suppression", "fire protection piping"], 34),  # 21 10 00 Fire Suppression
    (["fire pump"], 35),  # 21 30 00 Fire Pumps

    # MEP - Plumbing
    (["plumbing", "domestic water", "water distribution"], 37),  # 22 11 00 Water Distribution
    (["sanitary", "waste", "sewer"], 38),  # 22 13 00 Sanitary Sewerage
    (["storm drain", "storm drainage"], 39),  # 22 14 00 Storm Drainage

    # MEP - HVAC
    (["duct", "ductwork", "hvac duct"], 41),  # 23 31 00 HVAC Ducts
    (["diffuser", "vav", "air terminal"], 42),  # 23 36 00 Air Terminal Units
    (["ahu", "air handling", "air handler"], 43),  # 23 73 00 AHUs
    (["hvac", "mechanical"], 40),  # 23 05 00 Common Work Results for HVAC

    # MEP - Electrical
    (["conduit", "raceway", "junction box", "cable tray"], 46),  # 26 05 33 Raceway and Boxes
    (["wire", "cable", "conductor"], 45),  # 26 05 19 Conductors and Cables
    # Use specific panel terms to avoid matching "wall panel", "metal panel"
    (["switchboard", "panelboard", "electrical panel", "elec panel"], 47),  # 26 24 00 Switchboards
    (["receptacle", "outlet", "switch", "wiring device"], 48),  # 26 27 26 Wiring Devices
    (["lighting", "light fixture", "luminaire"], 49),  # 26 51 00 Interior Lighting
    (["electrical"], 44),  # 26 05 00 Common Work Results for Electrical

    # Earthwork
    (["pier", "drilled pier", "caisson", "pile", "deep foundation"], 52),  # 31 63 00 Bored Piles
    (["excavation", "backfill", "compaction", "soil", "proctor", "density", "earthwork", "grading"], 51),  # 31 23 00 Excavation and Fill
    (["clearing", "demolition", "site clearing"], 50),  # 31 10 00 Site Clearing

    # Special/General
    (["elevator", "escalator", "conveyor"], 33),  # 14 21 00 Elevators
    (["vibration", "acoustic", "sound control"], 32),  # 13 48 00 Sound and Vibration Control
    (["dock", "loading dock", "dock leveler"], 31),  # 11 13 19 Loading Dock Equipment
]

# Fallback: inspection_category to CSI section
CATEGORY_TO_CSI = {
    "Drywall": 26,  # 09 21 16 Gypsum Board Assemblies
    "Framing": 8,   # 05 40 00 Cold-Formed Metal Framing
    "Screw Inspection": 26,  # 09 21 16 - drywall screws
    "Concrete": 2,  # 03 30 00 Cast-in-Place Concrete
    "Structural Steel": 6,  # 05 12 00 Structural Steel Framing
    "Welding": 6,   # 05 12 00 - welding is structural steel
    "Drilled Pier/Foundation": 52,  # 31 63 00 Bored Piles
    "Firestop": 19,  # 07 84 00 Firestopping (default, may be overridden by keywords)
    "Coating/Painting": 29,  # 09 91 26 Painting - Building
    "Soil/Earthwork": 51,  # 31 23 00 Excavation and Fill
    "Reinforcing Steel": 2,  # 03 30 00 - rebar is part of CIP concrete
    "Masonry": 5,   # 04 20 00 Unit Masonry
    "Waterproofing": 11,  # 07 13 00 Sheet Waterproofing
    "MEP": 44,      # 26 05 00 Common Work Results for Electrical (generic MEP)
    "Visual/General": 1,  # 01 10 00 Summary (general inspections)
}


def infer_csi_section(inspection_type: str, inspection_category: str) -> Tuple[Optional[int], Optional[str], str]:
    """
    Infer CSI section from inspection type and category.

    Args:
        inspection_type: Raw inspection type string
        inspection_category: Categorized inspection type (from consolidation)

    Returns:
        Tuple of (csi_section_id, csi_section_code, inference_source)
        inference_source is 'keyword' or 'category'
    """
    if pd.isna(inspection_type) and pd.isna(inspection_category):
        return None, None, "none"

    # Try keyword matching first (more specific)
    if pd.notna(inspection_type):
        insp_lower = str(inspection_type).lower()

        for keywords, csi_id in KEYWORD_TO_CSI:
            for keyword in keywords:
                if keyword in insp_lower:
                    csi_code, _ = CSI_SECTIONS[csi_id]
                    return csi_id, csi_code, "keyword"

    # Fall back to category mapping
    if pd.notna(inspection_category):
        category = str(inspection_category)
        if category in CATEGORY_TO_CSI:
            csi_id = CATEGORY_TO_CSI[category]
            csi_code, _ = CSI_SECTIONS[csi_id]
            return csi_id, csi_code, "category"

    return None, None, "none"


def add_csi_to_raba(dry_run: bool = False):
    """Add CSI section IDs to RABA consolidated data (appends to original file)."""

    input_path = settings.PROCESSED_DATA_DIR / "raba" / "4.consolidate" / "raba_qc_inspections.csv"
    # Write back to the same file (append columns to original)
    output_path = input_path

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return

    print(f"Loading RABA data from: {input_path}")
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df):,} records")

    # Apply CSI inference
    print("Inferring CSI sections...")
    results = df.apply(
        lambda row: infer_csi_section(row.get('inspection_type'), row.get('inspection_category')),
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
    category_count = (df['csi_inference_source'] == 'category').sum()

    print(f"\nCSI Section Coverage: {coverage:.1f}%")
    print(f"  From keywords: {keyword_count:,} ({keyword_count/len(df)*100:.1f}%)")
    print(f"  From category: {category_count:,} ({category_count/len(df)*100:.1f}%)")
    print(f"  No match: {len(df) - keyword_count - category_count:,}")

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
    parser = argparse.ArgumentParser(description='Add CSI section IDs to RABA data')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing output')
    args = parser.parse_args()

    add_csi_to_raba(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
