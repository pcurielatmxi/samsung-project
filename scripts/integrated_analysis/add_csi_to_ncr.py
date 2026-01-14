#!/usr/bin/env python3
"""
Add CSI Section IDs to NCR (Non-Conformance Report) Data.

Maps NCR records to CSI MasterFormat sections using:
1. Keyword parsing of the description field
2. Discipline field as fallback

Input:
    {WINDOWS_DATA_DIR}/processed/projectsight/ncr_consolidated.csv

Output:
    {WINDOWS_DATA_DIR}/processed/projectsight/ncr_with_csi.csv

Usage:
    python -m scripts.integrated_analysis.add_csi_to_ncr
    python -m scripts.integrated_analysis.add_csi_to_ncr --dry-run
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

# Keyword patterns for NCR descriptions
# NCR codes often use abbreviations like COLS, RBAR, WELD, FNDN
DESCRIPTION_KEYWORD_TO_CSI = [
    # Concrete work
    (["rbar", "rebar", "reinforc"], 2),  # 03 30 00 Cast-in-Place Concrete
    (["conc", "concrete", "slab", "pour"], 2),  # 03 30 00 Cast-in-Place Concrete
    (["grout", "grouting"], 4),  # 03 60 00 Grouting
    (["fndn", "foundation", "footing"], 2),  # 03 30 00 Cast-in-Place Concrete

    # Precast
    (["pcst", "precast"], 3),  # 03 41 00 Structural Precast

    # Masonry - avoid "block" alone (matches "blocking", "blocked")
    (["cmu", "masonry", "concrete block", "cmu block"], 5),  # 04 20 00 Unit Masonry

    # Steel
    (["weld", "welding", "wps"], 6),  # 05 12 00 Structural Steel
    (["cols", "column", "beam", "steel erect"], 6),  # 05 12 00 Structural Steel
    (["deck", "decking"], 7),  # 05 31 00 Steel Decking
    (["framing", "stud", "cfmf", "metal frame"], 8),  # 05 40 00 Cold-Formed Metal Framing
    (["misc steel", "stair", "handrail", "railing"], 9),  # 05 50 00 Metal Fabrications

    # Waterproofing/Roofing
    (["waterproof", "wprf", "membrane"], 11),  # 07 13 00 Sheet Waterproofing
    (["roof", "roofing"], 16),  # 07 52 00 Membrane Roofing
    (["flash", "flashing", "coping"], 17),  # 07 71 00 Roof Specialties

    # Fire protection
    (["sfrm", "ifrm", "fireproof", "intumescent"], 18),  # 07 81 00 Applied Fireproofing
    (["fstn", "firestop", "fire stop", "penetration seal"], 19),  # 07 84 00 Firestopping

    # Insulation
    (["insul", "insulation"], 13),  # 07 21 16 Blanket Insulation

    # Panels - use specific terms to avoid matching "electrical panel", "control panel"
    # Note: "imp panel", "wall panel" are specific; bare "panel" removed
    (["imp panel", "wall panel", "metal panel", "cladding", "skin"], 15),  # 07 42 43 Composite Wall Panels

    # Openings
    (["door", "hollow metal", "hm"], 21),  # 08 11 13 Hollow Metal Doors
    (["glazing", "glass", "curtain wall", "window"], 24),  # 08 80 00 Glazing

    # Finishes
    (["drywall", "gyp", "gypsum", "sheetrock"], 26),  # 09 21 16 Gypsum Board
    (["paint", "coating", "primer", "epoxy floor"], 29),  # 09 91 26 Painting
    (["flooring", "tile", "vct"], 28),  # 09 65 00 Resilient Flooring
    # Ceilings - avoid bare "act" (matches "Actuator", "Manufacturer", "contract")
    (["ceiling", "acoustical", "act ceiling"], 27),  # 09 51 00 Acoustical Ceilings

    # MEP - Fire Suppression
    (["sprinkler", "fire suppression", "sprk"], 34),  # 21 10 00 Fire Suppression

    # MEP - Plumbing
    (["plumb", "pipe", "piping"], 36),  # 22 05 00 Common Work Results for Plumbing
    (["drain", "sanitary", "sewer"], 38),  # 22 13 00 Sanitary Sewerage

    # MEP - HVAC
    (["hvac", "duct", "ahu", "mechanical"], 40),  # 23 05 00 Common Work Results for HVAC

    # MEP - Electrical
    (["elec", "electrical", "conduit"], 44),  # 26 05 00 Common Work Results for Electrical
    (["cable", "wire", "conductor"], 45),  # 26 05 19 Conductors and Cables
    (["switchboard", "panelboard", "electrical panel", "elec panel"], 47),  # 26 24 00 Switchboards
    (["light", "lighting"], 49),  # 26 51 00 Interior Lighting

    # Earthwork
    (["excav", "excavat", "backfill", "site"], 51),  # 31 23 00 Excavation and Fill
    (["pier", "drilled", "pile", "caisson"], 52),  # 31 63 00 Bored Piles

    # Elevator
    (["elevator", "elev", "lift"], 33),  # 14 21 00 Electric Traction Elevators
]

# Discipline to CSI section fallback mapping
DISCIPLINE_TO_CSI = {
    'structural': 6,       # 05 12 00 Structural Steel (default structural)
    'architecture': 29,    # 09 91 26 Painting (most common architectural finish)
    'mechanical': 40,      # 23 05 00 Common Work Results for HVAC
    'electrical': 44,      # 26 05 00 Common Work Results for Electrical
    'civil': 51,           # 31 23 00 Excavation and Fill
    'plumbing': 36,        # 22 05 00 Common Work Results for Plumbing
    'fire protection': 34, # 21 10 00 Fire Suppression
    'fireproofing': 18,    # 07 81 00 Applied Fireproofing
    'waterproofing': 11,   # 07 13 00 Sheet Waterproofing
    'roofing': 16,         # 07 52 00 Membrane Roofing
    'insulation': 13,      # 07 21 16 Blanket Insulation
    'drywall': 26,         # 09 21 16 Gypsum Board
    'framing': 8,          # 05 40 00 Cold-Formed Metal Framing
    'concrete': 2,         # 03 30 00 Cast-in-Place Concrete
    'precast': 3,          # 03 41 00 Structural Precast
    'steel': 6,            # 05 12 00 Structural Steel
    'masonry': 5,          # 04 20 00 Unit Masonry
    'elevator': 33,        # 14 21 00 Electric Traction Elevators
    'paint': 29,           # 09 91 26 Painting
    'flooring': 28,        # 09 65 00 Resilient Flooring
    'doors': 21,           # 08 11 13 Hollow Metal Doors
    'glazing': 24,         # 08 80 00 Glazing
}


def infer_csi_from_ncr(description: str, discipline: str) -> Tuple[Optional[int], Optional[str], str]:
    """
    Infer CSI section from NCR description and discipline.

    Args:
        description: NCR description/title
        discipline: NCR discipline field

    Returns:
        Tuple of (csi_section_id, csi_section_code, inference_source)
    """
    # Try description keyword matching first (more specific)
    if pd.notna(description):
        desc_lower = str(description).lower()

        for keywords, csi_id in DESCRIPTION_KEYWORD_TO_CSI:
            for keyword in keywords:
                if keyword in desc_lower:
                    csi_code, _ = CSI_SECTIONS[csi_id]
                    return csi_id, csi_code, "description"

    # Fall back to discipline mapping
    if pd.notna(discipline):
        disc_lower = str(discipline).lower().strip()

        # Direct match
        if disc_lower in DISCIPLINE_TO_CSI:
            csi_id = DISCIPLINE_TO_CSI[disc_lower]
            csi_code, _ = CSI_SECTIONS[csi_id]
            return csi_id, csi_code, "discipline"

        # Partial match for compound disciplines
        for disc_key, csi_id in DISCIPLINE_TO_CSI.items():
            if disc_key in disc_lower:
                csi_code, _ = CSI_SECTIONS[csi_id]
                return csi_id, csi_code, "discipline"

    return None, None, "none"


def add_csi_to_ncr(dry_run: bool = False):
    """Add CSI section IDs to NCR data."""

    input_path = settings.PROJECTSIGHT_PROCESSED_DIR / "ncr_consolidated.csv"
    output_path = settings.PROJECTSIGHT_PROCESSED_DIR / "ncr_with_csi.csv"

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return

    print(f"Loading NCR data from: {input_path}")
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df):,} records")

    # Apply CSI inference
    print("Inferring CSI sections...")
    results = df.apply(
        lambda row: infer_csi_from_ncr(row.get('description'), row.get('discipline')),
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
    desc_count = (df['csi_inference_source'] == 'description').sum()
    disc_count = (df['csi_inference_source'] == 'discipline').sum()
    none_count = (df['csi_inference_source'] == 'none').sum()

    print(f"\nCSI Section Coverage: {coverage:.1f}%")
    print(f"  From description: {desc_count:,} ({desc_count/len(df)*100:.1f}%)")
    print(f"  From discipline: {disc_count:,} ({disc_count/len(df)*100:.1f}%)")
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
    parser = argparse.ArgumentParser(description='Add CSI section IDs to NCR data')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing output')
    args = parser.parse_args()

    add_csi_to_ncr(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
