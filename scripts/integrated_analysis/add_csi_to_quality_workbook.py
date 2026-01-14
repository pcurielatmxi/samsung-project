#!/usr/bin/env python3
"""
Add CSI Section IDs to Quality Workbook Files.

Processes both Yates WIR and SECAI inspection logs to add CSI section mapping.
- Yates: Uses 'Inspection Description' field for keyword matching
- SECAI: Uses 'Template' field which contains inspection type

Input:
    {WINDOWS_DATA_DIR}/processed/quality/yates_all_inspections.csv
    {WINDOWS_DATA_DIR}/processed/quality/secai_inspection_log.csv

Output:
    {WINDOWS_DATA_DIR}/processed/quality/yates_with_csi.csv
    {WINDOWS_DATA_DIR}/processed/quality/secai_with_csi.csv

Usage:
    python -m scripts.integrated_analysis.add_csi_to_quality_workbook
    python -m scripts.integrated_analysis.add_csi_to_quality_workbook --dry-run
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

# Keyword patterns for Yates inspection descriptions
# Order matters - more specific patterns should come first
YATES_KEYWORD_TO_CSI = [
    # Drywall/Framing - "door frame" must come BEFORE "frame" to avoid misclassification
    (["drywall", "1st layer", "2nd layer", "3rd layer", "gypsum", "sheetrock"], 26),  # 09 21 16 Gypsum Board
    (["door frame"], 21),  # 08 11 13 Hollow Metal Doors - specific pattern before generic "frame"
    (["framing", "stud", "track"], 8),  # 05 40 00 Cold-Formed Metal Framing (removed bare "frame")
    # Ceilings - avoid bare "act" (matches "Actuator", "action", etc.)
    (["ceiling grid", "ceiling tile", "acoustical", "act ceiling"], 27),  # 09 51 00 Acoustical Ceilings

    # Fire protection
    (["sfrm", "ifrm", "fireproofing", "fire spray", "intumescent"], 18),  # 07 81 00 Applied Fireproofing
    (["firestop", "fire stop", "fire caulk", "penetration seal"], 19),  # 07 84 00 Firestopping

    # Concrete
    (["rebar", "reinforcing", "reinforcement"], 2),  # 03 30 00 Cast-in-Place Concrete
    (["concrete placement", "concrete pour", "placement"], 2),  # 03 30 00 Cast-in-Place Concrete
    (["drill and epoxy", "drill & epoxy", "drill/epoxy", "epoxy anchor"], 2),  # 03 30 00 Cast-in-Place
    (["grout", "grouting"], 4),  # 03 60 00 Grouting

    # Steel
    (["weld", "bolt", "vt weld", "ut weld", "connection"], 6),  # 05 12 00 Structural Steel
    (["support steel", "penetration support"], 6),  # 05 12 00 Structural Steel
    (["steel deck", "decking"], 7),  # 05 31 00 Steel Decking
    (["misc steel", "stair", "handrail", "railing"], 9),  # 05 50 00 Metal Fabrications

    # Waterproofing/Roofing
    (["waterproof", "waterproofing"], 11),  # 07 13 00 Sheet Waterproofing
    (["roofing", "membrane"], 16),  # 07 52 00 Membrane Roofing
    (["flashing", "coping"], 17),  # 07 71 00 Roof Specialties

    # Insulation
    (["insulation", "insul"], 13),  # 07 21 16 Blanket Insulation

    # Panels - use "imp panel" to avoid matching "IMPROPER", "improvement"
    (["imp panel", "metal panel", "wall panel"], 15),  # 07 42 43 Composite Wall Panels

    # Finishes
    (["paint", "coating", "primer"], 29),  # 09 91 26 Painting
    (["flooring", "vct", "tile"], 28),  # 09 65 00 Resilient Flooring
    (["sealant", "caulk", "joint seal"], 20),  # 07 90 00 Joint Protection

    # Openings
    (["door", "hollow metal", "hm"], 21),  # 08 11 13 Hollow Metal Doors
    (["overhead door", "coiling", "roll-up"], 22),  # 08 33 23 Overhead Coiling Doors
    (["hardware"], 23),  # 08 71 00 Door Hardware
    (["glazing", "glass", "curtain wall", "storefront"], 24),  # 08 80 00 Glazing

    # MEP
    (["sprinkler", "fire suppression"], 34),  # 21 10 00 Fire Suppression
    (["plumbing", "pipe"], 36),  # 22 05 00 Common Work Results for Plumbing
    (["hvac", "duct", "mechanical"], 40),  # 23 05 00 Common Work Results for HVAC
    (["electrical", "conduit", "wire"], 44),  # 26 05 00 Common Work Results for Electrical

    # Elevator
    (["elevator"], 33),  # 14 21 00 Electric Traction Elevators
]

# Keyword patterns for SECAI templates
SECAI_KEYWORD_TO_CSI = [
    # Drywall/Gypsum
    (["gypsum board", "drywall", "gyp board"], 26),  # 09 21 16 Gypsum Board

    # Steel
    (["structural steel"], 6),  # 05 12 00 Structural Steel Framing
    (["cold-formed", "metal framing", "cfmf"], 8),  # 05 40 00 Cold-Formed Metal Framing

    # Waterproofing
    (["waterproofing", "crc waterproofing", "crc-3"], 11),  # 07 13 00 Sheet Waterproofing
    (["roof waterproofing"], 16),  # 07 52 00 Membrane Roofing
    (["epoxy coating"], 25),  # 09 06 65 Chemical-Resistant Coatings

    # Painting
    (["painting and coating", "painting"], 29),  # 09 91 26 Painting

    # Fire protection
    (["fireproofing", "sfrm", "ifrm"], 18),  # 07 81 00 Applied Fireproofing
    (["firestopping", "firestop"], 19),  # 07 84 00 Firestopping

    # MEP - Electrical
    (["low voltage cables", "cable"], 45),  # 26 05 19 Conductors and Cables
    (["raceway & boxes", "raceway"], 46),  # 26 05 33 Raceway and Boxes
    (["cable tray"], 46),  # 26 05 33 Raceway and Boxes
    (["panelboards", "panelboard"], 47),  # 26 24 00 Switchboards
    (["lighting", "light fixture"], 49),  # 26 51 00 Interior Lighting
    (["fire alarm"], 44),  # 26 05 00 Common Work Results for Electrical

    # MEP - Mechanical
    (["pipe general", "pipe", "piping"], 36),  # 22 05 00 Common Work Results for Plumbing
    (["s.gas", "chemical"], 36),  # 22 05 00 Plumbing (specialty gas)
    (["hvac", "duct"], 40),  # 23 05 00 Common Work Results for HVAC

    # Concrete
    (["concrete", "cast-in-place"], 2),  # 03 30 00 Cast-in-Place Concrete

    # Insulation
    (["insulation"], 13),  # 07 21 16 Blanket Insulation
]


def infer_csi_from_keywords(text: str, keyword_map: list) -> Tuple[Optional[int], Optional[str]]:
    """
    Infer CSI section from text using keyword matching.

    Args:
        text: Text to search for keywords
        keyword_map: List of (keywords, csi_id) tuples

    Returns:
        Tuple of (csi_section_id, csi_section_code) or (None, None)
    """
    if pd.isna(text):
        return None, None

    text_lower = str(text).lower()

    for keywords, csi_id in keyword_map:
        for keyword in keywords:
            if keyword in text_lower:
                csi_code, _ = CSI_SECTIONS[csi_id]
                return csi_id, csi_code

    return None, None


def process_yates(dry_run: bool = False):
    """Process Yates inspection data."""
    input_path = settings.PROCESSED_DATA_DIR / "quality" / "yates_all_inspections.csv"
    output_path = settings.PROCESSED_DATA_DIR / "quality" / "yates_with_csi.csv"

    if not input_path.exists():
        print(f"Yates file not found: {input_path}")
        return None

    print(f"\n=== YATES INSPECTIONS ===")
    print(f"Loading from: {input_path}")
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df):,} records")

    # Apply CSI inference from Inspection Description
    print("Inferring CSI sections from Inspection Description...")
    results = df['Inspection Description'].apply(
        lambda x: infer_csi_from_keywords(x, YATES_KEYWORD_TO_CSI)
    )

    df['dim_csi_section_id'] = results.apply(lambda x: x[0])
    df['csi_section'] = results.apply(lambda x: x[1])

    # Add CSI title
    df['csi_title'] = df['dim_csi_section_id'].apply(
        lambda x: CSI_SECTIONS[x][1] if pd.notna(x) and x in CSI_SECTIONS else None
    )

    # Calculate coverage
    coverage = df['dim_csi_section_id'].notna().mean() * 100
    print(f"CSI Section Coverage: {coverage:.1f}%")

    # Show distribution
    print("\nTop 10 CSI Sections:")
    csi_dist = df[df['dim_csi_section_id'].notna()].groupby(['csi_section', 'csi_title']).size().sort_values(ascending=False).head(10)
    for (section, title), count in csi_dist.items():
        print(f"  {section} {title}: {count:,}")

    if not dry_run:
        df.to_csv(output_path, index=False)
        print(f"Output written to: {output_path}")

    return df


def process_secai(dry_run: bool = False):
    """Process SECAI inspection data."""
    input_path = settings.PROCESSED_DATA_DIR / "quality" / "secai_inspection_log.csv"
    output_path = settings.PROCESSED_DATA_DIR / "quality" / "secai_with_csi.csv"

    if not input_path.exists():
        print(f"SECAI file not found: {input_path}")
        return None

    print(f"\n=== SECAI INSPECTION LOG ===")
    print(f"Loading from: {input_path}")
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df):,} records")

    # Apply CSI inference from Template field
    print("Inferring CSI sections from Template...")
    results = df['Template'].apply(
        lambda x: infer_csi_from_keywords(x, SECAI_KEYWORD_TO_CSI)
    )

    df['dim_csi_section_id'] = results.apply(lambda x: x[0])
    df['csi_section'] = results.apply(lambda x: x[1])

    # Add CSI title
    df['csi_title'] = df['dim_csi_section_id'].apply(
        lambda x: CSI_SECTIONS[x][1] if pd.notna(x) and x in CSI_SECTIONS else None
    )

    # Calculate coverage
    coverage = df['dim_csi_section_id'].notna().mean() * 100
    print(f"CSI Section Coverage: {coverage:.1f}%")

    # Show distribution
    print("\nTop 10 CSI Sections:")
    csi_dist = df[df['dim_csi_section_id'].notna()].groupby(['csi_section', 'csi_title']).size().sort_values(ascending=False).head(10)
    for (section, title), count in csi_dist.items():
        print(f"  {section} {title}: {count:,}")

    if not dry_run:
        df.to_csv(output_path, index=False)
        print(f"Output written to: {output_path}")

    return df


def add_csi_to_quality_workbook(dry_run: bool = False):
    """Process both Yates and SECAI quality files."""
    yates_df = process_yates(dry_run)
    secai_df = process_secai(dry_run)

    if not dry_run:
        print("\n" + "=" * 50)
        print("All quality workbook files processed.")


def main():
    parser = argparse.ArgumentParser(description='Add CSI section IDs to quality workbook files')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing output')
    args = parser.parse_args()

    add_csi_to_quality_workbook(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
