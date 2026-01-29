#!/usr/bin/env python3
"""
Consolidate ProjectSight Labor Entries with Dimension IDs and CSI Sections.

Enriches the parsed labor_entries.csv with:
- dim_company_id (company lookup)
- dim_csi_section_id (CSI section inference from activity and trade)

ProjectSight has NO location data - only company and trade information.

This script replaces the enrich_with_dimensions.py:enrich_projectsight() +
add_csi_to_projectsight.py by integrating all enrichment into the pipeline.

Input:  processed/projectsight/labor_entries.csv (from parse stage)
Output: processed/projectsight/labor_entries.csv (enriched, overwrites input)

Usage:
    python -m scripts.projectsight.process.consolidate_labor
    python -m scripts.projectsight.process.consolidate_labor --dry-run
"""

import argparse
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
    reset_cache,
)

# Import CSI definitions from add_csi_to_raba (shared)
from scripts.integrated_analysis.add_csi_to_raba import CSI_SECTIONS


# =============================================================================
# CSI Division to default Section mapping
# When we only have division info, use the "common work results" section
# =============================================================================

DIVISION_TO_DEFAULT_CSI = {
    "01": 1,   # 01 10 00 Summary (General Requirements)
    "02": 50,  # 31 10 00 Site Clearing (Existing Conditions -> Demo/Clearing)
    "03": 2,   # 03 30 00 Cast-in-Place Concrete
    "04": 5,   # 04 20 00 Unit Masonry
    "05": 6,   # 05 12 00 Structural Steel Framing
    "06": 8,   # 05 40 00 Cold-Formed Metal Framing (Wood/Plastics -> Framing)
    "07": 13,  # 07 21 16 Blanket Insulation (Thermal/Moisture default)
    "08": 21,  # 08 11 13 Hollow Metal Doors (Openings default)
    "09": 29,  # 09 91 26 Painting - Building (Finishes default)
    "10": 1,   # 01 10 00 Summary (Specialties -> General)
    "11": 31,  # 11 13 19 Loading Dock Equipment
    "12": 1,   # 01 10 00 Summary (Furnishings -> General)
    "13": 32,  # 13 48 00 Sound and Vibration Control
    "14": 33,  # 14 21 00 Electric Traction Elevators
    "21": 34,  # 21 10 00 Fire Suppression
    "22": 36,  # 22 05 00 Common Work Results for Plumbing
    "23": 40,  # 23 05 00 Common Work Results for HVAC
    "25": 44,  # 26 05 00 Electrical (Integrated Automation -> Electrical)
    "26": 44,  # 26 05 00 Common Work Results for Electrical
    "27": 44,  # 26 05 00 Electrical (Communications -> Electrical)
    "28": 44,  # 26 05 00 Electrical (Security -> Electrical)
    "31": 51,  # 31 23 00 Excavation and Fill
    "32": 51,  # 31 23 00 Excavation (Exterior Improvements)
    "33": 51,  # 31 23 00 Excavation (Utilities)
    "18": 6,   # 05 12 00 Structural Steel (Iron erection)
    "34": 1,   # 01 10 00 Summary (Transportation/specialized)
}

# Trade ID to CSI section mapping
# Uses dim_trade.trade_id from company's primary_trade_id
TRADE_ID_TO_CSI = {
    1: 2,    # CONCRETE -> 03 30 00 Cast-in-Place Concrete
    2: 6,    # STEEL -> 05 12 00 Structural Steel Framing
    3: 16,   # ROOFING -> 07 52 00 Membrane Roofing
    4: 26,   # DRYWALL -> 09 21 16 Gypsum Board Assemblies
    5: 29,   # FINISHES -> 09 91 26 Painting - Building
    6: 18,   # FIREPROOF -> 07 81 00 Applied Fireproofing (SFRM/IFRM)
    7: 44,   # MEP -> 26 05 00 Common Work Results for Electrical
    8: 13,   # INSULATION -> 07 21 16 Blanket Insulation
    9: 51,   # EARTHWORK -> 31 23 00 Excavation and Fill
    10: 3,   # PRECAST -> 03 41 00 Structural Precast Concrete
    11: 15,  # PANELS -> 07 42 43 Composite Wall Panels
    12: 1,   # GENERAL -> 01 10 00 Summary
    13: 5,   # MASONRY -> 04 20 00 Unit Masonry
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

# Patterns that indicate generic shift/crew names (not actual work activities)
SHIFT_CREW_PATTERNS = [
    re.compile(r'\b(day|night)\s*shift\b', re.IGNORECASE),  # "Day Shift", "Night Shift"
    re.compile(r'^[A-Za-z0-9\s]+ - (day|night)\s*shift$', re.IGNORECASE),  # "Paint 1 - Day Shift"
    re.compile(r'\bcrew\s*$', re.IGNORECASE),  # "Richards Crew", "Taylor Fab Crew"
    re.compile(r'^(day|night)\s*shift$', re.IGNORECASE),  # Just "Day Shift"
]


def is_shift_or_crew_name(activity: str) -> bool:
    """
    Check if activity is a generic shift/crew designation rather than actual work.

    Examples of shift/crew names (should return True):
    - "Day Shift"
    - "Paint 1 - Day Shift"
    - "Richards Crew"
    - "Taylor Fab Crew"

    These contain keywords like "paint" but are not actual painting activities.
    """
    if pd.isna(activity):
        return False
    activity = str(activity).strip()
    return any(pattern.search(activity) for pattern in SHIFT_CREW_PATTERNS)


def extract_division(trade_full: str) -> Optional[str]:
    """Extract 2-digit division code from trade_full like '03 - Concrete'."""
    if pd.isna(trade_full):
        return None
    trade_full = str(trade_full).strip()
    if len(trade_full) >= 2 and trade_full[:2].isdigit():
        return trade_full[:2]
    return None


def infer_csi_from_projectsight(activity: str, trade_full: str, trade_id: int = None) -> Tuple[Optional[int], Optional[str], str]:
    """
    Infer CSI section from ProjectSight activity, trade_full, and trade_id fields.

    Priority order:
    1. Activity keywords (most specific) - UNLESS activity is a shift/crew name
    2. Company's primary trade (from dim_trade_id, more specific than division)
    3. Division from trade_full (last resort - generic defaults)

    Args:
        activity: Activity description
        trade_full: CSI division string like '03 - Concrete'
        trade_id: dim_trade_id (from company's primary_trade_id)

    Returns:
        Tuple of (csi_section_id, csi_section_code, inference_source)
    """
    # Try activity keyword matching first (most specific)
    # BUT skip if activity is a shift/crew name (e.g., "Paint 1 - Day Shift")
    if pd.notna(activity) and not is_shift_or_crew_name(activity):
        activity_lower = str(activity).lower()

        for keywords, csi_id in ACTIVITY_TO_CSI:
            for keyword in keywords:
                if keyword in activity_lower:
                    csi_code, _ = CSI_SECTIONS[csi_id]
                    return csi_id, csi_code, "activity"

    # Try company's primary trade (more specific than division default)
    if pd.notna(trade_id):
        trade_id_int = int(trade_id)
        if trade_id_int in TRADE_ID_TO_CSI:
            csi_id = TRADE_ID_TO_CSI[trade_id_int]
            csi_code, _ = CSI_SECTIONS[csi_id]
            return csi_id, csi_code, "trade"

    # Fall back to division mapping (last resort - generic defaults)
    division = extract_division(trade_full)
    if division and division in DIVISION_TO_DEFAULT_CSI:
        csi_id = DIVISION_TO_DEFAULT_CSI[division]
        csi_code, _ = CSI_SECTIONS[csi_id]
        return csi_id, csi_code, "division"

    return None, None, "none"


def consolidate_labor(dry_run: bool = False) -> Dict[str, Any]:
    """
    Consolidate ProjectSight labor_entries.csv with dimension IDs.

    This replaces enrich_with_dimensions.py:enrich_projectsight() +
    add_csi_to_projectsight.py by integrating all enrichment into the pipeline.
    """
    input_path = Settings.PROCESSED_DATA_DIR / 'projectsight' / 'labor_entries.csv'
    output_path = Settings.PROCESSED_DATA_DIR / 'projectsight' / 'labor_entries.csv'

    if not input_path.exists():
        return {'status': 'skipped', 'reason': 'file not found'}

    print(f"Loading data from: {input_path}")
    df = pd.read_csv(input_path)
    original_count = len(df)
    print(f"Loaded {original_count:,} records")

    # ProjectSight has no location data - only company and trade
    df['dim_location_id'] = None  # No location available

    # Build lookup dictionaries for fast vectorized mapping
    print("  Building company lookup...")
    unique_companies = df['company'].dropna().unique()
    company_lookup = {c: get_company_id(c) for c in unique_companies}
    df['dim_company_id'] = df['company'].map(company_lookup)

    # Get company's primary_trade_id for CSI inference
    # Company's known trade is more reliable than ProjectSight's billing category
    print("  Loading company trade info for CSI inference...")
    dim_company = pd.read_csv(Settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'dim_company.csv')
    company_primary_trade = dict(zip(dim_company['company_id'], dim_company['primary_trade_id']))
    df['company_primary_trade_id'] = df['dim_company_id'].map(company_primary_trade)

    # Infer CSI section from activity, trade_full (division), and company's primary trade
    print("  Inferring CSI sections...")
    csi_results = df.apply(
        lambda row: infer_csi_from_projectsight(
            row.get('activity'),
            row.get('trade_full'),
            row.get('company_primary_trade_id')  # Uses company's actual primary trade
        ),
        axis=1
    )
    df['dim_csi_section_id'] = csi_results.apply(lambda x: x[0])
    df['csi_section'] = csi_results.apply(lambda x: x[1])
    df['csi_inference_source'] = csi_results.apply(lambda x: x[2])
    df['csi_title'] = df['dim_csi_section_id'].apply(
        lambda x: CSI_SECTIONS[x][1] if pd.notna(x) and x in CSI_SECTIONS else None
    )

    # Calculate coverage
    coverage = {
        'location': 0.0,  # No location in ProjectSight
        'company': df['dim_company_id'].notna().mean() * 100,
        'csi_section': df['dim_csi_section_id'].notna().mean() * 100,
    }

    # CSI inference source distribution
    csi_source_dist = df['csi_inference_source'].value_counts().to_dict()

    if not dry_run:
        print(f"  Writing output to: {output_path}")
        validated_df_to_csv(df, output_path, index=False)

    return {
        'status': 'success',
        'records': original_count,
        'coverage': coverage,
        'csi_sources': csi_source_dist,
        'output': str(output_path) if not dry_run else 'DRY RUN (validated)',
    }


def main():
    parser = argparse.ArgumentParser(description='Consolidate ProjectSight labor entries with dimension IDs')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without writing files')
    args = parser.parse_args()

    # Reset dimension cache to ensure fresh data
    reset_cache()

    print("=" * 70)
    print("PROJECTSIGHT LABOR CONSOLIDATION")
    print("=" * 70)

    result = consolidate_labor(dry_run=args.dry_run)

    if result['status'] == 'success':
        print(f"\nRecords: {result['records']:,}")
        print(f"\nCoverage:")
        for dim, pct in result['coverage'].items():
            print(f"  {dim}: {pct:.1f}%")

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
