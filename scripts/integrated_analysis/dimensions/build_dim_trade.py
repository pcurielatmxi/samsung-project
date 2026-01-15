#!/usr/bin/env python3
"""
Build dim_trade.csv - Coarse trade dimension table.

This script generates a dimension table with 13 coarse trade categories used
for high-level aggregation across data sources. These trades align with the
existing TRADE_NAME_TO_ID mappings in dimension_lookup.py.

For maximum granularity, use dim_csi_section.csv instead (52 CSI sections).
Both tables can be used together via map_csi_section_to_trade.csv.

Trade Categories (13):
- CONCRETE: Cast-in-place and precast concrete work
- STEEL: Structural steel erection, misc steel, welding
- ROOFING: Roofing and waterproofing
- DRYWALL: Metal framing and gypsum board
- FINISHES: Painting, flooring, ceilings, doors
- FIRE_PROTECTION: Fireproofing and firestopping
- MEP: Mechanical, electrical, plumbing
- INSULATION: Thermal and pipe insulation
- EARTHWORK: Excavation, foundations, grading
- PRECAST: Precast concrete elements
- PANELS: Metal panels and cladding
- GENERAL: General conditions, cleanup
- MASONRY: CMU and brick masonry

Usage:
    python -m scripts.integrated_analysis.dimensions.build_dim_trade

Output:
    {WINDOWS_DATA_DIR}/processed/integrated_analysis/dimensions/dim_trade.csv
"""

import csv
from pathlib import Path
from src.config.settings import settings

# Output location
OUTPUT_DIR = settings.PROCESSED_DATA_DIR / "integrated_analysis" / "dimensions"
OUTPUT_FILE = OUTPUT_DIR / "dim_trade.csv"

# Trade definitions - aligned with dimension_lookup.py:TRADE_NAME_TO_ID
# trade_id values must match the existing mappings
# Phase codes: STR=Structural, ENC=Enclosure, INT=Interior, ADM=Administrative
TRADES = [
    # trade_id, trade_code, trade_name, phase, csi_division, description
    (1, "CONCRETE", "Concrete", "STR", "03", "Cast-in-place concrete foundations slabs walls topping"),
    (2, "STEEL", "Structural Steel", "STR", "05", "Steel erection decking misc steel trusses"),
    (3, "ROOFING", "Roofing & Waterproofing", "ENC", "07", "Roofing membrane waterproofing EIFS"),
    (4, "DRYWALL", "Drywall & Framing", "INT", "09", "Metal stud framing gypsum board drywall"),
    (5, "FINISHES", "Architectural Finishes", "INT", "09", "Paint flooring tile ceilings specialties doors"),
    (6, "FIREPROOF", "Fire Protection", "INT", "07", "Fireproofing firestop fire caulk SFRM"),
    (7, "MEP", "MEP Systems", "INT", "22-26", "Mechanical electrical plumbing HVAC"),
    (8, "INSULATION", "Insulation", "INT", "07", "Thermal insulation pipe insulation"),
    (9, "EARTHWORK", "Earthwork & Foundations", "STR", "31", "Excavation backfill grading deep foundations"),
    (10, "PRECAST", "Precast Concrete", "STR", "03", "Precast panels precast erection"),
    (11, "PANELS", "Metal Panels & Cladding", "ENC", "07", "Metal wall panels IMP cladding skin"),
    (12, "GENERAL", "General Conditions", "ADM", "01", "General requirements temporary facilities"),
    (13, "MASONRY", "Masonry", "STR", "04", "CMU brick masonry grout"),
]


def build_dim_trade():
    """Generate dim_trade.csv from trade definitions."""

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Build trade records
    trades = []
    for trade_id, trade_code, trade_name, phase, csi_division, description in TRADES:
        trades.append({
            "trade_id": trade_id,
            "trade_code": trade_code,
            "trade_name": trade_name,
            "phase": phase,
            "csi_division": csi_division,
            "description": description,
        })

    # Write CSV
    fieldnames = ["trade_id", "trade_code", "trade_name", "phase", "csi_division", "description"]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(trades)

    print(f"Generated {OUTPUT_FILE}")
    print(f"  Total trades: {len(trades)}")

    # Print summary
    print("\nTrade Summary:")
    for t in trades:
        print(f"  {t['trade_id']:2d}. {t['trade_code']:<15} - {t['trade_name']}")

    return trades


if __name__ == "__main__":
    build_dim_trade()
