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
TRADES = [
    # trade_id, trade_code, trade_name, description, csi_divisions
    (1, "CONCRETE", "Concrete", "Cast-in-place concrete, topping slabs, SOG", "03"),
    (2, "STEEL", "Structural Steel", "Steel erection, misc steel, decking, welding", "05"),
    (3, "ROOFING", "Roofing", "Roofing, waterproofing, membrane systems", "07"),
    (4, "DRYWALL", "Drywall", "Metal framing, gypsum board, ceiling grid", "09"),
    (5, "FINISHES", "Finishes", "Painting, flooring, tile, doors, hardware", "09"),
    (6, "FIRE_PROTECTION", "Fire Protection", "Fireproofing (SFRM/IFRM), firestopping, fire caulk", "07"),
    (7, "MEP", "MEP", "Mechanical, electrical, plumbing, HVAC", "21,22,23,26"),
    (8, "INSULATION", "Insulation", "Thermal insulation, pipe insulation, vapor barriers", "07"),
    (9, "EARTHWORK", "Earthwork", "Excavation, backfill, grading, deep foundations", "31"),
    (10, "PRECAST", "Precast", "Precast concrete panels, waffle slabs, double-tees", "03"),
    (11, "PANELS", "Panels", "Metal panels, cladding, IMP, building skin", "07"),
    (12, "GENERAL", "General", "General conditions, cleanup, temporary facilities", "01"),
    (13, "MASONRY", "Masonry", "CMU, brick masonry, grout", "04"),
]


def build_dim_trade():
    """Generate dim_trade.csv from trade definitions."""

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Build trade records
    trades = []
    for trade_id, trade_code, trade_name, description, csi_divisions in TRADES:
        trades.append({
            "trade_id": trade_id,
            "trade_code": trade_code,
            "trade_name": trade_name,
            "description": description,
            "csi_divisions": csi_divisions,
        })

    # Write CSV
    fieldnames = ["trade_id", "trade_code", "trade_name", "description", "csi_divisions"]

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
