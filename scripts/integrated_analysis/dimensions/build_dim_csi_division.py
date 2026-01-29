#!/usr/bin/env python3
"""
Build dim_csi_division dimension table.

Creates a dimension table for 2-digit CSI divisions that can be linked
to narrative chunks which extract divisions (not full 6-digit codes).

Output: processed/integrated_analysis/dim_csi_division.csv
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from src.config.settings import Settings

settings = Settings()


def build_dim_csi_division():
    """Build CSI division dimension table."""

    # CSI MasterFormat 2016 divisions
    divisions = [
        ("00", "Procurement and Contracting Requirements"),
        ("01", "General Requirements"),
        ("02", "Existing Conditions"),
        ("03", "Concrete"),
        ("04", "Masonry"),
        ("05", "Metals"),
        ("06", "Wood, Plastics, and Composites"),
        ("07", "Thermal and Moisture Protection"),
        ("08", "Openings"),
        ("09", "Finishes"),
        ("10", "Specialties"),
        ("11", "Equipment"),
        ("12", "Furnishings"),
        ("13", "Special Construction"),
        ("14", "Conveying Equipment"),
        ("17", "Reserved"),
        ("18", "Reserved"),
        ("19", "Reserved"),
        ("20", "Reserved"),
        ("21", "Fire Suppression"),
        ("24", "Reserved"),
        ("22", "Plumbing"),
        ("23", "Heating, Ventilating, and Air Conditioning (HVAC)"),
        ("25", "Integrated Automation"),
        ("26", "Electrical"),
        ("27", "Communications"),
        ("28", "Electronic Safety and Security"),
        ("29", "Reserved"),
        ("30", "Reserved"),
        ("31", "Earthwork"),
        ("32", "Exterior Improvements"),
        ("33", "Utilities"),
        ("34", "Transportation"),
        ("35", "Waterway and Marine Construction"),
        ("40", "Process Integration"),
        ("41", "Material Processing and Handling Equipment"),
        ("42", "Process Heating, Cooling, and Drying Equipment"),
        ("43", "Process Gas and Liquid Handling, Purification, and Storage Equipment"),
        ("44", "Pollution and Waste Control Equipment"),
        ("45", "Industry-Specific Manufacturing Equipment"),
        ("46", "Water and Wastewater Equipment"),
        ("47", "Audio-Video Equipment"),
        ("48", "Electrical Power Generation"),
        ("49", "Reserved"),
        # Less common divisions that may appear
        ("15", "Mechanical (deprecated, split into 21-23)"),
        ("16", "Electrical (deprecated, now 26-28)"),
        ("50", "Reserved"),
        ("51", "Reserved"),
        ("52", "Reserved"),
        ("64", "Reserved"),
        ("66", "Reserved"),
        ("72", "Reserved"),
        ("92", "Reserved"),
        ("97", "Reserved"),
        ("98", "Reserved"),
        ("99", "Reserved"),
    ]

    df = pd.DataFrame(divisions, columns=['csi_division', 'division_name'])

    # Add surrogate key
    df.insert(0, 'csi_division_id', range(1, len(df) + 1))

    # Ensure csi_division is zero-padded 2-digit string (matches fact table format)
    df['csi_division'] = df['csi_division'].astype(str).str.zfill(2)

    # Add notes for deprecated/reserved
    df['notes'] = ''
    df.loc[df['division_name'].str.contains('deprecated'), 'notes'] = 'Deprecated in MasterFormat 2016'
    df.loc[df['division_name'].str.contains('Reserved'), 'notes'] = 'Reserved for future use'

    # Output path (flattened - all in integrated_analysis root)
    output_dir = settings.PROCESSED_DATA_DIR / 'integrated_analysis'
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / 'dim_csi_division.csv'

    # Save with quoting to preserve leading zeros
    df.to_csv(output_path, index=False, quoting=1)  # quoting=1 is csv.QUOTE_ALL

    print(f"✓ Created dim_csi_division")
    print(f"  Rows: {len(df)}")
    print(f"  Output: {output_path}")
    print(f"\nDivisions included:")

    # Show most common ones
    common = df[~df['division_name'].str.contains('Reserved|deprecated')]
    for _, row in common.iterrows():
        print(f"  {row['csi_division']} - {row['division_name']}")


if __name__ == '__main__':
    build_dim_csi_division()
