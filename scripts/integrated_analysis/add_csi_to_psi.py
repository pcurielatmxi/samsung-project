#!/usr/bin/env python3
"""
Add CSI Section IDs to PSI Quality Inspections.

Parses inspection_type field to determine the most specific CSI section code.
Uses the same inference logic as RABA for consistency across quality data sources.

Appends CSI columns to the original consolidated file (does not create separate file).
New columns added: dim_csi_section_id, csi_section, csi_inference_source, csi_title

Input/Output:
    {WINDOWS_DATA_DIR}/processed/psi/4.consolidate/psi_qc_inspections.csv

Usage:
    python -m scripts.integrated_analysis.add_csi_to_psi
    python -m scripts.integrated_analysis.add_csi_to_psi --dry-run
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Add project root to path
_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings

# Import shared CSI inference logic from RABA script
from scripts.integrated_analysis.add_csi_to_raba import (
    CSI_SECTIONS,
    infer_csi_section,
)


def add_csi_to_psi(dry_run: bool = False):
    """Add CSI section IDs to PSI consolidated data (appends to original file)."""

    input_path = settings.PROCESSED_DATA_DIR / "psi" / "4.consolidate" / "psi_qc_inspections.csv"
    # Write back to the same file (append columns to original)
    output_path = input_path

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return

    print(f"Loading PSI data from: {input_path}")
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df):,} records")

    # Apply CSI inference (same logic as RABA)
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
    parser = argparse.ArgumentParser(description='Add CSI section IDs to PSI data')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing output')
    args = parser.parse_args()

    add_csi_to_psi(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
