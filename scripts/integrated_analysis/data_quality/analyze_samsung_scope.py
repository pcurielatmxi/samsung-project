#!/usr/bin/env python3
"""
Analyze Samsung E&C Contractor Assignments.

Investigates which CSI sections and work types are labeled as Samsung E&C
to determine which should be re-attributed to Yates based on scope.

Samsung E&C is the project owner - when listed as "contractor" it's often
a placeholder meaning "Yates scope" rather than actual Samsung self-perform.
"""

import sys
from pathlib import Path
from typing import Dict, Set

import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings


def load_data():
    """Load required data sources."""
    raba = pd.read_csv(
        settings.PROCESSED_DATA_DIR / "raba" / "raba_consolidated.csv",
        low_memory=False
    )
    psi = pd.read_csv(
        settings.PROCESSED_DATA_DIR / "psi" / "psi_consolidated.csv",
        low_memory=False
    )
    dim_company = pd.read_csv(
        settings.PROCESSED_DATA_DIR / "integrated_analysis" / "dim_company.csv"
    )
    dim_csi = pd.read_csv(
        settings.PROCESSED_DATA_DIR / "integrated_analysis" / "dim_csi_section.csv"
    )
    p6_taxonomy = pd.read_csv(
        settings.PROCESSED_DATA_DIR / "primavera" / "p6_task_taxonomy.csv",
        low_memory=False
    )

    return raba, psi, dim_company, dim_csi, p6_taxonomy


def get_p6_csi_sections(p6_taxonomy: pd.DataFrame) -> Set[str]:
    """Get all CSI sections that exist in P6 (Yates scope)."""
    p6_csi = p6_taxonomy[p6_taxonomy['csi_section'].notna()]['csi_section'].unique()
    return set(p6_csi)


def analyze_samsung_inspections(df: pd.DataFrame, source_name: str, dim_company: pd.DataFrame, dim_csi: pd.DataFrame, p6_csi_sections: Set[str]):
    """Analyze inspections labeled as Samsung E&C."""
    print(f"\n{'='*80}")
    print(f"{source_name.upper()} - SAMSUNG E&C ANALYSIS")
    print(f"{'='*80}")

    # Join company names
    df_with_company = df.merge(
        dim_company[['company_id', 'canonical_name']],
        left_on='dim_company_id',
        right_on='company_id',
        how='left'
    )

    # Filter to Samsung only
    samsung_df = df_with_company[df_with_company['canonical_name'] == 'Samsung E&C'].copy()
    print(f"\nSamsung E&C inspections: {len(samsung_df):,} ({len(samsung_df)/len(df)*100:.1f}% of {source_name})")

    if len(samsung_df) == 0:
        print("No Samsung E&C inspections found")
        return

    # Analyze CSI section distribution
    if 'csi_section' in samsung_df.columns:
        print(f"\nCSI Section Distribution (Samsung-labeled inspections):")
        samsung_csi = samsung_df[samsung_df['csi_section'].notna()]
        csi_dist = samsung_csi['csi_section'].value_counts()

        # Get CSI titles (check if already in df)
        if 'csi_title' not in samsung_csi.columns:
            csi_with_title = samsung_csi.merge(
                dim_csi[['csi_section', 'csi_title']],
                on='csi_section',
                how='left'
            )
        else:
            csi_with_title = samsung_csi

        csi_title_dist = csi_with_title.groupby(['csi_section', 'csi_title']).size().sort_values(ascending=False)

        print(f"\nTop 20 CSI sections in Samsung-labeled inspections:")
        for (section, title), count in csi_title_dist.head(20).items():
            in_p6 = section in p6_csi_sections
            marker = "✓ IN P6" if in_p6 else "✗ NOT IN P6"
            pct = count / len(samsung_df) * 100
            print(f"  {section} {title:40s} {count:5,} ({pct:4.1f}%)  {marker}")

        # Summary: How many Samsung inspections are in P6 scope?
        samsung_in_p6 = samsung_csi[samsung_csi['csi_section'].isin(p6_csi_sections)]
        samsung_not_in_p6 = samsung_csi[~samsung_csi['csi_section'].isin(p6_csi_sections)]

        print(f"\n{'='*80}")
        print("SCOPE INFERENCE")
        print(f"{'='*80}")
        print(f"\nSamsung inspections with CSI in P6 scope (likely Yates): {len(samsung_in_p6):,} ({len(samsung_in_p6)/len(samsung_df)*100:.1f}%)")
        print(f"Samsung inspections with CSI NOT in P6 (likely Samsung): {len(samsung_not_in_p6):,} ({len(samsung_not_in_p6)/len(samsung_df)*100:.1f}%)")
        print(f"Samsung inspections without CSI: {len(samsung_df) - len(samsung_csi):,}")

    # Analyze trade distribution
    if 'dim_trade_id' in samsung_df.columns:
        print(f"\n{'='*80}")
        print("TRADE DISTRIBUTION")
        print(f"{'='*80}")
        trade_dist = samsung_df[samsung_df['dim_trade_id'].notna()]['dim_trade_id'].value_counts()
        print(f"\nTop trades in Samsung-labeled inspections:")
        for trade_id, count in trade_dist.head(10).items():
            pct = count / len(samsung_df) * 100
            print(f"  Trade {trade_id}: {count:,} ({pct:.1f}%)")

    # Check inspection types
    if 'inspection_type' in samsung_df.columns:
        print(f"\n{'='*80}")
        print("INSPECTION TYPES")
        print(f"{'='*80}")
        type_dist = samsung_df['inspection_type'].value_counts().head(20)
        print(f"\nTop 20 inspection types in Samsung-labeled inspections:")
        for insp_type, count in type_dist.items():
            pct = count / len(samsung_df) * 100
            print(f"  {str(insp_type):60s} {count:5,} ({pct:4.1f}%)")


def compare_samsung_vs_yates_csi(raba: pd.DataFrame, psi: pd.DataFrame, dim_company: pd.DataFrame, p6_csi_sections: Set[str]):
    """Compare CSI sections between Samsung and Yates labeled inspections."""
    print(f"\n{'='*80}")
    print("SAMSUNG vs YATES CSI COMPARISON")
    print(f"{'='*80}")

    # Combine RABA + PSI
    combined = pd.concat([
        raba.assign(source='RABA'),
        psi.assign(source='PSI')
    ], ignore_index=True)

    # Join company names
    combined = combined.merge(
        dim_company[['company_id', 'canonical_name']],
        left_on='dim_company_id',
        right_on='company_id',
        how='left'
    )

    # Filter to Samsung and Yates
    samsung_csi = combined[
        (combined['canonical_name'] == 'Samsung E&C') &
        (combined['csi_section'].notna())
    ]['csi_section'].value_counts()

    yates_csi = combined[
        (combined['canonical_name'] == 'Yates') &
        (combined['csi_section'].notna())
    ]['csi_section'].value_counts()

    # Find CSI sections that appear in both
    samsung_sections = set(samsung_csi.index)
    yates_sections = set(yates_csi.index)

    overlap = samsung_sections & yates_sections
    samsung_only = samsung_sections - yates_sections
    yates_only = yates_sections - samsung_sections

    print(f"\nCSI sections in both Samsung and Yates: {len(overlap)}")
    print(f"CSI sections ONLY in Samsung: {len(samsung_only)}")
    print(f"CSI sections ONLY in Yates: {len(yates_only)}")

    print(f"\n{'='*80}")
    print("CSI SECTIONS IN BOTH SAMSUNG AND YATES (overlap)")
    print(f"{'='*80}")
    print(f"\n{'CSI Section':12s} {'Samsung':>8s} {'Yates':>8s} {'In P6?':8s}")
    print("-" * 40)
    for section in sorted(overlap):
        s_count = samsung_csi[section]
        y_count = yates_csi[section]
        in_p6 = "✓" if section in p6_csi_sections else "✗"
        print(f"{section:12s} {s_count:8,} {y_count:8,} {in_p6:8s}")

    if samsung_only:
        print(f"\n{'='*80}")
        print("CSI SECTIONS ONLY IN SAMSUNG (likely owner self-perform)")
        print(f"{'='*80}")
        for section in sorted(samsung_only):
            count = samsung_csi[section]
            in_p6 = "✓" if section in p6_csi_sections else "✗"
            print(f"  {section}: {count:,} inspections  (In P6: {in_p6})")


def main():
    """Run Samsung scope analysis."""
    print("="*80)
    print("SAMSUNG E&C SCOPE ANALYSIS")
    print("="*80)
    print("\nPurpose: Identify which Samsung-labeled inspections should be")
    print("         re-attributed to Yates based on CSI scope patterns.")

    # Load data
    print("\nLoading data...")
    raba, psi, dim_company, dim_csi, p6_taxonomy = load_data()

    # Get P6 CSI sections (Yates scope)
    p6_csi_sections = get_p6_csi_sections(p6_taxonomy)
    print(f"\nP6 has {len(p6_csi_sections)} unique CSI sections (Yates scope)")

    # Analyze Samsung inspections in each source
    analyze_samsung_inspections(raba, "RABA", dim_company, dim_csi, p6_csi_sections)
    analyze_samsung_inspections(psi, "PSI", dim_company, dim_csi, p6_csi_sections)

    # Compare Samsung vs Yates CSI patterns
    compare_samsung_vs_yates_csi(raba, psi, dim_company, p6_csi_sections)

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()
