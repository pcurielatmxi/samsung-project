#!/usr/bin/env python3
"""
Investigate Company Scope in RABA/PSI Quality Data.

Analyzes contractor/company assignments to identify Yates vs AMTS scope,
particularly for the remaining CSI gap sections.
"""

import sys
from pathlib import Path
from typing import Dict, Set

import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings


def load_raba() -> pd.DataFrame:
    """Load RABA consolidated data."""
    path = settings.PROCESSED_DATA_DIR / "raba" / "raba_consolidated.csv"
    return pd.read_csv(path, low_memory=False)


def load_psi() -> pd.DataFrame:
    """Load PSI consolidated data."""
    path = settings.PROCESSED_DATA_DIR / "psi" / "psi_consolidated.csv"
    return pd.read_csv(path, low_memory=False)


def load_dim_company() -> pd.DataFrame:
    """Load company dimension."""
    path = settings.PROCESSED_DATA_DIR / "integrated_analysis" / "dim_company.csv"
    return pd.read_csv(path)


def load_dim_csi() -> pd.DataFrame:
    """Load CSI dimension."""
    path = settings.PROCESSED_DATA_DIR / "integrated_analysis" / "dim_csi_section.csv"
    return pd.read_csv(path)


def analyze_company_distribution(df: pd.DataFrame, source_name: str, dim_company: pd.DataFrame, dim_csi: pd.DataFrame):
    """Analyze company distribution in a data source."""
    print(f"\n{'='*80}")
    print(f"{source_name.upper()} - COMPANY DISTRIBUTION")
    print(f"{'='*80}")

    # Get company name via join
    df_with_company = df.merge(
        dim_company[['company_id', 'canonical_name']],
        left_on='dim_company_id',
        right_on='company_id',
        how='left'
    )

    # Overall company distribution
    print(f"\nTop 20 companies by inspection count:")
    company_counts = df_with_company['canonical_name'].value_counts()
    for company, count in company_counts.head(20).items():
        pct = count / len(df) * 100
        print(f"  {company:40s} {count:6,} ({pct:5.1f}%)")

    # Check if we have CSI sections
    if 'csi_section' in df_with_company.columns:
        # Focus on the gap sections
        gap_sections = ['09 51 00', '09 65 00']
        gap_df = df_with_company[df_with_company['csi_section'].isin(gap_sections)]

        if len(gap_df) > 0:
            print(f"\n{'-'*80}")
            print(f"GAP CSI SECTIONS - Company Breakdown")
            print(f"{'-'*80}")

            for csi_section in gap_sections:
                section_df = gap_df[gap_df['csi_section'] == csi_section]
                if len(section_df) == 0:
                    continue

                section_title = section_df['csi_title'].iloc[0] if 'csi_title' in section_df.columns else ''
                print(f"\n{csi_section} {section_title} ({len(section_df)} records):")

                # Company distribution for this section
                section_companies = section_df['canonical_name'].value_counts()
                for company, count in section_companies.items():
                    pct = count / len(section_df) * 100
                    print(f"  {company:40s} {count:6,} ({pct:5.1f}%)")

    # Look at raw company columns if available
    raw_company_cols = [col for col in df.columns if 'contractor' in col.lower() or 'testing_company' in col.lower() or 'subcontractor' in col.lower()]
    if raw_company_cols:
        print(f"\n{'-'*80}")
        print(f"RAW COMPANY COLUMNS")
        print(f"{'-'*80}")
        for col in raw_company_cols:
            print(f"\n{col}:")
            value_counts = df[col].value_counts().head(10)
            for value, count in value_counts.items():
                if pd.notna(value):
                    pct = count / len(df) * 100
                    print(f"  {str(value):40s} {count:6,} ({pct:5.1f}%)")


def check_yates_vs_amts(df: pd.DataFrame, source_name: str, dim_company: pd.DataFrame):
    """Check if we can distinguish Yates vs AMTS work."""
    print(f"\n{'='*80}")
    print(f"{source_name.upper()} - YATES vs AMTS ANALYSIS")
    print(f"{'='*80}")

    # Get company name
    df_with_company = df.merge(
        dim_company[['company_id', 'canonical_name']],
        left_on='dim_company_id',
        right_on='company_id',
        how='left'
    )

    # Count Yates-related companies
    yates_mask = df_with_company['canonical_name'].str.contains('Yates', case=False, na=False)
    yates_count = yates_mask.sum()

    # Count AMTS-related companies
    amts_mask = df_with_company['canonical_name'].str.contains('AMTS', case=False, na=False)
    amts_count = amts_mask.sum()

    # Other companies
    other_count = len(df_with_company) - yates_count - amts_count

    print(f"\nOverall breakdown:")
    print(f"  Yates-related: {yates_count:6,} ({yates_count/len(df)*100:5.1f}%)")
    print(f"  AMTS-related:  {amts_count:6,} ({amts_count/len(df)*100:5.1f}%)")
    print(f"  Other:         {other_count:6,} ({other_count/len(df)*100:5.1f}%)")

    # Check specific Yates/AMTS companies
    print(f"\nYates companies:")
    yates_companies = df_with_company[yates_mask]['canonical_name'].value_counts()
    for company, count in yates_companies.items():
        print(f"  {company}: {count:,}")

    print(f"\nAMTS companies:")
    amts_companies = df_with_company[amts_mask]['canonical_name'].value_counts()
    for company, count in amts_companies.items():
        print(f"  {company}: {count:,}")


def main():
    """Run company scope investigation."""
    print("="*80)
    print("INVESTIGATING COMPANY SCOPE IN RABA/PSI")
    print("="*80)

    # Load data
    print("\nLoading data...")
    raba = load_raba()
    psi = load_psi()
    dim_company = load_dim_company()
    dim_csi = load_dim_csi()

    print(f"  RABA: {len(raba):,} records")
    print(f"  PSI: {len(psi):,} records")
    print(f"  Companies: {len(dim_company):,}")
    print(f"  CSI Sections: {len(dim_csi):,}")

    # Analyze RABA
    analyze_company_distribution(raba, "RABA", dim_company, dim_csi)
    check_yates_vs_amts(raba, "RABA", dim_company)

    # Analyze PSI
    analyze_company_distribution(psi, "PSI", dim_company, dim_csi)
    check_yates_vs_amts(psi, "PSI", dim_company)

    print("\n" + "="*80)
    print("INVESTIGATION COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()
