#!/usr/bin/env python3
"""
Consolidate NCR (Non-Conformance Report) data with dimension IDs.

Reads the processed ncr.csv and enriches it with dimension IDs for Power BI:
- dim_company_id: Maps company name to company dimension
- dim_csi_section_id: Maps description/discipline to CSI section

Note: dim_trade_id has been superseded by dim_csi_section_id.

Output: ncr_consolidated.csv in processed/projectsight/
"""

import sys
from pathlib import Path
from typing import Dict, Optional
import pandas as pd

# Add project root to path for imports
_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings
from schemas.validator import validated_df_to_csv
from scripts.shared.dimension_lookup import (
    get_company_id,
)
from scripts.integrated_analysis.add_csi_to_ncr import (
    infer_csi_from_ncr,
    CSI_SECTIONS,
)


# Additional company aliases for NCR data
# These map NCR company values to canonical company names for dim lookup
NCR_COMPANY_ALIASES: Dict[str, str] = {
    'clayco': 'Clayco',
    'austin global': 'Austin Global',
    'ag': 'Austin Global',
    'nomura': 'Nomura',
    'emd': 'EMD',
    'sas': 'SAS',
    'nms': 'NMS',
    'linde': 'Linde',
    'iqa': 'IQA',
    'gbi': 'GBI',
    't1': 'T1 Project',
    'secai (variant)': 'SECAI',
}


def get_ncr_company_id(company: str) -> Optional[int]:
    """
    Map NCR company name to company_id.

    Tries NCR-specific aliases first, then falls back to dimension_lookup.
    """
    if not company or pd.isna(company):
        return None

    company_str = str(company).strip()
    company_lower = company_str.lower()

    # Check NCR-specific aliases first
    if company_lower in NCR_COMPANY_ALIASES:
        canonical = NCR_COMPANY_ALIASES[company_lower]
        # Try to look up the canonical name
        result = get_company_id(canonical)
        if result is not None:
            return result

    # Fallback to standard dimension lookup
    return get_company_id(company_str)


def consolidate_ncr(input_path: Path, output_path: Path) -> Dict:
    """
    Consolidate NCR data with dimension IDs.

    Args:
        input_path: Path to ncr.csv
        output_path: Path to write ncr_consolidated.csv

    Returns:
        Summary statistics
    """
    print(f"Loading NCR data from: {input_path}")
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df)} records")

    # Add dimension IDs
    print("\nAdding dimension IDs...")

    # Map company -> dim_company_id (using NCR-specific aliases)
    df['dim_company_id'] = df['company'].apply(get_ncr_company_id)

    # Note: dim_trade_id removed - use dim_csi_section_id for work type classification

    # Add _validation_issues column for consistency with RABA/PSI
    # NCR uses data_quality_flags for similar purpose, map it
    df['_validation_issues'] = df.get('data_quality_flags', None)

    # Infer CSI section from description and discipline
    print("  Inferring CSI sections...")
    csi_results = df.apply(
        lambda row: infer_csi_from_ncr(row.get('description'), row.get('discipline')),
        axis=1
    )
    df['dim_csi_section_id'] = csi_results.apply(lambda x: x[0])
    df['csi_section'] = csi_results.apply(lambda x: x[1])
    df['csi_inference_source'] = csi_results.apply(lambda x: x[2])
    df['csi_title'] = df['dim_csi_section_id'].apply(
        lambda x: CSI_SECTIONS[x][1] if pd.notna(x) and x in CSI_SECTIONS else None
    )

    # Calculate coverage
    company_mapped = df['dim_company_id'].notna().sum()
    csi_mapped = df['dim_csi_section_id'].notna().sum()
    total = len(df)

    coverage = {
        'company': {
            'mapped': company_mapped,
            'total': total,
            'pct': company_mapped / total * 100 if total > 0 else 0
        },
        'csi_section': {
            'mapped': csi_mapped,
            'total': total,
            'pct': csi_mapped / total * 100 if total > 0 else 0
        }
    }

    # Write output (with schema validation)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    validated_df_to_csv(df, output_path, index=False)
    print(f"\nWrote {len(df)} records to: {output_path} (validated)")

    # Print summary
    print("\n" + "=" * 60)
    print("NCR CONSOLIDATION SUMMARY")
    print("=" * 60)
    print(f"Total records: {total}")
    print(f"\nDimension Coverage:")
    print(f"  company:     {company_mapped}/{total} ({coverage['company']['pct']:.1f}%)")
    print(f"  csi_section: {csi_mapped}/{total} ({coverage['csi_section']['pct']:.1f}%)")

    # Show unmapped companies
    unmapped_companies = df[df['dim_company_id'].isna()]['company'].dropna().value_counts()
    if len(unmapped_companies) > 0:
        print(f"\nUnmapped companies ({len(unmapped_companies)} unique):")
        for company, count in unmapped_companies.head(15).items():
            print(f"  {company}: {count}")

    return {
        'total': total,
        'coverage': coverage,
        'unmapped_companies': unmapped_companies.to_dict(),
    }


def main():
    """Main entry point."""
    input_path = settings.PROJECTSIGHT_PROCESSED_DIR / 'ncr.csv'
    output_path = settings.PROJECTSIGHT_PROCESSED_DIR / 'ncr_consolidated.csv'

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        print("Run process_ncr_export.py first")
        sys.exit(1)

    consolidate_ncr(input_path, output_path)


if __name__ == "__main__":
    main()
