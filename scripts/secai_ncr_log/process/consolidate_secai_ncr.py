#!/usr/bin/env python3
"""
Consolidate SECAI NCR/QOR log data with dimension IDs.

Reads the parsed secai_ncr_qor.csv and enriches it with dimension IDs for Power BI:
- dim_company_id: Maps contractor name to company dimension
- dim_csi_section_id: Maps discipline/work_type to CSI section
- dim_location_id: Maps building to location dimension

Output: secai_ncr_consolidated.csv in processed/secai_ncr_log/
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

# Add project root to path for imports
_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings
from scripts.shared.dimension_lookup import (
    get_company_id,
    get_location_id,
)
from scripts.shared.pipeline_utils import get_output_path, write_fact_and_quality

# Import shared CSI inference
from scripts.integrated_analysis.add_csi_to_ncr import (
    infer_csi_from_ncr,
    CSI_SECTIONS,
)


# =============================================================================
# Data quality columns - moved to separate table for Power BI cleanliness
# =============================================================================

SECAI_NCR_DATA_QUALITY_COLUMNS = [
    # CSI inference metadata
    'csi_inference_source',
    # Source tracking
    'source_sheet',
    'row_number',
    # Validation
    '_validation_issues',
    'data_quality_flags',
]

# Additional company aliases for SECAI NCR data
# Maps SECAI contractor values to canonical company names for dim lookup
SECAI_COMPANY_ALIASES: Dict[str, str] = {
    'yates': 'Yates',
    'w.g. yates': 'Yates',
    'wg yates': 'Yates',
    'hensel phelps': 'Hensel Phelps',
    'hp': 'Hensel Phelps',
    'austin global': 'Austin Global',
    'ag': 'Austin Global',
    'austin': 'Austin Global',
    'pci': 'PCI',
    'performance contracting': 'Performance Contracting',
    'berg': 'Berg',
    'berg electric': 'Berg',
    'kiewit': 'Kiewit',
    'murray': 'Murray',
    'baker concrete': 'Baker Concrete',
    'baker': 'Baker Concrete',
    'sas': 'SAS',
    'gbi': 'GBI',
    'emj': 'EMJ',
    'secai': 'SECAI',
    # Additional contractors from unmapped list
    'triad': 'Triad',
    'dean/cec': 'Dean/CEC',
    'dean': 'Dean/CEC',
    'cec': 'Dean/CEC',
    'prism': 'Prism',
    'h&h': 'H&H',
    'way eng': 'WAY Engineering',
    'doota it': 'Doota IT',
    'young & pratt': 'Young & Pratt',
    'tcs': 'TCS',
    'jmor': 'JMOR',
    'schmidt': 'Schmidt',
    'firstclass': 'Firstclass',
    'tindall': 'Tindall',
    'ab&r': 'AB&R',
    'coram': 'Coram',
}


def get_secai_company_id(contractor: str) -> Optional[int]:
    """
    Map SECAI contractor name to company_id.

    Tries SECAI-specific aliases first, then falls back to dimension_lookup.
    """
    if not contractor or pd.isna(contractor):
        return None

    contractor_str = str(contractor).strip()
    contractor_lower = contractor_str.lower()

    # Check SECAI-specific aliases first
    if contractor_lower in SECAI_COMPANY_ALIASES:
        canonical = SECAI_COMPANY_ALIASES[contractor_lower]
        result = get_company_id(canonical)
        if result is not None:
            return result

    # Fallback to standard dimension lookup
    return get_company_id(contractor_str)


def infer_csi_from_secai(discipline: str, work_type: str, description: str) -> Tuple[Optional[int], Optional[str], str]:
    """
    Infer CSI section from SECAI NCR discipline, work_type, and description.

    SECAI uses discipline field more consistently than ProjectSight NCR,
    so we prioritize: discipline -> work_type -> description keywords.

    Args:
        discipline: SECAI discipline field
        work_type: SECAI work_type field
        description: NCR description

    Returns:
        Tuple of (csi_section_id, csi_section_code, inference_source)
    """
    # Try discipline first (SECAI uses specific discipline values)
    if pd.notna(discipline):
        disc_lower = str(discipline).lower().strip()

        # Direct mappings for SECAI discipline values
        SECAI_DISCIPLINE_TO_CSI = {
            'csa': 2,           # 03 30 00 Cast-in-Place Concrete (CSA = Concrete Structural Activity)
            'cast in place': 2, # 03 30 00 Cast-in-Place Concrete
            'concrete': 2,      # 03 30 00 Cast-in-Place Concrete
            'rebar': 2,         # 03 30 00 Cast-in-Place Concrete
            'structural': 6,    # 05 12 00 Structural Steel
            'steel': 6,         # 05 12 00 Structural Steel
            'welding': 6,       # 05 12 00 Structural Steel
            'piping': 36,       # 22 05 00 Common Work Results for Plumbing
            'mechanical': 40,   # 23 05 00 Common Work Results for HVAC
            'mech': 40,         # 23 05 00 Common Work Results for HVAC
            'electrical': 44,   # 26 05 00 Common Work Results for Electrical
            'elec': 44,         # 26 05 00 Common Work Results for Electrical
            'waterproofing': 11, # 07 13 00 Sheet Waterproofing
            'fireproofing': 18, # 07 81 00 Applied Fireproofing
            'firestopping': 19, # 07 84 00 Firestopping
            'roofing': 16,      # 07 52 00 Membrane Roofing
            'insulation': 13,   # 07 21 16 Blanket Insulation
            'drywall': 26,      # 09 21 16 Gypsum Board
            'framing': 8,       # 05 40 00 Cold-Formed Metal Framing
            'masonry': 5,       # 04 20 00 Unit Masonry
            'earthwork': 51,    # 31 23 00 Excavation and Fill
            'architecture': 29, # 09 91 26 Painting
            'arch': 29,         # 09 91 26 Painting
            'civil': 51,        # 31 23 00 Excavation and Fill
            'plumbing': 36,     # 22 05 00 Common Work Results for Plumbing
            'fire protection': 34, # 21 10 00 Fire Suppression
            'fp': 34,           # 21 10 00 Fire Suppression
        }

        if disc_lower in SECAI_DISCIPLINE_TO_CSI:
            csi_id = SECAI_DISCIPLINE_TO_CSI[disc_lower]
            csi_code, _ = CSI_SECTIONS[csi_id]
            return csi_id, csi_code, "discipline"

    # Try work_type field
    if pd.notna(work_type):
        work_lower = str(work_type).lower().strip()

        # Use NCR inference with work_type as description
        csi_id, csi_code, source = infer_csi_from_ncr(work_type, None)
        if csi_id is not None:
            return csi_id, csi_code, "work_type"

    # Fall back to description keyword matching
    if pd.notna(description):
        csi_id, csi_code, source = infer_csi_from_ncr(description, None)
        if csi_id is not None:
            return csi_id, csi_code, "description"

    return None, None, "none"


def consolidate_secai_ncr(dry_run: bool = False, staging_dir: Path = None) -> Dict:
    """
    Consolidate SECAI NCR/QOR data with dimension IDs.

    Args:
        dry_run: If True, preview without writing
        staging_dir: If provided, write outputs to staging directory

    Returns:
        Summary statistics
    """
    # Input path
    input_path = get_output_path('secai_ncr_log/secai_ncr_qor.csv', staging_dir)

    # Output paths (staging or final)
    fact_path = get_output_path('secai_ncr_log/secai_ncr_consolidated.csv', staging_dir)
    quality_path = get_output_path('secai_ncr_log/secai_ncr_data_quality.csv', staging_dir)

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        print("Run process_secai_ncr_log.py first")
        sys.exit(1)

    print(f"Loading SECAI NCR data from: {input_path}")
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df):,} records")

    # Add dimension IDs
    print("\nAdding dimension IDs...")

    # Map contractor -> dim_company_id
    print("  Mapping contractors to company dimension...")
    df['dim_company_id'] = df['contractor'].apply(get_secai_company_id)

    # Map building -> dim_location_id (no level info available in SECAI NCR)
    print("  Mapping buildings to location dimension...")
    df['dim_location_id'] = df.apply(
        lambda row: get_location_id(row.get('building'), None),
        axis=1
    )

    # Infer CSI section from discipline/work_type/description
    print("  Inferring CSI sections...")
    csi_results = df.apply(
        lambda row: infer_csi_from_secai(
            row.get('discipline'),
            row.get('work_type'),
            row.get('description')
        ),
        axis=1
    )
    df['dim_csi_section_id'] = csi_results.apply(lambda x: x[0])
    df['csi_section'] = csi_results.apply(lambda x: x[1])
    df['csi_inference_source'] = csi_results.apply(lambda x: x[2])
    df['csi_title'] = df['dim_csi_section_id'].apply(
        lambda x: CSI_SECTIONS[x][1] if pd.notna(x) and x in CSI_SECTIONS else None
    )

    # Add validation columns
    df['_validation_issues'] = None
    df['data_quality_flags'] = None

    # Calculate coverage
    total = len(df)
    company_mapped = df['dim_company_id'].notna().sum()
    location_mapped = df['dim_location_id'].notna().sum()
    csi_mapped = df['dim_csi_section_id'].notna().sum()

    coverage = {
        'company': {
            'mapped': company_mapped,
            'total': total,
            'pct': company_mapped / total * 100 if total > 0 else 0
        },
        'location': {
            'mapped': location_mapped,
            'total': total,
            'pct': location_mapped / total * 100 if total > 0 else 0
        },
        'csi_section': {
            'mapped': csi_mapped,
            'total': total,
            'pct': csi_mapped / total * 100 if total > 0 else 0
        }
    }

    # Print summary
    print("\n" + "=" * 60)
    print("SECAI NCR CONSOLIDATION SUMMARY")
    print("=" * 60)
    print(f"Total records: {total:,}")
    print(f"\nRecords by type:")
    for (rt, st), count in df.groupby(['record_type', 'source_type']).size().items():
        print(f"  {st} {rt}: {count:,}")

    print(f"\nDimension Coverage:")
    print(f"  company:     {company_mapped:,}/{total:,} ({coverage['company']['pct']:.1f}%)")
    print(f"  location:    {location_mapped:,}/{total:,} ({coverage['location']['pct']:.1f}%)")
    print(f"  csi_section: {csi_mapped:,}/{total:,} ({coverage['csi_section']['pct']:.1f}%)")

    # CSI inference breakdown
    print(f"\nCSI Inference Sources:")
    for source, count in df['csi_inference_source'].value_counts().items():
        print(f"  {source}: {count:,} ({count/total*100:.1f}%)")

    # Show unmapped contractors
    unmapped_contractors = df[df['dim_company_id'].isna()]['contractor'].dropna().value_counts()
    if len(unmapped_contractors) > 0:
        print(f"\nUnmapped contractors ({len(unmapped_contractors)} unique):")
        for contractor, count in unmapped_contractors.head(15).items():
            print(f"  {contractor}: {count}")

    # Show unmapped CSI
    unmapped_csi = df[df['dim_csi_section_id'].isna()][['discipline', 'work_type']].drop_duplicates()
    if len(unmapped_csi) > 0:
        print(f"\nUnmapped discipline/work_type combinations ({len(unmapped_csi)}):")
        for _, row in unmapped_csi.head(10).iterrows():
            print(f"  {row['discipline']} / {row['work_type']}")

    if not dry_run:
        # Write fact and data quality tables
        print(f"\nWriting fact table to: {fact_path}")
        print(f"Writing data quality table to: {quality_path}")
        fact_rows, quality_cols = write_fact_and_quality(
            df=df,
            primary_key='secai_ncr_id',
            quality_columns=SECAI_NCR_DATA_QUALITY_COLUMNS,
            fact_path=fact_path,
            quality_path=quality_path,
        )
        print(f"Wrote {fact_rows:,} rows, moved {quality_cols} columns to data quality table")
    else:
        print("\nDRY RUN - no changes written")

    return {
        'total': total,
        'coverage': coverage,
        'unmapped_contractors': unmapped_contractors.to_dict() if len(unmapped_contractors) > 0 else {},
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Consolidate SECAI NCR with dimension IDs')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing')
    parser.add_argument('--staging-dir', type=Path, default=None,
                        help='Write outputs to staging directory instead of final location')
    args = parser.parse_args()

    consolidate_secai_ncr(dry_run=args.dry_run, staging_dir=args.staging_dir)


if __name__ == "__main__":
    main()
