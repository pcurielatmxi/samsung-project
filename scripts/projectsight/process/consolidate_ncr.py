#!/usr/bin/env python3
"""
Consolidate NCR (Non-Conformance Report) data with dimension IDs.

Reads the processed ncr.csv and enriches it with dimension IDs for Power BI:
- dim_company_id: Maps company name to company dimension
- dim_trade_id: Maps discipline to trade dimension

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
from scripts.shared.dimension_lookup import (
    get_company_id,
    get_trade_id,
    get_trade_code,
)
from scripts.integrated_analysis.add_csi_to_ncr import (
    infer_csi_from_ncr,
    CSI_SECTIONS,
)


# NCR discipline to trade_id mapping
# NCR uses general discipline categories that map to our trade taxonomy
DISCIPLINE_TO_TRADE: Dict[str, int] = {
    # Primary disciplines (common in NCR data)
    'mechanical': 7,       # MEP
    'electrical': 7,       # MEP
    'plumbing': 7,         # MEP
    'architecture': 5,     # Finishes
    'architectural': 5,    # Finishes
    'structural': 1,       # Concrete (default for structural)
    'civil': 9,            # Earthwork
    'landscape': 9,        # Earthwork
    'landscape/site': 9,   # Earthwork
    'general': 12,         # General

    # Specific types found in data
    'fire protection': 6,  # Fire Protection
    'fireproofing': 6,     # Fire Protection
    'roofing': 3,          # Roofing
    'waterproofing': 3,    # Roofing & Waterproofing
    'insulation': 8,       # Insulation
    'drywall': 4,          # Drywall
    'framing': 4,          # Drywall & Framing
    'painting': 5,         # Finishes
    'flooring': 5,         # Finishes
    'ceilings': 5,         # Finishes
    'concrete': 1,         # Concrete
    'steel': 2,            # Structural Steel
    'metal panels': 11,    # Panels
    'cladding': 11,        # Panels
    'masonry': 13,         # Masonry

    # Abbreviations found in NCR data
    'fa': 7,               # Fire Alarm → MEP
    'fp': 6,               # Fire Protection
    'i&c': 7,              # Instrumentation & Controls → MEP
    'stel': 2,             # Structural Steel
    'fiber': 7,            # Fiber optics → MEP/Electrical
    'conduit': 7,          # Electrical conduit → MEP
    'terminations': 7,     # Electrical terminations → MEP
    'd': 4,                # Likely Drywall abbreviation
    'e': 7,                # Electrical
    'm': 7,                # Mechanical
    'p': 7,                # Plumbing
    'a': 5,                # Architecture/Finishes
    'bolting': 2,          # Bolting → Steel
    'fiz roof': 3,         # FIZ Roof → Roofing
    'csf': 7,              # CSF (likely MEP system)

    # Equipment and system codes
    'dc rectifier': 7,     # Electrical equipment → MEP
    'fsf eqp': 7,          # Fire safety equipment → MEP
    'cleanzone': 12,       # Cleanroom → General
    'gcs': 12,             # General/Site
    'fab': 12,             # FAB building → General
    'office': 5,           # Office → Finishes
    'e_office': 7,         # Electrical office → MEP

    # Exterior/Site
    'exterior': 11,        # Exterior panels/cladding
    'site': 9,             # Site work → Earthwork
    'grading': 9,          # Earthwork
    'curb': 1,             # Concrete curb
    'paving': 9,           # Site paving → Earthwork

    # Specialty items
    'expansion joint': 1,  # Concrete expansion joints
    'sfrm': 6,             # Spray fireproofing
    'weld': 2,             # Welding → Steel
    'anchor': 1,           # Concrete anchors
    'embed': 1,            # Concrete embeds
    'rebar': 1,            # Reinforcing steel (part of concrete)
    'grout': 1,            # Concrete grout
    'sprinkler': 6,        # Fire sprinkler → Fire Protection
    'door': 5,             # Doors → Finishes
    'hardware': 5,         # Hardware → Finishes
    'glazing': 5,          # Glazing → Finishes
    'duct': 7,             # HVAC duct → MEP
    'pipe': 7,             # Piping → MEP
    'valve': 7,            # Valves → MEP
    'pump': 7,             # Pumps → MEP
    'ahu': 7,              # Air handling unit → MEP
    'chiller': 7,          # Chiller → MEP
    'panel': 7,            # Electrical panel → MEP (not metal panels)
    'transformer': 7,      # Transformer → MEP
    'switchgear': 7,       # Switchgear → MEP
    'cable': 7,            # Cable → MEP
    'wire': 7,             # Wire → MEP
    'tray': 7,             # Cable tray → MEP
}

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


def get_trade_from_discipline(discipline: str) -> Optional[int]:
    """
    Map NCR discipline to trade_id.

    Uses direct mapping first, then falls back to dimension_lookup.get_trade_id().
    """
    if not discipline or pd.isna(discipline):
        return None

    # Normalize for lookup
    disc_lower = str(discipline).strip().lower()

    # Direct mapping
    if disc_lower in DISCIPLINE_TO_TRADE:
        return DISCIPLINE_TO_TRADE[disc_lower]

    # Partial match
    for key, trade_id in DISCIPLINE_TO_TRADE.items():
        if key in disc_lower or disc_lower in key:
            return trade_id

    # Fallback to dimension_lookup
    return get_trade_id(discipline)


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

    # Map discipline -> dim_trade_id
    df['dim_trade_id'] = df['discipline'].apply(get_trade_from_discipline)
    df['dim_trade_code'] = df['dim_trade_id'].apply(
        lambda x: get_trade_code(x) if pd.notna(x) else None
    )

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
    trade_mapped = df['dim_trade_id'].notna().sum()
    csi_mapped = df['dim_csi_section_id'].notna().sum()
    total = len(df)

    coverage = {
        'company': {
            'mapped': company_mapped,
            'total': total,
            'pct': company_mapped / total * 100 if total > 0 else 0
        },
        'trade': {
            'mapped': trade_mapped,
            'total': total,
            'pct': trade_mapped / total * 100 if total > 0 else 0
        },
        'csi_section': {
            'mapped': csi_mapped,
            'total': total,
            'pct': csi_mapped / total * 100 if total > 0 else 0
        }
    }

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"\nWrote {len(df)} records to: {output_path}")

    # Print summary
    print("\n" + "=" * 60)
    print("NCR CONSOLIDATION SUMMARY")
    print("=" * 60)
    print(f"Total records: {total}")
    print(f"\nDimension Coverage:")
    print(f"  company:     {company_mapped}/{total} ({coverage['company']['pct']:.1f}%)")
    print(f"  trade:       {trade_mapped}/{total} ({coverage['trade']['pct']:.1f}%)")
    print(f"  csi_section: {csi_mapped}/{total} ({coverage['csi_section']['pct']:.1f}%)")

    # Show unmapped companies
    unmapped_companies = df[df['dim_company_id'].isna()]['company'].dropna().value_counts()
    if len(unmapped_companies) > 0:
        print(f"\nUnmapped companies ({len(unmapped_companies)} unique):")
        for company, count in unmapped_companies.head(15).items():
            print(f"  {company}: {count}")

    # Show unmapped disciplines
    unmapped_disciplines = df[df['dim_trade_id'].isna()]['discipline'].dropna().value_counts()
    if len(unmapped_disciplines) > 0:
        print(f"\nUnmapped disciplines ({len(unmapped_disciplines)} unique):")
        for disc, count in unmapped_disciplines.head(15).items():
            print(f"  {disc}: {count}")

    return {
        'total': total,
        'coverage': coverage,
        'unmapped_companies': unmapped_companies.to_dict(),
        'unmapped_disciplines': unmapped_disciplines.to_dict(),
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
