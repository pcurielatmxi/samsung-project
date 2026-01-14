#!/usr/bin/env python3
"""
Verify dim_company Coverage Across All Data Sources

Checks all company names from all data sources against dim_company + aliases
and reports coverage statistics and unmapped company names.

Data Sources Checked:
- ProjectSight Labor (labor_entries.csv)
- TBM Daily Plans (work_entries.csv)
- RABA Quality Inspections (raba_consolidated.csv)
- PSI Quality Inspections (psi_consolidated.csv)
- ProjectSight NCR (ncr_consolidated.csv)
- QC Logs Master List (ARCH sheet)

Usage:
    python scripts/integrated_analysis/verify_company_coverage.py
    python scripts/integrated_analysis/verify_company_coverage.py --verbose
    python scripts/integrated_analysis/verify_company_coverage.py --export unmapped.csv
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
from difflib import SequenceMatcher

import pandas as pd

# Add project root to path
_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings
from scripts.shared.dimension_lookup import get_company_id, reset_cache


def load_source_companies() -> Dict[str, Dict[str, int]]:
    """
    Load company names and record counts from all data sources.

    Returns:
        Dict mapping source_name -> {company_name: record_count}
    """
    sources = {}

    # 1. ProjectSight Labor
    ps_file = settings.PROCESSED_DATA_DIR / 'projectsight' / 'labor_entries.csv'
    if ps_file.exists():
        df = pd.read_csv(ps_file, usecols=['company'])
        sources['projectsight_labor'] = df['company'].dropna().value_counts().to_dict()

    # 2. TBM Daily Plans - check multiple company columns
    tbm_file = settings.PROCESSED_DATA_DIR / 'tbm' / 'work_entries.csv'
    if tbm_file.exists():
        df = pd.read_csv(tbm_file)
        tbm_companies = defaultdict(int)
        for col in ['tier2_sc', 'tier1_gc', 'subcontractor_file']:
            if col in df.columns:
                for name, count in df[col].dropna().value_counts().items():
                    tbm_companies[name] += count
        sources['tbm'] = dict(tbm_companies)

    # 3. RABA Quality - check multiple company columns
    raba_file = settings.PROCESSED_DATA_DIR / 'raba' / 'raba_consolidated.csv'
    if raba_file.exists():
        df = pd.read_csv(raba_file)
        raba_companies = defaultdict(int)
        for col in ['contractor_raw', 'contractor', 'subcontractor_raw', 'subcontractor']:
            if col in df.columns:
                for name, count in df[col].dropna().value_counts().items():
                    raba_companies[name] += count
        sources['raba'] = dict(raba_companies)

    # 4. PSI Quality - check multiple company columns
    psi_file = settings.PROCESSED_DATA_DIR / 'psi' / 'psi_consolidated.csv'
    if psi_file.exists():
        df = pd.read_csv(psi_file)
        psi_companies = defaultdict(int)
        for col in ['contractor_raw', 'contractor', 'subcontractor_raw', 'subcontractor']:
            if col in df.columns:
                for name, count in df[col].dropna().value_counts().items():
                    psi_companies[name] += count
        sources['psi'] = dict(psi_companies)

    # 5. ProjectSight NCR
    ncr_file = settings.PROCESSED_DATA_DIR / 'projectsight' / 'ncr_consolidated.csv'
    if ncr_file.exists():
        df = pd.read_csv(ncr_file)
        if 'company' in df.columns:
            sources['ncr'] = df['company'].dropna().value_counts().to_dict()

    # 6. QC Logs Master List
    qc_master = settings.RAW_DATA_DIR / 'qc_logs' / 'MASTER LIST'
    if qc_master.exists():
        xlsx_files = list(qc_master.glob('*.xlsx')) + list(qc_master.glob('*.xlsm'))
        if xlsx_files:
            qc_companies = defaultdict(int)
            for xlsx_file in xlsx_files:
                try:
                    # Try reading ARCH, MECH, ELEC sheets
                    for sheet in ['ARCH', 'MECH', 'ELEC']:
                        try:
                            df = pd.read_excel(xlsx_file, sheet_name=sheet, header=1)
                            if 'Author Company' in df.columns:
                                for name, count in df['Author Company'].dropna().value_counts().items():
                                    qc_companies[name] += count
                        except:
                            pass
                except Exception as e:
                    print(f"  Warning: Could not read {xlsx_file.name}: {e}")
            if qc_companies:
                sources['qc_logs'] = dict(qc_companies)

    return sources


def find_similar_companies(name: str, dim_company: pd.DataFrame, threshold: float = 0.6) -> List[Tuple[str, float]]:
    """
    Find similar company names in dim_company using fuzzy matching.

    Args:
        name: Company name to match
        dim_company: DataFrame with canonical_name column
        threshold: Minimum similarity score (0-1)

    Returns:
        List of (canonical_name, similarity_score) tuples, sorted by score
    """
    matches = []
    name_lower = name.lower()

    for canonical in dim_company['canonical_name'].dropna().unique():
        canonical_lower = canonical.lower()

        # Calculate similarity
        score = SequenceMatcher(None, name_lower, canonical_lower).ratio()

        # Also check if one contains the other
        if name_lower in canonical_lower or canonical_lower in name_lower:
            score = max(score, 0.7)

        if score >= threshold:
            matches.append((canonical, score))

    return sorted(matches, key=lambda x: -x[1])[:3]


def verify_coverage(verbose: bool = False) -> Dict:
    """
    Verify company coverage across all sources.

    Returns:
        Dict with coverage statistics and unmapped companies
    """
    # Reset cache to ensure fresh data
    reset_cache()

    # Load dim_company for suggestions
    dim_company = pd.read_csv(
        settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'dimensions' / 'dim_company.csv'
    )

    # Load aliases for reference
    aliases_file = settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'mappings' / 'map_company_aliases.csv'
    aliases = pd.read_csv(aliases_file) if aliases_file.exists() else pd.DataFrame()

    print("Loading company names from all sources...")
    sources = load_source_companies()

    results = {
        'sources': {},
        'all_unmapped': {},
        'summary': {
            'total_sources': len(sources),
            'total_unique_names': 0,
            'total_mapped': 0,
            'total_unmapped': 0,
        }
    }

    # Track all unique names and their mapping status
    all_names = {}  # name -> {sources: [...], mapped: bool, company_id: int}

    print("\nVerifying coverage by source...")
    print("=" * 80)

    for source_name, companies in sources.items():
        source_stats = {
            'total_names': len(companies),
            'total_records': sum(companies.values()),
            'mapped_names': 0,
            'mapped_records': 0,
            'unmapped': [],
        }

        for name, count in companies.items():
            # Track in all_names
            if name not in all_names:
                company_id = get_company_id(name)
                all_names[name] = {
                    'sources': [],
                    'total_records': 0,
                    'mapped': company_id is not None,
                    'company_id': company_id,
                }

            all_names[name]['sources'].append(source_name)
            all_names[name]['total_records'] += count

            if all_names[name]['mapped']:
                source_stats['mapped_names'] += 1
                source_stats['mapped_records'] += count
            else:
                source_stats['unmapped'].append((name, count))

        # Calculate coverage
        source_stats['name_coverage'] = (
            source_stats['mapped_names'] / source_stats['total_names'] * 100
            if source_stats['total_names'] > 0 else 0
        )
        source_stats['record_coverage'] = (
            source_stats['mapped_records'] / source_stats['total_records'] * 100
            if source_stats['total_records'] > 0 else 0
        )

        results['sources'][source_name] = source_stats

        # Print source summary
        print(f"\n{source_name.upper()}")
        print(f"  Unique names: {source_stats['total_names']:,}")
        print(f"  Total records: {source_stats['total_records']:,}")
        print(f"  Mapped names: {source_stats['mapped_names']:,} ({source_stats['name_coverage']:.1f}%)")
        print(f"  Mapped records: {source_stats['mapped_records']:,} ({source_stats['record_coverage']:.1f}%)")

        if source_stats['unmapped'] and verbose:
            print(f"  Unmapped ({len(source_stats['unmapped'])}):")
            for name, count in sorted(source_stats['unmapped'], key=lambda x: -x[1])[:10]:
                print(f"    {count:>8,}  {name}")
            if len(source_stats['unmapped']) > 10:
                print(f"    ... and {len(source_stats['unmapped']) - 10} more")

    # Compile all unmapped with suggestions
    print("\n" + "=" * 80)
    print("UNMAPPED COMPANY NAMES (across all sources)")
    print("=" * 80)

    unmapped_list = []
    for name, info in all_names.items():
        if not info['mapped']:
            suggestions = find_similar_companies(name, dim_company)
            unmapped_list.append({
                'name': name,
                'sources': info['sources'],
                'total_records': info['total_records'],
                'suggestions': suggestions,
            })
            results['all_unmapped'][name] = {
                'sources': info['sources'],
                'total_records': info['total_records'],
                'suggestions': [(s[0], round(s[1], 2)) for s in suggestions],
            }

    # Sort by total records descending
    unmapped_list.sort(key=lambda x: -x['total_records'])

    print(f"\nTotal unmapped: {len(unmapped_list)} unique names")
    print(f"\n{'Company Name':<45} {'Records':>10} {'Sources':<30} Suggested Match")
    print("-" * 120)

    for item in unmapped_list:
        sources_str = ', '.join(item['sources'][:3])
        if len(item['sources']) > 3:
            sources_str += f" +{len(item['sources'])-3}"

        suggestion_str = ""
        if item['suggestions']:
            best = item['suggestions'][0]
            suggestion_str = f"{best[0]} ({best[1]:.0%})"

        print(f"{item['name']:<45} {item['total_records']:>10,} {sources_str:<30} {suggestion_str}")

    # Update summary
    results['summary']['total_unique_names'] = len(all_names)
    results['summary']['total_mapped'] = sum(1 for v in all_names.values() if v['mapped'])
    results['summary']['total_unmapped'] = len(unmapped_list)
    results['summary']['overall_name_coverage'] = (
        results['summary']['total_mapped'] / results['summary']['total_unique_names'] * 100
        if results['summary']['total_unique_names'] > 0 else 0
    )

    # Print overall summary
    print("\n" + "=" * 80)
    print("OVERALL SUMMARY")
    print("=" * 80)
    print(f"Data sources checked: {results['summary']['total_sources']}")
    print(f"Unique company names: {results['summary']['total_unique_names']:,}")
    print(f"Mapped to dim_company: {results['summary']['total_mapped']:,} ({results['summary']['overall_name_coverage']:.1f}%)")
    print(f"Unmapped: {results['summary']['total_unmapped']:,}")

    # Print dim_company stats
    print(f"\ndim_company table: {len(dim_company)} companies")
    print(f"Aliases table: {len(aliases)} aliases")

    return results


def export_unmapped(results: Dict, output_file: Path):
    """Export unmapped companies to CSV for review."""
    rows = []
    for name, info in results['all_unmapped'].items():
        suggestion = info['suggestions'][0] if info['suggestions'] else ('', 0)
        rows.append({
            'company_name': name,
            'total_records': info['total_records'],
            'sources': ', '.join(info['sources']),
            'num_sources': len(info['sources']),
            'suggested_match': suggestion[0],
            'match_score': suggestion[1],
        })

    df = pd.DataFrame(rows)
    df = df.sort_values('total_records', ascending=False)
    df.to_csv(output_file, index=False)
    print(f"\nExported {len(df)} unmapped companies to {output_file}")


def main():
    parser = argparse.ArgumentParser(description='Verify dim_company coverage across all data sources')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Show unmapped names per source')
    parser.add_argument('--export', type=str, metavar='FILE',
                       help='Export unmapped companies to CSV file')
    args = parser.parse_args()

    print("=" * 80)
    print("DIM_COMPANY COVERAGE VERIFICATION")
    print("=" * 80)

    results = verify_coverage(verbose=args.verbose)

    if args.export:
        export_unmapped(results, Path(args.export))

    # Return non-zero if there are unmapped companies
    return 0 if results['summary']['total_unmapped'] == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
