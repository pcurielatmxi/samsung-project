#!/usr/bin/env python3
"""
Company Dimension Coverage Quality Check.

Validates dim_company_id assignments across all data sources and identifies:
1. Records missing company dimension linkage
2. Company names not resolved to dim_company
3. Trade ID coverage for company records
4. Alias resolution effectiveness

Usage:
    python -m scripts.integrated_analysis.data_quality.check_company_coverage
    python -m scripts.integrated_analysis.data_quality.check_company_coverage --verbose
"""

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings


@dataclass
class CompanyCoverageResult:
    """Results from company coverage check for a single source."""
    source_name: str
    total_records: int
    with_company_id: int
    with_trade_id: int
    coverage_pct: float
    company_distribution: Dict[str, int]
    unresolved_companies: Set[str]
    issues: List[str] = field(default_factory=list)


def load_dim_company() -> pd.DataFrame:
    """Load the company dimension table."""
    path = settings.PROCESSED_DATA_DIR / "integrated_analysis" / "dimensions" / "dim_company.csv"
    return pd.read_csv(path)


def load_company_aliases() -> pd.DataFrame:
    """Load the company aliases mapping."""
    path = settings.PROCESSED_DATA_DIR / "integrated_analysis" / "mappings" / "map_company_aliases.csv"
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def check_source_company_coverage(
    name: str,
    df: pd.DataFrame,
    dim_company: pd.DataFrame,
    aliases: pd.DataFrame,
) -> CompanyCoverageResult:
    """Check company coverage for a single data source."""
    total = len(df)
    issues = []

    # Determine column names
    company_id_col = 'dim_company_id'
    company_name_cols = ['company', 'company_name', 'contractor', 'subcontractor']
    trade_id_col = 'dim_trade_id'

    # Find the company name column
    company_col = None
    for col in company_name_cols:
        if col in df.columns:
            company_col = col
            break

    # Check company ID coverage
    has_company_id = company_id_col in df.columns
    if has_company_id:
        with_company_id = df[company_id_col].notna().sum()
    else:
        with_company_id = 0

    # Check trade ID coverage
    has_trade_id = trade_id_col in df.columns
    if has_trade_id:
        with_trade_id = df[trade_id_col].notna().sum()
    else:
        with_trade_id = 0

    # Calculate coverage
    coverage_pct = (with_company_id / total * 100) if total > 0 else 0

    # Get company distribution
    company_dist = {}
    if company_col:
        company_dist = df[df[company_col].notna()][company_col].value_counts().head(20).to_dict()

    # Find unresolved companies
    unresolved = set()
    if company_col and has_company_id:
        # Records with company name but no company ID
        mask = df[company_col].notna() & df[company_id_col].isna()
        unresolved = set(df[mask][company_col].unique())

    if len(unresolved) > 0:
        issues.append(f"{len(unresolved)} company names not resolved to dim_company")

    if coverage_pct < 80 and total > 100:
        issues.append(f"Low company coverage: {coverage_pct:.1f}%")

    if has_trade_id and with_trade_id < with_company_id * 0.5:
        issues.append(f"Low trade coverage: {with_trade_id/total*100:.1f}%")

    return CompanyCoverageResult(
        source_name=name,
        total_records=total,
        with_company_id=with_company_id,
        with_trade_id=with_trade_id,
        coverage_pct=coverage_pct,
        company_distribution=company_dist,
        unresolved_companies=unresolved,
        issues=issues,
    )


def check_company_coverage(verbose: bool = False) -> Dict:
    """
    Run full company coverage quality check.

    Returns dict with:
        - dim_company_stats: Statistics about the company dimension
        - source_coverage: Coverage results per source
        - unresolved_companies: All unique unresolved company names
        - issues: List of issues found
        - recommendations: Suggested fixes
    """
    print("="*80)
    print("COMPANY DIMENSION COVERAGE QUALITY CHECK")
    print("="*80)

    # Load dimension tables
    dim_company = load_dim_company()
    aliases = load_company_aliases()

    print(f"\nReference: dim_company has {len(dim_company)} companies")
    if len(aliases) > 0:
        print(f"           map_company_aliases has {len(aliases)} alias mappings")

    # Analyze dim_company
    dim_stats = {
        'total_companies': len(dim_company),
        'with_trade': dim_company['dim_trade_id'].notna().sum() if 'dim_trade_id' in dim_company.columns else 0,
        'alias_count': len(aliases),
    }

    # Define sources to check
    processed = settings.PROCESSED_DATA_DIR
    source_configs = [
        ('RABA', processed / "raba" / "raba_consolidated.csv"),
        ('PSI', processed / "psi" / "psi_consolidated.csv"),
        ('TBM', processed / "tbm" / "work_entries_enriched.csv"),
        ('ProjectSight', processed / "projectsight" / "labor_entries_enriched.csv"),
        ('Weekly_Labor', processed / "weekly_reports" / "labor_detail_by_company_enriched.csv"),
        ('NCR', processed / "projectsight" / "ncr_consolidated.csv"),
    ]

    # Check coverage per source
    print("\n" + "-"*80)
    print("COVERAGE BY SOURCE")
    print("-"*80)

    coverage_results = {}
    all_issues = []
    all_unresolved = set()

    for name, path in source_configs:
        if not path.exists():
            print(f"\n⚠️  {name}: File not found")
            continue

        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception as e:
            print(f"\n⚠️  {name}: Error loading - {e}")
            continue

        result = check_source_company_coverage(name, df, dim_company, aliases)
        coverage_results[name] = result
        all_issues.extend(result.issues)
        all_unresolved |= result.unresolved_companies

        status = "✓" if result.coverage_pct >= 80 else "⚠️"
        print(f"\n{status} {name}:")
        print(f"   Records: {result.total_records:,}")
        print(f"   With dim_company_id: {result.with_company_id:,} ({result.coverage_pct:.1f}%)")
        print(f"   With dim_trade_id: {result.with_trade_id:,} ({result.with_trade_id/result.total_records*100:.1f}%)")

        if result.unresolved_companies and verbose:
            print(f"   Unresolved companies ({len(result.unresolved_companies)}):")
            for company in sorted(result.unresolved_companies)[:10]:
                print(f"      - {company}")

        if verbose and result.company_distribution:
            print(f"   Top companies:")
            for company, count in list(result.company_distribution.items())[:5]:
                print(f"      {company}: {count:,}")

    # Summary of unresolved companies
    if all_unresolved:
        print("\n" + "-"*80)
        print(f"ALL UNRESOLVED COMPANY NAMES ({len(all_unresolved)} unique)")
        print("-"*80)
        for company in sorted(all_unresolved)[:30]:
            print(f"  - {company}")
        if len(all_unresolved) > 30:
            print(f"  ... and {len(all_unresolved) - 30} more")

    # Generate recommendations
    recommendations = []
    if all_unresolved:
        recommendations.append(
            f"Add {len(all_unresolved)} unresolved company names to map_company_aliases.csv "
            "to improve company dimension coverage."
        )

    for name, result in coverage_results.items():
        if result.coverage_pct < 90:
            recommendations.append(f"Improve {name} company coverage ({result.coverage_pct:.1f}%)")

    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"\nIssues found: {len(all_issues)}")
    for issue in all_issues:
        print(f"  - {issue}")

    print(f"\nRecommendations: {len(recommendations)}")
    for i, rec in enumerate(recommendations, 1):
        print(f"  {i}. {rec}")

    return {
        'dim_company_stats': dim_stats,
        'source_coverage': coverage_results,
        'unresolved_companies': all_unresolved,
        'issues': all_issues,
        'recommendations': recommendations,
    }


def main():
    parser = argparse.ArgumentParser(description='Check company dimension coverage across data sources')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show detailed output')
    args = parser.parse_args()

    check_company_coverage(verbose=args.verbose)


if __name__ == "__main__":
    main()
