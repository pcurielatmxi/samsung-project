#!/usr/bin/env python3
"""
CSI Section Coverage Quality Check.

Validates CSI section assignments across all data sources and identifies:
1. CSI sections in quality data (RABA/PSI) but not in P6 schedule
2. CSI sections in P6 but not in quality data
3. Missing CSI assignments (null coverage)
4. Potential misclassifications (e.g., FIR code used for non-fireproofing work)

Usage:
    python -m scripts.integrated_analysis.data_quality.check_csi_coverage
    python -m scripts.integrated_analysis.data_quality.check_csi_coverage --verbose
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
class CSICoverageResult:
    """Results from CSI coverage check."""
    source_name: str
    total_records: int
    with_csi: int
    without_csi: int
    coverage_pct: float
    unique_csi_sections: Set[str]
    csi_distribution: Dict[str, int]
    issues: List[str] = field(default_factory=list)


@dataclass
class CSIComparisonResult:
    """Results from cross-source CSI comparison."""
    sections_in_quality_not_p6: Dict[str, Dict[str, int]]  # CSI -> {source: count}
    sections_in_p6_not_quality: Dict[str, int]  # CSI -> P6 count
    common_sections: Set[str]
    p6_misclassifications: List[Dict]  # Potential FIR/FST confusion


def load_source_data() -> Dict[str, pd.DataFrame]:
    """Load all data sources with CSI columns."""
    sources = {}
    processed = settings.PROCESSED_DATA_DIR

    # Define source paths and CSI column names
    source_configs = {
        'P6': (processed / "primavera" / "p6_task_taxonomy.csv", 'csi_section'),
        'RABA': (processed / "raba" / "raba_consolidated.csv", 'csi_section'),
        'PSI': (processed / "psi" / "psi_consolidated.csv", 'csi_section'),
        'ProjectSight': (processed / "projectsight" / "projectsight_with_csi.csv", 'csi_section'),
        'TBM': (processed / "tbm" / "tbm_with_csi.csv", 'csi_section'),
        'Yates_QC': (processed / "quality" / "yates_with_csi.csv", 'csi_section'),
        'SECAI_QC': (processed / "quality" / "secai_with_csi.csv", 'csi_section'),
        'NCR': (processed / "projectsight" / "ncr_with_csi.csv", 'csi_section'),
    }

    for name, (path, csi_col) in source_configs.items():
        if path.exists():
            try:
                df = pd.read_csv(path, low_memory=False)
                if csi_col in df.columns:
                    sources[name] = df
            except Exception as e:
                print(f"Warning: Could not load {name}: {e}")

    return sources


def check_source_coverage(name: str, df: pd.DataFrame, csi_col: str = 'csi_section') -> CSICoverageResult:
    """Check CSI coverage for a single data source."""
    total = len(df)
    with_csi = df[csi_col].notna().sum()
    without_csi = total - with_csi
    coverage = (with_csi / total * 100) if total > 0 else 0

    unique_sections = set(df[df[csi_col].notna()][csi_col].unique())
    distribution = df[df[csi_col].notna()][csi_col].value_counts().to_dict()

    issues = []
    if coverage < 80:
        issues.append(f"Low CSI coverage: {coverage:.1f}%")

    return CSICoverageResult(
        source_name=name,
        total_records=total,
        with_csi=with_csi,
        without_csi=without_csi,
        coverage_pct=coverage,
        unique_csi_sections=unique_sections,
        csi_distribution=distribution,
        issues=issues,
    )


def compare_csi_across_sources(
    sources: Dict[str, pd.DataFrame],
    dim_csi: pd.DataFrame,
) -> CSIComparisonResult:
    """Compare CSI sections across all sources."""

    # Get CSI sets for each source
    p6_csi = set()
    quality_csi = {}  # source -> csi set

    for name, df in sources.items():
        csi_set = set(df[df['csi_section'].notna()]['csi_section'].unique())
        if name == 'P6':
            p6_csi = csi_set
        elif name in ('RABA', 'PSI'):
            quality_csi[name] = csi_set

    # Find sections in quality but not P6
    all_quality_csi = set()
    for csi_set in quality_csi.values():
        all_quality_csi |= csi_set

    in_quality_not_p6 = all_quality_csi - p6_csi
    in_p6_not_quality = p6_csi - all_quality_csi
    common = p6_csi & all_quality_csi

    # Build detailed results
    sections_in_quality_not_p6 = {}
    for csi in in_quality_not_p6:
        sections_in_quality_not_p6[csi] = {}
        for name, df in sources.items():
            if name in ('RABA', 'PSI'):
                count = len(df[df['csi_section'] == csi])
                if count > 0:
                    sections_in_quality_not_p6[csi][name] = count

    sections_in_p6_not_quality = {}
    if 'P6' in sources:
        p6_df = sources['P6']
        for csi in in_p6_not_quality:
            sections_in_p6_not_quality[csi] = len(p6_df[p6_df['csi_section'] == csi])

    # Check for P6 misclassifications (FIR code issues)
    misclassifications = []
    if 'P6' in sources:
        p6_df = sources['P6']
        # Load task names
        task_path = settings.PRIMAVERA_PROCESSED_DIR / "task.csv"
        if task_path.exists():
            tasks = pd.read_csv(task_path, low_memory=False, usecols=['task_id', 'task_name'])
            merged = p6_df.merge(tasks, on='task_id', how='left')

            # Check FIR-coded tasks for firestopping keywords
            fir_tasks = merged[merged['sub_trade'] == 'FIR']
            fs_keywords = ['firestop', 'fire stop', 'fire-stop', 'penetration seal', 'fire caulk']
            sprinkler_keywords = ['sprinkler', 'fire suppression']
            alarm_keywords = ['fire alarm', 'fa ']

            for _, row in fir_tasks.iterrows():
                task_name = str(row.get('task_name', '')).lower()
                actual_csi = row.get('csi_section')

                # Check if it's actually firestopping
                if any(kw in task_name for kw in fs_keywords):
                    if actual_csi != '07 84 00':
                        misclassifications.append({
                            'task_id': row['task_id'],
                            'task_name': row.get('task_name', '')[:60],
                            'current_csi': actual_csi,
                            'suggested_csi': '07 84 00',
                            'reason': 'Firestopping task coded as FIR (Fireproofing)',
                        })

                # Check if it's actually fire suppression
                elif any(kw in task_name for kw in sprinkler_keywords):
                    if actual_csi != '21 10 00':
                        misclassifications.append({
                            'task_id': row['task_id'],
                            'task_name': row.get('task_name', '')[:60],
                            'current_csi': actual_csi,
                            'suggested_csi': '21 10 00',
                            'reason': 'Fire suppression task coded as FIR',
                        })

    return CSIComparisonResult(
        sections_in_quality_not_p6=sections_in_quality_not_p6,
        sections_in_p6_not_quality=sections_in_p6_not_quality,
        common_sections=common,
        p6_misclassifications=misclassifications[:100],  # Limit to 100
    )


def check_csi_coverage(verbose: bool = False) -> Dict:
    """
    Run full CSI coverage quality check.

    Returns dict with:
        - source_coverage: Coverage results per source
        - comparison: Cross-source comparison results
        - issues: List of issues found
        - recommendations: Suggested fixes
    """
    print("="*80)
    print("CSI SECTION COVERAGE QUALITY CHECK")
    print("="*80)

    # Load dimension table
    dim_csi_path = settings.PROCESSED_DATA_DIR / "integrated_analysis" / "dimensions" / "dim_csi_section.csv"
    dim_csi = pd.read_csv(dim_csi_path)
    valid_csi = set(dim_csi['csi_section'].values)

    print(f"\nReference: dim_csi_section has {len(valid_csi)} valid CSI sections")

    # Load all sources
    print("\nLoading data sources...")
    sources = load_source_data()
    print(f"Loaded {len(sources)} sources: {', '.join(sources.keys())}")

    # Check coverage per source
    print("\n" + "-"*80)
    print("COVERAGE BY SOURCE")
    print("-"*80)

    coverage_results = {}
    all_issues = []

    for name, df in sources.items():
        result = check_source_coverage(name, df)
        coverage_results[name] = result
        all_issues.extend(result.issues)

        status = "✓" if result.coverage_pct >= 80 else "⚠️"
        print(f"\n{status} {name}:")
        print(f"   Records: {result.total_records:,}")
        print(f"   With CSI: {result.with_csi:,} ({result.coverage_pct:.1f}%)")
        print(f"   Unique CSI sections: {len(result.unique_csi_sections)}")

        if verbose:
            print(f"   Top 5 CSI sections:")
            for csi, count in sorted(result.csi_distribution.items(), key=lambda x: -x[1])[:5]:
                title = dim_csi[dim_csi['csi_section']==csi]['csi_title'].values[0] if csi in valid_csi else "UNKNOWN"
                print(f"      {csi} {title}: {count:,}")

    # Cross-source comparison
    print("\n" + "-"*80)
    print("CROSS-SOURCE CSI COMPARISON")
    print("-"*80)

    comparison = compare_csi_across_sources(sources, dim_csi)

    print(f"\nCSI sections common to P6, RABA, and PSI: {len(comparison.common_sections)}")

    if comparison.sections_in_quality_not_p6:
        print(f"\n⚠️  CSI sections in quality data (RABA/PSI) but NOT in P6: {len(comparison.sections_in_quality_not_p6)}")
        print("   These quality inspections cannot be joined to P6 tasks by CSI:")
        for csi in sorted(comparison.sections_in_quality_not_p6.keys()):
            title = dim_csi[dim_csi['csi_section']==csi]['csi_title'].values[0] if csi in valid_csi else "UNKNOWN"
            counts = comparison.sections_in_quality_not_p6[csi]
            count_str = ", ".join(f"{k}: {v:,}" for k, v in counts.items())
            print(f"      {csi} {title} ({count_str})")
            all_issues.append(f"CSI {csi} in quality data but not P6")

    if comparison.sections_in_p6_not_quality:
        print(f"\nCSI sections in P6 but NOT in quality data: {len(comparison.sections_in_p6_not_quality)}")
        for csi, count in sorted(comparison.sections_in_p6_not_quality.items()):
            title = dim_csi[dim_csi['csi_section']==csi]['csi_title'].values[0] if csi in valid_csi else "UNKNOWN"
            print(f"      {csi} {title}: {count:,} P6 tasks")

    if comparison.p6_misclassifications:
        print(f"\n⚠️  Potential P6 CSI misclassifications: {len(comparison.p6_misclassifications)}")
        if verbose:
            for mc in comparison.p6_misclassifications[:10]:
                print(f"      {mc['task_name']}")
                print(f"         Current: {mc['current_csi']} → Suggested: {mc['suggested_csi']}")
                print(f"         Reason: {mc['reason']}")
        all_issues.append(f"{len(comparison.p6_misclassifications)} potential P6 misclassifications (FIR code)")

    # Generate recommendations
    recommendations = []
    if comparison.sections_in_quality_not_p6:
        if '07 84 00' in comparison.sections_in_quality_not_p6:
            recommendations.append(
                "CRITICAL: Add keyword-based Firestopping (07 84 00) detection to P6 CSI inference. "
                "Currently, FIR sub_trade maps to Fireproofing (07 81 00) even for firestopping tasks."
            )
        recommendations.append(
            f"Review {len(comparison.sections_in_quality_not_p6)} CSI sections in quality data "
            "that don't match P6 schedule - may need P6 taxonomy updates."
        )

    if comparison.p6_misclassifications:
        recommendations.append(
            "Update add_csi_to_p6_tasks.py to check task_name keywords before sub_trade mapping, "
            "similar to how RABA/PSI scripts work."
        )

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
        'source_coverage': coverage_results,
        'comparison': comparison,
        'issues': all_issues,
        'recommendations': recommendations,
    }


def main():
    parser = argparse.ArgumentParser(description='Check CSI section coverage across data sources')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show detailed output')
    args = parser.parse_args()

    check_csi_coverage(verbose=args.verbose)


if __name__ == "__main__":
    main()
