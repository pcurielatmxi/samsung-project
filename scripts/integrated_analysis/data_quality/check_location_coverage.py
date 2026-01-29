#!/usr/bin/env python3
"""
Location Dimension Coverage Quality Check.

Validates dim_location_id assignments across all data sources and identifies:
1. Records missing location dimension linkage
2. Location codes not in dim_location
3. Grid coordinate coverage for spatial joins
4. Building/level coverage breakdown

Usage:
    python -m scripts.integrated_analysis.data_quality.check_location_coverage
    python -m scripts.integrated_analysis.data_quality.check_location_coverage --verbose
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
class LocationCoverageResult:
    """Results from location coverage check for a single source."""
    source_name: str
    total_records: int
    with_location_id: int
    with_building: int
    with_level: int
    with_grid: int
    coverage_pct: float
    building_distribution: Dict[str, int]
    level_distribution: Dict[str, int]
    issues: List[str] = field(default_factory=list)


def load_dim_location() -> pd.DataFrame:
    """Load the location dimension table."""
    path = settings.PROCESSED_DATA_DIR / "integrated_analysis" / "dimensions" / "dim_location.csv"
    return pd.read_csv(path)


def check_source_location_coverage(
    name: str,
    df: pd.DataFrame,
    dim_location: pd.DataFrame,
) -> LocationCoverageResult:
    """Check location coverage for a single data source."""
    total = len(df)
    issues = []

    # Check for dim_location_id column
    location_id_col = 'dim_location_id'
    has_location_id = location_id_col in df.columns

    if has_location_id:
        with_location_id = df[location_id_col].notna().sum()
    else:
        with_location_id = 0
        issues.append(f"No {location_id_col} column")

    # Check for building column
    building_col = 'building'
    if building_col in df.columns:
        with_building = df[building_col].notna().sum()
        building_dist = df[df[building_col].notna()][building_col].value_counts().to_dict()
    else:
        with_building = 0
        building_dist = {}

    # Check for level column
    level_col = 'level' if 'level' in df.columns else 'building_level' if 'building_level' in df.columns else None
    if level_col:
        with_level = df[level_col].notna().sum()
        level_dist = df[df[level_col].notna()][level_col].value_counts().to_dict()
    else:
        with_level = 0
        level_dist = {}

    # Check for grid coordinates (for quality data)
    grid_cols = ['grid_row_min', 'grid_col_min']
    has_grid = all(col in df.columns for col in grid_cols)
    if has_grid:
        with_grid = df[grid_cols[0]].notna().sum()
    else:
        with_grid = 0

    # Calculate coverage percentage (based on location_id or building)
    if has_location_id:
        coverage_pct = (with_location_id / total * 100) if total > 0 else 0
    else:
        coverage_pct = (with_building / total * 100) if total > 0 else 0

    # Check for location codes not in dim_location
    location_code_col = 'location_code'
    if location_code_col in df.columns:
        valid_codes = set(dim_location['location_code'].values)
        source_codes = set(df[df[location_code_col].notna()][location_code_col].unique())
        invalid_codes = source_codes - valid_codes
        if invalid_codes and len(invalid_codes) <= 20:
            issues.append(f"Location codes not in dim_location: {invalid_codes}")
        elif invalid_codes:
            issues.append(f"{len(invalid_codes)} location codes not in dim_location")

    if coverage_pct < 80:
        issues.append(f"Low location coverage: {coverage_pct:.1f}%")

    return LocationCoverageResult(
        source_name=name,
        total_records=total,
        with_location_id=with_location_id,
        with_building=with_building,
        with_level=with_level,
        with_grid=with_grid,
        coverage_pct=coverage_pct,
        building_distribution=building_dist,
        level_distribution=level_dist,
        issues=issues,
    )


def check_location_coverage(verbose: bool = False) -> Dict:
    """
    Run full location coverage quality check.

    Returns dict with:
        - dim_location_stats: Statistics about the location dimension
        - source_coverage: Coverage results per source
        - issues: List of issues found
        - recommendations: Suggested fixes
    """
    print("="*80)
    print("LOCATION DIMENSION COVERAGE QUALITY CHECK")
    print("="*80)

    # Load dimension table
    dim_location = load_dim_location()

    print(f"\nReference: dim_location has {len(dim_location)} location codes")

    # Analyze dim_location
    dim_stats = {
        'total_locations': len(dim_location),
        'with_grid': dim_location['grid_row_min'].notna().sum(),
        'by_type': dim_location['location_type'].value_counts().to_dict(),
        'by_building': dim_location['building'].value_counts().to_dict(),
    }

    print("\nLocation dimension breakdown:")
    print(f"  Total locations: {dim_stats['total_locations']}")
    print(f"  With grid coordinates: {dim_stats['with_grid']} ({dim_stats['with_grid']/dim_stats['total_locations']*100:.1f}%)")
    print(f"  By type: {dict(sorted(dim_stats['by_type'].items(), key=lambda x: -x[1]))}")

    # Define sources to check
    processed = settings.PROCESSED_DATA_DIR
    source_configs = [
        ('P6', processed / "primavera" / "p6_task_taxonomy.csv"),
        ('RABA', processed / "raba" / "raba_consolidated.csv"),
        ('PSI', processed / "psi" / "psi_consolidated.csv"),
        ('TBM', processed / "tbm" / "work_entries_enriched.csv"),
        ('ProjectSight', processed / "projectsight" / "labor_entries_enriched.csv"),
    ]

    # Check coverage per source
    print("\n" + "-"*80)
    print("COVERAGE BY SOURCE")
    print("-"*80)

    coverage_results = {}
    all_issues = []

    for name, path in source_configs:
        if not path.exists():
            print(f"\n⚠️  {name}: File not found")
            continue

        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception as e:
            print(f"\n⚠️  {name}: Error loading - {e}")
            continue

        result = check_source_location_coverage(name, df, dim_location)
        coverage_results[name] = result
        all_issues.extend(result.issues)

        status = "✓" if result.coverage_pct >= 80 else "⚠️"
        print(f"\n{status} {name}:")
        print(f"   Records: {result.total_records:,}")
        print(f"   With dim_location_id: {result.with_location_id:,} ({result.with_location_id/result.total_records*100:.1f}%)")
        print(f"   With building: {result.with_building:,}")
        print(f"   With level: {result.with_level:,}")
        if result.with_grid > 0:
            print(f"   With grid coordinates: {result.with_grid:,} ({result.with_grid/result.total_records*100:.1f}%)")

        if verbose and result.building_distribution:
            print(f"   Building distribution:")
            for bldg, count in sorted(result.building_distribution.items(), key=lambda x: -x[1])[:5]:
                print(f"      {bldg}: {count:,}")

    # Generate recommendations
    recommendations = []
    for name, result in coverage_results.items():
        if result.coverage_pct < 90:
            recommendations.append(f"Improve {name} location coverage ({result.coverage_pct:.1f}%)")
        if result.with_grid > 0 and result.with_grid < result.total_records * 0.5:
            recommendations.append(
                f"Add grid coordinates to more {name} records for spatial joins "
                f"({result.with_grid}/{result.total_records})"
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
        'dim_location_stats': dim_stats,
        'source_coverage': coverage_results,
        'issues': all_issues,
        'recommendations': recommendations,
    }


def main():
    parser = argparse.ArgumentParser(description='Check location dimension coverage across data sources')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show detailed output')
    args = parser.parse_args()

    check_location_coverage(verbose=args.verbose)


if __name__ == "__main__":
    main()
