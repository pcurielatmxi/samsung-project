"""
Dimension Coverage Quality Check Module.

This module provides comprehensive validation of dimension table coverage
(dim_location, dim_company, dim_csi_section) across all fact tables.

Usage:
    python -m scripts.integrated_analysis.data_quality.dimension_coverage
    python -m scripts.integrated_analysis.data_quality.dimension_coverage --verbose
    python -m scripts.integrated_analysis.data_quality.dimension_coverage --output report.md

Programmatic usage:
    from scripts.integrated_analysis.data_quality.dimension_coverage import (
        check_dimension_coverage,
        load_and_analyze_sources,
    )

    results = check_dimension_coverage()
    coverage = results['coverage']  # Dict[str, SourceCoverage]

Module Structure:
    __init__.py      - Public API and main entry point
    __main__.py      - CLI entry point
    models.py        - Data classes (DimensionStats, SourceCoverage)
    loaders.py       - Dimension table and source data loaders
    analyzers.py     - Coverage calculation logic
    formatters.py    - Report formatting (console and markdown)
    config.py        - Source configurations
"""

from scripts.integrated_analysis.data_quality.dimension_coverage.models import (
    DimensionStats,
    SourceCoverage,
)
from scripts.integrated_analysis.data_quality.dimension_coverage.loaders import (
    load_dimensions,
    load_and_analyze_sources,
    get_location_type_lookup,
)
from scripts.integrated_analysis.data_quality.dimension_coverage.analyzers import (
    calculate_source_coverage,
    get_dimension_stats,
)
from scripts.integrated_analysis.data_quality.dimension_coverage.formatters import (
    print_full_report,
    generate_markdown_report,
)
from scripts.integrated_analysis.data_quality.dimension_coverage.config import (
    SOURCE_CONFIGS,
)

__all__ = [
    # Models
    'DimensionStats',
    'SourceCoverage',
    # Loaders
    'load_dimensions',
    'load_and_analyze_sources',
    'get_location_type_lookup',
    # Analyzers
    'calculate_source_coverage',
    'get_dimension_stats',
    # Formatters
    'print_full_report',
    'generate_markdown_report',
    # Config
    'SOURCE_CONFIGS',
    # Main function
    'check_dimension_coverage',
]


def check_dimension_coverage(verbose: bool = False, output_file: str = None) -> dict:
    """
    Run comprehensive dimension coverage check.

    This is the main entry point for the dimension coverage analysis.
    It loads all dimension tables and fact tables, calculates coverage
    metrics, and prints a formatted report.

    Args:
        verbose: Show detailed output (currently unused, reserved for future)
        output_file: Optional path to save markdown report

    Returns:
        Dict containing:
            - coverage: Dict[str, SourceCoverage] - Coverage metrics per source
            - dim_stats: Dict[str, DimensionStats] - Dimension table statistics
            - dim_location: pd.DataFrame - Location dimension table
            - dim_company: pd.DataFrame - Company dimension table
            - dim_csi: pd.DataFrame - CSI section dimension table

    Example:
        >>> results = check_dimension_coverage()
        >>> for name, cov in results['coverage'].items():
        ...     print(f"{name}: {cov.location_id_pct:.1f}% location coverage")
    """
    from datetime import datetime

    print("\n" + "=" * 100)
    print("DIMENSION COVERAGE QUALITY REPORT")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)

    # Load dimensions
    dim_location, dim_company, dim_csi = load_dimensions()
    dim_stats = get_dimension_stats(dim_location, dim_company, dim_csi)

    # Load and analyze sources
    print("\nLoading data sources...")
    coverage = load_and_analyze_sources()
    print(f"Loaded {len(coverage)} sources: {', '.join(coverage.keys())}")

    # Print full report
    print_full_report(coverage, dim_stats, dim_csi, dim_company)

    # Generate markdown report if requested
    if output_file:
        generate_markdown_report(coverage, dim_stats, dim_csi, output_file)
        print(f"\n📄 Report saved to: {output_file}")

    return {
        'coverage': coverage,
        'dim_stats': dim_stats,
        'dim_location': dim_location,
        'dim_company': dim_company,
        'dim_csi': dim_csi,
    }
