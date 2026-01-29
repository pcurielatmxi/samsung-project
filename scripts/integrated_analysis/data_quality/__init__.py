"""
Data Quality Check Framework for Integrated Analysis.

Provides reusable quality checks for dimension coverage and consistency
across all data sources (P6, RABA, PSI, ProjectSight, TBM, etc.).

Available checks:
    - dimension_coverage: Comprehensive dimension coverage report (recommended)
    - check_csi_coverage: CSI section coverage only
    - check_location_coverage: Location dimension coverage only
    - check_company_coverage: Company dimension coverage only

Usage:
    # Run comprehensive dimension coverage report (recommended)
    python -m scripts.integrated_analysis.data_quality.dimension_coverage

    # Run all legacy checks
    python -m scripts.integrated_analysis.data_quality

    # Run individual legacy checks
    python -m scripts.integrated_analysis.data_quality.check_csi_coverage
"""

from .run_all_checks import run_all_checks
from .check_csi_coverage import check_csi_coverage
from .check_location_coverage import check_location_coverage
from .check_company_coverage import check_company_coverage

# Import from new modular dimension_coverage package
from .dimension_coverage import check_dimension_coverage

__all__ = [
    'run_all_checks',
    'check_csi_coverage',
    'check_location_coverage',
    'check_company_coverage',
    'check_dimension_coverage',
]
