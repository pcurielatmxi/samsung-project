"""
Data Quality Check Framework for Integrated Analysis.

Provides dimension coverage analysis across all data sources
(P6, RABA, PSI, ProjectSight, TBM, NCR).

Usage:
    python -m scripts.integrated_analysis.data_quality.dimension_coverage

The report includes:
    - Coverage matrix (Source x Dimension)
    - Location granularity breakdown
    - CSI section joinability analysis
    - Unresolved company names
    - Actionable recommendations
"""

from .dimension_coverage import check_dimension_coverage

__all__ = [
    'check_dimension_coverage',
]
