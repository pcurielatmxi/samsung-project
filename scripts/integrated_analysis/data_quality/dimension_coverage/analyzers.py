"""
Coverage analysis logic for dimension coverage checks.

This module contains the core analysis functions that calculate
coverage metrics for each source against dimension tables.
"""

from typing import Dict, Any

import pandas as pd

from scripts.integrated_analysis.data_quality.dimension_coverage.models import (
    DimensionStats,
    SourceCoverage,
)


def get_dimension_stats(
    dim_location: pd.DataFrame,
    dim_company: pd.DataFrame,
    dim_csi: pd.DataFrame,
) -> Dict[str, DimensionStats]:
    """
    Calculate statistics for each dimension table.

    Args:
        dim_location: Location dimension DataFrame
        dim_company: Company dimension DataFrame
        dim_csi: CSI section dimension DataFrame

    Returns:
        Dict with keys 'location', 'company', 'csi' containing DimensionStats
    """
    return {
        'location': DimensionStats(
            name='dim_location',
            total_records=len(dim_location),
            breakdown=dim_location['location_type'].value_counts().to_dict(),
        ),
        'company': DimensionStats(
            name='dim_company',
            total_records=len(dim_company),
            breakdown=(
                dim_company['tier'].value_counts().to_dict()
                if 'tier' in dim_company.columns
                else {}
            ),
        ),
        'csi': DimensionStats(
            name='dim_csi_section',
            total_records=len(dim_csi),
            breakdown=(
                dim_csi['csi_division'].value_counts().to_dict()
                if 'csi_division' in dim_csi.columns
                else {}
            ),
        ),
    }


def calculate_source_coverage(
    name: str,
    df: pd.DataFrame,
    config: Dict[str, Any],
) -> SourceCoverage:
    """
    Calculate dimension coverage metrics for a single source.

    This analyzes a fact table to determine:
    - What percentage of rows have dimension IDs populated
    - Distribution of location types (granularity)
    - Which company names couldn't be resolved
    - CSI section distribution

    Args:
        name: Source name (e.g., 'RABA', 'PSI')
        df: Source DataFrame
        config: Source configuration from SOURCE_CONFIGS

    Returns:
        SourceCoverage with all metrics populated
    """
    # Import here to avoid circular dependency
    from scripts.integrated_analysis.data_quality.dimension_coverage.loaders import (
        get_location_type_lookup,
    )

    total = len(df)

    # -------------------------------------------------------------------------
    # Location Coverage
    # -------------------------------------------------------------------------
    loc_id_col = config['location_id_col']
    loc_type_col = config['location_type_col']

    loc_count = 0
    if loc_id_col and loc_id_col in df.columns:
        loc_count = df[loc_id_col].notna().sum()

    loc_type_dist = {}

    # Try to get location_type from the source itself
    if loc_type_col and loc_type_col in df.columns:
        loc_type_dist = (
            df[df[loc_type_col].notna()][loc_type_col]
            .value_counts()
            .to_dict()
        )
    # Otherwise lookup from dim_location via dim_location_id
    elif loc_id_col and loc_id_col in df.columns and loc_count > 0:
        lookup = get_location_type_lookup()
        loc_types = (
            df[df[loc_id_col].notna()][loc_id_col]
            .astype(int)
            .map(lookup)
        )
        loc_type_dist = loc_types.value_counts().to_dict()

    # -------------------------------------------------------------------------
    # Company Coverage
    # -------------------------------------------------------------------------
    comp_id_col = config['company_id_col']
    comp_name_col = config['company_name_col']

    comp_count = 0
    if comp_id_col and comp_id_col in df.columns:
        comp_count = df[comp_id_col].notna().sum()

    unresolved = set()
    if (
        comp_name_col
        and comp_id_col
        and comp_name_col in df.columns
        and comp_id_col in df.columns
    ):
        # Find rows with company name but no company ID
        mask = df[comp_name_col].notna() & df[comp_id_col].isna()
        unresolved = set(df[mask][comp_name_col].unique())

    # -------------------------------------------------------------------------
    # CSI Coverage
    # -------------------------------------------------------------------------
    csi_col = config['csi_col']

    csi_count = 0
    csi_dist = {}

    if csi_col and csi_col in df.columns:
        csi_count = df[csi_col].notna().sum()
        csi_dist = (
            df[df[csi_col].notna()][csi_col]
            .value_counts()
            .to_dict()
        )

    # -------------------------------------------------------------------------
    # Build Result
    # -------------------------------------------------------------------------
    return SourceCoverage(
        source_name=name,
        total_records=total,
        location_id_count=loc_count,
        location_id_pct=(loc_count / total * 100) if total > 0 else 0,
        location_type_distribution=loc_type_dist,
        company_id_count=comp_count,
        company_id_pct=(comp_count / total * 100) if total > 0 else 0,
        unresolved_companies=unresolved,
        csi_section_count=csi_count,
        csi_section_pct=(csi_count / total * 100) if total > 0 else 0,
        csi_distribution=csi_dist,
    )
