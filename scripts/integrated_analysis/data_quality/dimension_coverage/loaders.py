"""
Data loaders for dimension coverage analysis.

This module handles loading dimension tables and fact tables from disk.
It provides caching to avoid redundant file reads.
"""

import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

# Add project root to path
_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings
from scripts.integrated_analysis.data_quality.dimension_coverage.config import (
    SOURCE_CONFIGS,
)
from scripts.integrated_analysis.data_quality.dimension_coverage.models import (
    SourceCoverage,
)
from scripts.integrated_analysis.data_quality.dimension_coverage.analyzers import (
    calculate_source_coverage,
)


# =============================================================================
# Dimension Table Cache
# =============================================================================

_dim_location_df: Optional[pd.DataFrame] = None
_dim_company_df: Optional[pd.DataFrame] = None
_dim_csi_df: Optional[pd.DataFrame] = None
_location_type_lookup: Optional[Dict[int, str]] = None


def load_dimensions() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load all dimension tables.

    Caches the results for subsequent calls within the same session.

    Returns:
        Tuple of (dim_location, dim_company, dim_csi) DataFrames

    Raises:
        FileNotFoundError: If any dimension table is missing
    """
    global _dim_location_df, _dim_company_df, _dim_csi_df

    dim_path = settings.PROCESSED_DATA_DIR / "integrated_analysis" / "dimensions"

    if _dim_location_df is None:
        _dim_location_df = pd.read_csv(dim_path / "dim_location.csv")

    if _dim_company_df is None:
        _dim_company_df = pd.read_csv(dim_path / "dim_company.csv")

    if _dim_csi_df is None:
        _dim_csi_df = pd.read_csv(dim_path / "dim_csi_section.csv")

    return _dim_location_df, _dim_company_df, _dim_csi_df


def get_location_type_lookup() -> Dict[int, str]:
    """
    Get location_id -> location_type mapping.

    This is used when a fact table has dim_location_id but not
    location_type. We look up the type from the dimension table.

    Returns:
        Dict mapping location_id (int) to location_type (str)
    """
    global _location_type_lookup

    if _location_type_lookup is None:
        dim_location, _, _ = load_dimensions()
        _location_type_lookup = dict(
            zip(dim_location['location_id'], dim_location['location_type'])
        )

    return _location_type_lookup


def reset_cache() -> None:
    """
    Clear all cached data.

    Call this if you need to reload dimension tables after they've changed.
    """
    global _dim_location_df, _dim_company_df, _dim_csi_df, _location_type_lookup

    _dim_location_df = None
    _dim_company_df = None
    _dim_csi_df = None
    _location_type_lookup = None


# =============================================================================
# Source Data Loaders
# =============================================================================

def load_source(name: str) -> Optional[pd.DataFrame]:
    """
    Load a single source file.

    Args:
        name: Source name (must be key in SOURCE_CONFIGS)

    Returns:
        DataFrame or None if file not found
    """
    if name not in SOURCE_CONFIGS:
        raise ValueError(f"Unknown source: {name}. Valid sources: {list(SOURCE_CONFIGS.keys())}")

    config = SOURCE_CONFIGS[name]
    path = settings.PROCESSED_DATA_DIR / config['path']

    if not path.exists():
        return None

    return pd.read_csv(path, low_memory=False)


def load_and_analyze_sources() -> Dict[str, SourceCoverage]:
    """
    Load all configured sources and calculate coverage metrics.

    Iterates through SOURCE_CONFIGS, loads each file, and calculates
    dimension coverage metrics.

    Returns:
        Dict mapping source name to SourceCoverage object

    Note:
        Sources that don't exist on disk are silently skipped.
        Errors during loading are printed as warnings.
    """
    results = {}

    for name, config in SOURCE_CONFIGS.items():
        path = settings.PROCESSED_DATA_DIR / config['path']

        if not path.exists():
            continue

        try:
            df = pd.read_csv(path, low_memory=False)
            results[name] = calculate_source_coverage(name, df, config)
        except Exception as e:
            print(f"Warning: Could not load {name}: {e}")

    return results
