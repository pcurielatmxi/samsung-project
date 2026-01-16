"""
Output data schemas for validation.

This module defines Pydantic models for all output CSV files to ensure
schema stability and prevent breaking changes to downstream consumers
(e.g., Power BI dashboards).

Usage:
    from schemas import validate_output_file
    from schemas.dimensions import DimLocation, DimCompany

    # Validate a file
    errors = validate_output_file('dim_location.csv', DimLocation)

    # Or use the registry
    from schemas import SCHEMA_REGISTRY
    schema = SCHEMA_REGISTRY['dim_location']
"""

from .validator import (
    validate_output_file,
    validate_dataframe,
    validated_df_to_csv,
    SchemaValidationError,
)
from .registry import SCHEMA_REGISTRY, get_schema_for_file

__all__ = [
    'validate_output_file',
    'validate_dataframe',
    'validated_df_to_csv',
    'SchemaValidationError',
    'SCHEMA_REGISTRY',
    'get_schema_for_file',
]
