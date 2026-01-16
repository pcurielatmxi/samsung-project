"""
Unit tests for schema definitions.

Tests schema structure and validation logic without requiring actual data files.
"""

import pytest
from pydantic import BaseModel

from schemas.dimensions import DimLocation, DimCompany, DimTrade, DimCSISection
from schemas.mappings import MapCompanyAliases, MapCompanyLocation, MapProjectSightTrade
from schemas.quality import QCInspectionConsolidated
from schemas.tbm import TbmFiles, TbmWorkEntries, TbmWorkEntriesEnriched
from schemas.ncr import NcrConsolidated
from schemas.weekly_reports import WeeklyReports, KeyIssues, LaborDetail
from schemas.registry import SCHEMA_REGISTRY, get_schema_for_file, list_registered_files
from schemas.validator import (
    validate_dataframe,
    pandas_dtype_to_python_type,
    types_compatible,
    validate_schema_compatibility,
)


class TestSchemaDefinitions:
    """Test that schema definitions are valid Pydantic models."""

    @pytest.mark.parametrize("schema", [
        DimLocation,
        DimCompany,
        DimTrade,
        DimCSISection,
        MapCompanyAliases,
        MapCompanyLocation,
        MapProjectSightTrade,
        QCInspectionConsolidated,
        TbmFiles,
        TbmWorkEntries,
        TbmWorkEntriesEnriched,
        NcrConsolidated,
        WeeklyReports,
        KeyIssues,
        LaborDetail,
    ])
    def test_schema_is_pydantic_model(self, schema):
        """Each schema should be a valid Pydantic BaseModel."""
        assert issubclass(schema, BaseModel)

    @pytest.mark.parametrize("schema", [
        DimLocation,
        DimCompany,
        DimTrade,
        DimCSISection,
    ])
    def test_dimension_schema_has_primary_key(self, schema):
        """Dimension tables should have an ID column."""
        fields = schema.model_fields
        # Check for *_id field
        id_fields = [f for f in fields if f.endswith('_id')]
        assert len(id_fields) > 0, f"{schema.__name__} should have an ID field"


class TestSchemaRegistry:
    """Test schema registry functionality."""

    def test_registry_not_empty(self):
        """Registry should have registered schemas."""
        assert len(SCHEMA_REGISTRY) > 0

    def test_all_registered_schemas_are_valid(self):
        """All registered schemas should be Pydantic models."""
        for filename, schema in SCHEMA_REGISTRY.items():
            assert issubclass(schema, BaseModel), f"Schema for {filename} is not a Pydantic model"

    def test_get_schema_for_file_found(self):
        """Should return schema for registered file."""
        schema = get_schema_for_file('dim_location.csv')
        assert schema == DimLocation

    def test_get_schema_for_file_with_path(self):
        """Should work with full path."""
        schema = get_schema_for_file('/some/path/to/dim_location.csv')
        assert schema == DimLocation

    def test_get_schema_for_file_not_found(self):
        """Should return None for unregistered file."""
        schema = get_schema_for_file('unknown_file.csv')
        assert schema is None

    def test_list_registered_files(self):
        """Should list all registered files."""
        files = list_registered_files()
        assert 'dim_location.csv' in files
        assert 'raba_consolidated.csv' in files
        assert 'work_entries.csv' in files


class TestTypeMapping:
    """Test pandas to Python type conversion."""

    def test_pandas_int_type(self):
        """Int types should map to 'int'."""
        assert pandas_dtype_to_python_type('int64') == 'int'
        assert pandas_dtype_to_python_type('int32') == 'int'

    def test_pandas_float_type(self):
        """Float types should map to 'float'."""
        assert pandas_dtype_to_python_type('float64') == 'float'
        assert pandas_dtype_to_python_type('float32') == 'float'

    def test_pandas_object_type(self):
        """Object type should map to 'str'."""
        assert pandas_dtype_to_python_type('object') == 'str'

    def test_types_compatible_exact_match(self):
        """Exact type matches should be compatible."""
        assert types_compatible('int', 'int')
        assert types_compatible('str', 'str')
        assert types_compatible('float', 'float')

    def test_types_compatible_float_for_int(self):
        """Float in pandas can represent nullable int."""
        assert types_compatible('float', 'int')

    def test_types_compatible_numeric_interop(self):
        """Numeric types should be interoperable."""
        assert types_compatible('int', 'float')
        assert types_compatible('float', 'int')


class TestSchemaCompatibility:
    """Test backward compatibility checking."""

    def test_identical_schemas_compatible(self):
        """Identical schemas should be compatible."""
        errors = validate_schema_compatibility(DimLocation, DimLocation)
        assert len(errors) == 0

    def test_detect_removed_column(self):
        """Should detect removed columns."""
        # Create a "new" schema missing a column
        class NewDimLocation(BaseModel):
            location_id: int
            # Missing location_code

        errors = validate_schema_compatibility(DimLocation, NewDimLocation)
        assert len(errors) > 0
        assert any('Removed columns' in e for e in errors)

    def test_added_column_allowed(self):
        """Adding columns should be allowed."""
        # Create a "new" schema with extra column
        class NewDimLocation(DimLocation):
            new_column: str

        errors = validate_schema_compatibility(DimLocation, NewDimLocation)
        # No forbidden errors (added columns are allowed)
        forbidden_errors = [e for e in errors if 'FORBIDDEN' in e]
        assert len(forbidden_errors) == 0


class TestDataFrameValidation:
    """Test DataFrame validation against schemas."""

    def test_validate_empty_dataframe(self):
        """Empty dataframe should pass if columns match."""
        import pandas as pd

        df = pd.DataFrame(columns=['location_id', 'location_code', 'location_type',
                                    'room_name', 'building', 'level', 'grid_row_min',
                                    'grid_row_max', 'grid_col_min', 'grid_col_max',
                                    'status', 'task_count', 'building_level'])

        # Add dtypes that match
        df = df.astype({
            'location_id': 'int64',
            'location_code': 'object',
            'location_type': 'object',
            'building': 'object',
        })

        errors = validate_dataframe(df, DimLocation)
        # Should not have missing column errors
        missing_errors = [e for e in errors if 'Missing required' in e]
        assert len(missing_errors) == 0

    def test_validate_missing_columns(self):
        """Should detect missing required columns."""
        import pandas as pd

        df = pd.DataFrame({'location_id': [1, 2]})
        errors = validate_dataframe(df, DimLocation)

        assert len(errors) > 0
        assert any('Missing required columns' in e for e in errors)

    def test_validate_strict_mode_extra_columns(self):
        """Strict mode should flag extra columns."""
        import pandas as pd

        # Create minimal valid df with extra column
        df = pd.DataFrame({
            'company_id': [1],
            'alias': ['test'],
            'source': ['manual'],
            'extra_column': ['value'],
        })

        errors = validate_dataframe(df, MapCompanyAliases, strict=True)
        assert any('Unexpected columns' in e for e in errors)

    def test_validate_non_strict_allows_extra(self):
        """Non-strict mode should allow extra columns."""
        import pandas as pd

        df = pd.DataFrame({
            'company_id': [1],
            'alias': ['test'],
            'source': ['manual'],
            'extra_column': ['value'],
        })

        errors = validate_dataframe(df, MapCompanyAliases, strict=False)
        # Should not have unexpected column errors
        extra_errors = [e for e in errors if 'Unexpected columns' in e]
        assert len(extra_errors) == 0
