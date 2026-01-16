"""
Integration tests for output file schema validation.

These tests validate actual output CSV files against their schemas.
Tests are skipped if files don't exist (allows CI to run without data).

Run with: pytest tests/integration/test_output_schemas.py -v
"""

import pytest
from pathlib import Path
import pandas as pd

from src.config.settings import settings
from schemas.validator import validate_output_file, validate_dataframe
from schemas.registry import SCHEMA_REGISTRY, SOURCE_FILES, get_schema_for_file


def get_output_file_path(source_subdir: str, filename: str) -> Path:
    """Get the full path to an output file."""
    return settings.PROCESSED_DATA_DIR / source_subdir / filename


def file_exists(source_subdir: str, filename: str) -> bool:
    """Check if an output file exists."""
    return get_output_file_path(source_subdir, filename).exists()


class TestDimensionTableSchemas:
    """Test dimension table output files against schemas."""

    @pytest.fixture
    def dims_dir(self):
        return settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'dimensions'

    @pytest.mark.parametrize("filename", [
        'dim_location.csv',
        'dim_company.csv',
        'dim_trade.csv',
        'dim_csi_section.csv',
    ])
    def test_dimension_file_schema(self, dims_dir, filename):
        """Validate dimension files against their schemas."""
        file_path = dims_dir / filename
        if not file_path.exists():
            pytest.skip(f"File not found: {file_path}")

        schema = get_schema_for_file(filename)
        assert schema is not None, f"No schema registered for {filename}"

        errors = validate_output_file(file_path, schema)
        assert len(errors) == 0, f"Schema validation errors for {filename}: {errors}"


class TestMappingTableSchemas:
    """Test mapping table output files against schemas."""

    @pytest.fixture
    def maps_dir(self):
        return settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'mappings'

    @pytest.mark.parametrize("filename", [
        'map_company_aliases.csv',
        'map_company_location.csv',
        'map_projectsight_trade.csv',
    ])
    def test_mapping_file_schema(self, maps_dir, filename):
        """Validate mapping files against their schemas."""
        file_path = maps_dir / filename
        if not file_path.exists():
            pytest.skip(f"File not found: {file_path}")

        schema = get_schema_for_file(filename)
        assert schema is not None, f"No schema registered for {filename}"

        errors = validate_output_file(file_path, schema)
        assert len(errors) == 0, f"Schema validation errors for {filename}: {errors}"


class TestQualityDataSchemas:
    """Test quality data output files against schemas."""

    @pytest.mark.parametrize("source,filename", [
        ('raba', 'raba_consolidated.csv'),
        ('psi', 'psi_consolidated.csv'),
    ])
    def test_quality_file_schema(self, source, filename):
        """Validate quality files against their schemas."""
        file_path = settings.PROCESSED_DATA_DIR / source / filename
        if not file_path.exists():
            pytest.skip(f"File not found: {file_path}")

        schema = get_schema_for_file(filename)
        assert schema is not None, f"No schema registered for {filename}"

        errors = validate_output_file(file_path, schema)
        assert len(errors) == 0, f"Schema validation errors for {filename}: {errors}"


class TestTbmDataSchemas:
    """Test TBM output files against schemas."""

    @pytest.fixture
    def tbm_dir(self):
        return settings.TBM_PROCESSED_DIR

    @pytest.mark.parametrize("filename", [
        'tbm_files.csv',
        'work_entries.csv',
        'work_entries_enriched.csv',
    ])
    def test_tbm_file_schema(self, tbm_dir, filename):
        """Validate TBM files against their schemas."""
        file_path = tbm_dir / filename
        if not file_path.exists():
            pytest.skip(f"File not found: {file_path}")

        schema = get_schema_for_file(filename)
        assert schema is not None, f"No schema registered for {filename}"

        errors = validate_output_file(file_path, schema)
        assert len(errors) == 0, f"Schema validation errors for {filename}: {errors}"


class TestNcrDataSchemas:
    """Test NCR output files against schemas."""

    def test_ncr_consolidated_schema(self):
        """Validate NCR consolidated file against schema."""
        file_path = settings.PROJECTSIGHT_PROCESSED_DIR / 'ncr_consolidated.csv'
        if not file_path.exists():
            pytest.skip(f"File not found: {file_path}")

        schema = get_schema_for_file('ncr_consolidated.csv')
        assert schema is not None

        errors = validate_output_file(file_path, schema)
        assert len(errors) == 0, f"Schema validation errors: {errors}"


class TestWeeklyReportSchemas:
    """Test weekly report output files against schemas."""

    @pytest.fixture
    def wr_dir(self):
        return settings.WEEKLY_REPORTS_PROCESSED_DIR

    @pytest.mark.parametrize("filename", [
        'weekly_reports.csv',
        'key_issues.csv',
        'work_progressing.csv',
        'procurement.csv',
        'labor_detail.csv',
        'labor_detail_by_company.csv',
        'addendum_files.csv',
        'addendum_manpower.csv',
        'addendum_rfi_log.csv',
        'addendum_submittal_log.csv',
    ])
    def test_weekly_report_file_schema(self, wr_dir, filename):
        """Validate weekly report files against their schemas."""
        file_path = wr_dir / filename
        if not file_path.exists():
            pytest.skip(f"File not found: {file_path}")

        schema = get_schema_for_file(filename)
        assert schema is not None, f"No schema registered for {filename}"

        errors = validate_output_file(file_path, schema)
        assert len(errors) == 0, f"Schema validation errors for {filename}: {errors}"


class TestAllRegisteredSchemas:
    """Test all registered schemas against their files."""

    def test_all_registered_files_exist_or_skip(self):
        """
        For each registered schema, verify the file validates if it exists.
        This is a comprehensive test that catches any drift.
        """
        results = {}

        for source_subdir, files in SOURCE_FILES.items():
            for filename, schema in files.items():
                file_path = settings.PROCESSED_DATA_DIR / source_subdir / filename

                if not file_path.exists():
                    results[filename] = 'SKIPPED (file not found)'
                    continue

                errors = validate_output_file(file_path, schema)
                if errors:
                    results[filename] = f'FAILED: {errors}'
                else:
                    results[filename] = 'PASSED'

        # Report results
        failed = {k: v for k, v in results.items() if v.startswith('FAILED')}
        if failed:
            fail_msg = '\n'.join(f"  {k}: {v}" for k, v in failed.items())
            pytest.fail(f"Schema validation failures:\n{fail_msg}")


class TestSchemaColumnCoverage:
    """Test that schema columns match actual file columns."""

    @pytest.mark.parametrize("source,filename", [
        ('integrated_analysis/dimensions', 'dim_location.csv'),
        ('integrated_analysis/dimensions', 'dim_company.csv'),
        ('raba', 'raba_consolidated.csv'),
        ('tbm', 'work_entries.csv'),
    ])
    def test_schema_covers_all_columns(self, source, filename):
        """
        Check that schema defines all columns present in the file.

        Note: Extra columns in file are allowed (schema doesn't require
        them all), but this test warns if schema is incomplete.
        """
        file_path = settings.PROCESSED_DATA_DIR / source / filename
        if not file_path.exists():
            pytest.skip(f"File not found: {file_path}")

        schema = get_schema_for_file(filename)
        assert schema is not None

        df = pd.read_csv(file_path, nrows=1)
        actual_columns = set(df.columns)

        # Get schema columns, accounting for aliases
        schema_columns = set()
        for field_name, field_info in schema.model_fields.items():
            # Use alias if defined, otherwise use field name
            if hasattr(field_info, 'alias') and field_info.alias:
                schema_columns.add(field_info.alias)
            else:
                schema_columns.add(field_name)

        # Columns in file but not in schema
        extra_in_file = actual_columns - schema_columns
        if extra_in_file:
            # This is a warning, not a failure - schemas can be subset
            print(f"\nNote: {filename} has columns not in schema: {extra_in_file}")

        # Columns in schema but not in file (this IS a problem)
        missing_in_file = schema_columns - actual_columns
        if missing_in_file:
            pytest.fail(
                f"Schema requires columns not present in {filename}: {missing_in_file}"
            )
