"""Unit tests for transformers."""
import pytest
from src.transformers.system_specific.fieldwire_transformer import FieldwireTransformer
from src.transformers.system_specific.projectsight_transformer import ProjectSightTransformer


class TestProjectSightTransformer:
    """Test ProjectSight transformer."""

    def test_transformer_initialization(self):
        """Test transformer initialization."""
        transformer = ProjectSightTransformer()
        assert transformer.name == 'projectsight'

    def test_transform_record(self):
        """Test transforming a single record."""
        transformer = ProjectSightTransformer()
        raw_data = [
            {
                'project_id': '1',
                'project_name': 'Test Project',
                'status': 'active',
                'start_date': '2025-01-01',
            }
        ]
        transformed = transformer.transform(raw_data)
        assert len(transformed) == 1
        assert transformed[0]['source'] == 'projectsight'
        assert transformed[0]['source_id'] == '1'

    def test_validate_transformation_valid(self):
        """Test validation of transformed data."""
        transformer = ProjectSightTransformer()
        valid_data = [
            {
                'source': 'projectsight',
                'source_id': '1',
                'name': 'Test',
                'status': 'active',
            }
        ]
        assert transformer.validate_transformation(valid_data) is True

    def test_validate_transformation_invalid(self):
        """Test validation fails with invalid data."""
        transformer = ProjectSightTransformer()
        invalid_data = [{'source': 'projectsight'}]  # Missing required fields
        assert transformer.validate_transformation(invalid_data) is False


class TestFieldwireTransformer:
    """Test Fieldwire transformer."""

    def test_transformer_initialization(self):
        """Test transformer initialization."""
        transformer = FieldwireTransformer()
        assert transformer.name == 'fieldwire'

    def test_transform_record(self):
        """Test transforming a single record."""
        transformer = FieldwireTransformer()
        raw_data = [
            {
                'id': '123',
                'name': 'Test Task',
                'status': 'open',
            }
        ]
        transformed = transformer.transform(raw_data)
        assert len(transformed) == 1
        assert transformed[0]['source'] == 'fieldwire'
        assert transformed[0]['source_id'] == '123'
