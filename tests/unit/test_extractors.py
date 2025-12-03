"""Unit tests for extractors."""
import pytest
from src.extractors.base_extractor import BaseExtractor
from src.extractors.system_specific.fieldwire_extractor import FieldwireExtractor
from src.extractors.system_specific.projectsight_extractor import ProjectSightExtractor


class TestBaseExtractor:
    """Test base extractor functionality."""

    def test_metadata(self):
        """Test extraction metadata."""
        # Note: BaseExtractor is abstract, so we can't instantiate directly
        # This test documents the interface
        pass


class TestFieldwireExtractor:
    """Test Fieldwire extractor."""

    def test_extractor_initialization(self):
        """Test Fieldwire extractor can be initialized."""
        extractor = FieldwireExtractor()
        assert extractor.name == 'fieldwire'
        assert extractor.connector is not None

    def test_validate_extraction_missing_id(self, sample_extract_data):
        """Test validation fails when id field is missing."""
        invalid_data = [{'name': 'Test'}]
        result = extractor.validate_extraction(invalid_data)
        assert result is False


class TestProjectSightExtractor:
    """Test ProjectSight extractor."""

    def test_extractor_initialization(self):
        """Test ProjectSight extractor can be initialized."""
        extractor = ProjectSightExtractor()
        assert extractor.name == 'projectsight'
        assert extractor.connector is not None

    def test_validate_extraction_missing_fields(self):
        """Test validation fails when required fields are missing."""
        extractor = ProjectSightExtractor()
        invalid_data = [{'project_id': '1'}]  # Missing project_name, status
        result = extractor.validate_extraction(invalid_data)
        assert result is False

    def test_validate_extraction_valid(self):
        """Test validation passes with valid data."""
        extractor = ProjectSightExtractor()
        valid_data = [
            {
                'project_id': '1',
                'project_name': 'Test',
                'status': 'active',
            }
        ]
        result = extractor.validate_extraction(valid_data)
        assert result is True
