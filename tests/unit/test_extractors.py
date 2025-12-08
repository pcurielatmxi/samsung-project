"""Unit tests for extractors."""
import pytest
from src.extractors.base_extractor import BaseExtractor
from src.extractors.system_specific.fieldwire_extractor import FieldwireExtractor


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
