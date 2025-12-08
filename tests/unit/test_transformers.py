"""Unit tests for transformers."""
import pytest
from src.transformers.system_specific.fieldwire_transformer import FieldwireTransformer


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
