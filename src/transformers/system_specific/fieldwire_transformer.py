"""Transformer for Fieldwire API data."""
from typing import Any, List, Dict
import logging
from datetime import datetime

from src.transformers.base_transformer import BaseTransformer

logger = logging.getLogger(__name__)


class FieldwireTransformer(BaseTransformer):
    """Transform Fieldwire API data into standardized format."""

    def __init__(self):
        """Initialize Fieldwire transformer."""
        super().__init__('fieldwire')

    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform Fieldwire API data.

        Tasks:
        - Normalize field names
        - Convert data types
        - Flatten nested structures if needed
        - Standardize dates
        - Handle null values consistently

        Args:
            data: Raw Fieldwire API response data

        Returns:
            Transformed data
        """
        self.logger.info(f'Transforming {len(data)} Fieldwire records...')

        transformed = []
        for record in data:
            try:
                transformed_record = self._transform_record(record)
                transformed.append(transformed_record)
            except Exception as e:
                self.logger.error(f'Failed to transform record {record}: {str(e)}')
                continue

        self.logger.info(f'Successfully transformed {len(transformed)} records')
        return transformed

    def _transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single Fieldwire record."""
        return {
            'source': 'fieldwire',
            'source_id': record.get('id'),
            'name': record.get('name'),
            'status': record.get('status'),
            'created_at': record.get('created_at'),
            'updated_at': record.get('updated_at'),
            'extracted_at': datetime.now().isoformat(),
            'raw_data': record,  # Keep raw data for debugging/auditing
        }

    def validate_transformation(self, data: List[Dict[str, Any]]) -> bool:
        """
        Validate transformed data has required fields.

        Args:
            data: Transformed data to validate

        Returns:
            True if all records are valid
        """
        required_fields = {'source', 'source_id', 'extracted_at'}

        for record in data:
            missing = required_fields - set(record.keys())
            if missing:
                self.logger.error(f'Record missing fields: {missing}')
                return False

        self.logger.info(f'Validated {len(data)} transformed records')
        return True
