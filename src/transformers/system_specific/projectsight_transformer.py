"""Transformer for ProjectSight extracted data."""
from typing import Any, List, Dict
import logging
from datetime import datetime

from src.transformers.base_transformer import BaseTransformer

logger = logging.getLogger(__name__)


class ProjectSightTransformer(BaseTransformer):
    """Transform ProjectSight data into standardized format."""

    def __init__(self):
        """Initialize ProjectSight transformer."""
        super().__init__('projectsight')

    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform ProjectSight data.

        Tasks:
        - Normalize field names
        - Convert data types
        - Handle missing values
        - Standardize dates
        - Calculate derived fields

        Args:
            data: Raw ProjectSight project data

        Returns:
            Transformed data
        """
        self.logger.info(f'Transforming {len(data)} ProjectSight records...')

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
        """Transform a single ProjectSight record."""
        return {
            'source': 'projectsight',
            'source_id': record.get('project_id'),
            'name': record.get('project_name'),
            'status': record.get('status'),
            'start_date': self._parse_date(record.get('start_date')),
            'end_date': self._parse_date(record.get('end_date')),
            'extracted_at': datetime.now().isoformat(),
        }

    def _parse_date(self, date_str: str) -> str:
        """Parse date string to ISO format."""
        if not date_str:
            return None
        try:
            # Try common date formats
            for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.isoformat()
                except ValueError:
                    continue
            self.logger.warning(f'Could not parse date: {date_str}')
            return date_str
        except Exception as e:
            self.logger.warning(f'Date parsing error: {str(e)}')
            return date_str

    def validate_transformation(self, data: List[Dict[str, Any]]) -> bool:
        """
        Validate transformed data has required fields.

        Args:
            data: Transformed data to validate

        Returns:
            True if all records are valid
        """
        required_fields = {'source', 'source_id', 'name', 'status'}

        for record in data:
            missing = required_fields - set(record.keys())
            if missing:
                self.logger.error(f'Record missing fields: {missing}')
                return False

        self.logger.info(f'Validated {len(data)} transformed records')
        return True
