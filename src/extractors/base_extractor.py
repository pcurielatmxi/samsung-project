"""Base extractor class for all data sources."""
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """
    Abstract base class for all data extractors.
    Defines the interface that all extractors must implement.
    """

    def __init__(self, name: str):
        """
        Initialize the extractor.

        Args:
            name: Name of the extractor (for logging)
        """
        self.name = name
        self.logger = logging.getLogger(f'{__name__}.{name}')
        self.extracted_at = None
        self.record_count = 0

    @abstractmethod
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract data from the source system.

        Returns:
            List of dictionaries containing extracted data
        """
        pass

    @abstractmethod
    def validate_extraction(self, data: List[Dict[str, Any]]) -> bool:
        """
        Validate the extracted data.

        Args:
            data: Extracted data to validate

        Returns:
            True if validation passes, False otherwise
        """
        pass

    def log_extraction(self, record_count: int) -> None:
        """Log extraction completion details."""
        self.extracted_at = datetime.now()
        self.record_count = record_count
        self.logger.info(
            f'Extraction completed: {record_count} records extracted at '
            f'{self.extracted_at.isoformat()}'
        )

    def get_metadata(self) -> Dict[str, Any]:
        """Get metadata about the extraction."""
        return {
            'extractor': self.name,
            'extracted_at': self.extracted_at.isoformat() if self.extracted_at else None,
            'record_count': self.record_count,
        }
