"""Base loader class for loading data to destinations."""
from abc import ABC, abstractmethod
from typing import Any, List, Dict
import logging

logger = logging.getLogger(__name__)


class BaseLoader(ABC):
    """
    Abstract base class for all data loaders.
    Defines the interface for loading data to various destinations.
    """

    def __init__(self, name: str):
        """
        Initialize the loader.

        Args:
            name: Name of the loader (for logging)
        """
        self.name = name
        self.logger = logging.getLogger(f'{__name__}.{name}')

    @abstractmethod
    def load(self, data: List[Dict[str, Any]], **kwargs) -> bool:
        """
        Load data to the destination.

        Args:
            data: List of dictionaries to load
            **kwargs: Additional parameters (table_name, etc.)

        Returns:
            True if load successful, False otherwise
        """
        pass

    @abstractmethod
    def validate_load(self, record_count: int) -> bool:
        """
        Validate that data was loaded successfully.

        Args:
            record_count: Number of records that should have been loaded

        Returns:
            True if validation passes, False otherwise
        """
        pass

    def get_load_stats(self) -> Dict[str, Any]:
        """Get statistics about the load operation."""
        return {
            'loader': self.name,
        }
