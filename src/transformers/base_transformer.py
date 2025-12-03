"""Base transformer class for data transformations."""
from abc import ABC, abstractmethod
from typing import Any, List, Dict
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class BaseTransformer(ABC):
    """
    Abstract base class for all data transformers.
    Defines the interface that all transformers must implement.
    """

    def __init__(self, name: str):
        """
        Initialize the transformer.

        Args:
            name: Name of the transformer (for logging)
        """
        self.name = name
        self.logger = logging.getLogger(f'{__name__}.{name}')

    @abstractmethod
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform the input data.

        Args:
            data: List of dictionaries to transform

        Returns:
            Transformed list of dictionaries
        """
        pass

    @abstractmethod
    def validate_transformation(self, data: List[Dict[str, Any]]) -> bool:
        """
        Validate the transformed data.

        Args:
            data: Transformed data to validate

        Returns:
            True if validation passes, False otherwise
        """
        pass

    def to_dataframe(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        """Convert data to pandas DataFrame."""
        return pd.DataFrame(data)

    def from_dataframe(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert pandas DataFrame to list of dictionaries."""
        return df.to_dict('records')
