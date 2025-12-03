"""Base connector class for all external system connections."""
from abc import ABC, abstractmethod
from typing import Any, Optional
import logging

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """
    Abstract base class for all connectors to external systems.
    Defines the interface that all connectors must implement.
    """

    def __init__(self, name: str, timeout: int = 30):
        """
        Initialize the connector.

        Args:
            name: Name of the connector (for logging)
            timeout: Request timeout in seconds
        """
        self.name = name
        self.timeout = timeout
        self.logger = logging.getLogger(f'{__name__}.{name}')

    @abstractmethod
    def authenticate(self) -> bool:
        """
        Authenticate with the external system.

        Returns:
            True if authentication successful, False otherwise
        """
        pass

    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Validate that connection to the system is working.

        Returns:
            True if connection is valid, False otherwise
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the connection to the external system."""
        pass

    def __enter__(self):
        """Context manager entry."""
        self.authenticate()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
