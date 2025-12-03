"""API connector for HTTP-based APIs."""
import requests
from typing import Any, Dict, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

from .base_connector import BaseConnector

logger = logging.getLogger(__name__)


class APIConnector(BaseConnector):
    """
    Connector for REST APIs.
    Handles authentication, retries, and error handling.
    """

    def __init__(
        self,
        name: str,
        base_url: str,
        api_key: Optional[str] = None,
        timeout: int = 30,
        retry_attempts: int = 3,
        retry_delay: int = 5,
    ):
        """
        Initialize API connector.

        Args:
            name: Name of the API service
            base_url: Base URL for the API
            api_key: Optional API key for authentication
            timeout: Request timeout in seconds
            retry_attempts: Number of retry attempts
            retry_delay: Delay between retries in seconds
        """
        super().__init__(name, timeout)
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.session = requests.Session()
        self._setup_retry_strategy()

    def _setup_retry_strategy(self) -> None:
        """Configure retry strategy for the session."""
        retry_strategy = Retry(
            total=self.retry_attempts,
            backoff_factor=self.retry_delay,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=['GET', 'POST', 'PUT', 'DELETE'],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def authenticate(self) -> bool:
        """
        Authenticate with the API.
        For API key auth, this just sets headers.
        """
        if self.api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {self.api_key}',
                'User-Agent': 'mxi-samsung-etl/1.0',
            })
        self.logger.info(f'Authenticated with {self.name}')
        return True

    def validate_connection(self) -> bool:
        """Validate API connection by making a simple request."""
        try:
            response = self.session.get(
                f'{self.base_url}/health',
                timeout=self.timeout,
            )
            is_valid = response.status_code < 400
            if is_valid:
                self.logger.info(f'Connection to {self.name} validated')
            else:
                self.logger.warning(
                    f'Connection validation failed: {response.status_code}'
                )
            return is_valid
        except Exception as e:
            self.logger.error(f'Connection validation error: {str(e)}')
            return False

    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Make a GET request.

        Args:
            endpoint: API endpoint (relative to base_url)
            params: Query parameters
            headers: Additional headers

        Returns:
            JSON response as dictionary

        Raises:
            requests.RequestException: If request fails
        """
        url = f'{self.base_url}/{endpoint.lstrip("/")}'
        merged_headers = {**self.session.headers}
        if headers:
            merged_headers.update(headers)

        response = self.session.get(
            url,
            params=params,
            headers=merged_headers,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def post(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Make a POST request.

        Args:
            endpoint: API endpoint
            data: Form data
            json: JSON body
            headers: Additional headers

        Returns:
            JSON response as dictionary
        """
        url = f'{self.base_url}/{endpoint.lstrip("/")}'
        merged_headers = {**self.session.headers}
        if headers:
            merged_headers.update(headers)

        response = self.session.post(
            url,
            data=data,
            json=json,
            headers=merged_headers,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def close(self) -> None:
        """Close the session."""
        self.session.close()
        self.logger.info(f'Closed connection to {self.name}')
