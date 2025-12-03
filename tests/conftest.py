"""Pytest configuration and fixtures."""
import pytest
from unittest.mock import Mock, MagicMock
from typing import List, Dict, Any


@pytest.fixture
def sample_extract_data() -> List[Dict[str, Any]]:
    """Sample extracted data for testing."""
    return [
        {
            'id': '1',
            'name': 'Test Project 1',
            'status': 'active',
            'start_date': '2025-01-01',
        },
        {
            'id': '2',
            'name': 'Test Project 2',
            'status': 'inactive',
            'start_date': '2025-01-15',
        },
    ]


@pytest.fixture
def mock_api_connector():
    """Mock API connector."""
    connector = MagicMock()
    connector.authenticate.return_value = True
    connector.validate_connection.return_value = True
    connector.get.return_value = {
        'id': '1',
        'name': 'Test',
        'status': 'active',
    }
    return connector


@pytest.fixture
def mock_web_scraper():
    """Mock web scraper connector."""
    scraper = MagicMock()
    scraper.authenticate.return_value = True
    scraper.validate_connection.return_value = True
    scraper.find_element.return_value = MagicMock()
    scraper.find_elements.return_value = []
    return scraper


@pytest.fixture
def mock_database_connection():
    """Mock database connection."""
    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    conn.commit.return_value = None
    conn.close.return_value = None
    return conn
