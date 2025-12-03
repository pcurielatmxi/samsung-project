"""Extractor for Fieldwire API."""
from typing import Any, List, Dict, Optional
import logging

from src.extractors.base_extractor import BaseExtractor
from src.connectors.api_connector import APIConnector
from src.config.settings import settings

logger = logging.getLogger(__name__)


class FieldwireExtractor(BaseExtractor):
    """
    Extractor for Fieldwire project management platform.
    Uses REST API to extract project, task, and resource data.
    """

    def __init__(self):
        """Initialize Fieldwire extractor."""
        super().__init__('fieldwire')
        self.connector = APIConnector(
            name='Fieldwire',
            base_url=settings.FIELDWIRE_BASE_URL,
            api_key=settings.FIELDWIRE_API_KEY,
            timeout=settings.FIELDWIRE_TIMEOUT,
            retry_attempts=settings.FIELDWIRE_RETRY_ATTEMPTS,
            retry_delay=settings.FIELDWIRE_RETRY_DELAY,
        )

    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract data from Fieldwire API.

        Keyword Args:
            resource_type: Type of resource to extract (projects, tasks, etc.)
            project_id: Optional project ID to filter by
            limit: Optional record limit (default: all)

        Returns:
            List of extracted records
        """
        try:
            with self.connector:
                resource_type = kwargs.get('resource_type', 'projects')
                data = self._extract_resource(resource_type, **kwargs)
                self.log_extraction(len(data))
                return data
        except Exception as e:
            self.logger.error(f'Extraction failed: {str(e)}')
            raise

    def _extract_resource(
        self,
        resource_type: str,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        Extract a specific resource type from Fieldwire API.

        Supported resource types:
            - projects
            - tasks
            - workers
            - checklists

        Args:
            resource_type: Type of resource to extract
            **kwargs: Additional filters

        Returns:
            List of records
        """
        self.logger.info(f'Extracting {resource_type} from Fieldwire...')

        resources = {
            'projects': self._extract_projects,
            'tasks': self._extract_tasks,
            'workers': self._extract_workers,
            'checklists': self._extract_checklists,
        }

        if resource_type not in resources:
            raise ValueError(f'Unknown resource type: {resource_type}')

        return resources[resource_type](**kwargs)

    def _extract_projects(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract projects from Fieldwire.

        TODO: Implement actual API calls.
        """
        # TODO: Call Fieldwire API endpoint for projects
        # self.connector.get('/projects', params=...)
        return []

    def _extract_tasks(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract tasks from Fieldwire.

        TODO: Implement actual API calls.
        """
        # TODO: Call Fieldwire API endpoint for tasks
        # self.connector.get('/tasks', params=...)
        return []

    def _extract_workers(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract workers from Fieldwire.

        TODO: Implement actual API calls.
        """
        # TODO: Call Fieldwire API endpoint for workers
        # self.connector.get('/workers', params=...)
        return []

    def _extract_checklists(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract checklists from Fieldwire.

        TODO: Implement actual API calls.
        """
        # TODO: Call Fieldwire API endpoint for checklists
        # self.connector.get('/checklists', params=...)
        return []

    def validate_extraction(self, data: List[Dict[str, Any]]) -> bool:
        """
        Validate extracted Fieldwire data.

        All records should have at least an ID field.

        Args:
            data: Extracted data to validate

        Returns:
            True if all records have required fields
        """
        for record in data:
            if 'id' not in record:
                self.logger.error(f'Record missing id field: {record}')
                return False

        self.logger.info(f'Validated {len(data)} Fieldwire records')
        return True
