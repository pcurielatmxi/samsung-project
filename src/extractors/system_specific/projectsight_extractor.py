"""Extractor for ProjectSight (Trimble) via web scraping with Playwright."""
from typing import Any, List, Dict, Optional
import logging
import time

from src.extractors.base_extractor import BaseExtractor
from src.connectors.web_scraper import WebScraperConnector
from src.config.settings import settings

logger = logging.getLogger(__name__)


class ProjectSightExtractor(BaseExtractor):
    """
    Extractor for ProjectSight (Trimble) project management platform.
    Uses Playwright for web scraping to handle:
    - JavaScript-rendered dashboards
    - Modal dialogs for project details
    - Dynamic content loading
    - User authentication
    """

    def __init__(self):
        """Initialize ProjectSight extractor."""
        super().__init__('projectsight')
        self.connector = WebScraperConnector(
            name='ProjectSight',
            base_url=settings.PROJECTSIGHT_BASE_URL,
            username=settings.PROJECTSIGHT_USERNAME,
            password=settings.PROJECTSIGHT_PASSWORD,
            timeout=settings.PROJECTSIGHT_TIMEOUT,
            headless=settings.PROJECTSIGHT_HEADLESS,
        )

    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract projects from ProjectSight.

        Keyword Args:
            project_ids: Optional list of specific project IDs to extract
            date_from: Optional start date for filtering
            debug: If True, saves screenshots for debugging

        Returns:
            List of project dictionaries
        """
        try:
            if not self.connector.authenticate():
                raise RuntimeError('Failed to authenticate with ProjectSight')

            projects = self._scrape_projects(**kwargs)
            self.log_extraction(len(projects))
            return projects
        except Exception as e:
            self.logger.error(f'Extraction failed: {str(e)}')
            raise
        finally:
            self.connector.close()

    def _scrape_projects(self, debug: bool = False, **kwargs) -> List[Dict[str, Any]]:
        """
        Scrape project data from ProjectSight.

        Strategy for modal-based UI:
        1. Navigate to projects list page
        2. Log in if required
        3. Extract project list from table
        4. For each project, click to open modal
        5. Extract project details from modal
        6. Close modal and continue
        7. Handle pagination

        Args:
            debug: If True, saves screenshots
            **kwargs: Additional parameters

        Returns:
            List of project dictionaries
        """
        self.logger.info('Starting ProjectSight project scraping...')
        projects = []

        try:
            # Step 1: Navigate to projects page
            if not self.connector.navigate_to('/projects'):
                self.logger.error('Failed to navigate to projects page')
                return projects

            time.sleep(2)  # Wait for page load

            if debug:
                self.connector.take_screenshot('/tmp/projectsight_projects_page.png')

            # Step 2: Extract projects from list view
            project_rows = self._extract_project_list()
            self.logger.info(f'Found {len(project_rows)} projects in list')

            # Step 3: For each project, open modal and extract details
            for idx, project_row in enumerate(project_rows):
                try:
                    self.logger.debug(f'Processing project {idx + 1}/{len(project_rows)}')

                    # Extract basic info from list view
                    project = self._extract_row_data(project_row)

                    # Click project to open modal
                    if self.connector.click_element(element=project_row):
                        time.sleep(1)  # Wait for modal to appear

                        if debug:
                            self.connector.take_screenshot(f'/tmp/projectsight_modal_{idx}.png')

                        # Extract detailed info from modal
                        modal_data = self._extract_modal_data()
                        project.update(modal_data)

                        # Close modal
                        if self.connector.close_modal():
                            time.sleep(0.5)
                    else:
                        self.logger.warning(f'Failed to click project {idx}')

                    projects.append(project)

                except Exception as e:
                    self.logger.warning(f'Failed to extract project {idx}: {str(e)}')
                    # Try to close modal in case it's still open
                    try:
                        self.connector.close_modal()
                    except:
                        pass
                    continue

            return projects

        except Exception as e:
            self.logger.error(f'Error during scraping: {str(e)}')
            return projects

    def _extract_project_list(self) -> List[Any]:
        """
        Extract project rows from the list table.

        Returns list of element handles for each project row.
        """
        try:
            # Adjust selector based on actual ProjectSight DOM
            # Common patterns: table rows, divs with role="row", list items
            project_rows = self.connector.find_elements(
                'table tbody tr, [role="row"][data-project-id], .project-item'
            )

            self.logger.debug(f'Found {len(project_rows)} project rows')
            return project_rows
        except Exception as e:
            self.logger.error(f'Failed to extract project list: {str(e)}')
            return []

    def _extract_row_data(self, row: Any) -> Dict[str, Any]:
        """
        Extract basic project data from a table row.

        Typical columns: ID, Name, Status, Manager, Date

        Args:
            row: Table row element handle

        Returns:
            Dictionary with basic project information
        """
        try:
            # Adjust selectors to match your actual ProjectSight HTML
            cells = row.query_selector_all('td, [role="cell"]')

            return {
                'project_id': cells[0].text_content().strip() if len(cells) > 0 else '',
                'project_name': cells[1].text_content().strip() if len(cells) > 1 else '',
                'status': cells[2].text_content().strip() if len(cells) > 2 else '',
                'manager': cells[3].text_content().strip() if len(cells) > 3 else '',
                'start_date': cells[4].text_content().strip() if len(cells) > 4 else '',
            }
        except Exception as e:
            self.logger.warning(f'Failed to extract row data: {str(e)}')
            return {}

    def _extract_modal_data(self) -> Dict[str, Any]:
        """
        Extract detailed project data from modal dialog.

        Modal typically contains:
        - Full project description
        - Budget information
        - Team members
        - Dates and milestones

        Returns:
            Dictionary with detailed project information
        """
        try:
            # Wait for modal to be visible
            if not self.connector.wait_for_selector('.modal-body, [role="dialog"] .content', state='visible'):
                self.logger.warning('Modal did not appear')
                return {}

            # Extract modal data - adjust selectors to match your modal
            data = {
                'description': self.connector.extract_text(selector='.modal-body .description'),
                'end_date': self.connector.extract_text(selector='.modal-body .end-date'),
                'budget': self.connector.extract_text(selector='.modal-body .budget'),
                'location': self.connector.extract_text(selector='.modal-body .location'),
            }

            return {k: v for k, v in data.items() if v}  # Remove empty values
        except Exception as e:
            self.logger.warning(f'Failed to extract modal data: {str(e)}')
            return {}

    def validate_extraction(self, data: List[Dict[str, Any]]) -> bool:
        """
        Validate extracted ProjectSight data.

        Required fields:
            - project_id
            - project_name
            - status

        Args:
            data: Extracted data to validate

        Returns:
            True if all records have required fields
        """
        required_fields = {'project_id', 'project_name', 'status'}

        for idx, record in enumerate(data):
            missing = required_fields - set(record.keys())
            if missing:
                self.logger.error(
                    f'Record {idx} missing required fields: {missing}'
                )
                return False

            # Check that values are not empty
            for field in required_fields:
                if not record.get(field):
                    self.logger.error(
                        f'Record {idx} has empty required field: {field}'
                    )
                    return False

        self.logger.info(f'Validated {len(data)} ProjectSight records')
        return True
