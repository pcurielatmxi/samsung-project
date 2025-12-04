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
        # Attach login method to connector instance
        self.connector._perform_login = self._perform_login

    def _perform_login(self) -> bool:
        """
        Perform ProjectSight login flow.

        This method is called by WebScraperConnector.authenticate() when
        no valid session exists.

        Flow:
        1. Fill login form (username + password)
        2. Submit form
        3. Detect and handle MFA if required
        4. Verify home screen reached

        Returns:
            True if login successful, False otherwise
        """
        try:
            self.logger.info('Starting ProjectSight login flow')

            # Step 1: Fill and submit login form
            if not self._fill_login_form():
                self.logger.error('Failed to fill login form')
                return False

            # Step 2: Detect and handle MFA
            if not self._detect_and_handle_mfa():
                self.logger.error('MFA handling failed')
                return False

            # Step 3: Verify home screen
            if not self._verify_home_screen():
                self.logger.error('Failed to verify home screen')
                return False

            self.logger.info('ProjectSight login successful')
            return True

        except Exception as e:
            self.logger.error(f'Login failed with error: {str(e)}')
            return False

    def _fill_login_form(self) -> bool:
        """
        Navigate to login page and fill credentials.

        Returns:
            True if form filled and submitted successfully, False otherwise
        """
        try:
            # Navigate to login URL
            login_url = settings.PROJECTSIGHT_LOGIN_URL
            if not login_url:
                # If no login URL configured, try base URL
                login_url = settings.PROJECTSIGHT_BASE_URL

            self.logger.info(f'Navigating to login page: {login_url}')
            self.connector.navigate_to(login_url)
            time.sleep(2)  # Wait for page load

            # Find and fill username field
            username_selector = settings.PROJECTSIGHT_SELECTOR_USERNAME
            self.logger.debug(f'Filling username field: {username_selector}')

            if not self.connector.send_keys(selector=username_selector, text=settings.PROJECTSIGHT_USERNAME):
                self.logger.error(f'Failed to fill username field with selector: {username_selector}')
                return False

            # Find and fill password field
            password_selector = settings.PROJECTSIGHT_SELECTOR_PASSWORD
            self.logger.debug(f'Filling password field: {password_selector}')

            if not self.connector.send_keys(selector=password_selector, text=settings.PROJECTSIGHT_PASSWORD):
                self.logger.error(f'Failed to fill password field with selector: {password_selector}')
                return False

            # Find and click submit button
            submit_selector = settings.PROJECTSIGHT_SELECTOR_SUBMIT
            self.logger.debug(f'Clicking submit button: {submit_selector}')

            if not self.connector.click_element(submit_selector):
                self.logger.error(f'Failed to click submit button with selector: {submit_selector}')
                return False

            # Wait for form submission and page transition
            time.sleep(3)

            self.logger.info('Login form submitted successfully')
            return True

        except Exception as e:
            self.logger.error(f'Failed to fill login form: {str(e)}')
            return False

    def _detect_and_handle_mfa(self) -> bool:
        """
        Detect MFA prompt and handle it.

        Checks if MFA input field appears after login.
        If detected, waits for manual user entry.

        Returns:
            True if MFA handled or not required, False if MFA failed
        """
        try:
            mfa_selector = settings.PROJECTSIGHT_SELECTOR_MFA_INPUT

            # Check if MFA prompt appears (5-second timeout)
            self.logger.debug('Checking for MFA prompt...')
            mfa_detected = self.connector.wait_for_selector(
                mfa_selector,
                state='visible',
                timeout=5  # Short timeout - if no MFA in 5 seconds, assume not required
            )

            if mfa_detected:
                self.logger.info('MFA prompt detected')
                return self._handle_manual_mfa()
            else:
                self.logger.info('No MFA prompt detected, proceeding')
                return True

        except Exception as e:
            # Timeout or error checking for MFA - assume not required
            self.logger.debug(f'No MFA detected (expected): {str(e)}')
            return True

    def _handle_manual_mfa(self) -> bool:
        """
        Wait for user to manually enter MFA code.

        Displays instructions and waits for home screen indicator to appear.
        User manually enters code in the visible browser window.

        Returns:
            True if home screen appears (MFA successful), False otherwise
        """
        try:
            # Log instructions for user
            self.logger.info('=' * 60)
            self.logger.info('MFA CODE REQUIRED')
            self.logger.info('Please enter your MFA code in the browser window')
            self.logger.info('Waiting for MFA submission (timeout: 5 minutes)...')
            self.logger.info('=' * 60)

            # Take screenshot to help user identify MFA prompt
            screenshot_path = '/tmp/projectsight_mfa_prompt.png'
            self.connector.take_screenshot(screenshot_path)
            self.logger.info(f'Screenshot saved to: {screenshot_path}')

            # Wait for home screen indicator to appear (indicates MFA success)
            home_selector = settings.PROJECTSIGHT_SELECTOR_HOME_INDICATOR
            mfa_timeout = 300  # 5 minutes in seconds

            home_screen_visible = self.connector.wait_for_selector(
                home_selector,
                state='visible',
                timeout=mfa_timeout
            )

            if home_screen_visible:
                self.logger.info('MFA code accepted, home screen reached')
                return True
            else:
                self.logger.error('MFA timeout - home screen did not appear')
                return False

        except Exception as e:
            self.logger.error(f'MFA handling failed: {str(e)}')
            return False

    def _verify_home_screen(self) -> bool:
        """
        Verify that login was successful by checking for home screen indicator.

        Returns:
            True if home screen verified, False otherwise
        """
        try:
            home_selector = settings.PROJECTSIGHT_SELECTOR_HOME_INDICATOR

            self.logger.debug(f'Verifying home screen with selector: {home_selector}')

            # Wait for home screen indicator
            home_verified = self.connector.wait_for_selector(
                home_selector,
                state='visible',
                timeout=10
            )

            if home_verified:
                self.logger.info('Home screen verified successfully')

                # Take screenshot for confirmation
                screenshot_path = '/tmp/projectsight_home_screen.png'
                self.connector.take_screenshot(screenshot_path)
                self.logger.info(f'Home screen screenshot saved to: {screenshot_path}')

                return True
            else:
                self.logger.error('Home screen indicator not found')
                return False

        except Exception as e:
            self.logger.error(f'Failed to verify home screen: {str(e)}')
            return False

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
