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

        Trimble Identity uses a two-step login:
        1. Enter username and press Enter (navigates to password page)
        2. Enter password and press Enter (submits login)

        IMPORTANT: Must navigate to ProjectSight base URL (not Trimble Identity directly)
        to get proper redirect context.

        Returns:
            True if form filled and submitted successfully, False otherwise
        """
        try:
            # IMPORTANT: Navigate to ProjectSight base URL, which will redirect to
            # Trimble Identity with proper return URL parameters
            self.logger.info(f'Navigating to ProjectSight: {settings.PROJECTSIGHT_BASE_URL}')
            self.connector.navigate_to(settings.PROJECTSIGHT_BASE_URL)
            time.sleep(3)  # Wait for redirect to Trimble Identity

            # Step 1: Fill username field (uses getByRole for Trimble Identity compatibility)
            username_selector = settings.PROJECTSIGHT_SELECTOR_USERNAME
            self.logger.debug(f'Filling username field with role label: {username_selector}')

            if not self.connector.send_keys(selector=username_selector, text=settings.PROJECTSIGHT_USERNAME, use_role=True):
                self.logger.error(f'Failed to fill username field with selector: {username_selector}')
                return False

            # Submit username by pressing Enter (Trimble Identity requires this)
            self.logger.debug('Submitting username with Enter key')
            self.connector.page.keyboard.press('Enter')
            time.sleep(3)  # Wait for password page to load

            # Step 2: Fill password field (now on password page)
            password_selector = settings.PROJECTSIGHT_SELECTOR_PASSWORD
            self.logger.debug(f'Filling password field with role label: {password_selector}')

            if not self.connector.send_keys(selector=password_selector, text=settings.PROJECTSIGHT_PASSWORD, use_role=True):
                self.logger.error(f'Failed to fill password field with selector: {password_selector}')
                return False

            # Submit password by pressing Enter
            self.logger.debug('Submitting password with Enter key')
            self.connector.page.keyboard.press('Enter')

            # Wait for navigation to complete (simple timeout approach to avoid crashes)
            self.logger.debug('Waiting for login to complete and redirect...')
            time.sleep(5)  # Give time for redirect to occur

            # Log current URL for debugging
            try:
                current_url = self.connector.page.url
                self.logger.info(f'After password submission, current URL: {current_url}')
            except Exception as e:
                self.logger.warning(f'Could not get current URL: {str(e)}')

            self.logger.info('Login form submitted successfully')
            return True

        except Exception as e:
            self.logger.error(f'Failed to fill login form: {str(e)}')
            return False

    def _detect_and_handle_mfa(self) -> bool:
        """
        Detect MFA/Verification code prompt and handle it.

        Checks for various verification prompts:
        - MFA code input
        - Verification code input
        - Security code input

        If detected, waits for manual user entry.

        Returns:
            True if MFA handled or not required, False if MFA failed
        """
        try:
            # Wait a moment for any verification prompts to appear
            time.sleep(2)

            # Check current URL for verification indicators or success
            current_url = self.connector.page.url
            self.logger.debug(f'Checking for verification prompt at URL: {current_url}')

            # Check if already on Projects page (login succeeded without MFA)
            if '/web/app/Projects' in current_url:
                self.logger.info('Already on Projects page - login succeeded without MFA')
                return True

            # Check if still on sign-in page (might need MFA or login failed)
            if 'sign_in.html' not in current_url.lower():
                # Not on login page anymore, assume success
                self.logger.info(f'Navigated away from login page to: {current_url}')
                return True

            # Still on login page - check for verification indicators in URL only
            verification_indicators = [
                'verification',
                'verify',
                'code',
                'mfa',
                'two-factor',
                '2fa',
                'otp',
            ]

            url_needs_verification = any(
                indicator in current_url.lower()
                for indicator in verification_indicators
            )

            # Try to check for verification input fields (without reading page content)
            field_needs_verification = False
            try:
                # Try to find verification code input field
                verification_input = self.connector.page.query_selector(
                    'input[type="text"][placeholder*="code"], '
                    'input[type="text"][placeholder*="verification"], '
                    'input[name*="code"], '
                    'input[name*="otp"]'
                )
                if verification_input and verification_input.is_visible():
                    field_needs_verification = True
            except Exception as e:
                self.logger.debug(f'Could not check for verification input: {str(e)}')

            if url_needs_verification or field_needs_verification:
                self.logger.info('Verification code prompt detected')
                return self._handle_manual_verification()
            else:
                self.logger.info('No verification prompt detected, proceeding')
                return True

        except Exception as e:
            # If we can't check, assume no verification needed
            self.logger.debug(f'Could not check for verification: {str(e)}')
            return True

    def _handle_manual_verification(self) -> bool:
        """
        Wait for user to manually enter verification code.

        Displays instructions and waits for Projects page to appear.
        User manually enters code in the browser or via terminal input.

        Returns:
            True if verification successful and logged in, False otherwise
        """
        try:
            # Log instructions for user
            self.logger.info('=' * 70)
            self.logger.info('VERIFICATION CODE REQUIRED')
            self.logger.info('A verification code has been sent to your email/phone.')
            self.logger.info('')
            self.logger.info('OPTIONS:')
            self.logger.info('  1. Enter the code in the browser window (if visible)')
            self.logger.info('  2. Check your email/phone for the code and enter it')
            self.logger.info('')
            self.logger.info('Waiting for verification to complete (timeout: 5 minutes)...')
            self.logger.info('=' * 70)

            # Take screenshot to help user
            try:
                screenshot_path = '/tmp/projectsight_verification_prompt.png'
                self.connector.take_screenshot(screenshot_path)
                self.logger.info(f'Screenshot saved to: {screenshot_path}')
            except Exception as e:
                self.logger.warning(f'Could not save screenshot: {str(e)}')

            # Wait for navigation to Projects page (indicates verification success)
            verification_timeout = 300000  # 5 minutes in milliseconds
            start_time = time.time()

            self.logger.info('Waiting for you to enter the verification code...')

            while time.time() - start_time < 300:  # 5 minutes
                try:
                    current_url = self.connector.page.url

                    # Check if we've reached the Projects page
                    if '/web/app/Projects' in current_url:
                        self.logger.info('✅ Verification successful! Logged into ProjectSight')
                        time.sleep(2)  # Wait for page to stabilize
                        return True

                    # Check if still on login page
                    if 'sign_in.html' in current_url:
                        # Still waiting for verification
                        time.sleep(2)
                        continue

                    # Some other page - might be success
                    if 'projectsight' in current_url.lower():
                        self.logger.info(f'Navigated to ProjectSight page: {current_url}')
                        return True

                except Exception as e:
                    self.logger.debug(f'Error checking URL: {str(e)}')
                    time.sleep(2)
                    continue

            self.logger.error('Verification timeout - Projects page did not appear')
            return False

        except Exception as e:
            self.logger.error(f'Verification handling failed: {str(e)}')
            return False

    def _verify_home_screen(self) -> bool:
        """
        Verify that login was successful by checking for home screen.

        After successful login, ProjectSight redirects to:
        https://prod.projectsightapp.trimble.com/web/app/Projects

        Returns:
            True if home screen verified, False otherwise
        """
        try:
            # Wait for URL to change to Projects page
            self.logger.debug('Waiting for navigation to Projects page')

            # Check if we're on the projects page by URL
            current_url = self.connector.page.url
            if '/web/app/Projects' in current_url:
                self.logger.info(f'✅ Login successful! Navigated to Projects page: {current_url}')

                # Wait for projects page to load (JS rendering takes time)
                time.sleep(settings.PROJECTSIGHT_PAGE_LOAD_WAIT)

                # Optionally verify home indicator (but don't fail if it crashes)
                try:
                    home_selector = settings.PROJECTSIGHT_SELECTOR_HOME_INDICATOR
                    self.logger.debug(f'Attempting to verify home indicator: {home_selector}')

                    home_verified = self.connector.page.get_by_text(home_selector).is_visible(timeout=5000)

                    if home_verified:
                        self.logger.info('Home screen indicator verified')
                except Exception as e:
                    # Don't fail if we can't verify the indicator - URL check is sufficient
                    self.logger.debug(f'Could not verify home indicator (not critical): {str(e)}')

                # Try to take screenshot for confirmation (optional)
                try:
                    screenshot_path = '/tmp/projectsight_home_screen.png'
                    self.connector.take_screenshot(screenshot_path)
                    self.logger.info(f'Home screen screenshot saved to: {screenshot_path}')
                except Exception as e:
                    self.logger.debug(f'Could not save screenshot: {str(e)}')

                return True
            else:
                self.logger.warning(f'Unexpected URL after login: {current_url}')
                # Still return True if we're on projectsight domain (not login page)
                if 'projectsightapp.trimble.com' in current_url and 'sign_in' not in current_url:
                    self.logger.info('On ProjectSight domain - considering login successful')
                    return True
                return False

        except Exception as e:
            self.logger.error(f'Failed to verify home screen: {str(e)}')
            # Check URL one more time before failing
            try:
                current_url = self.connector.page.url
                if '/web/app/Projects' in current_url or 'projectsightapp.trimble.com' in current_url:
                    self.logger.info('URL check passed despite error - considering login successful')
                    return True
            except:
                pass
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
