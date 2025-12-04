"""Web scraper connector for systems without APIs using Playwright."""
from typing import Optional, List, Dict, Any
import logging
import time
import json
from pathlib import Path
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext

from .base_connector import BaseConnector

logger = logging.getLogger(__name__)


class WebScraperConnector(BaseConnector):
    """
    Connector for web scraping using Playwright.
    Handles authentication and data extraction from web pages with modal support.

    Key features for ProjectSight:
    - Login with username/password
    - Handle modals and dynamic content
    - Wait for JavaScript rendering
    - Extract data from modal dialogs
    - Debug with screenshots and HTML inspection
    """

    def __init__(
        self,
        name: str,
        base_url: str,
        username: str,
        password: str,
        timeout: int = 30,
        headless: bool = True,
    ):
        """
        Initialize web scraper connector.

        Args:
            name: Name of the website/service
            base_url: Base URL of the website
            username: Login username
            password: Login password
            timeout: Playwright wait timeout in seconds
            headless: Whether to run browser in headless mode
        """
        super().__init__(name, timeout)
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.headless = headless
        self.timeout_ms = timeout * 1000

        # Playwright objects
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    def authenticate(self) -> bool:
        """
        Initialize Playwright browser and authenticate with the website.

        Flow:
        1. Try to load saved session (cookies)
        2. If session valid, use it and skip login
        3. If no valid session, initialize browser and call _perform_login()
        4. Save session after successful login

        Subclasses should implement _perform_login() for system-specific login logic.
        """
        try:
            # Initialize Playwright
            self.playwright = sync_playwright().start()

            # Browser args for container stability
            browser_args = [
                '--disable-dev-shm-usage',  # Overcome limited resource problems
                '--disable-blink-features=AutomationControlled',  # Avoid detection
                '--no-sandbox',  # Required for container environments
                '--disable-setuid-sandbox',
                '--disable-gpu',  # Disable GPU hardware acceleration
            ]

            self.browser = self.playwright.chromium.launch(
                headless=self.headless,
                args=browser_args
            )

            # Try to load saved session first
            saved_cookies = self._load_session()

            if saved_cookies:
                self.logger.info(f'Loading saved session for {self.name}')
                self.context = self.browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    ignore_https_errors=True,
                )
                self.page = self.context.new_page()
                self.page.set_default_navigation_timeout(self.timeout_ms)
                self.page.set_default_timeout(self.timeout_ms)

                # Add cookies to context
                self.context.add_cookies(saved_cookies)
                self.logger.info(f'Session restored from cookies for {self.name}')
                return True
            else:
                # No valid session, need fresh login
                self.logger.info(f'No valid session found, performing fresh login for {self.name}')
                self.context = self.browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    ignore_https_errors=True,
                )
                self.page = self.context.new_page()
                self.page.set_default_navigation_timeout(self.timeout_ms)
                self.page.set_default_timeout(self.timeout_ms)

                # Call subclass login implementation
                if hasattr(self, '_perform_login') and callable(getattr(self, '_perform_login')):
                    login_success = self._perform_login()
                    if login_success:
                        # Save session after successful login
                        self._save_session()
                        self.logger.info(f'Login successful and session saved for {self.name}')
                        return True
                    else:
                        self.logger.error(f'Login failed for {self.name}')
                        return False
                else:
                    # No login method, just return success (backward compatible)
                    self.logger.info(f'Playwright browser initialized for {self.name}')
                    return True

        except Exception as e:
            self.logger.error(f'Failed to authenticate: {str(e)}')
            return False

    def validate_connection(self) -> bool:
        """Validate that the website is accessible."""
        try:
            if not self.page:
                self.authenticate()
            self.page.goto(self.base_url)
            self.logger.info(f'Connection to {self.name} validated')
            return True
        except Exception as e:
            self.logger.error(f'Connection validation error: {str(e)}')
            return False

    def navigate_to(self, url: str) -> bool:
        """
        Navigate to a URL.

        Args:
            url: URL to navigate to (absolute or relative to base_url)

        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.page:
                self.authenticate()

            # Handle relative URLs
            if not url.startswith('http'):
                url = f'{self.base_url}{url}'

            self.page.goto(url)
            return True
        except Exception as e:
            self.logger.error(f'Failed to navigate to {url}: {str(e)}')
            return False

    def wait_for_selector(self, selector: str, state: str = 'visible') -> bool:
        """
        Wait for an element to appear.

        Args:
            selector: CSS selector to wait for
            state: 'visible', 'hidden', or 'attached'

        Returns:
            True if element appears, False if timeout
        """
        try:
            if not self.page:
                return False
            self.page.wait_for_selector(selector, state=state, timeout=self.timeout_ms)
            return True
        except Exception as e:
            self.logger.warning(f'Timeout waiting for selector {selector}: {str(e)}')
            return False

    def wait_for_url(self, url_pattern: str) -> bool:
        """
        Wait for URL to change to match pattern.

        Args:
            url_pattern: URL pattern to match (regex or simple string)

        Returns:
            True if URL matches, False if timeout
        """
        try:
            if not self.page:
                return False
            self.page.wait_for_url(url_pattern, timeout=self.timeout_ms)
            return True
        except Exception as e:
            self.logger.warning(f'Timeout waiting for URL {url_pattern}: {str(e)}')
            return False

    def find_element(
        self,
        selector: str,
        wait: bool = True,
    ) -> Optional[Any]:
        """
        Find a single element on the page using CSS selector.

        Args:
            selector: CSS selector
            wait: Whether to wait for element presence

        Returns:
            Element handle if found, None otherwise
        """
        try:
            if not self.page:
                return None

            if wait:
                self.page.wait_for_selector(selector, timeout=self.timeout_ms)

            element = self.page.query_selector(selector)
            if element:
                return element

            self.logger.warning(f'Element not found: {selector}')
            return None
        except Exception as e:
            self.logger.warning(f'Failed to find element {selector}: {str(e)}')
            return None

    def find_elements(self, selector: str) -> List[Any]:
        """
        Find multiple elements on the page using CSS selector.

        Args:
            selector: CSS selector

        Returns:
            List of element handles
        """
        try:
            if not self.page:
                return []

            elements = self.page.query_selector_all(selector)
            return elements if elements else []
        except Exception as e:
            self.logger.warning(f'Failed to find elements {selector}: {str(e)}')
            return []

    def extract_text(self, element: Any = None, selector: str = None) -> str:
        """
        Extract text from an element.

        Args:
            element: Element handle (from find_element)
            selector: CSS selector (if element not provided)

        Returns:
            Text content
        """
        try:
            if not self.page:
                return ''

            if element:
                return element.text_content().strip()
            elif selector:
                text = self.page.text_content(selector)
                return text.strip() if text else ''
            return ''
        except Exception as e:
            self.logger.warning(f'Failed to extract text: {str(e)}')
            return ''

    def extract_attribute(
        self,
        element: Any = None,
        selector: str = None,
        attribute: str = 'href',
    ) -> str:
        """
        Extract an attribute value from an element.

        Args:
            element: Element handle
            selector: CSS selector (if element not provided)
            attribute: Attribute name to extract

        Returns:
            Attribute value
        """
        try:
            if not self.page:
                return ''

            if element:
                return element.get_attribute(attribute) or ''
            elif selector:
                value = self.page.get_attribute(selector, attribute)
                return value or ''
            return ''
        except Exception as e:
            self.logger.warning(f'Failed to extract attribute {attribute}: {str(e)}')
            return ''

    def click_element(self, element: Any = None, selector: str = None) -> bool:
        """
        Click an element.

        Args:
            element: Element handle
            selector: CSS selector (if element not provided)

        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.page:
                return False

            if element:
                element.click()
            elif selector:
                self.page.click(selector)
            else:
                return False
            return True
        except Exception as e:
            self.logger.error(f'Failed to click element: {str(e)}')
            return False

    def send_keys(self, element: Any = None, selector: str = None, text: str = '', use_role: bool = False) -> bool:
        """
        Send text to an input element.

        Args:
            element: Element handle
            selector: CSS selector or role label (if element not provided)
            text: Text to type
            use_role: If True, use getByRole('textbox', name=selector) instead of CSS selector

        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.page:
                return False

            if selector:
                if use_role:
                    # Use Playwright's getByRole for better compatibility with modern forms
                    self.page.get_by_role('textbox', name=selector).fill(text)
                else:
                    # Traditional CSS selector
                    self.page.fill(selector, text)
            elif element:
                element.fill(text)
            else:
                return False
            return True
        except Exception as e:
            self.logger.error(f'Failed to send keys: {str(e)}')
            return False

    def wait_for_modal(self, modal_selector: str = '.modal, [role="dialog"]') -> bool:
        """
        Wait for a modal dialog to appear (important for ProjectSight).

        Args:
            modal_selector: CSS selector for modal element

        Returns:
            True if modal appears, False if timeout
        """
        try:
            if not self.page:
                return False
            self.page.wait_for_selector(modal_selector, state='visible', timeout=self.timeout_ms)
            self.logger.debug('Modal appeared')
            return True
        except Exception as e:
            self.logger.warning(f'Timeout waiting for modal: {str(e)}')
            return False

    def close_modal(self, close_button_selector: str = '[class*="close"], .close-btn, [aria-label="Close"]') -> bool:
        """
        Close a modal dialog.

        Args:
            close_button_selector: CSS selector for close button

        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.page:
                return False

            close_btn = self.page.query_selector(close_button_selector)
            if close_btn:
                close_btn.click()
                # Wait for modal to disappear
                self.page.wait_for_selector('.modal, [role="dialog"]', state='hidden', timeout=2000)
                self.logger.debug('Modal closed')
                return True
            return False
        except Exception as e:
            self.logger.warning(f'Failed to close modal: {str(e)}')
            return False

    def get_page_content(self) -> str:
        """Get the current page's HTML content."""
        try:
            if not self.page:
                return ''
            return self.page.content()
        except Exception as e:
            self.logger.warning(f'Failed to get page content: {str(e)}')
            return ''

    def take_screenshot(self, filename: str) -> bool:
        """
        Take a screenshot (useful for debugging).

        Args:
            filename: Path to save screenshot

        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.page:
                return False
            self.page.screenshot(path=filename)
            self.logger.info(f'Screenshot saved to {filename}')
            return True
        except Exception as e:
            self.logger.error(f'Failed to take screenshot: {str(e)}')
            return False

    def evaluate_javascript(self, script: str) -> Any:
        """
        Execute JavaScript on the page.

        Args:
            script: JavaScript code to execute

        Returns:
            Result of JavaScript execution
        """
        try:
            if not self.page:
                return None
            return self.page.evaluate(script)
        except Exception as e:
            self.logger.warning(f'Failed to evaluate JavaScript: {str(e)}')
            return None

    def _get_session_file_path(self) -> Path:
        """
        Generate session file path based on connector name and username.

        Returns:
            Path to session file
        """
        # Import here to avoid circular dependency
        from src.config.settings import settings

        # Ensure session directory exists
        session_dir = settings.PROJECTSIGHT_SESSION_DIR
        session_dir.mkdir(parents=True, exist_ok=True)

        # Use name and username to create unique session file
        safe_name = self.name.lower().replace(' ', '_')
        safe_username = self.username.replace('@', '_at_').replace('.', '_')
        session_file = session_dir / f'{safe_name}_{safe_username}_cookies.json'

        return session_file

    def _load_session(self) -> Optional[List[Dict[str, Any]]]:
        """
        Load session cookies from file if valid.

        Returns:
            List of cookies if session is valid, None otherwise
        """
        try:
            session_file = self._get_session_file_path()

            if not session_file.exists():
                self.logger.debug(f'No session file found at {session_file}')
                return None

            # Load session data
            with open(session_file, 'r') as f:
                session_data = json.load(f)

            # Validate session
            if not self._is_session_valid(session_data):
                self.logger.info('Session expired or invalid, clearing')
                self._clear_session()
                return None

            self.logger.info(f'Valid session found for {self.name}')
            return session_data.get('cookies', [])

        except Exception as e:
            self.logger.warning(f'Failed to load session: {str(e)}')
            return None

    def _save_session(self) -> bool:
        """
        Save current session cookies to file.

        Returns:
            True if saved successfully, False otherwise
        """
        try:
            if not self.context:
                self.logger.warning('No browser context to save')
                return False

            # Get all cookies from context
            cookies = self.context.cookies()

            # Prepare session data
            session_data = {
                'username': self.username,
                'base_url': self.base_url,
                'saved_at': datetime.now().isoformat(),
                'cookies': cookies
            }

            # Save to file
            session_file = self._get_session_file_path()
            with open(session_file, 'w') as f:
                json.dump(session_data, f, indent=2)

            # Set restrictive permissions (owner read/write only)
            session_file.chmod(0o600)

            self.logger.info(f'Session saved to {session_file}')
            return True

        except Exception as e:
            self.logger.warning(f'Failed to save session: {str(e)}')
            return False

    def _is_session_valid(self, session_data: Dict[str, Any]) -> bool:
        """
        Validate session data.

        Args:
            session_data: Session dictionary with cookies and metadata

        Returns:
            True if session is valid, False otherwise
        """
        try:
            # Import here to avoid circular dependency
            from src.config.settings import settings

            # Check required fields
            if not all(key in session_data for key in ['username', 'base_url', 'saved_at', 'cookies']):
                self.logger.debug('Session missing required fields')
                return False

            # Check username matches
            if session_data['username'] != self.username:
                self.logger.debug('Session username mismatch')
                return False

            # Check base_url matches
            if session_data['base_url'] != self.base_url:
                self.logger.debug('Session base_url mismatch')
                return False

            # Check age
            saved_at = datetime.fromisoformat(session_data['saved_at'])
            age = datetime.now() - saved_at
            validity_days = settings.PROJECTSIGHT_SESSION_VALIDITY_DAYS

            if age > timedelta(days=validity_days):
                self.logger.debug(f'Session expired (age: {age.days} days, max: {validity_days} days)')
                return False

            # Check has cookies
            if not session_data['cookies']:
                self.logger.debug('Session has no cookies')
                return False

            return True

        except Exception as e:
            self.logger.warning(f'Session validation error: {str(e)}')
            return False

    def _clear_session(self) -> bool:
        """
        Delete saved session file.

        Returns:
            True if cleared successfully, False otherwise
        """
        try:
            session_file = self._get_session_file_path()

            if session_file.exists():
                session_file.unlink()
                self.logger.info(f'Session file deleted: {session_file}')
                return True
            else:
                self.logger.debug('No session file to clear')
                return False

        except Exception as e:
            self.logger.warning(f'Failed to clear session: {str(e)}')
            return False

    def close(self) -> None:
        """Close the browser and Playwright resources."""
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
            self.logger.info(f'Closed connection to {self.name}')
        except Exception as e:
            self.logger.warning(f'Error closing browser: {str(e)}')
