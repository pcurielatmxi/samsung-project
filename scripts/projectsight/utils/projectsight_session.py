#!/usr/bin/env python3
"""
ProjectSight Session Manager

Shared utilities for managing ProjectSight login and session persistence
across multiple scraping scripts.

Usage:
    from scripts.projectsight.utils.projectsight_session import ProjectSightSession

    with ProjectSightSession(headless=True) as session:
        session.navigate_to(url)
        # ... do work with session.page
"""

import os
import time
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load environment variables (override=True to use .env values over shell env)
load_dotenv(override=True)


class ProjectConfig:
    """Configuration for a ProjectSight project."""

    def __init__(self, name: str, org_id: str, project_id: str,
                 trimble_project_id: str, root_folder_id: str):
        self.name = name
        self.org_id = org_id
        self.project_id = project_id
        self.trimble_project_id = trimble_project_id
        self.root_folder_id = root_folder_id

    @property
    def library_url(self) -> str:
        return f"{ProjectSightSession.BASE_URL}/web/app/Project?pt=205&orgid={self.org_id}&projid={self.project_id}"

    @property
    def daily_reports_url(self) -> str:
        return f"{ProjectSightSession.BASE_URL}/web/app/Project?listid=-4038&orgid={self.org_id}&projid={self.project_id}"

    @property
    def ncr_list_url(self) -> str:
        """URL for NCR/QOR/SOR/SWN/VR records list (Quality non-conformance records)."""
        return f"{ProjectSightSession.BASE_URL}/web/app/Project?listid=-4067&orgid={self.org_id}&projid={self.project_id}"

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'orgId': self.org_id,
            'projectId': self.project_id,
            'trimbleProjectId': self.trimble_project_id,
            'rootFolderId': self.root_folder_id,
        }


# Available projects - add more as needed
PROJECTS = {
    'taylor_fab1': ProjectConfig(
        name='Taylor Fab1 58202',
        org_id='ffd5880a-42ec-41fa-a552-db0c9a000326',
        project_id='300',
        trimble_project_id='jFeM-GUk7QI',
        root_folder_id='GBVZIZkHPRc',
    ),
    'tpjt_fab1': ProjectConfig(
        name='T-PJT FAB1 Construction',
        org_id='4540f425-f7b5-4ad8-837d-c270d5d09490',
        project_id='3',
        trimble_project_id='TKiTy1XRugw',
        root_folder_id='NDbcNje2RPA',
    ),
}

DEFAULT_PROJECT = 'taylor_fab1'


class ProjectSightSession:
    """Manages ProjectSight browser session with login and persistence."""

    BASE_URL = "https://prod.projectsightapp.trimble.com"

    def __init__(self, headless: bool = False, session_dir: str = None,
                 project: str = None):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        # Project configuration
        project_key = project or DEFAULT_PROJECT
        if project_key not in PROJECTS:
            raise ValueError(f"Unknown project '{project_key}'. Available: {list(PROJECTS.keys())}")
        self.project = PROJECTS[project_key]

        # Session persistence
        self.session_dir = Path(session_dir) if session_dir else Path.home() / '.projectsight_sessions'
        self.session_file = self.session_dir / 'session_state.json'
        self.session_dir.mkdir(parents=True, exist_ok=True)

        # Credentials
        self.username = os.getenv('PROJECTSIGHT_USERNAME')
        self.password = os.getenv('PROJECTSIGHT_PASSWORD')

        if not self.username or not self.password:
            raise ValueError("PROJECTSIGHT_USERNAME and PROJECTSIGHT_PASSWORD must be set in .env")

    def __enter__(self):
        """Context manager entry - start browser."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - stop browser."""
        self.stop()
        return False

    def start(self):
        """Start the browser, loading existing session if available."""
        from playwright.sync_api import sync_playwright

        self.playwright = sync_playwright().start()

        # Choose browser - default to Chromium
        use_firefox = os.getenv('PROJECTSIGHT_USE_FIREFOX', 'false').lower() == 'true'

        if use_firefox:
            self.browser = self.playwright.firefox.launch(
                headless=self.headless,
                slow_mo=100,
            )
        else:
            self.browser = self.playwright.chromium.launch(
                headless=self.headless,
                slow_mo=100,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-gpu',
                    '--disable-dev-shm-usage',
                ]
            )

        # Check if we have a saved session
        storage_state = None
        if self.session_file.exists():
            try:
                session_age = time.time() - self.session_file.stat().st_mtime
                max_age = int(os.getenv('PROJECTSIGHT_SESSION_VALIDITY_DAYS', '7')) * 86400
                if session_age < max_age:
                    storage_state = str(self.session_file)
                    print(f"  Loading saved session from {self.session_file}")
                else:
                    print(f"  Session expired ({session_age/86400:.1f} days old), will re-login")
                    self.session_file.unlink()
            except Exception as e:
                print(f"  Error loading session: {e}")

        # Create context with realistic viewport and optional saved session
        context_args = {
            'viewport': {'width': 1920, 'height': 1080},
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        if storage_state:
            context_args['storage_state'] = storage_state

        self.context = self.browser.new_context(**context_args)
        self.page = self.context.new_page()
        self.page.set_default_timeout(60000)  # 60 second timeout

        print(f"Browser started (headless={self.headless})")

    def stop(self):
        """Stop the browser."""
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        print("Browser stopped")

    def save_session(self):
        """Save current session state for future reuse."""
        try:
            self.context.storage_state(path=str(self.session_file))
            print(f"  Session saved to {self.session_file}")
        except Exception as e:
            print(f"  Error saving session: {e}")

    def login(self, target_url: str = None) -> bool:
        """Login to ProjectSight via Trimble SSO (two-step login).

        Args:
            target_url: URL to navigate to (will redirect to login if needed)

        Returns:
            True if login successful or session valid
        """
        target_url = target_url or self.project.library_url
        print("Checking session / logging in to ProjectSight...")

        try:
            self.page.goto(target_url, wait_until='domcontentloaded', timeout=60000)
            time.sleep(3)

            # Check if login is needed - either via redirect or login iframe
            needs_login = False
            if 'id.trimble.com' in self.page.url or 'sign_in' in self.page.url:
                needs_login = True
                print("  Redirected to login page")
            else:
                # Check for login iframe (session expired but no redirect)
                # Wait longer for iframe to appear
                print(f"  Checking for login/library iframe...")
                for check in range(45):  # Wait up to 45 seconds
                    time.sleep(1)

                    # Check if we got redirected to login page
                    if 'id.trimble.com' in self.page.url:
                        needs_login = True
                        print(f"  Redirected to login page after {check+1}s")
                        break

                    # Use DOM selectors to detect iframes (more reliable than page.frames)
                    iframe_count = self.page.locator('iframe').count()

                    # Check for login iframe using page content
                    login_iframe_count = self.page.locator('iframe[src*="id.trimble.com"]').count()

                    # Check for library iframe (fraMenuContent)
                    library_iframe_count = self.page.locator('iframe[name="fraMenuContent"]').count()

                    if (check + 1) % 5 == 0:  # Print every 5 seconds
                        print(f"    Check {check+1}: total={iframe_count}, login={login_iframe_count}, library={library_iframe_count}")

                    if login_iframe_count > 0:
                        needs_login = True
                        print(f"  Login iframe detected after {check+1}s - session expired")
                        break

                    if library_iframe_count > 0:
                        # Library iframe found - now check its content
                        # The library iframe should have Trimble Connect content if session is valid
                        print(f"  Library iframe found after {check+1}s, checking content...")
                        time.sleep(3)  # Give it time to load

                        # Check if login is within the library iframe
                        frame_loc = self.page.frame_locator('iframe[name="fraMenuContent"]')
                        inner_login = frame_loc.locator('iframe[src*="id.trimble.com"]')
                        if inner_login.count() > 0:
                            needs_login = True
                            print(f"  Login iframe found inside fraMenuContent - session expired")
                            break
                        else:
                            print(f"  Library iframe loaded without login - session is valid")
                            break

            if not needs_login:
                # Check if projectsight content is loading
                if 'projectsight' in self.page.url.lower():
                    print("  Session still valid - no login needed!")
                    return True

            # Need to login
            if needs_login:
                # Determine if login is in iframe or on main page
                login_frame = None
                if 'id.trimble.com' in self.page.url:
                    # Direct redirect - login on main page
                    login_context = self.page
                    print("  Login on main page")
                else:
                    # Login in iframe - find the login frame
                    login_frames = [f for f in self.page.frames if f.url and 'id.trimble.com' in f.url]
                    if login_frames:
                        login_frame = login_frames[0]
                        login_context = login_frame
                        print(f"  Login in iframe: {login_frame.url[:50]}...")
                    else:
                        print("  Error: Could not find login context")
                        return False

                print("  Step 1: Entering username...")

                # Handle cookie consent if present (on main page)
                try:
                    accept_btn = self.page.locator('button:has-text("Accept All")')
                    if accept_btn.count() > 0:
                        accept_btn.click()
                        time.sleep(1)
                except:
                    pass

                # Wait for username field in the appropriate context
                login_context.wait_for_selector('#username-field', timeout=15000)

                # Enter username
                username_input = login_context.locator('#username-field')
                username_input.click()
                username_input.fill('')
                username_input.type(self.username, delay=50)
                username_input.press('Tab')
                time.sleep(1)

                # Wait for Next button to be enabled and click
                login_context.wait_for_function(
                    "document.querySelector('#enter_username_submit') && !document.querySelector('#enter_username_submit').disabled",
                    timeout=10000
                )
                login_context.locator('#enter_username_submit').click()
                time.sleep(2)

                print("  Step 2: Entering password...")

                # Wait for password field
                login_context.wait_for_selector('input[name="password"]:visible', timeout=15000)

                # Enter password
                password_input = login_context.locator('input[name="password"]')
                password_input.click()
                password_input.type(self.password, delay=50)
                password_input.press('Tab')
                time.sleep(1)

                # Wait for Sign in button to be enabled and click
                login_context.wait_for_function(
                    "document.querySelector('button[name=\"password-submit\"]') && !document.querySelector('button[name=\"password-submit\"]').disabled",
                    timeout=10000
                )
                login_context.locator('button[name="password-submit"]').click()
                time.sleep(3)

                # Check for login errors
                try:
                    error_msg = login_context.locator('.error, .alert-error, [role="alert"], .error-message')
                    if error_msg.count() > 0:
                        error_text = error_msg.first.text_content()
                        print(f"  Login error detected: {error_text}")
                except:
                    pass

                # Wait for login to complete
                print("  Waiting for login to complete...")
                for attempt in range(90):
                    time.sleep(1)
                    current_url = self.page.url
                    if attempt % 10 == 0:
                        print(f"    Still waiting... ({attempt}s) URL: {current_url[:50]}...")

                    # Check if redirected to projectsight
                    if 'projectsight' in current_url.lower() and 'id.trimble.com' not in current_url:
                        print(f"  Redirected to projectsight after {attempt+1}s")
                        self.page.wait_for_load_state('networkidle', timeout=60000)
                        break
                else:
                    print(f"  Timeout waiting for login completion. Final URL: {self.page.url}")
                    return False

            # Verify login success
            if 'projectsight' in self.page.url.lower():
                print("  Login successful!")
                self.save_session()
                return True
            else:
                print(f"  Login may have failed. Current URL: {self.page.url}")
                return False

        except Exception as e:
            print(f"  Login error: {e}")
            import traceback
            traceback.print_exc()
            return False

    def navigate_to(self, url: str, wait_for_iframe: bool = False) -> bool:
        """Navigate to a URL within ProjectSight.

        Args:
            url: URL to navigate to
            wait_for_iframe: If True, wait for fraMenuContent iframe to load
        """
        try:
            self.page.goto(url, wait_until='networkidle')
            time.sleep(2)

            if wait_for_iframe:
                self.page.frame_locator('iframe[name="fraMenuContent"]').locator('body').wait_for(timeout=15000)

            return True
        except Exception as e:
            print(f"  Navigation error: {e}")
            return False

    def get_library_frame(self):
        """Get the nested iframe locator for the library file explorer."""
        return self.page.frame_locator('iframe[name="fraMenuContent"]').frame_locator('#fmm-tc-emb')
