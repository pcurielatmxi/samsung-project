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


class ProjectSightSession:
    """Manages ProjectSight browser session with login and persistence."""

    # Project constants
    ORG_ID = "ffd5880a-42ec-41fa-a552-db0c9a000326"
    PROJECT_ID = "300"
    TRIMBLE_PROJECT_ID = "jFeM-GUk7QI"  # Trimble Connect project ID
    ROOT_FOLDER_ID = "GBVZIZkHPRc"  # Library root folder

    # URL templates
    BASE_URL = "https://prod.projectsightapp.trimble.com"
    LIBRARY_URL = f"{BASE_URL}/web/app/Project?pt=205&orgid={ORG_ID}&projid={PROJECT_ID}"
    DAILY_REPORTS_URL = f"{BASE_URL}/web/app/Project?listid=-4038&orgid={ORG_ID}&projid={PROJECT_ID}"

    def __init__(self, headless: bool = False, session_dir: str = None):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

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
        target_url = target_url or self.LIBRARY_URL
        print("Checking session / logging in to ProjectSight...")

        try:
            self.page.goto(target_url, wait_until='domcontentloaded', timeout=60000)
            time.sleep(3)

            # Check if already logged in
            if 'projectsight' in self.page.url.lower() and 'id.trimble.com' not in self.page.url:
                print("  Session still valid - no login needed!")
                return True

            # Need to login
            if 'id.trimble.com' in self.page.url or 'sign_in' in self.page.url:
                print("  Step 1: Entering username...")

                # Handle cookie consent if present
                try:
                    accept_btn = self.page.locator('button:has-text("Accept All")')
                    if accept_btn.count() > 0:
                        accept_btn.click()
                        time.sleep(1)
                except:
                    pass

                # Wait for username field
                self.page.wait_for_selector('#username-field', timeout=15000)

                # Enter username
                username_input = self.page.locator('#username-field')
                username_input.click()
                username_input.fill('')
                username_input.type(self.username, delay=50)
                username_input.press('Tab')
                time.sleep(1)

                # Wait for Next button to be enabled and click
                self.page.wait_for_function(
                    "document.querySelector('#enter_username_submit') && !document.querySelector('#enter_username_submit').disabled",
                    timeout=10000
                )
                self.page.locator('#enter_username_submit').click()
                time.sleep(2)

                print("  Step 2: Entering password...")

                # Wait for password field
                self.page.wait_for_selector('input[name="password"]:visible', timeout=15000)

                # Enter password
                password_input = self.page.locator('input[name="password"]')
                password_input.click()
                password_input.type(self.password, delay=50)
                password_input.press('Tab')
                time.sleep(1)

                # Wait for Sign in button to be enabled and click
                self.page.wait_for_function(
                    "document.querySelector('button[name=\"password-submit\"]') && !document.querySelector('button[name=\"password-submit\"]').disabled",
                    timeout=10000
                )
                self.page.locator('button[name="password-submit"]').click()

                # Wait for redirect to projectsight
                print("  Waiting for login to complete...")
                for attempt in range(90):
                    time.sleep(1)
                    if 'projectsight' in self.page.url.lower():
                        print(f"  Redirected to projectsight after {attempt+1}s")
                        self.page.wait_for_load_state('networkidle', timeout=60000)
                        break
                    if attempt % 10 == 0:
                        print(f"    Still waiting... ({attempt}s)")
                else:
                    print("  Timeout waiting for projectsight redirect")
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
