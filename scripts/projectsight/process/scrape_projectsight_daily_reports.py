#!/usr/bin/env python3
"""
ProjectSight Daily Reports Scraper

Extracts daily reports from ProjectSight including both tabs:
- Daily Report tab (Weather, Labor, Equipment, Notes, Status)
- History tab (Created by, timestamps, field changes)

RECOMMENDED: Run on Windows with headed mode (real display).
The script uses Chromium by default and navigates using the "Next record" button.

Usage:
    # On Windows - best approach (headed mode with real display)
    python scripts/projectsight/process/scrape_projectsight_daily_reports.py --limit 10

    # On Linux with Xvfb (virtual display)
    xvfb-run -a python scripts/projectsight/process/scrape_projectsight_daily_reports.py --limit 10

    # Full extraction (all 415 records)
    python scripts/projectsight/process/scrape_projectsight_daily_reports.py --limit 0

    # Idempotent mode - skip already extracted reports and merge with existing data
    python scripts/projectsight/process/scrape_projectsight_daily_reports.py --skip-existing --limit 50

    # Continue extraction from where you left off (run multiple times safely)
    python scripts/projectsight/process/scrape_projectsight_daily_reports.py --skip-existing --limit 0

Environment Variables (from .env):
    PROJECTSIGHT_USERNAME - Login email
    PROJECTSIGHT_PASSWORD - Login password
    PROJECTSIGHT_HEADLESS - Set to 'true' for headless mode (default: false)
    PROJECTSIGHT_USE_FIREFOX - Set to 'true' for Firefox (default: false = Chromium)

Installation:
    pip install playwright python-dotenv
    playwright install chromium
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

# Load environment variables (override=True to use .env values over shell env)
load_dotenv(override=True)


def create_scraper():
    """Create and configure Playwright scraper."""
    from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext

    class ProjectSightScraper:
        """Scraper for ProjectSight Daily Reports."""

        def __init__(self, headless: bool = False, session_dir: str = None):
            self.headless = headless
            self.playwright = None
            self.browser: Optional[Browser] = None
            self.context: Optional[BrowserContext] = None
            self.page: Optional[Page] = None

            # Session persistence
            self.session_dir = Path(session_dir) if session_dir else Path.home() / '.projectsight_sessions'
            self.session_file = self.session_dir / 'session_state.json'
            self.session_dir.mkdir(parents=True, exist_ok=True)

            # URLs
            self.base_url = os.getenv('PROJECTSIGHT_BASE_URL', 'https://prod.projectsightapp.trimble.com/')
            self.login_url = os.getenv('PROJECTSIGHT_LOGIN_URL', 'https://id.trimble.com/ui/sign_in.html')

            # Credentials
            self.username = os.getenv('PROJECTSIGHT_USERNAME')
            self.password = os.getenv('PROJECTSIGHT_PASSWORD')

            if not self.username or not self.password:
                raise ValueError("PROJECTSIGHT_USERNAME and PROJECTSIGHT_PASSWORD must be set in .env")

            # Project-specific URL (Taylor Fab1 58202 - Yates Construction Portfolio)
            self.daily_reports_url = (
                "https://prod.projectsightapp.trimble.com/web/app/Project"
                "?listid=-4038&orgid=ffd5880a-42ec-41fa-a552-db0c9a000326&projid=300"
            )

        def start(self):
            """Start the browser, loading existing session if available."""
            self.playwright = sync_playwright().start()

            # Choose browser - default to Chromium (better on Windows)
            # Set PROJECTSIGHT_USE_FIREFOX=true to use Firefox instead
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
                    # Check if session is not too old (7 days)
                    session_age = time.time() - self.session_file.stat().st_mtime
                    max_age = int(os.getenv('PROJECTSIGHT_SESSION_VALIDITY_DAYS', '7')) * 86400
                    if session_age < max_age:
                        storage_state = str(self.session_file)
                        print(f"  Loading saved session from {self.session_file}")
                    else:
                        print(f"  Session expired ({session_age/86400:.1f} days old), will re-login")
                        self.session_file.unlink()  # Delete expired session
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
            self.page.set_default_timeout(30000)  # 30 second timeout

            print(f"Browser started (headless={self.headless})")

        def save_session(self):
            """Save current session state for future reuse."""
            try:
                self.context.storage_state(path=str(self.session_file))
                print(f"  Session saved to {self.session_file}")
            except Exception as e:
                print(f"  Error saving session: {e}")

        def stop(self):
            """Stop the browser."""
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
            print("Browser stopped")

        def login(self) -> bool:
            """Login to ProjectSight via Trimble SSO (two-step login)."""
            print("Checking session / logging in to ProjectSight...")

            try:
                # Navigate to daily reports (will redirect to login if session invalid)
                self.page.goto(self.daily_reports_url, wait_until='networkidle')
                time.sleep(2)

                # Check if we're already logged in (session still valid)
                if 'projectsight' in self.page.url.lower() and 'id.trimble.com' not in self.page.url:
                    print("  Session still valid - no login needed!")
                    return True

                # Check if we need to login
                if 'id.trimble.com' in self.page.url or 'sign_in' in self.page.url:
                    print("  Step 1: Entering username...")

                    # Wait for username field (Trimble uses id=username-field)
                    self.page.wait_for_selector('#username-field', timeout=15000)

                    # Enter username - use type() to trigger JS validation
                    username_input = self.page.locator('#username-field')
                    username_input.click()
                    username_input.fill('')  # Clear first
                    username_input.type(self.username, delay=50)  # Type with delay to trigger validation

                    # Trigger blur/change events to ensure validation runs
                    username_input.press('Tab')
                    time.sleep(1)

                    # Wait for the Next button to become enabled after username entry
                    next_btn = self.page.locator('#enter_username_submit')
                    # Wait for button to be enabled (not just visible)
                    self.page.wait_for_function(
                        "document.querySelector('#enter_username_submit') && !document.querySelector('#enter_username_submit').disabled",
                        timeout=10000
                    )
                    next_btn.click()
                    time.sleep(2)

                    print("  Step 2: Entering password...")

                    # Wait for password field to become visible (use specific name selector)
                    self.page.wait_for_selector('input[name="password"]:visible', timeout=15000)

                    # Enter password - use type() to trigger JS validation
                    password_input = self.page.locator('input[name="password"]')
                    password_input.click()
                    password_input.type(self.password, delay=50)

                    # Trigger validation
                    password_input.press('Tab')
                    time.sleep(1)

                    # Wait for sign in button to be enabled
                    self.page.wait_for_function(
                        "document.querySelector('button[name=\"password-submit\"]') && !document.querySelector('button[name=\"password-submit\"]').disabled",
                        timeout=10000
                    )
                    sign_in_btn = self.page.locator('button[name="password-submit"]')
                    sign_in_btn.click()

                    # Wait a moment and take a debug screenshot
                    time.sleep(3)
                    self.page.screenshot(path='/tmp/after_login_click.png')
                    print(f"  Debug screenshot saved to /tmp/after_login_click.png")
                    print(f"  Current URL after click: {self.page.url[:80]}...")

                    # Check for error messages on page
                    error_msg = self.page.locator('.error, .alert-danger, [class*="error"]').first
                    if error_msg.count() > 0:
                        print(f"  Error on page: {error_msg.text_content()}")

                    # Wait for redirect to projectsight (can take a while due to SSO redirects)
                    print("  Waiting for login to complete (this may take 30-60 seconds)...")

                    # Poll for projectsight URL (up to 90 seconds)
                    for attempt in range(90):
                        time.sleep(1)
                        current_url = self.page.url.lower()
                        if 'projectsight' in current_url:
                            print(f"  Redirected to projectsight after {attempt+1}s")
                            # Wait for page to fully load
                            self.page.wait_for_load_state('networkidle', timeout=60000)
                            break
                        if attempt % 10 == 0:
                            print(f"    Still waiting... ({attempt}s, current: {self.page.url[:60]}...)")
                    else:
                        print(f"  Timeout waiting for projectsight redirect")

                # Handle any alert dialogs
                self.page.on('dialog', lambda dialog: dialog.accept())

                # Verify we're on the right page
                if 'projectsight' in self.page.url.lower():
                    print("  Login successful!")
                    # Save session for future reuse
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

        def navigate_to_daily_reports(self) -> bool:
            """Navigate to the Daily Reports list."""
            print("Navigating to Daily Reports...")

            try:
                self.page.goto(self.daily_reports_url, wait_until='networkidle')
                time.sleep(3)

                # Handle any dialogs
                try:
                    self.page.locator('button:has-text("OK"), button:has-text("Close")').click(timeout=2000)
                except:
                    pass

                # Wait for the list iframe to load
                list_frame = self.page.frame_locator('iframe[name="fraMenuContent"]')
                list_frame.locator('.ui-iggrid').wait_for(timeout=15000)

                print("  Daily Reports list loaded")
                return True

            except Exception as e:
                print(f"  Navigation error: {e}")
                return False

        def get_report_count(self) -> int:
            """Get total number of daily reports."""
            try:
                list_frame = self.page.frame_locator('iframe[name="fraMenuContent"]')
                count_text = list_frame.locator('text=/Viewing \\d+ Daily reports/').text_content(timeout=5000)
                count = int(count_text.split()[1])
                return count
            except:
                return 0

        def get_report_list(self) -> List[Dict]:
            """Get list of all reports from the grid."""
            print("Extracting report list from grid...")

            reports = []
            list_frame = self.page.frame_locator('iframe[name="fraMenuContent"]')

            # Get all rows from the grid
            rows = list_frame.locator('tr[data-id]').all()

            for row in rows:
                try:
                    cells = row.locator('td').all()
                    if len(cells) >= 7:
                        report = {
                            'date': cells[1].text_content().strip(),
                            'status': cells[2].text_content().strip(),
                            'weather': cells[3].text_content().strip(),
                            'hours': cells[4].text_content().strip(),
                            'comments': cells[5].text_content().strip(),
                            'links': cells[6].text_content().strip(),
                        }
                        reports.append(report)
                except Exception as e:
                    continue

            print(f"  Found {len(reports)} reports in grid")
            return reports

        def click_first_report(self) -> bool:
            """Click on the first report row to open detail view."""
            return self.click_report_at_position(0)

        def click_report_at_position(self, position: int) -> bool:
            """Click on a report row at a specific position (0-indexed) to open detail view.

            This allows starting extraction from any row in the grid, enabling
            efficient skip of already-extracted records.
            """
            try:
                list_frame = self.page.frame_locator('iframe[name="fraMenuContent"]')

                # Get all rows and click on the specified position
                rows = list_frame.locator('tr[data-id]')
                row_count = rows.count()

                if position >= row_count:
                    print(f"    Position {position} exceeds available rows ({row_count})")
                    return False

                # Click on the row at the specified position
                target_row = rows.nth(position)
                target_row.locator('td').nth(1).click()
                time.sleep(3)  # Give time for detail panel to load

                # Wait for detail panel to load - use more specific selector
                detail_frame = self.page.frame_locator('iframe[name="fraDef"]')
                # Wait for the tab button specifically (has class "tabButton")
                detail_frame.locator('.tabButton, [data-gainsight*="tab"]').first.wait_for(timeout=15000)

                return True
            except Exception as e:
                print(f"    Error clicking report at position {position}: {e}")
                return False

        def click_next_record(self) -> bool:
            """Click the 'Next record' button to go to next report."""
            try:
                detail_frame = self.page.frame_locator('iframe[name="fraDef"]')

                # Find and click the Next record button
                next_btn = detail_frame.locator('[title="Next record"] a, [title="Next record"]').first
                next_btn.click()
                time.sleep(1.5)

                return True
            except Exception as e:
                print(f"    Error clicking next: {e}")
                return False

        def get_current_record_number(self) -> str:
            """Get the current record number (e.g., '5 of 415')."""
            try:
                detail_frame = self.page.frame_locator('iframe[name="fraDef"]')
                record_text = detail_frame.locator('text=/\\d+ of \\d+/').first.text_content(timeout=3000)
                return record_text.strip()
            except:
                return "unknown"

        def extract_daily_report_tab(self) -> Dict:
            """Extract data from the Daily Report tab."""
            data = {
                'weather': {},
                'labor': {},
                'equipment': {},
                'notes': {},
                'details': {},
                'raw_content': ''
            }

            try:
                detail_frame = self.page.frame_locator('iframe[name="fraDef"]')

                # Click on Daily report tab to ensure it's active
                daily_tab = detail_frame.locator('text="Daily report"').first
                if daily_tab.count() > 0:
                    daily_tab.click()
                    time.sleep(1)

                # Get full page content for this tab
                try:
                    # Screenshot for debugging (optional)
                    # self.page.screenshot(path=f'/tmp/daily_report_tab.png')

                    # Get all text from the detail frame
                    all_text = detail_frame.locator('body').inner_text(timeout=5000)
                    data['raw_content'] = all_text[:5000] if all_text else ''  # Limit size
                except:
                    pass

                # Extract specific sections with better selectors

                # Weather section - look for weather card/section
                try:
                    weather_card = detail_frame.locator('[class*="weather"], [data-section="weather"]').first
                    if weather_card.count() > 0:
                        data['weather']['section_text'] = weather_card.text_content(timeout=2000)
                except:
                    pass

                # Extract right panel details (date, status, etc.)
                try:
                    # Date field - usually a date picker input
                    date_inputs = detail_frame.locator('input[type="date"], input[placeholder*="date"], input.date-input').all()
                    for inp in date_inputs[:3]:
                        try:
                            val = inp.input_value(timeout=1000)
                            if val:
                                data['details']['date'] = val
                                break
                        except:
                            continue

                    # Also try disabled input (display-only date)
                    if not data['details'].get('date'):
                        disabled_input = detail_frame.locator('input[disabled]').first
                        if disabled_input.count() > 0:
                            data['details']['date'] = disabled_input.input_value(timeout=1000)

                    # Status dropdown
                    status_select = detail_frame.locator('select, [role="combobox"]').first
                    if status_select.count() > 0:
                        data['details']['status'] = status_select.text_content(timeout=1000)

                except Exception as e:
                    data['details']['error'] = str(e)

                # Labor summary
                try:
                    labor_section = detail_frame.locator('text=/Labor|Workers|Workforce/i').first
                    if labor_section.count() > 0:
                        parent = labor_section.locator('..').locator('..')
                        data['labor']['section_text'] = parent.text_content(timeout=2000)
                except:
                    pass

                # Equipment summary
                try:
                    equip_section = detail_frame.locator('text=/Equipment/i').first
                    if equip_section.count() > 0:
                        parent = equip_section.locator('..').locator('..')
                        data['equipment']['section_text'] = parent.text_content(timeout=2000)
                except:
                    pass

            except Exception as e:
                data['error'] = str(e)
                print(f"    Error extracting Daily Report tab: {e}")

            return data

        def extract_report_date(self) -> Optional[str]:
            """Extract the report date from the header."""
            try:
                detail_frame = self.page.frame_locator('iframe[name="fraDef"]')
                # The date is shown in a disabled input or date display
                date_input = detail_frame.locator('input[disabled]').first
                return date_input.input_value(timeout=2000)
            except:
                return None

        def extract_history_tab(self) -> Dict:
            """Extract complete data from the History tab."""
            data = {
                'created_by': None,
                'created_at': None,
                'last_modified_by': None,
                'last_modified_at': None,
                'changes': [],
                'raw_content': '',
                'revision_entries': []
            }

            try:
                detail_frame = self.page.frame_locator('iframe[name="fraDef"]')

                # Click on History tab
                history_tab = detail_frame.locator('text="History"')
                if history_tab.count() > 0:
                    history_tab.first.click()
                    time.sleep(1.5)  # Give time for history to load

                    # Get the full raw content of the history tab
                    try:
                        all_text = detail_frame.locator('body').inner_text(timeout=5000)
                        data['raw_content'] = all_text[:10000] if all_text else ''  # Keep more for history
                    except:
                        pass

                    # Extract creation info - look for "Created by" pattern
                    try:
                        created_elements = detail_frame.locator('text=/Created by/i').all()
                        for elem in created_elements[:3]:
                            text = elem.text_content(timeout=1000)
                            if text and 'Created by' in text:
                                data['created_by'] = text.strip()
                                break
                    except:
                        pass

                    # Look for history entries/revisions
                    # Common patterns: timeline items, list items, cards
                    history_selectors = [
                        '.history-item',
                        '.timeline-item',
                        '.revision-entry',
                        '.audit-entry',
                        '[class*="history"]',
                        '.mat-list-item',
                        'mat-expansion-panel',
                        '.change-entry',
                        'tr[class*="history"]',
                        '.activity-item'
                    ]

                    for selector in history_selectors:
                        try:
                            items = detail_frame.locator(selector).all()
                            if items and len(items) > 0:
                                for item in items[:100]:  # Limit to 100 entries
                                    try:
                                        entry_text = item.text_content(timeout=1000)
                                        if entry_text and entry_text.strip():
                                            data['revision_entries'].append({
                                                'text': entry_text.strip(),
                                                'selector': selector
                                            })
                                    except:
                                        continue
                                if data['revision_entries']:
                                    break  # Found entries, stop trying other selectors
                        except:
                            continue

                    # If no structured entries found, try to extract from raw text
                    if not data['revision_entries'] and data['raw_content']:
                        # Split by common patterns
                        import re
                        # Look for date patterns that might separate entries
                        date_pattern = r'(\d{1,2}/\d{1,2}/\d{4})'
                        parts = re.split(date_pattern, data['raw_content'])

                        # Reconstruct entries
                        i = 0
                        while i < len(parts) - 1:
                            if re.match(date_pattern, parts[i]):
                                entry = parts[i]
                                if i + 1 < len(parts):
                                    entry += parts[i + 1]
                                data['changes'].append(entry.strip())
                                i += 2
                            else:
                                i += 1

                    # Extract timestamps - look for date/time patterns
                    try:
                        time_elements = detail_frame.locator('text=/\\d{1,2}:\\d{2}|\\d{1,2}\\/\\d{1,2}\\/\\d{4}/').all()
                        timestamps = []
                        for elem in time_elements[:20]:
                            try:
                                timestamps.append(elem.text_content(timeout=500))
                            except:
                                pass
                        if timestamps:
                            data['timestamps'] = timestamps
                    except:
                        pass

                    # Look for "Modified by" or "Updated by" entries
                    try:
                        modified_elements = detail_frame.locator('text=/Modified by|Updated by|Changed by/i').all()
                        for elem in modified_elements[:5]:
                            text = elem.text_content(timeout=1000)
                            if text:
                                if not data['last_modified_by']:
                                    data['last_modified_by'] = text.strip()
                                data['changes'].append(text.strip())
                    except:
                        pass

            except Exception as e:
                data['error'] = str(e)
                print(f"    Error extracting History tab: {e}")

            return data

        def extract_additional_info_tab(self) -> Dict:
            """Extract data from the Additional Info tab."""
            data = {
                'raw_content': '',
                'fields': {}
            }

            try:
                detail_frame = self.page.frame_locator('iframe[name="fraDef"]')

                # Click on Additional Info tab
                additional_info_tab = detail_frame.locator('text="Additional Info"').first
                if additional_info_tab.count() > 0:
                    additional_info_tab.click()
                    time.sleep(1)

                    # Get the full raw content
                    try:
                        all_text = detail_frame.locator('body').inner_text(timeout=5000)
                        data['raw_content'] = all_text[:10000] if all_text else ''
                    except:
                        pass

                    # Try to extract labeled fields
                    try:
                        labels = detail_frame.locator('label, .field-label, .mat-form-field-label').all()
                        for label in labels[:50]:
                            try:
                                label_text = label.text_content(timeout=500)
                                if label_text:
                                    label_text = label_text.strip().rstrip(':')
                                    parent = label.locator('..')
                                    value_elem = parent.locator('input, select, textarea, .value, span').first
                                    if value_elem.count() > 0:
                                        try:
                                            value = value_elem.input_value(timeout=500)
                                        except:
                                            value = value_elem.text_content(timeout=500)
                                        if value:
                                            data['fields'][label_text] = value.strip()
                            except:
                                continue
                    except:
                        pass

            except Exception as e:
                data['error'] = str(e)
                print(f"    Error extracting Additional Info tab: {e}")

            return data

        def extract_current_report(self) -> Dict:
            """Extract data from the currently open report.

            Extracts data from exactly 3 tabs:
            - Daily report: Weather, labor, equipment, notes, status
            - Additional Info: Extra metadata fields
            - History: Audit trail, created/modified by, changes
            """
            record_num = self.get_current_record_number()
            report_date = self.extract_report_date()

            print(f"    Extracting record {record_num}, date: {report_date}")

            report_data = {
                'recordNumber': record_num,
                'reportDate': report_date,
                'extractedAt': datetime.now().isoformat(),
                'dailyReport': self.extract_daily_report_tab(),
                'additionalInfo': self.extract_additional_info_tab(),
                'history': self.extract_history_tab(),
            }

            return report_data

        def extract_all_reports(self, limit: Optional[int] = None, skip_dates: Optional[set] = None) -> List[Dict]:
            """Extract all reports using Next record navigation.

            Args:
                limit: Maximum number of NEW reports to extract (0 = all remaining)
                skip_dates: Set of report dates to skip (for idempotent extraction)

            Returns:
                List of extracted report dictionaries

            Optimization: When skip_dates is provided, the extraction starts directly
            from position len(skip_dates) in the grid, avoiding sequential navigation
            through already-extracted records.
            """
            reports = []
            skip_dates = skip_dates or set()

            total_count = self.get_report_count()
            print(f"Total reports available: {total_count}")

            # Calculate starting position - skip directly to first un-extracted record
            start_position = len(skip_dates) if skip_dates else 0
            remaining_count = total_count - start_position

            if remaining_count <= 0:
                print(f"All {total_count} reports already extracted!")
                return reports

            # Calculate how many to extract
            if limit and limit > 0:
                extract_count = min(limit, remaining_count)
            else:
                extract_count = remaining_count

            print(f"Starting from position: {start_position + 1} (skipping {start_position} already extracted)")
            print(f"Will extract: {extract_count} new reports")

            # Click on the first un-extracted report (at start_position)
            print(f"Opening report at position {start_position + 1}...")
            if not self.click_report_at_position(start_position):
                print(f"Failed to open report at position {start_position}!")
                return reports

            # Extract first report
            report = self.extract_current_report()
            reports.append(report)
            print(f"  Extracted 1/{extract_count}")

            # Navigate through remaining reports using Next record button
            for i in range(1, extract_count):
                if not self.click_next_record():
                    print(f"  Failed to navigate to record {i + 1}")
                    break

                time.sleep(0.5)  # Wait for content to load

                report = self.extract_current_report()
                reports.append(report)
                print(f"  Extracted {i + 1}/{extract_count}")

            return reports

        def _safe_get_text(self, frame, selector: str) -> Optional[str]:
            """Safely get text content from a selector."""
            try:
                element = frame.locator(selector).first
                return element.text_content(timeout=2000)
            except:
                return None

        def _safe_get_input_value(self, frame, selector: str) -> Optional[str]:
            """Safely get input value."""
            try:
                element = frame.locator(selector).first
                return element.input_value(timeout=2000)
            except:
                return None

        def _safe_get_combobox_value(self, frame, selector: str) -> Optional[str]:
            """Safely get combobox value."""
            try:
                parent = frame.locator(selector).first.locator('..')
                combobox = parent.locator('[role="combobox"], select').first
                return combobox.text_content(timeout=2000)
            except:
                return None

    return ProjectSightScraper


def load_existing_data(output_file: Path) -> tuple[dict, set]:
    """Load existing extraction data and return (data, extracted_dates)."""
    if not output_file.exists():
        return None, set()

    try:
        with open(output_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Extract dates from existing records
        extracted_dates = set()
        for record in data.get('records', []):
            report_date = record.get('reportDate')
            if report_date:
                extracted_dates.add(report_date)

        print(f"  Loaded {len(extracted_dates)} existing records from {output_file}")
        return data, extracted_dates
    except Exception as e:
        print(f"  Warning: Could not load existing data: {e}")
        return None, set()


def merge_records(existing_records: list, new_records: list) -> list:
    """Merge new records with existing, avoiding duplicates by date."""
    # Use report date as the unique key
    records_by_date = {}

    # Add existing records first
    for record in existing_records:
        date = record.get('reportDate')
        if date:
            records_by_date[date] = record

    # Update/add new records (newer extractions overwrite older ones)
    for record in new_records:
        date = record.get('reportDate')
        if date:
            records_by_date[date] = record

    # Return sorted by date (most recent first)
    return sorted(records_by_date.values(),
                  key=lambda x: x.get('reportDate', ''),
                  reverse=True)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Scrape ProjectSight Daily Reports')
    parser.add_argument('--limit', type=int, default=10, help='Limit number of reports to extract')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    parser.add_argument('--output', type=str, default=None, help='Output file path')
    parser.add_argument('--skip-existing', action='store_true',
                        help='Skip reports that have already been extracted (idempotent mode)')
    args = parser.parse_args()

    # Check headless setting from env
    headless = args.headless or os.getenv('PROJECTSIGHT_HEADLESS', 'false').lower() == 'true'

    print("=" * 60)
    print("ProjectSight Daily Reports Scraper")
    print("=" * 60)
    print(f"Headless mode: {headless}")
    print(f"Record limit: {args.limit}")
    print(f"Skip existing: {args.skip_existing}")

    # Create output path using project settings
    try:
        # Add project root to path for imports
        project_root = Path(__file__).parent.parent.parent.parent
        sys.path.insert(0, str(project_root))
        from src.config.settings import Settings
        output_dir = Settings.PROJECTSIGHT_RAW_DIR / 'extracted'
    except ImportError:
        # Fallback if settings not available
        output_dir = Path(__file__).parent.parent.parent.parent / 'data' / 'projectsight' / 'extracted'
    output_dir.mkdir(parents=True, exist_ok=True)

    # Use a consistent output file name for idempotent mode
    if args.skip_existing:
        output_file = Path(args.output) if args.output else output_dir / 'daily_reports.json'
    else:
        output_file = Path(args.output) if args.output else output_dir / f'daily_reports_test_{args.limit}.json'

    # Load existing data if in idempotent mode
    existing_data, extracted_dates = None, set()
    if args.skip_existing:
        existing_data, extracted_dates = load_existing_data(output_file)
        if extracted_dates:
            print(f"  Will skip {len(extracted_dates)} already-extracted dates")

    # Create and run scraper
    ScraperClass = create_scraper()
    scraper = ScraperClass(headless=headless)

    try:
        scraper.start()

        if not scraper.login():
            print("Login failed!")
            return 1

        if not scraper.navigate_to_daily_reports():
            print("Navigation failed!")
            return 1

        reports = scraper.extract_all_reports(
            limit=args.limit,
            skip_dates=extracted_dates if args.skip_existing else None
        )

        # Merge with existing records if in idempotent mode
        if args.skip_existing and existing_data:
            existing_records = existing_data.get('records', [])
            all_records = merge_records(existing_records, reports)
            new_count = len(reports)
            total_count = len(all_records)
        else:
            all_records = reports
            new_count = len(reports)
            total_count = len(reports)

        # Save results
        output_data = {
            'extractedAt': datetime.now().isoformat(),
            'source': 'ProjectSight Standalone Scraper',
            'project': 'Yates Construction Portfolio > Taylor Fab1 58202',
            'totalAvailable': scraper.get_report_count(),
            'extractedCount': total_count,
            'records': all_records
        }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        print(f"\nExtraction complete!")
        if args.skip_existing:
            print(f"Extracted {new_count} new reports (total: {total_count})")
        else:
            print(f"Extracted {len(reports)} reports")
        print(f"Output saved to: {output_file}")

        return 0

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        scraper.stop()


if __name__ == '__main__':
    sys.exit(main())
