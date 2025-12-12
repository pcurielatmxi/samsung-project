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
    python scripts/scrape_projectsight_daily_reports.py --limit 10

    # On Linux with Xvfb (virtual display)
    xvfb-run -a python scripts/scrape_projectsight_daily_reports.py --limit 10

    # Full extraction (all 415 records)
    python scripts/scrape_projectsight_daily_reports.py --limit 0

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

# Load environment variables
load_dotenv()


def create_scraper():
    """Create and configure Playwright scraper."""
    from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext

    class ProjectSightScraper:
        """Scraper for ProjectSight Daily Reports."""

        def __init__(self, headless: bool = False):
            self.headless = headless
            self.playwright = None
            self.browser: Optional[Browser] = None
            self.context: Optional[BrowserContext] = None
            self.page: Optional[Page] = None

            # URLs
            self.base_url = os.getenv('PROJECTSIGHT_BASE_URL', 'https://prod.projectsightapp.trimble.com/')
            self.login_url = os.getenv('PROJECTSIGHT_LOGIN_URL', 'https://id.trimble.com/ui/sign_in.html')

            # Credentials
            self.username = os.getenv('PROJECTSIGHT_USERNAME')
            self.password = os.getenv('PROJECTSIGHT_PASSWORD')

            if not self.username or not self.password:
                raise ValueError("PROJECTSIGHT_USERNAME and PROJECTSIGHT_PASSWORD must be set in .env")

            # Project-specific URL
            self.daily_reports_url = (
                "https://prod.projectsightapp.trimble.com/web/app/Project"
                "?listid=-4038&orgid=4540f425-f7b5-4ad8-837d-c270d5d09490&projid=3"
            )

        def start(self):
            """Start the browser."""
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

            # Create context with realistic viewport
            self.context = self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )

            self.page = self.context.new_page()
            self.page.set_default_timeout(30000)  # 30 second timeout

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

        def login(self) -> bool:
            """Login to ProjectSight via Trimble SSO."""
            print("Logging in to ProjectSight...")

            try:
                # Navigate to daily reports (will redirect to login)
                self.page.goto(self.daily_reports_url, wait_until='networkidle')
                time.sleep(2)

                # Check if we need to login
                if 'id.trimble.com' in self.page.url or 'sign_in' in self.page.url:
                    print("  Entering credentials...")

                    # Wait for login form
                    self.page.wait_for_selector('input[type="text"], input[type="email"]', timeout=15000)

                    # Enter username
                    username_input = self.page.locator('input[type="text"], input[type="email"]').first
                    username_input.fill(self.username)

                    # Enter password
                    password_input = self.page.locator('input[type="password"]').first
                    password_input.fill(self.password)

                    # Click sign in
                    self.page.locator('button[type="submit"], input[type="submit"]').first.click()

                    # Wait for redirect
                    print("  Waiting for login to complete...")
                    self.page.wait_for_url('**/projectsight**', timeout=30000)
                    time.sleep(3)

                # Handle any alert dialogs
                self.page.on('dialog', lambda dialog: dialog.accept())

                # Verify we're on the right page
                if 'projectsight' in self.page.url.lower():
                    print("  Login successful!")
                    return True
                else:
                    print(f"  Login may have failed. Current URL: {self.page.url}")
                    return False

            except Exception as e:
                print(f"  Login error: {e}")
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
            try:
                list_frame = self.page.frame_locator('iframe[name="fraMenuContent"]')

                # Click on the first row's date cell
                first_row = list_frame.locator('tr[data-id]').first
                first_row.locator('td').nth(1).click()
                time.sleep(2)

                # Wait for detail panel to load
                detail_frame = self.page.frame_locator('iframe[name="fraDef"]')
                detail_frame.locator('text=/Daily report/').wait_for(timeout=10000)

                return True
            except Exception as e:
                print(f"    Error clicking first report: {e}")
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
                'details': {}
            }

            try:
                detail_frame = self.page.frame_locator('iframe[name="fraDef"]')

                # Click on Daily report tab to ensure it's active
                detail_frame.locator('text="Daily report"').first.click()
                time.sleep(0.5)

                # Extract Weather section
                try:
                    weather_section = detail_frame.locator('text="Weather"').first.locator('..')
                    data['weather'] = {
                        'time': self._safe_get_text(detail_frame, 'text=/Time.*N\\/A|Time.*\\d/'),
                        'conditions': self._safe_get_text(detail_frame, '[title="Conditions"]'),
                        'temperature': self._safe_get_text(detail_frame, 'text=/Temperature.*N\\/A|Temperature.*\\d/'),
                        'humidity': self._safe_get_text(detail_frame, 'text=/Humidity.*N\\/A|Humidity.*\\d/'),
                        'wind': self._safe_get_text(detail_frame, 'text=/Wind.*N\\/A|Wind.*\\d/'),
                    }
                except:
                    pass

                # Extract Labor section summary
                try:
                    data['labor'] = {
                        'workers': self._safe_get_text(detail_frame, 'text=/\\d+ approved workers/'),
                        'hours': self._safe_get_text(detail_frame, 'text=/\\d+.*hours|0 hours/'),
                    }
                except:
                    pass

                # Extract Equipment section summary
                try:
                    equipment_section = detail_frame.locator('text="Equipment"').first.locator('..')
                    data['equipment'] = {
                        'count': self._safe_get_text(detail_frame, 'text=/local_shipping.*\\d/'),
                        'hours': self._safe_get_text(detail_frame, 'text=/alarm.*\\d/'),
                    }
                except:
                    pass

                # Extract Notes section summary
                try:
                    data['notes'] = {
                        'comments_count': self._safe_get_text(detail_frame, 'text=/Comments.*\\d/'),
                        'links_count': self._safe_get_text(detail_frame, 'text=/Links.*\\d/'),
                    }
                except:
                    pass

                # Extract right panel details
                try:
                    data['details'] = {
                        'date': self._safe_get_input_value(detail_frame, 'input[disabled]'),
                        'status': self._safe_get_combobox_value(detail_frame, 'text="Status"'),
                        'total_rainfall': self._safe_get_text(detail_frame, 'text=/Total rainfall/'),
                        'total_snowfall': self._safe_get_text(detail_frame, 'text=/Total snowfall/'),
                        'lost_productivity': self._safe_get_text(detail_frame, 'text=/Lost productivity/'),
                        'uom': self._safe_get_combobox_value(detail_frame, 'text="UOM"'),
                    }
                except:
                    pass

            except Exception as e:
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
            """Extract data from the History tab."""
            data = {
                'created_by': None,
                'created_at': None,
                'changes': []
            }

            try:
                detail_frame = self.page.frame_locator('iframe[name="fraDef"]')

                # Click on History tab
                history_tab = detail_frame.locator('text="History"')
                if history_tab.count() > 0:
                    history_tab.first.click()
                    time.sleep(1)

                    # Extract creation info
                    created_text = self._safe_get_text(detail_frame, 'text=/Created by/')
                    if created_text:
                        data['created_by'] = created_text

                    # Extract timestamp
                    timestamp_text = self._safe_get_text(detail_frame, 'text=/\\d{1,2}\\/\\d{1,2}\\/\\d{4}/')
                    if timestamp_text:
                        data['created_at'] = timestamp_text

                    # Extract changes list
                    change_items = detail_frame.locator('.history-change, [class*="change"]').all()
                    for item in change_items:
                        try:
                            data['changes'].append(item.text_content())
                        except:
                            pass

            except Exception as e:
                data['error'] = str(e)

            return data

        def extract_current_report(self) -> Dict:
            """Extract data from the currently open report."""
            record_num = self.get_current_record_number()
            report_date = self.extract_report_date()

            print(f"    Extracting record {record_num}, date: {report_date}")

            report_data = {
                'recordNumber': record_num,
                'reportDate': report_date,
                'extractedAt': datetime.now().isoformat(),
                'dailyReport': self.extract_daily_report_tab(),
                'history': self.extract_history_tab(),
            }

            return report_data

        def extract_all_reports(self, limit: Optional[int] = None) -> List[Dict]:
            """Extract all reports using Next record navigation."""
            reports = []

            total_count = self.get_report_count()
            print(f"Total reports available: {total_count}")

            extract_count = min(limit, total_count) if limit and limit > 0 else total_count
            print(f"Will extract: {extract_count} reports")

            # Click first report to open detail view
            print("Opening first report...")
            if not self.click_first_report():
                print("Failed to open first report!")
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


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Scrape ProjectSight Daily Reports')
    parser.add_argument('--limit', type=int, default=10, help='Limit number of reports to extract')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    parser.add_argument('--output', type=str, default=None, help='Output file path')
    args = parser.parse_args()

    # Check headless setting from env
    headless = args.headless or os.getenv('PROJECTSIGHT_HEADLESS', 'false').lower() == 'true'

    print("=" * 60)
    print("ProjectSight Daily Reports Scraper")
    print("=" * 60)
    print(f"Headless mode: {headless}")
    print(f"Record limit: {args.limit}")

    # Create output path
    output_dir = Path('/workspaces/mxi-samsung/data/projectsight/extracted')
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = args.output or output_dir / f'daily_reports_test_{args.limit}.json'

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

        reports = scraper.extract_all_reports(limit=args.limit)

        # Save results
        output_data = {
            'extractedAt': datetime.now().isoformat(),
            'source': 'ProjectSight Standalone Scraper',
            'project': 'T-PJT > FAB1 > Construction',
            'totalAvailable': scraper.get_report_count(),
            'extractedCount': len(reports),
            'records': reports
        }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        print(f"\nExtraction complete!")
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
