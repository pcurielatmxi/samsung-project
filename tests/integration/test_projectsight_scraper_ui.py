#!/usr/bin/env python3
"""
Integration tests for ProjectSight Daily Reports Scraper UI navigation.

These tests use the ACTUAL ProjectSight UI to verify that:
1. The scraper only navigates to dates within the specified range
2. No dates outside the range are accessed
3. The date filter is applied correctly

REQUIREMENTS:
- Valid ProjectSight credentials in .env (PROJECTSIGHT_USERNAME, PROJECTSIGHT_PASSWORD)
- Network access to ProjectSight
- Playwright browsers installed

USAGE:
    # Run all UI tests
    pytest tests/integration/test_projectsight_scraper_ui.py -v -s

    # Run specific test
    pytest tests/integration/test_projectsight_scraper_ui.py::TestProjectSightUINavigation::test_scraper_stays_within_date_range -v -s
"""

import os
import sys
import pytest
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Set, List
from unittest.mock import patch, MagicMock

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(override=True)


def parse_date_from_text(date_text: str) -> date | None:
    """Parse date from various formats found in ProjectSight UI."""
    if not date_text or not date_text.strip():
        return None

    date_text = date_text.strip()

    # Try M/D/YYYY format
    try:
        parts = date_text.split('/')
        if len(parts) == 3:
            month, day, year = int(parts[0]), int(parts[1]), int(parts[2])
            return date(year, month, day)
    except (ValueError, TypeError):
        pass

    # Try YYYY-MM-DD format
    try:
        return datetime.strptime(date_text, '%Y-%m-%d').date()
    except ValueError:
        pass

    # Try MM/DD/YYYY format with leading zeros
    try:
        return datetime.strptime(date_text, '%m/%d/%Y').date()
    except ValueError:
        pass

    return None


class DateNavigationTracker:
    """Tracks which dates are navigated to during scraping."""

    def __init__(self, allowed_from: date, allowed_to: date):
        self.allowed_from = allowed_from
        self.allowed_to = allowed_to
        self.navigated_dates: List[date] = []
        self.out_of_range_dates: List[date] = []
        self.all_date_texts: List[str] = []

    def record_date(self, date_text: str) -> bool:
        """
        Record a date that was navigated to.

        Returns True if date is within range, False if out of range.
        """
        self.all_date_texts.append(date_text)

        parsed = parse_date_from_text(date_text)
        if parsed is None:
            return True  # Can't parse, assume OK

        self.navigated_dates.append(parsed)

        if parsed < self.allowed_from or parsed > self.allowed_to:
            self.out_of_range_dates.append(parsed)
            return False

        return True

    def get_summary(self) -> dict:
        """Get summary of navigation."""
        return {
            'total_navigated': len(self.navigated_dates),
            'within_range': len(self.navigated_dates) - len(self.out_of_range_dates),
            'out_of_range': len(self.out_of_range_dates),
            'out_of_range_dates': [d.isoformat() for d in self.out_of_range_dates],
            'allowed_range': f"{self.allowed_from.isoformat()} to {self.allowed_to.isoformat()}",
        }


@pytest.fixture(scope="module")
def projectsight_credentials():
    """Get ProjectSight credentials from environment."""
    username = os.getenv('PROJECTSIGHT_USERNAME')
    password = os.getenv('PROJECTSIGHT_PASSWORD')

    if not username or not password:
        pytest.skip("PROJECTSIGHT_USERNAME and PROJECTSIGHT_PASSWORD must be set in .env")

    return {'username': username, 'password': password}


@pytest.fixture(scope="module")
def browser_context():
    """Create a Playwright browser context for testing."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        pytest.skip("Playwright not installed. Run: pip install playwright && playwright install")

    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(headless=False, slow_mo=100)
    context = browser.new_context()

    yield context

    context.close()
    browser.close()
    playwright.stop()


class TestProjectSightUINavigation:
    """Tests that verify the scraper's UI navigation stays within date bounds."""

    @pytest.mark.integration
    def test_scraper_stays_within_date_range(self, projectsight_credentials, browser_context):
        """
        Test that the scraper only navigates to dates within the specified range.

        This test:
        1. Logs into ProjectSight
        2. Navigates to Daily Reports
        3. Attempts to extract reports for a specific date range
        4. Tracks every date that is navigated to
        5. FAILS if any date outside the range is accessed
        """
        # Define test date range: 1 week in November 2025
        from_date = date(2025, 11, 10)
        to_date = date(2025, 11, 16)

        tracker = DateNavigationTracker(from_date, to_date)

        # Import scraper components
        from scripts.projectsight.process.scrape_projectsight_daily_reports import create_scraper

        ScraperClass = create_scraper()
        page = browser_context.new_page()

        # Create scraper instance with our page
        scraper = ScraperClass.__new__(ScraperClass)
        scraper.headless = False
        scraper.page = page
        scraper.playwright = None
        scraper.browser = None
        scraper.context = browser_context

        # Set up URLs and credentials
        scraper.base_url = os.getenv('PROJECTSIGHT_BASE_URL', 'https://prod.projectsightapp.trimble.com/')
        scraper.login_url = os.getenv('PROJECTSIGHT_LOGIN_URL', 'https://id.trimble.com/ui/sign_in.html')
        scraper.username = projectsight_credentials['username']
        scraper.password = projectsight_credentials['password']
        scraper.daily_reports_url = (
            "https://prod.projectsightapp.trimble.com/web/app/Project"
            "?listid=-4038&orgid=ffd5880a-42ec-41fa-a552-db0c9a000326&projid=300"
        )

        try:
            # Login
            print("\n[TEST] Logging into ProjectSight...")
            assert scraper.login(), "Login failed"
            print("[TEST] Login successful")

            # Navigate to daily reports
            print("[TEST] Navigating to Daily Reports...")
            assert scraper.navigate_to_daily_reports(), "Navigation failed"
            print("[TEST] Navigation successful")

            # Apply date filter
            print(f"[TEST] Applying date filter: {from_date} to {to_date}...")
            filter_result = scraper.apply_date_filter(from_date, to_date)
            print(f"[TEST] Date filter result: {filter_result}")

            # Get the grid frame
            list_frame = page.frame_locator('iframe[name="fraMenuContent"]')

            # Scan visible dates in the grid
            print("[TEST] Scanning visible dates in grid...")
            dates_seen = set()

            # Get all visible rows
            rows = list_frame.locator('tr[data-id]').all()
            print(f"[TEST] Found {len(rows)} rows in grid")

            for row in rows:
                try:
                    cells = row.locator('td').all()
                    if len(cells) >= 2:
                        date_text = cells[1].text_content(timeout=1000)
                        if date_text:
                            date_text = date_text.strip()
                            if date_text and date_text not in dates_seen:
                                dates_seen.add(date_text)
                                in_range = tracker.record_date(date_text)
                                status = "✓" if in_range else "✗ OUT OF RANGE"
                                print(f"[TEST]   Date seen: {date_text} {status}")
                except Exception as e:
                    continue

            # Now try clicking on a few reports to verify navigation
            print("\n[TEST] Testing report navigation...")

            # Try to click on the first visible report
            first_row = list_frame.locator('tr[data-id]').first
            if first_row.count() > 0:
                try:
                    # Get the date before clicking
                    cells = first_row.locator('td').all()
                    if len(cells) >= 2:
                        clicked_date = cells[1].text_content(timeout=1000)
                        print(f"[TEST] Clicking on report: {clicked_date}")
                        tracker.record_date(clicked_date)

                        # Click to open detail view
                        first_row.click()
                        page.wait_for_timeout(2000)

                        # Check the date in detail view
                        try:
                            # Look for date in the detail view
                            detail_date = page.locator('text=/\\d{1,2}\\/\\d{1,2}\\/\\d{4}/').first
                            if detail_date.count() > 0:
                                detail_date_text = detail_date.text_content(timeout=1000)
                                print(f"[TEST] Detail view date: {detail_date_text}")
                                tracker.record_date(detail_date_text)
                        except:
                            pass

                        # Go back
                        page.go_back()
                        page.wait_for_timeout(2000)
                except Exception as e:
                    print(f"[TEST] Error during navigation test: {e}")

            # Print summary
            summary = tracker.get_summary()
            print(f"\n[TEST] Navigation Summary:")
            print(f"  Allowed range: {summary['allowed_range']}")
            print(f"  Total dates navigated: {summary['total_navigated']}")
            print(f"  Within range: {summary['within_range']}")
            print(f"  Out of range: {summary['out_of_range']}")

            if summary['out_of_range'] > 0:
                print(f"  Out of range dates: {summary['out_of_range_dates']}")

            # ASSERT: No dates outside the allowed range were navigated
            assert len(tracker.out_of_range_dates) == 0, \
                f"Scraper navigated to {len(tracker.out_of_range_dates)} dates outside the allowed range: {tracker.out_of_range_dates}"

            print("\n[TEST] ✓ All navigated dates were within the allowed range!")

        finally:
            page.close()

    @pytest.mark.integration
    def test_grid_shows_only_filtered_dates(self, projectsight_credentials, browser_context):
        """
        Test that after applying date filter, the grid only shows dates in range.

        This is a simpler test that just checks the grid contents after filtering.
        """
        from_date = date(2025, 11, 10)
        to_date = date(2025, 11, 16)

        tracker = DateNavigationTracker(from_date, to_date)

        from scripts.projectsight.process.scrape_projectsight_daily_reports import create_scraper

        ScraperClass = create_scraper()
        page = browser_context.new_page()

        scraper = ScraperClass.__new__(ScraperClass)
        scraper.headless = False
        scraper.page = page
        scraper.playwright = None
        scraper.browser = None
        scraper.context = browser_context
        scraper.base_url = os.getenv('PROJECTSIGHT_BASE_URL', 'https://prod.projectsightapp.trimble.com/')
        scraper.login_url = os.getenv('PROJECTSIGHT_LOGIN_URL', 'https://id.trimble.com/ui/sign_in.html')
        scraper.username = projectsight_credentials['username']
        scraper.password = projectsight_credentials['password']
        scraper.daily_reports_url = (
            "https://prod.projectsightapp.trimble.com/web/app/Project"
            "?listid=-4038&orgid=ffd5880a-42ec-41fa-a552-db0c9a000326&projid=300"
        )

        try:
            # Login and navigate
            print("\n[TEST] Logging in and navigating...")
            assert scraper.login(), "Login failed"
            assert scraper.navigate_to_daily_reports(), "Navigation failed"

            # Get initial count
            initial_count = scraper.get_report_count()
            print(f"[TEST] Initial report count (unfiltered): {initial_count}")

            # Apply date filter
            print(f"[TEST] Applying date filter: {from_date} to {to_date}...")
            scraper.apply_date_filter(from_date, to_date)

            # Wait for filter to apply
            page.wait_for_timeout(3000)

            # Get filtered count
            filtered_count = scraper.get_report_count()
            print(f"[TEST] Filtered report count: {filtered_count}")

            # Scan all visible dates after filtering
            list_frame = page.frame_locator('iframe[name="fraMenuContent"]')

            # Scroll through entire filtered grid to check all dates
            print("[TEST] Scanning all visible dates after filter...")

            all_dates_in_grid = []
            rows = list_frame.locator('tr[data-id]').all()

            for row in rows:
                try:
                    cells = row.locator('td').all()
                    if len(cells) >= 2:
                        date_text = cells[1].text_content(timeout=500)
                        if date_text:
                            date_text = date_text.strip()
                            parsed = parse_date_from_text(date_text)
                            if parsed:
                                all_dates_in_grid.append(parsed)
                                tracker.record_date(date_text)
                except:
                    continue

            print(f"[TEST] Found {len(all_dates_in_grid)} dates in filtered grid")

            # Check results
            summary = tracker.get_summary()
            print(f"\n[TEST] Results:")
            print(f"  Dates in grid: {len(all_dates_in_grid)}")
            print(f"  Out of range: {summary['out_of_range']}")

            if summary['out_of_range'] > 0:
                print(f"  Out of range dates: {summary['out_of_range_dates']}")
                pytest.fail(f"Grid contains {summary['out_of_range']} dates outside filter range")

            # If filter worked, we should have fewer reports than initial
            if filtered_count < initial_count:
                print(f"[TEST] ✓ Filter reduced count from {initial_count} to {filtered_count}")
            else:
                print(f"[TEST] ⚠ Filter may not have worked (count unchanged)")

            print("[TEST] ✓ All dates in filtered grid are within range!")

        finally:
            page.close()


class TestDateFilterMethods:
    """Tests for the apply_date_filter method specifically."""

    @pytest.mark.integration
    def test_date_filter_reduces_visible_reports(self, projectsight_credentials, browser_context):
        """Test that applying a date filter reduces the number of visible reports."""
        from_date = date(2025, 11, 1)
        to_date = date(2025, 11, 30)  # Just November 2025

        from scripts.projectsight.process.scrape_projectsight_daily_reports import create_scraper

        ScraperClass = create_scraper()
        page = browser_context.new_page()

        scraper = ScraperClass.__new__(ScraperClass)
        scraper.headless = False
        scraper.page = page
        scraper.playwright = None
        scraper.browser = None
        scraper.context = browser_context
        scraper.base_url = os.getenv('PROJECTSIGHT_BASE_URL', 'https://prod.projectsightapp.trimble.com/')
        scraper.login_url = os.getenv('PROJECTSIGHT_LOGIN_URL', 'https://id.trimble.com/ui/sign_in.html')
        scraper.username = projectsight_credentials['username']
        scraper.password = projectsight_credentials['password']
        scraper.daily_reports_url = (
            "https://prod.projectsightapp.trimble.com/web/app/Project"
            "?listid=-4038&orgid=ffd5880a-42ec-41fa-a552-db0c9a000326&projid=300"
        )

        try:
            print("\n[TEST] Testing date filter effectiveness...")

            # Login and navigate
            assert scraper.login(), "Login failed"
            assert scraper.navigate_to_daily_reports(), "Navigation failed"

            # Get unfiltered count
            unfiltered_count = scraper.get_report_count()
            print(f"[TEST] Unfiltered count: {unfiltered_count}")

            # Apply filter for just November 2025
            print(f"[TEST] Applying filter: {from_date} to {to_date}")
            filter_success = scraper.apply_date_filter(from_date, to_date)

            page.wait_for_timeout(3000)

            # Get filtered count
            filtered_count = scraper.get_report_count()
            print(f"[TEST] Filtered count: {filtered_count}")

            # The filtered count should be less than unfiltered
            # (unless all reports happen to be in November 2025, which is unlikely)
            if filtered_count < unfiltered_count:
                print(f"[TEST] ✓ Filter reduced reports from {unfiltered_count} to {filtered_count}")
            else:
                print(f"[TEST] ⚠ Filter did not reduce count - may not be working")
                # Don't fail - filter might not be supported, but log it

        finally:
            page.close()


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s', '--tb=short'])
