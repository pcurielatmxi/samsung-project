#!/usr/bin/env python3
"""
RABA Quality Reports Scraper - Individual Downloads

Downloads quality inspection reports from RABA RKIS system (celvis.rkci.com)
as individual PDFs (one per inspection/assignment), rather than daily batches.

This approach solves several issues with batch downloads:
1. No need to split batch PDFs afterward
2. Works for days with only 1 report (batch button unavailable)
3. Each PDF is named by assignment number for direct processing
4. Handles pagination for months with many reports

Output structure:
    raw/raba/individual/{assignment_number}.pdf
    Example: raw/raba/individual/A22-016871.pdf

The script:
1. Logs into RABA system
2. Iterates through each MONTH from start_date to end_date
3. Sets date range filter to full month
4. Handles pagination to get all reports
5. Downloads each report individually by clicking on report link
6. Saves PDFs named by assignment number
7. Maintains a manifest.json tracking downloaded assignments

Usage:
    # Download all reports from project start to now
    python scripts/raba/process/scrape_raba_individual.py

    # Download specific date range
    python scripts/raba/process/scrape_raba_individual.py --start-date 2022-05-01 --end-date 2022-06-30

    # Force re-download of existing files
    python scripts/raba/process/scrape_raba_individual.py --force

    # Run in headless mode
    python scripts/raba/process/scrape_raba_individual.py --headless

    # Limit total reports (for testing)
    python scripts/raba/process/scrape_raba_individual.py --limit 10
"""

import os
import re
import sys
import json
import time
import logging
import argparse
import tempfile
import shutil
from calendar import monthrange
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

load_dotenv(override=True)

# Global logger
logger = logging.getLogger('raba_individual_scraper')

# Constants
PROJECT_START_DATE = datetime(2022, 5, 1)  # May 2022 - project start
RABA_LOGIN_URL = "https://celvis.rkci.com/CelvisClient/Login.aspx"
RABA_REPORTS_URL = "https://celvis.rkci.com/CelvisClient/DesktopDefault.aspx?tabindex=1&tabid=2"
PROJECT_CODE = "AAD22-067-00:Samsung Taylor - FAB1 Project"


@dataclass
class ReportInfo:
    """Information about a single report."""
    assignment_number: str
    service_date: str  # YYYY-MM-DD
    project_code: Optional[str] = None
    report_type: Optional[str] = None
    status: str = 'pending'  # pending, success, error, skipped
    file: Optional[str] = None
    file_size: Optional[int] = None
    downloaded_at: Optional[str] = None
    error: Optional[str] = None


def setup_logging(log_file: Path, verbose: bool = False) -> logging.Logger:
    """Configure logging to file and console."""
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)

    # File handler
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger


def generate_months(start_date: datetime, end_date: datetime) -> List[Tuple[str, datetime, datetime]]:
    """Generate list of months between start and end dates.

    Returns:
        List of tuples: (month_key, month_start, month_end)
        - month_key: YYYY-MM format string
        - month_start: First day of month (or start_date if partial)
        - month_end: Last day of month (or end_date if partial)
    """
    months = []
    current = start_date.replace(day=1)

    while current <= end_date:
        month_key = current.strftime('%Y-%m')

        # Calculate month boundaries
        _, last_day = monthrange(current.year, current.month)
        month_start = max(current, start_date)
        month_end = min(current.replace(day=last_day), end_date)

        months.append((month_key, month_start, month_end))

        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return months


def atomic_json_save(data: dict, output_file: Path):
    """Save JSON data atomically using temp file and rename."""
    output_file.parent.mkdir(parents=True, exist_ok=True)

    temp_fd, temp_path = tempfile.mkstemp(
        suffix='.json',
        prefix='.tmp_',
        dir=output_file.parent
    )
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        shutil.move(temp_path, output_file)
    except Exception:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise


def load_manifest(manifest_file: Path) -> dict:
    """Load existing manifest or create empty one."""
    manifest = {
        'source': 'RABA RKIS Quality Reports (Individual)',
        'project': PROJECT_CODE,
        'created_at': datetime.now().isoformat(),
        'reports': {},  # Track each report by assignment number
        'dates_processed': [],  # Legacy - kept for backward compatibility
        'month_status': {}  # Track month status: 'complete', 'partial', 'error'
    }

    if manifest_file.exists():
        try:
            with open(manifest_file, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
                manifest.update(loaded)
                if 'reports' not in manifest:
                    manifest['reports'] = {}
                if 'dates_processed' not in manifest:
                    manifest['dates_processed'] = []
                if 'month_status' not in manifest:
                    manifest['month_status'] = {}

                # Migrate old months_processed list to month_status dict
                # Assume old entries were complete (best effort migration)
                if 'months_processed' in manifest and isinstance(manifest.get('months_processed'), list):
                    for month_key in manifest['months_processed']:
                        if month_key not in manifest['month_status']:
                            # Check month_stats to determine actual status
                            month_stats = manifest.get('month_stats', {}).get(month_key, {})
                            errors = month_stats.get('errors', 0)
                            if errors > 0:
                                manifest['month_status'][month_key] = 'partial'
                            else:
                                manifest['month_status'][month_key] = 'complete'
        except (json.JSONDecodeError, IOError):
            pass

    return manifest


class RABAIndividualScraper:
    """Scraper for RABA RKIS quality inspection reports - individual downloads."""

    def __init__(self, headless: bool = False, download_dir: Path = None):
        """Initialize the scraper."""
        self.headless = headless
        self.download_dir = download_dir
        self.page = None
        self.browser = None
        self.context = None
        self.playwright = None

        # Load credentials from environment
        self.username = os.getenv('RABA_USERNAME')
        self.password = os.getenv('RABA_PASSWORD')

        if not self.username or not self.password:
            raise ValueError("RABA_USERNAME and RABA_PASSWORD must be set in .env file")

    def __enter__(self):
        """Start browser session."""
        from playwright.sync_api import sync_playwright

        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless)

        # Create context with download handling
        self.context = self.browser.new_context(
            accept_downloads=True,
            viewport={'width': 1280, 'height': 900}
        )
        self.page = self.context.new_page()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close browser session."""
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()

    def login(self) -> bool:
        """Log into RABA system."""
        logger.info("Logging into RABA system...")

        try:
            self.page.goto(RABA_LOGIN_URL, timeout=30000)
            time.sleep(2)

            # Fill login form
            self.page.fill('#txtLoginName', self.username)
            self.page.fill('#txtOriginalPassword', self.password)

            # Click login button
            self.page.click('input[type="submit"][value="Login"]')

            # Wait for navigation
            self.page.wait_for_load_state('networkidle', timeout=30000)
            time.sleep(2)

            # Check if login successful
            if self.page.locator('a:has-text("Logoff")').count() > 0:
                logger.info("Login successful")
                return True
            else:
                logger.error("Login failed - logout link not found")
                return False

        except Exception as e:
            logger.error(f"Login error: {e}")
            return False

    def navigate_to_reports(self) -> bool:
        """Navigate to the View Report page."""
        try:
            self.page.goto(RABA_REPORTS_URL, timeout=30000)
            self.page.wait_for_load_state('networkidle', timeout=30000)
            time.sleep(2)

            # Wait for the date inputs to be present
            self.page.wait_for_selector('#_ctl4_txtDateFrom', timeout=10000)
            return True
        except Exception as e:
            logger.error(f"Navigation error: {e}")
            return False

    def recover_page_state(self) -> bool:
        """Recover from a corrupted page state by navigating back to reports page.

        Call this when a download error leaves the page in a bad state.
        Returns True if recovery was successful.
        """
        try:
            logger.info("Attempting page state recovery...")

            # Close any extra tabs/popups that might be open
            try:
                pages = self.context.pages
                if len(pages) > 1:
                    for p in pages[1:]:
                        try:
                            p.close()
                        except Exception:
                            pass
                    logger.info(f"Closed {len(pages) - 1} extra tab(s)")
            except Exception:
                pass

            # Navigate back to reports page
            if self.navigate_to_reports():
                logger.info("Page state recovered successfully")
                return True
            else:
                logger.error("Failed to recover page state")
                return False

        except Exception as e:
            logger.error(f"Recovery failed: {e}")
            return False

    def select_project(self) -> bool:
        """Select the Samsung Taylor FAB1 project."""
        try:
            project_dropdown = self.page.locator('#_ctl4_ddlProjectCodeName')
            if project_dropdown.count() > 0:
                project_dropdown.select_option(label=PROJECT_CODE)
                time.sleep(1)
                logger.debug(f"Selected project: {PROJECT_CODE}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error selecting project: {e}")
            return False

    def set_date_range(self, start_date: datetime, end_date: datetime) -> bool:
        """Set the service date range filter."""
        try:
            start_str = start_date.strftime('%m/%d/%Y')
            end_str = end_date.strftime('%m/%d/%Y')

            start_input = self.page.locator('#_ctl4_txtDateFrom')
            end_input = self.page.locator('#_ctl4_txtDateTo')

            start_input.clear()
            start_input.fill(start_str)

            end_input.clear()
            end_input.fill(end_str)

            logger.debug(f"Set date range: {start_str} to {end_str}")
            return True
        except Exception as e:
            logger.error(f"Error setting date range: {e}")
            return False

    def search_reports(self) -> int:
        """Click search and return number of records found."""
        try:
            # Click search button
            search_btn = self.page.locator('#_ctl4_btnSearchDB')
            search_btn.click()

            # Wait for results
            self.page.wait_for_load_state('networkidle', timeout=30000)
            time.sleep(2)

            # Get record count
            count = 0
            record_cells = self.page.locator('td').all()
            for cell in record_cells:
                try:
                    text = cell.text_content()
                    if 'Record(s) meet the defined search criteria' in text:
                        match = re.search(r'(\d+)\s+Record', text)
                        if match:
                            count = int(match.group(1))
                            logger.debug(f"Found {count} records")
                            break
                except Exception:
                    continue

            return count
        except Exception as e:
            logger.error(f"Error searching: {e}")
            return -1

    def get_current_page_reports(self) -> List[ReportInfo]:
        """Extract list of reports from the current page of results.

        The RABA interface shows results in a table with checkboxes.
        Each row has a checkbox for selection and contains the assignment number.
        """
        reports = []

        try:
            # Find all checkboxes in the results table (these mark data rows)
            checkboxes = self.page.locator('input[type="checkbox"]').all()

            for checkbox in checkboxes:
                try:
                    # Skip the "Select All" checkbox if any
                    checkbox_id = checkbox.get_attribute('id') or ''
                    checkbox_name = checkbox.get_attribute('name') or ''
                    if 'selectall' in checkbox_id.lower() or 'selectall' in checkbox_name.lower():
                        continue

                    # Get the immediate parent row containing this checkbox
                    # Using ancestor::tr[1] to get only the first (immediate) ancestor tr
                    parent_row = checkbox.locator('xpath=ancestor::tr[1]')
                    if parent_row.count() == 0:
                        continue

                    # Get all cells in the row to extract column data
                    # Table structure: Checkbox | Assignment ID | Project Code | Report Type | Service Date
                    cells = parent_row.locator('td').all()
                    row_text = parent_row.text_content() or ''

                    # Look for assignment number pattern in the row
                    assignment_match = re.search(r'([A-Z]\d{2}-\d{6})', row_text)

                    if assignment_match:
                        assignment_number = assignment_match.group(1)

                        # Extract project code (AAD#######)
                        project_code = None
                        project_match = re.search(r'(AAD\d{7,8})', row_text)
                        if project_match:
                            project_code = project_match.group(1)

                        # Extract report type - it's usually between project code and date
                        # Try to get it from the cells if we have enough
                        report_type = None
                        if len(cells) >= 4:
                            try:
                                # Report type is typically in the 3rd column (index 2, after checkbox and assignment)
                                report_type_cell = cells[2].text_content() or ''
                                # Clean up - remove project code and assignment if they leaked in
                                report_type_cell = re.sub(r'AAD\d{7,8}', '', report_type_cell)
                                report_type_cell = re.sub(r'[A-Z]\d{2}-\d{6}', '', report_type_cell)
                                report_type_cell = report_type_cell.strip()
                                if report_type_cell and len(report_type_cell) > 3:
                                    report_type = report_type_cell
                            except Exception:
                                pass

                        # Extract service date from the row
                        service_date = None
                        date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', row_text)
                        if date_match:
                            try:
                                parsed_date = datetime.strptime(date_match.group(1), '%m/%d/%Y')
                                service_date = parsed_date.strftime('%Y-%m-%d')
                            except ValueError:
                                pass

                        reports.append(ReportInfo(
                            assignment_number=assignment_number,
                            service_date=service_date or 'unknown',
                            project_code=project_code,
                            report_type=report_type
                        ))

                except Exception as e:
                    logger.debug(f"Error parsing checkbox row: {e}")
                    continue

            logger.debug(f"Found {len(reports)} reports on current page")
            return reports

        except Exception as e:
            logger.error(f"Error getting current page reports: {e}")
            return []

    def download_individual_report(self, assignment_number: str, output_path: Path,
                                    timeout: int = 60000) -> bool:
        """Download a single report by its assignment number.

        Strategy:
        1. Find the row containing this assignment number
        2. Check its checkbox
        3. Click the batch download button (works for single selection too)
        4. Handle the popup with the PDF
        5. Save the PDF
        6. Uncheck the checkbox (for next report)
        """
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # First, uncheck all checkboxes to start fresh
            # Click "Unselect All" if available, otherwise we'll just check individual
            unselect_link = self.page.locator('a:has-text("Unselect All")')
            if unselect_link.count() > 0:
                try:
                    unselect_link.click()
                    time.sleep(0.5)
                except Exception:
                    pass

            # Find all checkboxes
            checkboxes = self.page.locator('input[type="checkbox"]').all()

            target_checkbox = None
            for checkbox in checkboxes:
                try:
                    # Skip select all checkbox
                    checkbox_id = checkbox.get_attribute('id') or ''
                    if 'selectall' in checkbox_id.lower():
                        continue

                    # Get immediate parent row and check if it contains our assignment number
                    parent_row = checkbox.locator('xpath=ancestor::tr[1]')
                    if parent_row.count() == 0:
                        continue

                    row_text = parent_row.text_content() or ''
                    if assignment_number in row_text:
                        target_checkbox = checkbox
                        break
                except Exception:
                    continue

            if not target_checkbox:
                logger.warning(f"Could not find checkbox for {assignment_number}")
                return False

            # Check the checkbox
            target_checkbox.check()
            time.sleep(0.5)

            # Click batch download button and wait for popup
            batch_btn = self.page.locator('#_ctl4_btnBatch')
            if batch_btn.count() == 0:
                logger.error(f"Batch download button not found for {assignment_number}")
                return False

            try:
                with self.page.expect_popup(timeout=timeout) as popup_info:
                    batch_btn.click()
                new_page = popup_info.value

                # Wait for the popup to finish its initial navigation
                # This is crucial - the popup opens with about:blank then redirects
                try:
                    new_page.wait_for_load_state('domcontentloaded', timeout=30000)
                except Exception as load_err:
                    logger.debug(f"Popup load state wait: {load_err}")

            except Exception as popup_err:
                logger.error(f"Popup did not open for {assignment_number}: {popup_err}")
                return False

            # Additional wait for page to stabilize
            time.sleep(1)

            def is_valid_pdf_url(url: str) -> bool:
                """Check if URL is a valid HTTP(S) URL for PDF download."""
                if not url:
                    return False
                if url in ('about:blank', ':', 'about:srcdoc'):
                    return False
                # Must be a proper HTTP(S) URL
                return url.startswith('http://') or url.startswith('https://')

            # Get the PDF URL - wait for it to be a real HTTP(S) URL
            pdf_url = new_page.url
            retries = 0
            while not is_valid_pdf_url(pdf_url) and retries < 15:
                time.sleep(0.5)
                pdf_url = new_page.url
                retries += 1
                if retries % 5 == 0:
                    logger.debug(f"Waiting for valid URL (attempt {retries}): current='{pdf_url}'")

            logger.info(f"PDF URL for {assignment_number}: {pdf_url}")

            if not is_valid_pdf_url(pdf_url):
                logger.error(f"Could not get valid PDF URL for {assignment_number} (got: '{pdf_url}')")
                new_page.close()
                return False

            # Fetch the PDF using the popup page directly instead of a separate request
            try:
                # Try getting content directly from the page if it's a blob or data URL
                if pdf_url.startswith('blob:') or pdf_url.startswith('data:'):
                    logger.info(f"Detected blob/data URL, trying alternate download method")
                    # Use page.pdf() to capture the content if it's rendered
                    try:
                        pdf_bytes = new_page.pdf()
                        with open(output_path, 'wb') as f:
                            f.write(pdf_bytes)
                        new_page.close()
                        if output_path.exists() and output_path.stat().st_size > 0:
                            logger.info(f"Downloaded via page.pdf(): {assignment_number}.pdf ({output_path.stat().st_size:,} bytes)")
                            return True
                    except Exception as pdf_err:
                        logger.warning(f"page.pdf() failed: {pdf_err}")

                response = self.context.request.get(pdf_url)
                if response.status != 200:
                    logger.error(f"HTTP error {response.status} fetching PDF for {assignment_number}")
                    new_page.close()
                    return False

                pdf_bytes = response.body()

                with open(output_path, 'wb') as f:
                    f.write(pdf_bytes)

            except Exception as fetch_err:
                logger.error(f"Fetch failed for {assignment_number}: {fetch_err}")
                new_page.close()
                return False

            # Close the PDF tab
            try:
                new_page.close()
            except Exception:
                pass

            # Uncheck the checkbox
            try:
                target_checkbox.uncheck()
            except Exception:
                pass

            # Verify file
            if output_path.exists() and output_path.stat().st_size > 0:
                logger.info(f"Downloaded: {assignment_number}.pdf ({output_path.stat().st_size:,} bytes)")
                return True
            else:
                logger.error(f"Download failed or empty: {assignment_number}")
                return False

        except Exception as e:
            logger.error(f"Download error for {assignment_number}: {e}")
            return False

    def download_reports_on_current_page(self, output_dir: Path, manifest: dict,
                                          manifest_file: Path, force: bool = False,
                                          limit: int = None,
                                          month_start: datetime = None,
                                          month_end: datetime = None) -> Tuple[int, int, int, bool, bool]:
        """Download all reports visible on the current page.

        Returns tuple of (downloaded_count, skipped_count, error_count, limit_reached, needs_recovery)
        The needs_recovery flag indicates the page state is corrupted and caller should recover.
        """
        downloaded = 0
        skipped = 0
        errors = 0
        consecutive_errors = 0
        max_consecutive_errors = 3  # Trigger recovery after 3 consecutive errors

        # Get reports on current page
        reports = self.get_current_page_reports()

        for i, report in enumerate(reports):
            assignment = report.assignment_number

            # Check limit first
            if limit:
                total_success = len([r for r in manifest.get('reports', {}).values()
                                     if r.get('status') == 'success'])
                if total_success >= limit:
                    logger.info(f"Reached limit of {limit} reports")
                    return (downloaded, skipped, errors, True, False)

            # Check if already downloaded (idempotency)
            existing = manifest.get('reports', {}).get(assignment)
            if existing and existing.get('status') == 'success' and not force:
                logger.debug(f"  {assignment}: Skipping (already downloaded)")
                skipped += 1
                consecutive_errors = 0  # Reset on skip (page is working)
                continue

            # Download
            output_path = output_dir / f"{assignment}.pdf"
            logger.info(f"  Downloading {assignment}...")

            if self.download_individual_report(assignment, output_path):
                report.status = 'success'
                report.file = f"{assignment}.pdf"
                report.file_size = output_path.stat().st_size
                report.downloaded_at = datetime.now().isoformat()
                downloaded += 1
                consecutive_errors = 0  # Reset on success
            else:
                report.status = 'error'
                report.error = 'Download failed'
                errors += 1
                consecutive_errors += 1

                # Check if we need to recover
                if consecutive_errors >= max_consecutive_errors:
                    logger.warning(f"Hit {consecutive_errors} consecutive errors, triggering recovery")
                    # Update manifest before recovery
                    manifest['reports'][assignment] = asdict(report)
                    manifest['last_updated'] = datetime.now().isoformat()
                    atomic_json_save(manifest, manifest_file)
                    return (downloaded, skipped, errors, False, True)  # needs_recovery=True

            # Update manifest immediately (for resume support)
            manifest['reports'][assignment] = asdict(report)
            manifest['last_updated'] = datetime.now().isoformat()
            atomic_json_save(manifest, manifest_file)

            # Brief delay between downloads
            time.sleep(1)

        return (downloaded, skipped, errors, False, False)

    def extract_month(self, month_key: str, month_start: datetime, month_end: datetime,
                      output_dir: Path, manifest: dict, manifest_file: Path,
                      force: bool = False, limit: int = None) -> Tuple[int, int, int, int]:
        """Extract all individual reports for a single month.

        Downloads reports page by page to avoid losing context after pagination.

        Returns tuple of (downloaded_count, skipped_count, error_count, expected_count)
        """
        downloaded = 0
        skipped = 0
        errors = 0
        expected_count = 0

        try:
            # Set date range for the full month
            if not self.set_date_range(month_start, month_end):
                logger.error(f"{month_key}: Failed to set date range")
                return (0, 0, 1, 0)

            # Search
            record_count = self.search_reports()
            expected_count = record_count if record_count > 0 else 0

            if record_count == 0:
                logger.info(f"{month_key}: No records - marking complete")
                # Mark month as complete (empty month is complete)
                manifest.setdefault('month_status', {})[month_key] = 'complete'
                manifest.setdefault('month_stats', {})[month_key] = {
                    'expected': 0, 'downloaded': 0, 'skipped': 0, 'errors': 0
                }
                # Legacy: also update months_processed list
                if month_key not in manifest.get('months_processed', []):
                    manifest.setdefault('months_processed', []).append(month_key)
                atomic_json_save(manifest, manifest_file)
                return (0, 0, 0, 0)

            if record_count < 0:
                logger.error(f"{month_key}: Search failed - marking as error")
                manifest.setdefault('month_status', {})[month_key] = 'error'
                manifest.setdefault('month_stats', {})[month_key] = {
                    'expected': 0, 'downloaded': 0, 'skipped': 0, 'errors': 1, 'search_failed': True
                }
                atomic_json_save(manifest, manifest_file)
                return (0, 0, 1, 0)

            logger.info(f"{month_key}: Found {record_count} reports, processing page by page...")

            # Process reports page by page
            page_num = 1
            recovery_attempts = 0
            max_recovery_attempts = 3

            while True:
                logger.info(f"  Page {page_num}: Processing...")

                # Download reports on current page
                page_dl, page_skip, page_err, limit_reached, needs_recovery = self.download_reports_on_current_page(
                    output_dir, manifest, manifest_file, force=force, limit=limit,
                    month_start=month_start, month_end=month_end
                )
                downloaded += page_dl
                skipped += page_skip
                errors += page_err

                if limit_reached:
                    logger.info(f"Limit reached during page {page_num}")
                    break

                # Handle recovery if needed
                if needs_recovery:
                    recovery_attempts += 1
                    if recovery_attempts > max_recovery_attempts:
                        logger.error(f"Max recovery attempts ({max_recovery_attempts}) reached, aborting month")
                        break

                    logger.info(f"Recovery attempt {recovery_attempts}/{max_recovery_attempts}")
                    if self.recover_page_state():
                        # Re-search and navigate back to where we were
                        if not self.set_date_range(month_start, month_end):
                            logger.error("Failed to re-set date range after recovery")
                            break
                        new_count = self.search_reports()
                        if new_count <= 0:
                            logger.error("Failed to re-search after recovery")
                            break
                        # Navigate to current page
                        for p in range(1, page_num):
                            next_p = self.page.locator(f'a:has-text("{p + 1}")').first
                            if next_p.count() > 0:
                                next_p.click()
                                self.page.wait_for_load_state('networkidle', timeout=30000)
                                time.sleep(1)
                        logger.info(f"Recovered and returned to page {page_num}")
                        continue  # Retry current page
                    else:
                        logger.error("Recovery failed, aborting month")
                        break

                # Check for next page link
                next_link = None

                # Try finding a "Next" link or page number
                next_page_num = str(page_num + 1)
                page_links = self.page.locator(f'a:has-text("{next_page_num}")').all()
                for link in page_links:
                    try:
                        text = link.text_content().strip()
                        if text == next_page_num:
                            next_link = link
                            break
                    except Exception:
                        continue

                if not next_link:
                    logger.debug(f"No more pages after page {page_num}")
                    break

                # Click next page
                try:
                    next_link.click()
                    self.page.wait_for_load_state('networkidle', timeout=30000)
                    time.sleep(1)
                    page_num += 1
                except Exception as e:
                    logger.warning(f"Error clicking next page: {e}")
                    break

                # Safety limit
                if page_num > 100:
                    logger.warning("Reached page limit (100)")
                    break

            # Track month stats (expected vs actual)
            manifest.setdefault('month_stats', {})[month_key] = {
                'expected': expected_count,
                'downloaded': downloaded,
                'skipped': skipped,
                'errors': errors,
                'total_processed': downloaded + skipped + errors
            }

            # Determine month status based on results
            # - 'complete': all expected reports downloaded (no errors)
            # - 'partial': some reports downloaded but errors occurred
            # - 'error': couldn't process the month at all
            actual_success = downloaded + skipped
            if errors == 0 and actual_success >= expected_count:
                month_status = 'complete'
            elif downloaded > 0 or skipped > 0:
                month_status = 'partial'
            else:
                month_status = 'error'

            manifest.setdefault('month_status', {})[month_key] = month_status

            # Legacy: also update months_processed list for backward compatibility
            if month_key not in manifest.get('months_processed', []):
                manifest.setdefault('months_processed', []).append(month_key)

            atomic_json_save(manifest, manifest_file)

            # Log with expected vs actual comparison and status
            if month_status == 'complete':
                logger.info(f"{month_key}: COMPLETE - {expected_count} reports (Downloaded {downloaded}, Skipped {skipped})")
            elif month_status == 'partial':
                logger.warning(f"{month_key}: PARTIAL - Expected {expected_count}, got {actual_success} (Downloaded {downloaded}, Skipped {skipped}, Errors {errors})")
            else:
                logger.error(f"{month_key}: ERROR - Failed to process month")

            return (downloaded, skipped, errors, expected_count)

        except Exception as e:
            logger.error(f"{month_key}: Error - {e}")
            import traceback
            logger.error(traceback.format_exc())
            return (downloaded, skipped, errors + 1, expected_count)

    def extract_all_months(self, months: List[Tuple[str, datetime, datetime]],
                           output_dir: Path, manifest: dict, manifest_file: Path,
                           force: bool = False, dry_run: bool = False,
                           limit: int = None) -> dict:
        """Extract individual reports for all specified months."""
        total_months = len(months)
        total_downloaded = 0
        total_skipped = 0
        total_errors = 0
        total_expected = 0

        for i, (month_key, month_start, month_end) in enumerate(months):
            # Check month status - only skip if 'complete' (unless force)
            month_status = manifest.get('month_status', {}).get(month_key, None)
            month_stats = manifest.get('month_stats', {}).get(month_key, {})

            if not force and month_status == 'complete':
                # Add existing month's expected count to total
                total_expected += month_stats.get('expected', 0)
                logger.info(f"[{i+1}/{total_months}] {month_key}: Skipping (complete)")
                continue

            # Log if retrying a partial month
            if month_status == 'partial':
                prev_errors = month_stats.get('errors', 0)
                logger.info(f"[{i+1}/{total_months}] {month_key}: Retrying (partial - {prev_errors} previous errors)")

            logger.info(f"\n[{i+1}/{total_months}] Processing {month_key} "
                        f"({month_start.strftime('%Y-%m-%d')} to {month_end.strftime('%Y-%m-%d')})")

            if dry_run:
                if not self.set_date_range(month_start, month_end):
                    continue
                count = self.search_reports()
                total_expected += count if count > 0 else 0
                logger.info(f"  Would process {count} reports")
                continue

            # Extract reports for this month
            downloaded, skipped, errors, expected = self.extract_month(
                month_key, month_start, month_end,
                output_dir, manifest, manifest_file,
                force=force, limit=limit
            )

            total_downloaded += downloaded
            total_skipped += skipped
            total_errors += errors
            total_expected += expected

            # Update manifest summary with expected vs actual tracking
            month_stats = manifest.get('month_stats', {})
            month_statuses = manifest.get('month_status', {})
            manifest['summary'] = {
                'total_expected': sum(m.get('expected', 0) for m in month_stats.values()),
                'total_reports': len(manifest.get('reports', {})),
                'successful_downloads': len([r for r in manifest.get('reports', {}).values()
                                             if r.get('status') == 'success']),
                'errors': len([r for r in manifest.get('reports', {}).values()
                               if r.get('status') == 'error']),
                'months_complete': len([s for s in month_statuses.values() if s == 'complete']),
                'months_partial': len([s for s in month_statuses.values() if s == 'partial']),
                'months_error': len([s for s in month_statuses.values() if s == 'error']),
                'months_processed': len(manifest.get('months_processed', []))  # Legacy
            }
            atomic_json_save(manifest, manifest_file)

            # Check limit
            if limit:
                total_success = manifest['summary']['successful_downloads']
                if total_success >= limit:
                    logger.info(f"Reached limit of {limit} reports, stopping")
                    break

            # Delay between months
            time.sleep(2)

        # Final summary with expected vs actual
        month_statuses = manifest.get('month_status', {})
        complete_count = len([s for s in month_statuses.values() if s == 'complete'])
        partial_count = len([s for s in month_statuses.values() if s == 'partial'])
        error_count = len([s for s in month_statuses.values() if s == 'error'])

        logger.info("\n" + "=" * 50)
        logger.info("Extraction Summary")
        logger.info("=" * 50)
        logger.info(f"Total months:    {total_months}")
        logger.info(f"  Complete:      {complete_count}")
        logger.info(f"  Partial:       {partial_count}")
        logger.info(f"  Error:         {error_count}")
        logger.info(f"Expected:        {total_expected}")
        logger.info(f"Downloaded:      {total_downloaded}")
        logger.info(f"Skipped:         {total_skipped}")
        logger.info(f"Errors:          {total_errors}")

        # Check for discrepancies
        actual_processed = total_downloaded + total_skipped
        if actual_processed != total_expected and total_expected > 0:
            discrepancy = total_expected - actual_processed
            logger.warning(f"DISCREPANCY: Expected {total_expected}, processed {actual_processed} (difference: {discrepancy})")

        # Show months that need attention (partial or error status)
        if partial_count > 0 or error_count > 0:
            logger.warning("\nMonths needing attention:")
            for month, status in sorted(month_statuses.items()):
                if status in ('partial', 'error'):
                    stats = manifest.get('month_stats', {}).get(month, {})
                    expected = stats.get('expected', 0)
                    downloaded = stats.get('downloaded', 0)
                    errors = stats.get('errors', 0)
                    logger.warning(f"  {month} [{status.upper()}]: expected {expected}, downloaded {downloaded}, errors {errors}")
            logger.info("\nRe-run the script to retry failed months (successfully downloaded reports will be skipped)")

        return manifest


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Scrape RABA quality inspection reports (individual downloads)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Parallel Processing Examples:
  # Run 3 workers in parallel (in separate terminals):
  python scrape_raba_individual.py --workers 3 --worker-id 0 --headless
  python scrape_raba_individual.py --workers 3 --worker-id 1 --headless
  python scrape_raba_individual.py --workers 3 --worker-id 2 --headless

  # Each worker processes every Nth month:
  #   Worker 0: months 0, 3, 6, 9, ...
  #   Worker 1: months 1, 4, 7, 10, ...
  #   Worker 2: months 2, 5, 8, 11, ...
        """
    )
    parser.add_argument('--start-date', type=str, default=None,
                        help='Start date (YYYY-MM-DD), default: 2022-05-01')
    parser.add_argument('--end-date', type=str, default=None,
                        help='End date (YYYY-MM-DD), default: today')
    parser.add_argument('--headless', action='store_true',
                        help='Run in headless mode')
    parser.add_argument('--force', action='store_true',
                        help='Force re-download of existing files')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be downloaded without downloading')
    parser.add_argument('--output', type=str, default=None,
                        help='Output directory path')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Show DEBUG level output')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit total number of reports to download (for testing)')
    parser.add_argument('--workers', type=int, default=1,
                        help='Total number of parallel workers (default: 1)')
    parser.add_argument('--worker-id', type=int, default=0,
                        help='This worker\'s ID (0 to workers-1, default: 0)')
    args = parser.parse_args()

    # Validate worker arguments
    if args.worker_id >= args.workers:
        print(f"Error: worker-id ({args.worker_id}) must be less than workers ({args.workers})")
        return 1
    if args.worker_id < 0:
        print(f"Error: worker-id must be >= 0")
        return 1

    # Parse dates
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d') if args.start_date else PROJECT_START_DATE
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d') if args.end_date else datetime.now()

    # Setup output directory
    try:
        from src.config.settings import Settings
        output_dir = Path(args.output) if args.output else Settings.RABA_RAW_DIR / 'individual'
    except ImportError:
        output_dir = Path(args.output) if args.output else project_root / 'data' / 'raw' / 'raba' / 'individual'

    output_dir.mkdir(parents=True, exist_ok=True)

    # Setup logging - include worker ID in log filename if using multiple workers
    if args.workers > 1:
        log_file = output_dir.parent / f'individual_scraper_worker{args.worker_id}.log'
    else:
        log_file = output_dir.parent / 'individual_scraper.log'
    setup_logging(log_file, verbose=args.verbose)

    # Load manifest (shared between workers)
    manifest_file = output_dir.parent / 'individual_manifest.json'
    manifest = load_manifest(manifest_file)

    # Generate all months
    all_months = generate_months(start_date, end_date)

    # Filter months for this worker (round-robin assignment)
    # Worker 0 gets months 0, N, 2N, ...
    # Worker 1 gets months 1, N+1, 2N+1, ...
    if args.workers > 1:
        months = [m for i, m in enumerate(all_months) if i % args.workers == args.worker_id]
        worker_label = f"Worker {args.worker_id}/{args.workers}"
    else:
        months = all_months
        worker_label = "Single Worker"

    logger.info("=" * 60)
    logger.info("RABA Quality Reports Scraper (Individual Mode)")
    logger.info("=" * 60)
    logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"Total months in range: {len(all_months)}")
    if args.workers > 1:
        logger.info(f"Worker: {args.worker_id} of {args.workers} (processing {len(months)} months)")
        month_keys = [m[0] for m in months]
        logger.info(f"Assigned months: {', '.join(month_keys[:5])}{'...' if len(month_keys) > 5 else ''}")
    else:
        logger.info(f"Processing all {len(months)} months")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Headless mode: {args.headless}")
    logger.info(f"Force re-download: {args.force}")
    logger.info(f"Limit: {args.limit or 'None'}")

    if args.dry_run:
        logger.info("\n--- DRY RUN MODE ---\n")

    try:
        with RABAIndividualScraper(headless=args.headless, download_dir=output_dir) as scraper:
            # Login
            if not scraper.login():
                logger.error("Login failed!")
                return 1

            # Navigate to reports page
            if not scraper.navigate_to_reports():
                logger.error("Failed to navigate to reports page!")
                return 1

            # Select project
            if not scraper.select_project():
                logger.error("Failed to select project!")
                return 1

            # Extract all months
            manifest = scraper.extract_all_months(
                months, output_dir, manifest, manifest_file,
                force=args.force,
                dry_run=args.dry_run,
                limit=args.limit
            )

            # Save final manifest
            atomic_json_save(manifest, manifest_file)
            logger.info(f"Manifest saved: {manifest_file}")

            return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == '__main__':
    sys.exit(main())
