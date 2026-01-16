#!/usr/bin/env python3
"""
RABA Quality Reports Scraper

Downloads quality inspection reports from RABA RKIS system (celvis.rkci.com).
Extracts reports day by day in an idempotent manner, using batch download.

The script:
1. Logs into RABA system
2. Iterates through each day from start_date to end_date
3. For each day, filters by date, selects all, and batch downloads all reports as one PDF
4. Saves PDFs to raw/raba/daily/{YYYY-MM-DD}.pdf
5. Maintains a manifest.json tracking downloaded dates (including empty dates)

Parallel Mode:
    The script supports parallel processing with --workers N. Each worker:
    - Runs its own browser instance
    - Processes one month at a time
    - When a month completes, picks up the next unprocessed month
    - Uses file-based locking to coordinate month assignments

Usage:
    # Download all days from project start to now
    python scripts/raba/process/scrape_raba_reports.py

    # Download specific date range
    python scripts/raba/process/scrape_raba_reports.py --start-date 2022-05-01 --end-date 2022-06-30

    # Parallel mode with 4 workers (processes 4 months concurrently)
    python scripts/raba/process/scrape_raba_reports.py --workers 4

    # Force re-download of existing days
    python scripts/raba/process/scrape_raba_reports.py --force

    # Run in headless mode
    python scripts/raba/process/scrape_raba_reports.py --headless

    # Dry run (show what would be downloaded)
    python scripts/raba/process/scrape_raba_reports.py --dry-run
"""

import os
import re
import sys
import json
import time
import fcntl
import logging
import argparse
import tempfile
import shutil
import multiprocessing
from calendar import monthrange
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

load_dotenv(override=True)

# Global logger
logger = logging.getLogger('raba_scraper')

# Constants
PROJECT_START_DATE = datetime(2022, 5, 1)  # May 2022 - project start
RABA_LOGIN_URL = "https://celvis.rkci.com/CelvisClient/Login.aspx"
RABA_REPORTS_URL = "https://celvis.rkci.com/CelvisClient/DesktopDefault.aspx?tabindex=1&tabid=2"
PROJECT_CODE = "AAD22-067-00:Samsung Taylor - FAB1 Project"


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


def generate_dates(start_date: datetime, end_date: datetime) -> List[datetime]:
    """Generate list of dates between start and end (inclusive)."""
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


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


class MonthAssignmentManager:
    """Manages month assignments across parallel workers using file-based locking."""

    def __init__(self, assignment_file: Path):
        self.assignment_file = assignment_file
        self.lock_file = assignment_file.with_suffix('.lock')

    def _load_assignments(self) -> dict:
        """Load current assignments from file."""
        if self.assignment_file.exists():
            try:
                with open(self.assignment_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {'assigned': {}, 'completed': [], 'failed': []}

    def _save_assignments(self, data: dict):
        """Save assignments to file."""
        atomic_json_save(data, self.assignment_file)

    def claim_month(self, worker_id: str, available_months: List[str],
                    retry_failed: bool = True) -> Optional[str]:
        """Try to claim an unassigned month for this worker.

        Args:
            worker_id: Unique identifier for this worker
            available_months: List of month keys (YYYY-MM) to consider
            retry_failed: If True, also claim months that previously failed

        Returns:
            Month key if claimed, None if no months available
        """
        # Use file locking for coordination
        with open(self.lock_file, 'w') as lock_f:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
            try:
                data = self._load_assignments()

                # Find first unassigned, uncompleted month
                for month_key in available_months:
                    is_assigned = month_key in data['assigned']
                    is_completed = month_key in data['completed']
                    is_failed = month_key in data['failed']

                    # Skip if assigned or completed
                    if is_assigned or is_completed:
                        continue

                    # Skip failed unless retry_failed is True
                    if is_failed and not retry_failed:
                        continue

                    # Claim it (remove from failed if retrying)
                    if is_failed:
                        data['failed'].remove(month_key)

                    data['assigned'][month_key] = {
                        'worker_id': worker_id,
                        'started_at': datetime.now().isoformat(),
                        'is_retry': is_failed
                    }
                    self._save_assignments(data)
                    return month_key

                return None
            finally:
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)

    def mark_completed(self, month_key: str, worker_id: str):
        """Mark a month as completed."""
        with open(self.lock_file, 'w') as lock_f:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
            try:
                data = self._load_assignments()
                if month_key in data['assigned']:
                    del data['assigned'][month_key]
                if month_key not in data['completed']:
                    data['completed'].append(month_key)
                self._save_assignments(data)
            finally:
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)

    def mark_failed(self, month_key: str, worker_id: str, error: str):
        """Mark a month as failed."""
        with open(self.lock_file, 'w') as lock_f:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
            try:
                data = self._load_assignments()
                if month_key in data['assigned']:
                    del data['assigned'][month_key]
                if month_key not in data['failed']:
                    data['failed'].append(month_key)
                self._save_assignments(data)
            finally:
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)

    def release_assignment(self, month_key: str, worker_id: str):
        """Release a month assignment (e.g., on worker crash recovery)."""
        with open(self.lock_file, 'w') as lock_f:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
            try:
                data = self._load_assignments()
                if month_key in data['assigned']:
                    del data['assigned'][month_key]
                self._save_assignments(data)
            finally:
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)

    def get_status(self) -> dict:
        """Get current assignment status."""
        with open(self.lock_file, 'w') as lock_f:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
            try:
                return self._load_assignments()
            finally:
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)

    def reset(self):
        """Reset all assignments (use with caution)."""
        if self.assignment_file.exists():
            self.assignment_file.unlink()
        if self.lock_file.exists():
            self.lock_file.unlink()

    def release_stale_assignments(self):
        """Release all currently assigned months (for crash recovery).

        When the script starts, any months marked as 'assigned' are stale
        because no workers are currently running. This releases them so
        they can be picked up by new workers.
        """
        with open(self.lock_file, 'w') as lock_f:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
            try:
                data = self._load_assignments()
                stale_count = len(data['assigned'])
                if stale_count > 0:
                    stale_months = list(data['assigned'].keys())
                    data['assigned'] = {}
                    self._save_assignments(data)
                    return stale_months
                return []
            finally:
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)


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
        'source': 'RABA RKIS Quality Reports',
        'project': PROJECT_CODE,
        'created_at': datetime.now().isoformat(),
        'dates': {}  # Track each date by YYYY-MM-DD key
    }

    if manifest_file.exists():
        try:
            with open(manifest_file, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
                # Merge loaded data, ensuring 'dates' key exists
                manifest.update(loaded)
                if 'dates' not in manifest:
                    manifest['dates'] = {}
        except (json.JSONDecodeError, IOError):
            pass

    return manifest


class RABAScraper:
    """Scraper for RABA RKIS quality inspection reports."""

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

            # Fill login form using actual element IDs
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

    def set_date_range(self, date: datetime) -> bool:
        """Set the service date range filter to a single day."""
        try:
            date_str = date.strftime('%m/%d/%Y')

            # Use the actual element IDs
            start_input = self.page.locator('#_ctl4_txtDateFrom')
            end_input = self.page.locator('#_ctl4_txtDateTo')

            # Clear and fill with same date for both
            start_input.clear()
            start_input.fill(date_str)

            end_input.clear()
            end_input.fill(date_str)

            logger.debug(f"Set date: {date_str}")
            return True
        except Exception as e:
            logger.error(f"Error setting date: {e}")
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

            # Get record count by scanning all td cells
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

    def select_all_and_download(self, output_path: Path, timeout: int = 120000) -> bool:
        """Select all records and download as batch PDF.

        The RABA system opens the PDF in a new tab with a URL like:
        https://celvis.rkci.com/CelvisClient/Report/BatchPDF.aspx?f=...Batch.pdf

        We handle this by:
        1. Clicking Select All
        2. Clicking the batch button (opens new tab with PDF URL)
        3. Using expect_popup to capture the new tab
        4. Using context.request.get() to fetch the PDF bytes directly
        5. Saving the PDF to disk

        Args:
            output_path: Path to save the PDF
            timeout: Download timeout in milliseconds
        """
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Click "Select All" link
            select_all_link = self.page.locator('a:has-text("Select All")')
            if select_all_link.count() == 0:
                logger.error("Select All link not found")
                return False

            select_all_link.click()
            time.sleep(1)

            # Click batch button and wait for popup using expect_popup
            batch_btn = self.page.locator('#_ctl4_btnBatch')

            try:
                with self.page.expect_popup(timeout=timeout) as popup_info:
                    batch_btn.click()
                new_page = popup_info.value
            except Exception as popup_err:
                logger.error(f"Popup did not open: {popup_err}")
                return False

            # Wait a moment for the page URL to be set
            time.sleep(2)

            # Get the PDF URL
            pdf_url = new_page.url
            logger.debug(f"PDF URL: {pdf_url}")

            # Verify it's a PDF URL
            if 'BatchPDF.aspx' not in pdf_url and '.pdf' not in pdf_url.lower():
                logger.error(f"Unexpected URL format: {pdf_url}")
                new_page.close()
                return False

            # Fetch the PDF using context.request.get() - this preserves cookies/session
            try:
                response = self.context.request.get(pdf_url)
                if response.status != 200:
                    logger.error(f"HTTP error {response.status} fetching PDF")
                    new_page.close()
                    return False

                pdf_bytes = response.body()

                with open(output_path, 'wb') as f:
                    f.write(pdf_bytes)

            except Exception as fetch_err:
                logger.error(f"Fetch failed: {fetch_err}")
                new_page.close()
                return False

            # Close the PDF tab
            try:
                new_page.close()
            except Exception:
                pass

            # Verify file
            if output_path.exists() and output_path.stat().st_size > 0:
                logger.info(f"Downloaded: {output_path.name} ({output_path.stat().st_size:,} bytes)")
                return True
            else:
                logger.error(f"Download failed or empty: {output_path}")
                return False

        except Exception as e:
            logger.error(f"Download error: {e}")
            return False

    def extract_date(self, date: datetime, output_dir: Path, manifest: dict,
                     force: bool = False) -> dict:
        """Extract all reports for a single date using batch download.

        Args:
            date: The date to extract
            output_dir: Base directory for output files
            manifest: Manifest dict with existing date info
            force: Re-download even if file exists
        """
        date_str = date.strftime('%Y-%m-%d')
        output_file = output_dir / f"{date_str}.pdf"

        result = {
            'date': date_str,
            'status': 'pending',
            'record_count': 0,
            'file': None,
            'file_size': None,
            'extracted_at': None,
            'error': None
        }

        # Check if already processed (including empty dates)
        existing = manifest.get('dates', {}).get(date_str)
        if existing and not force:
            if existing.get('status') in ('success', 'empty'):
                logger.debug(f"{date_str}: Skipping (already processed - {existing.get('status')})")
                return None  # Signal to skip

        try:
            # Set date range to single day
            if not self.set_date_range(date):
                result['status'] = 'error'
                result['error'] = 'Failed to set date'
                return result

            # Search for reports
            record_count = self.search_reports()
            result['record_count'] = record_count

            if record_count == 0:
                # No records - mark as empty so we don't retry
                result['status'] = 'empty'
                result['extracted_at'] = datetime.now().isoformat()
                logger.info(f"{date_str}: No records (marked as empty)")
                return result

            if record_count < 0:
                result['status'] = 'error'
                result['error'] = 'Search failed'
                return result

            logger.info(f"{date_str}: Found {record_count} reports, downloading batch...")

            # Select all and download
            if self.select_all_and_download(output_file):
                result['status'] = 'success'
                result['file'] = f"{date_str}.pdf"
                result['file_size'] = output_file.stat().st_size
                result['extracted_at'] = datetime.now().isoformat()
            else:
                result['status'] = 'error'
                result['error'] = 'Download failed'

            return result

        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
            logger.error(f"{date_str}: Error - {e}")
            return result

    def extract_all_dates(self, dates: List[datetime], output_dir: Path,
                          manifest: dict, manifest_file: Path = None,
                          force: bool = False, dry_run: bool = False) -> dict:
        """Extract reports for all specified dates."""
        total = len(dates)
        downloaded_count = 0
        empty_count = 0
        error_count = 0
        skipped_count = 0

        for i, date in enumerate(dates):
            date_str = date.strftime('%Y-%m-%d')
            logger.info(f"[{i+1}/{total}] Processing {date_str}")

            if dry_run:
                existing = manifest.get('dates', {}).get(date_str)
                if existing and existing.get('status') in ('success', 'empty') and not force:
                    logger.info(f"  Would skip (already {existing.get('status')})")
                else:
                    logger.info(f"  Would process")
                continue

            # Extract this date
            result = self.extract_date(date, output_dir, manifest, force)

            if result is None:
                # Skipped (already processed)
                skipped_count += 1
                continue

            # Update manifest with result
            manifest['dates'][date_str] = result

            # Update counters
            if result['status'] == 'success':
                downloaded_count += 1
            elif result['status'] == 'empty':
                empty_count += 1
            elif result['status'] == 'error':
                error_count += 1

            # Update manifest summary
            manifest['last_updated'] = datetime.now().isoformat()
            manifest['summary'] = {
                'total_dates_processed': len([d for d in manifest.get('dates', {}).values()
                                              if d.get('status') in ('success', 'empty')]),
                'dates_with_reports': len([d for d in manifest.get('dates', {}).values()
                                           if d.get('status') == 'success']),
                'empty_dates': len([d for d in manifest.get('dates', {}).values()
                                    if d.get('status') == 'empty']),
                'error_dates': len([d for d in manifest.get('dates', {}).values()
                                    if d.get('status') == 'error']),
            }

            # Save manifest incrementally
            if manifest_file:
                atomic_json_save(manifest, manifest_file)

            # Delay between dates
            time.sleep(2)

        # Summary
        logger.info("=" * 50)
        logger.info("Extraction Summary")
        logger.info("=" * 50)
        logger.info(f"Total dates:    {total}")
        logger.info(f"Downloaded:     {downloaded_count}")
        logger.info(f"Empty dates:    {empty_count}")
        logger.info(f"Skipped:        {skipped_count}")
        logger.info(f"Errors:         {error_count}")

        return manifest


def worker_process(worker_id: str, months_info: List[Tuple[str, datetime, datetime]],
                   output_dir: Path, manifest_file: Path, assignment_file: Path,
                   headless: bool, force: bool, verbose: bool, retry_failed: bool = True):
    """Worker process that processes months one at a time.

    Each worker:
    1. Claims an unassigned month from the assignment manager
    2. Processes all dates in that month
    3. Marks the month as completed
    4. Repeats until no more months available
    """
    # Setup worker-specific logging
    log_file = output_dir.parent / f'scraper_worker_{worker_id}.log'
    worker_logger = logging.getLogger(f'raba_scraper.worker.{worker_id}')
    worker_logger.handlers.clear()
    worker_logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        f'%(asctime)s | W{worker_id} | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    worker_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    console_formatter = logging.Formatter(f'[W{worker_id}] %(message)s')
    console_handler.setFormatter(console_formatter)
    worker_logger.addHandler(console_handler)

    # Build month lookup
    month_lookup = {m[0]: (m[1], m[2]) for m in months_info}
    month_keys = [m[0] for m in months_info]

    assignment_mgr = MonthAssignmentManager(assignment_file)
    months_processed = 0

    worker_logger.info(f"Worker {worker_id} starting")

    try:
        with RABAScraper(headless=headless, download_dir=output_dir) as scraper:
            # Login once
            if not scraper.login():
                worker_logger.error("Login failed!")
                return

            if not scraper.navigate_to_reports():
                worker_logger.error("Failed to navigate to reports page!")
                return

            if not scraper.select_project():
                worker_logger.error("Failed to select project!")
                return

            # Process months until none available
            while True:
                # Try to claim a month
                month_key = assignment_mgr.claim_month(worker_id, month_keys, retry_failed=retry_failed)

                if month_key is None:
                    worker_logger.info("No more months available - worker done")
                    break

                month_start, month_end = month_lookup[month_key]
                dates = generate_dates(month_start, month_end)

                worker_logger.info(f"Processing {month_key}: {len(dates)} days "
                                   f"({month_start.strftime('%Y-%m-%d')} to {month_end.strftime('%Y-%m-%d')})")

                try:
                    # Load manifest (with file locking for thread safety)
                    manifest = load_manifest_locked(manifest_file)

                    # Process all dates in this month
                    downloaded = 0
                    empty = 0
                    errors = 0
                    skipped = 0

                    for i, date in enumerate(dates):
                        date_str = date.strftime('%Y-%m-%d')

                        result = scraper.extract_date(date, output_dir, manifest, force)

                        if result is None:
                            skipped += 1
                            continue

                        # Update manifest with result (locked)
                        update_manifest_locked(manifest_file, date_str, result)

                        if result['status'] == 'success':
                            downloaded += 1
                        elif result['status'] == 'empty':
                            empty += 1
                        elif result['status'] == 'error':
                            errors += 1

                        # Brief delay between dates
                        time.sleep(1)

                    worker_logger.info(f"Month {month_key} complete: "
                                       f"{downloaded} downloaded, {empty} empty, "
                                       f"{skipped} skipped, {errors} errors")

                    assignment_mgr.mark_completed(month_key, worker_id)
                    months_processed += 1

                except Exception as e:
                    worker_logger.error(f"Error processing month {month_key}: {e}")
                    import traceback
                    worker_logger.error(traceback.format_exc())
                    assignment_mgr.mark_failed(month_key, worker_id, str(e))

    except Exception as e:
        worker_logger.error(f"Worker fatal error: {e}")
        import traceback
        worker_logger.error(traceback.format_exc())

    worker_logger.info(f"Worker {worker_id} finished - processed {months_processed} months")


def load_manifest_locked(manifest_file: Path) -> dict:
    """Load manifest with file locking."""
    lock_file = manifest_file.with_suffix('.manifest.lock')

    with open(lock_file, 'w') as lock_f:
        fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
        try:
            return load_manifest(manifest_file)
        finally:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)


def update_manifest_locked(manifest_file: Path, date_str: str, result: dict):
    """Update manifest with a single date result, using file locking."""
    lock_file = manifest_file.with_suffix('.manifest.lock')

    with open(lock_file, 'w') as lock_f:
        fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
        try:
            manifest = load_manifest(manifest_file)
            manifest['dates'][date_str] = result
            manifest['last_updated'] = datetime.now().isoformat()

            # Update summary
            manifest['summary'] = {
                'total_dates_processed': len([d for d in manifest.get('dates', {}).values()
                                              if d.get('status') in ('success', 'empty')]),
                'dates_with_reports': len([d for d in manifest.get('dates', {}).values()
                                           if d.get('status') == 'success']),
                'empty_dates': len([d for d in manifest.get('dates', {}).values()
                                    if d.get('status') == 'empty']),
                'error_dates': len([d for d in manifest.get('dates', {}).values()
                                    if d.get('status') == 'error']),
            }

            atomic_json_save(manifest, manifest_file)
        finally:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)


def run_parallel(num_workers: int, start_date: datetime, end_date: datetime,
                 output_dir: Path, manifest_file: Path, headless: bool,
                 force: bool, verbose: bool, reset_assignments: bool = False,
                 skip_failed: bool = False):
    """Run the scraper in parallel mode with multiple workers."""
    # Generate months
    months = generate_months(start_date, end_date)
    month_keys = [m[0] for m in months]

    logger.info("=" * 60)
    logger.info("RABA Quality Reports Scraper (Parallel Mode)")
    logger.info("=" * 60)
    logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"Total months: {len(months)}")
    logger.info(f"Workers: {num_workers}")
    logger.info(f"Output directory: {output_dir}")
    logger.info("")
    logger.info("Months to process:")
    for month_key, m_start, m_end in months:
        days = (m_end - m_start).days + 1
        logger.info(f"  {month_key}: {days} days ({m_start.strftime('%Y-%m-%d')} to {m_end.strftime('%Y-%m-%d')})")

    # Setup assignment file
    assignment_file = output_dir.parent / 'parallel_assignments.json'

    mgr = MonthAssignmentManager(assignment_file)

    if reset_assignments:
        logger.info("Resetting month assignments...")
        mgr.reset()
    else:
        # Release any stale assignments from previous crashed runs
        stale = mgr.release_stale_assignments()
        if stale:
            logger.info(f"Released {len(stale)} stale month assignments from previous run:")
            for m in sorted(stale):
                logger.info(f"  {m} (will be retried)")

    # Show current assignment status
    status = mgr.get_status()
    if status['completed']:
        logger.info(f"\nAlready completed months: {len(status['completed'])}")
        for m in sorted(status['completed']):
            logger.info(f"  {m}")
    if status['failed']:
        logger.info(f"\nFailed months: {len(status['failed'])}")
        for m in sorted(status['failed']):
            logger.info(f"  {m}")

    # Calculate remaining (failed months will be retried automatically)
    remaining_new = [m for m in month_keys
                     if m not in status['completed'] and m not in status['failed']]
    remaining_retry = [m for m in month_keys
                       if m in status['failed']]
    total_remaining = len(remaining_new) + len(remaining_retry)

    logger.info(f"\nMonths remaining: {total_remaining} ({len(remaining_new)} new, {len(remaining_retry)} retry)")

    if total_remaining == 0:
        logger.info("All months already processed!")
        return 0

    if remaining_retry:
        logger.info(f"Failed months to retry: {remaining_retry}")
        logger.info("(Previously downloaded dates within failed months will be skipped)")

    # Start worker processes
    logger.info(f"\nStarting {num_workers} worker processes...")

    retry_failed = not skip_failed  # Invert the flag for clarity

    processes = []
    for i in range(num_workers):
        worker_id = str(i + 1)
        p = multiprocessing.Process(
            target=worker_process,
            args=(worker_id, months, output_dir, manifest_file, assignment_file,
                  headless, force, verbose, retry_failed)
        )
        p.start()
        processes.append(p)
        logger.info(f"Started worker {worker_id} (PID: {p.pid})")
        time.sleep(2)  # Stagger worker starts to avoid login collisions

    # Wait for all workers to complete
    logger.info("\nWaiting for workers to complete...")
    for p in processes:
        p.join()

    # Final status
    status = mgr.get_status()
    logger.info("\n" + "=" * 60)
    logger.info("Parallel Processing Complete")
    logger.info("=" * 60)
    logger.info(f"Completed months: {len(status['completed'])}")
    logger.info(f"Failed months: {len(status['failed'])}")

    if status['failed']:
        logger.warning(f"Failed months: {status['failed']}")
        return 1

    return 0


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Scrape RABA quality inspection reports')
    parser.add_argument('--start-date', type=str, default=None,
                        help='Start date (YYYY-MM-DD), default: 2022-05-01')
    parser.add_argument('--end-date', type=str, default=None,
                        help='End date (YYYY-MM-DD), default: today')
    parser.add_argument('--headless', action='store_true',
                        help='Run in headless mode')
    parser.add_argument('--force', action='store_true',
                        help='Force re-download of existing dates')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be downloaded without downloading')
    parser.add_argument('--output', type=str, default=None,
                        help='Output directory path')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Show DEBUG level output')
    parser.add_argument('--workers', '-w', type=int, default=1,
                        help='Number of parallel workers (default: 1 = sequential mode)')
    parser.add_argument('--reset-assignments', action='store_true',
                        help='Reset parallel month assignments (use with --workers)')
    parser.add_argument('--skip-failed', action='store_true',
                        help='Skip previously failed months instead of retrying them')
    args = parser.parse_args()

    # Parse dates
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d') if args.start_date else PROJECT_START_DATE
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d') if args.end_date else datetime.now()

    # Setup output directory
    try:
        from src.config.settings import Settings
        output_dir = Path(args.output) if args.output else Settings.RABA_RAW_DIR / 'daily'
    except ImportError:
        output_dir = Path(args.output) if args.output else project_root / 'data' / 'raw' / 'raba' / 'daily'

    output_dir.mkdir(parents=True, exist_ok=True)

    # Setup logging
    log_file = output_dir.parent / 'scraper.log'
    setup_logging(log_file, verbose=args.verbose)

    # Load manifest
    manifest_file = output_dir.parent / 'manifest.json'

    # Check for parallel mode
    if args.workers > 1:
        if args.dry_run:
            # Show parallel plan without running
            months = generate_months(start_date, end_date)
            manifest = load_manifest(manifest_file)
            logger.info("=" * 60)
            logger.info("RABA Quality Reports Scraper (Parallel Mode - DRY RUN)")
            logger.info("=" * 60)
            logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            logger.info(f"Total months: {len(months)}")
            logger.info(f"Workers: {args.workers}")
            logger.info("")
            for month_key, m_start, m_end in months:
                days = (m_end - m_start).days + 1
                dates_in_month = generate_dates(m_start, m_end)
                pending = sum(1 for d in dates_in_month
                              if manifest.get('dates', {}).get(d.strftime('%Y-%m-%d'), {}).get('status')
                              not in ('success', 'empty') or args.force)
                logger.info(f"  {month_key}: {days} days total, {pending} pending")
            return 0

        # Parallel mode - process by months with multiple workers
        return run_parallel(
            num_workers=args.workers,
            start_date=start_date,
            end_date=end_date,
            output_dir=output_dir,
            manifest_file=manifest_file,
            headless=args.headless,
            force=args.force,
            verbose=args.verbose,
            reset_assignments=args.reset_assignments,
            skip_failed=args.skip_failed
        )

    # Sequential mode (original behavior)
    manifest = load_manifest(manifest_file)

    # Generate dates to process
    dates = generate_dates(start_date, end_date)

    logger.info("=" * 60)
    logger.info("RABA Quality Reports Scraper (Daily Batch Mode)")
    logger.info("=" * 60)
    logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"Total dates: {len(dates)}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Headless mode: {args.headless}")
    logger.info(f"Force re-download: {args.force}")
    logger.info(f"Dry run: {args.dry_run}")

    if args.dry_run:
        logger.info("\n--- DRY RUN MODE ---\n")
        for date in dates:
            date_str = date.strftime('%Y-%m-%d')
            existing = manifest.get('dates', {}).get(date_str)
            if existing and existing.get('status') in ('success', 'empty') and not args.force:
                status = f"skip ({existing.get('status')})"
            else:
                status = "download"
            logger.info(f"  {date_str}: would {status}")
        return 0

    try:
        with RABAScraper(headless=args.headless, download_dir=output_dir) as scraper:
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

            # Extract all dates
            manifest = scraper.extract_all_dates(
                dates, output_dir, manifest,
                manifest_file=manifest_file,
                force=args.force, dry_run=args.dry_run
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
