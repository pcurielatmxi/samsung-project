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

Usage:
    # Download all days from project start to now
    python scripts/raba/process/scrape_raba_reports.py

    # Download specific date range
    python scripts/raba/process/scrape_raba_reports.py --start-date 2022-05-01 --end-date 2022-06-30

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
import logging
import argparse
import tempfile
import shutil
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
