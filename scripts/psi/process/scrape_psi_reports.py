#!/usr/bin/env python3
"""
PSI (Construction Hive) Quality Reports Scraper

Downloads quality inspection reports from PSI Construction Hive system.
Extracts reports with metadata in an idempotent manner with resume capability.

The script:
1. Logs into PSI system (constructionhive.com)
2. Searches for all documents in the specified project
3. Iterates through paginated results (10 per page)
4. Downloads each PDF and extracts metadata
5. Maintains a manifest.json tracking downloaded documents

Usage:
    # Download all reports (default: TAYLOR - SAMSUNG FAB1 project)
    python scripts/psi/process/scrape_psi_reports.py

    # Limit number of documents to download
    python scripts/psi/process/scrape_psi_reports.py --limit 100

    # Resume from specific offset
    python scripts/psi/process/scrape_psi_reports.py --start-offset 1000

    # Force re-download of existing documents
    python scripts/psi/process/scrape_psi_reports.py --force

    # Run in headless mode
    python scripts/psi/process/scrape_psi_reports.py --headless

    # Dry run (show what would be downloaded)
    python scripts/psi/process/scrape_psi_reports.py --dry-run

Environment Variables (from .env):
    PSI_USERNAME - Login email
    PSI_PASSWORD - Login password
    PSI_BASE_URL - Base URL (default: https://www.constructionhive.com/)
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
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode, quote
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

load_dotenv(override=True)

# Global logger
logger = logging.getLogger('psi_scraper')

# Constants
PSI_BASE_URL = os.getenv('PSI_BASE_URL', 'https://www.constructionhive.com/')
PSI_LOGIN_URL = PSI_BASE_URL.rstrip('/') + '/'
PSI_SEARCH_URL = PSI_BASE_URL.rstrip('/') + '/Search/Filter'

# Samsung Taylor FAB1 Project UUID
DEFAULT_PROJECT_UUID = 'c6f360d9-512c-4bb8-9019-b14c01219b69'
DEFAULT_PROJECT_NAME = 'TAYLOR - SAMSUNG FAB1'

# Pagination settings
PAGE_SIZE = 10  # PSI returns 10 results per page


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
        'source': 'PSI Construction Hive',
        'project_uuid': DEFAULT_PROJECT_UUID,
        'project_name': DEFAULT_PROJECT_NAME,
        'created_at': datetime.now().isoformat(),
        'documents': {}  # Track each document by UUID
    }

    if manifest_file.exists():
        try:
            with open(manifest_file, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
                manifest.update(loaded)
                if 'documents' not in manifest:
                    manifest['documents'] = {}
        except (json.JSONDecodeError, IOError):
            pass

    return manifest


def update_manifest_summary(manifest: dict) -> dict:
    """Update manifest summary statistics."""
    docs = manifest.get('documents', {})
    manifest['summary'] = {
        'total_documents': len(docs),
        'successful': len([d for d in docs.values() if d.get('status') == 'success']),
        'errors': len([d for d in docs.values() if d.get('status') == 'error']),
        'skipped': len([d for d in docs.values() if d.get('status') == 'skipped']),
    }
    manifest['last_updated'] = datetime.now().isoformat()
    return manifest


class PSIScraper:
    """Scraper for PSI Construction Hive quality reports."""

    def __init__(self, headless: bool = False, download_dir: Path = None):
        """Initialize the scraper."""
        self.headless = headless
        self.download_dir = download_dir
        self.page = None
        self.browser = None
        self.context = None
        self.playwright = None

        # Load credentials from environment
        self.username = os.getenv('PSI_USERNAME')
        self.password = os.getenv('PSI_PASSWORD')

        if not self.username or not self.password:
            raise ValueError("PSI_USERNAME and PSI_PASSWORD must be set in .env file")

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
        """Log into PSI system."""
        logger.info("Logging into PSI system...")

        try:
            self.page.goto(PSI_LOGIN_URL, timeout=30000)
            self.page.wait_for_load_state('networkidle', timeout=30000)
            time.sleep(2)

            # Use the visible text-wide class input (desktop version)
            username_field = self.page.locator('input#username.text-wide')
            username_field.wait_for(state='visible', timeout=10000)
            username_field.fill(self.username)

            password_field = self.page.locator('input#password.text-wide')
            password_field.wait_for(state='visible', timeout=10000)
            password_field.fill(self.password)

            # Click sign in button
            submit_btn = self.page.get_by_role('button', name='Sign in')
            submit_btn.click()

            # Wait for navigation
            self.page.wait_for_load_state('networkidle', timeout=30000)
            time.sleep(3)

            # Check if login successful - look for user menu or dashboard elements
            # After login, we should be on the main page
            current_url = self.page.url
            if 'Login' not in current_url and 'login' not in current_url:
                logger.info("Login successful")
                return True
            else:
                logger.error("Login failed - still on login page")
                return False

        except Exception as e:
            logger.error(f"Login error: {e}")
            return False

    def get_total_documents(self, project_uuid: str) -> int:
        """Get total number of documents for a project."""
        try:
            search_url = f"{PSI_SEARCH_URL}?Keywords=ProjectUUID%3A{project_uuid}&Sort=WorkDate&SortDirection=DESC&Offset=0"
            self.page.goto(search_url, timeout=30000)
            self.page.wait_for_load_state('networkidle', timeout=30000)
            time.sleep(1)

            # Extract total count from "Results 1-10 of X"
            results_text = self.page.locator('p:has-text("Results")').text_content()
            match = re.search(r'of\s*"?(\d+)"?', results_text)
            if match:
                total = int(match.group(1))
                logger.info(f"Found {total} total documents")
                return total

            return 0
        except Exception as e:
            logger.error(f"Error getting total documents: {e}")
            return 0

    def get_documents_page(self, project_uuid: str, offset: int) -> List[Dict]:
        """Get documents from a single search results page.

        Returns list of document metadata dicts.
        """
        documents = []

        try:
            search_url = f"{PSI_SEARCH_URL}?Keywords=ProjectUUID%3A{project_uuid}&Sort=WorkDate&SortDirection=DESC&Offset={offset}"
            self.page.goto(search_url, timeout=30000)
            self.page.wait_for_load_state('networkidle', timeout=30000)
            time.sleep(1)

            # Find all document links that match the pattern /Document/{uuid}
            # These are the actual document links with IDs like "DFR:0306103-9671-O1"
            doc_links = self.page.locator('a[href^="/Document/"]').all()

            seen_uuids = set()
            for link in doc_links:
                try:
                    href = link.get_attribute('href')
                    if not href:
                        continue

                    # Skip download/content links
                    if '/Document/Download' in href or '/Document/Content' in href:
                        continue

                    # Skip project overview links
                    if '/Project/' in href or '/Company/' in href:
                        continue

                    # Extract UUID from href
                    uuid_match = re.search(r'/Document/([a-f0-9-]{36})', href)
                    if not uuid_match:
                        continue

                    doc_uuid = uuid_match.group(1)

                    # Skip duplicates
                    if doc_uuid in seen_uuids:
                        continue
                    seen_uuids.add(doc_uuid)

                    # Get document ID (link text)
                    doc_id = link.text_content().strip()

                    # Skip non-document links (project names, company names)
                    if not doc_id or doc_id.startswith('TAYLOR') or doc_id.startswith('0306 -'):
                        continue

                    # Document ID should match pattern like DFR:0306103-9671-O1
                    if not re.match(r'^[A-Z]{2,}:', doc_id):
                        continue

                    # Get the parent row to extract metadata
                    # Navigate up to find the listitem containing this link
                    parent_row = link.locator('xpath=ancestor::li[ul]').first

                    activity_date = None
                    published_date = None
                    doc_type = 'Report'

                    if parent_row.count():
                        row_text = parent_row.text_content()

                        # Extract activity date from title attribute
                        activity_item = parent_row.locator('li[title^="Activity at:"]').first
                        if activity_item.count():
                            activity_title = activity_item.get_attribute('title')
                            activity_match = re.search(r'Activity at:\s*(\d{1,2}/\d{1,2}/\d{4})', activity_title or '')
                            if activity_match:
                                activity_date = activity_match.group(1)

                        # Extract published date from title attribute
                        published_item = parent_row.locator('li[title^="Published at:"]').first
                        if published_item.count():
                            published_title = published_item.get_attribute('title')
                            published_match = re.search(r'Published at:\s*(\d{1,2}/\d{1,2}/\d{4})', published_title or '')
                            if published_match:
                                published_date = published_match.group(1)

                        # Document type
                        if 'Daily Field Report' in row_text:
                            doc_type = 'Daily Field Report'
                        elif 'Laboratory' in row_text:
                            doc_type = 'Laboratory Report'

                    documents.append({
                        'uuid': doc_uuid,
                        'document_id': doc_id,
                        'document_type': doc_type,
                        'activity_date': activity_date,
                        'published_date': published_date,
                        'download_url': f"/Document/Download?DocumentUUID={doc_uuid}&RevisionNumber=1"
                    })

                    logger.debug(f"Found document: {doc_id} ({doc_uuid})")

                except Exception as e:
                    logger.debug(f"Error parsing document link: {e}")
                    continue

            logger.debug(f"Page offset {offset}: found {len(documents)} documents")
            return documents

        except Exception as e:
            logger.error(f"Error getting documents page at offset {offset}: {e}")
            return []

    def download_document(self, doc: Dict, output_dir: Path) -> Tuple[bool, Optional[str], Optional[int]]:
        """Download a single document PDF.

        Returns: (success, filename, file_size)
        """
        doc_uuid = doc['uuid']
        doc_id = doc['document_id']

        # Create safe filename from document ID
        safe_filename = re.sub(r'[^\w\-.]', '_', doc_id) + '.pdf'
        output_path = output_dir / safe_filename

        try:
            download_url = PSI_BASE_URL.rstrip('/') + doc['download_url']

            # Use JavaScript to trigger the download
            # This avoids page navigation issues with direct download URLs
            with self.page.expect_download(timeout=60000) as download_info:
                # Create a temporary link and click it to trigger download
                self.page.evaluate(f'''() => {{
                    const link = document.createElement('a');
                    link.href = "{download_url}";
                    link.download = "{safe_filename}";
                    document.body.appendChild(link);
                    link.click();
                    document.body.removeChild(link);
                }}''')

            download = download_info.value

            # Save to our output path
            download.save_as(output_path)

            if output_path.exists() and output_path.stat().st_size > 0:
                file_size = output_path.stat().st_size
                logger.info(f"Downloaded: {safe_filename} ({file_size:,} bytes)")
                return True, safe_filename, file_size
            else:
                logger.error(f"Download failed or empty: {safe_filename}")
                return False, None, None

        except Exception as e:
            logger.error(f"Download error for {doc_id}: {e}")
            return False, None, None

    def scrape_all(self, project_uuid: str, output_dir: Path, manifest: dict,
                   manifest_file: Path, start_offset: int = 0, limit: int = 0,
                   force: bool = False, dry_run: bool = False) -> dict:
        """Scrape all documents from the project.

        Args:
            project_uuid: Project UUID to scrape
            output_dir: Directory to save PDFs
            manifest: Manifest dict to update
            manifest_file: Path to save manifest
            start_offset: Starting offset for pagination
            limit: Maximum documents to download (0 = all)
            force: Re-download existing documents
            dry_run: Only show what would be downloaded
        """
        # Get total count
        total = self.get_total_documents(project_uuid)
        if total == 0:
            logger.error("No documents found or error getting count")
            return manifest

        total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
        start_page = start_offset // PAGE_SIZE

        logger.info(f"Total documents: {total}")
        logger.info(f"Total pages: {total_pages}")
        logger.info(f"Starting from page {start_page + 1} (offset {start_offset})")

        if limit > 0:
            logger.info(f"Limiting to {limit} documents")

        downloaded_count = 0
        skipped_count = 0
        error_count = 0

        current_offset = start_offset
        docs_processed = 0

        while current_offset < total:
            page_num = current_offset // PAGE_SIZE + 1
            logger.info(f"[Page {page_num}/{total_pages}] Fetching documents at offset {current_offset}...")

            # Get documents from this page
            documents = self.get_documents_page(project_uuid, current_offset)

            if not documents:
                logger.warning(f"No documents found on page {page_num}, moving to next")
                current_offset += PAGE_SIZE
                continue

            for doc in documents:
                doc_uuid = doc['uuid']
                doc_id = doc['document_id']

                # Check if already processed
                existing = manifest.get('documents', {}).get(doc_uuid)
                if existing and existing.get('status') == 'success' and not force:
                    logger.info(f"Skipping {doc_id} (already downloaded)")
                    skipped_count += 1
                    docs_processed += 1

                    if limit > 0 and docs_processed >= limit:
                        break
                    continue

                if dry_run:
                    logger.info(f"Would download: {doc_id} ({doc['activity_date']})")
                    docs_processed += 1
                    if limit > 0 and docs_processed >= limit:
                        break
                    continue

                # Download the document
                success, filename, file_size = self.download_document(doc, output_dir)

                # Update manifest
                manifest['documents'][doc_uuid] = {
                    'uuid': doc_uuid,
                    'document_id': doc_id,
                    'document_type': doc.get('document_type'),
                    'activity_date': doc.get('activity_date'),
                    'published_date': doc.get('published_date'),
                    'status': 'success' if success else 'error',
                    'filename': filename,
                    'file_size': file_size,
                    'extracted_at': datetime.now().isoformat(),
                    'error': None if success else 'Download failed'
                }

                if success:
                    downloaded_count += 1
                else:
                    error_count += 1

                docs_processed += 1

                # Save manifest periodically
                if docs_processed % 10 == 0:
                    update_manifest_summary(manifest)
                    atomic_json_save(manifest, manifest_file)

                # Check limit
                if limit > 0 and docs_processed >= limit:
                    logger.info(f"Reached limit of {limit} documents")
                    break

                # Brief delay between downloads
                time.sleep(0.5)

            # Check if we've hit the limit
            if limit > 0 and docs_processed >= limit:
                break

            current_offset += PAGE_SIZE

        # Final summary
        logger.info("=" * 50)
        logger.info("Scraping Summary")
        logger.info("=" * 50)
        logger.info(f"Documents processed: {docs_processed}")
        logger.info(f"Downloaded:          {downloaded_count}")
        logger.info(f"Skipped:             {skipped_count}")
        logger.info(f"Errors:              {error_count}")

        # Update and save final manifest
        update_manifest_summary(manifest)
        atomic_json_save(manifest, manifest_file)

        return manifest


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Scrape PSI quality inspection reports')
    parser.add_argument('--project-uuid', type=str, default=DEFAULT_PROJECT_UUID,
                        help=f'Project UUID (default: {DEFAULT_PROJECT_UUID})')
    parser.add_argument('--start-offset', type=int, default=0,
                        help='Starting offset for pagination (default: 0)')
    parser.add_argument('--limit', type=int, default=0,
                        help='Maximum documents to download (0 = all)')
    parser.add_argument('--headless', action='store_true',
                        help='Run in headless mode')
    parser.add_argument('--force', action='store_true',
                        help='Force re-download of existing documents')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be downloaded without downloading')
    parser.add_argument('--output', type=str, default=None,
                        help='Output directory path')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Show DEBUG level output')
    args = parser.parse_args()

    # Setup output directory
    try:
        from src.config.settings import Settings
        output_dir = Path(args.output) if args.output else Settings.PSI_RAW_DIR / 'reports'
    except (ImportError, AttributeError):
        # Fallback to environment variable
        windows_data_dir = os.getenv('WINDOWS_DATA_DIR')
        if windows_data_dir:
            # Convert Windows path to WSL path if needed
            if windows_data_dir.startswith('C:'):
                windows_data_dir = '/mnt/c' + windows_data_dir[2:].replace('\\', '/')
            output_dir = Path(windows_data_dir) / 'raw' / 'psi' / 'reports'
        else:
            output_dir = Path(args.output) if args.output else project_root / 'data' / 'raw' / 'psi' / 'reports'

    output_dir.mkdir(parents=True, exist_ok=True)

    # Setup logging
    log_file = output_dir.parent / 'scraper.log'
    setup_logging(log_file, verbose=args.verbose)

    # Load manifest
    manifest_file = output_dir.parent / 'manifest.json'
    manifest = load_manifest(manifest_file)

    logger.info("=" * 60)
    logger.info("PSI Quality Reports Scraper")
    logger.info("=" * 60)
    logger.info(f"Project UUID: {args.project_uuid}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Start offset: {args.start_offset}")
    logger.info(f"Limit: {args.limit if args.limit > 0 else 'None'}")
    logger.info(f"Headless mode: {args.headless}")
    logger.info(f"Force re-download: {args.force}")
    logger.info(f"Dry run: {args.dry_run}")

    try:
        with PSIScraper(headless=args.headless, download_dir=output_dir) as scraper:
            # Login
            if not scraper.login():
                logger.error("Login failed!")
                return 1

            # Scrape documents
            manifest = scraper.scrape_all(
                project_uuid=args.project_uuid,
                output_dir=output_dir,
                manifest=manifest,
                manifest_file=manifest_file,
                start_offset=args.start_offset,
                limit=args.limit,
                force=args.force,
                dry_run=args.dry_run
            )

            logger.info(f"Manifest saved: {manifest_file}")
            return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == '__main__':
    sys.exit(main())
