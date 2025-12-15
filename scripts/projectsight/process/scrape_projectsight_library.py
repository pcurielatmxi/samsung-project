#!/usr/bin/env python3
"""
ProjectSight Library Scraper

Extracts file and folder structure from ProjectSight Library.
Recursively traverses all folders to build a complete file list.

Usage:
    # Extract library structure (default: just root level)
    python scripts/projectsight/process/scrape_projectsight_library.py

    # Recursive extraction (all folders)
    python scripts/projectsight/process/scrape_projectsight_library.py --recursive

    # Limit depth
    python scripts/projectsight/process/scrape_projectsight_library.py --recursive --max-depth 2

    # Headless mode
    python scripts/projectsight/process/scrape_projectsight_library.py --headless --recursive

    # Skip folders scanned in the last 7 days (idempotent mode)
    python scripts/projectsight/process/scrape_projectsight_library.py --recursive --skip-scanned-days 7
"""

import os
import sys
import json
import time
import logging
import argparse
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Callable
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

load_dotenv(override=True)

# Global logger - configured in main()
logger = logging.getLogger('library_scraper')


def setup_logging(log_file: Path, verbose: bool = False) -> logging.Logger:
    """Configure logging to file and console.

    Args:
        log_file: Path to log file (will be overwritten each run)
        verbose: If True, show DEBUG level on console; otherwise INFO

    Returns:
        Configured logger instance
    """
    # Clear any existing handlers
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)

    # File handler - always DEBUG level, overwrites each run
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler - INFO or DEBUG based on verbose flag
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger


class ErrorTracker:
    """Tracks errors and warnings during scraping for summary reporting."""

    def __init__(self):
        self.errors: List[Dict] = []
        self.warnings: List[Dict] = []
        self.navigation_failures: List[Dict] = []
        self.extraction_failures: List[Dict] = []
        self.folders_processed = 0
        self.folders_skipped = 0
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

    def start(self):
        self.start_time = datetime.now()

    def stop(self):
        self.end_time = datetime.now()

    def add_error(self, message: str, folder_id: str = None, path: str = None, exception: Exception = None):
        self.errors.append({
            'timestamp': datetime.now().isoformat(),
            'message': message,
            'folder_id': folder_id,
            'path': path,
            'exception': str(exception) if exception else None,
        })
        logger.error(f"{message} (folder={folder_id}, path={path})")

    def add_warning(self, message: str, folder_id: str = None, path: str = None, exception: Exception = None):
        self.warnings.append({
            'timestamp': datetime.now().isoformat(),
            'message': message,
            'folder_id': folder_id,
            'path': path,
            'exception': str(exception) if exception else None,
        })
        log_msg = f"{message} (folder={folder_id}, path={path})"
        if exception:
            log_msg += f" - {exception}"
        logger.warning(log_msg)

    def add_navigation_failure(self, folder_id: str, path: str, strategies_tried: List[str] = None):
        self.navigation_failures.append({
            'timestamp': datetime.now().isoformat(),
            'folder_id': folder_id,
            'path': path,
            'strategies_tried': strategies_tried or [],
        })
        logger.warning(f"Navigation failed for {path} (folder={folder_id})")

    def add_extraction_failure(self, folder_id: str, path: str, exception: Exception = None):
        self.extraction_failures.append({
            'timestamp': datetime.now().isoformat(),
            'folder_id': folder_id,
            'path': path,
            'exception': str(exception) if exception else None,
        })
        logger.error(f"Extraction failed for {path}: {exception}")

    def log_summary(self):
        """Log a summary of all errors and statistics."""
        duration = (self.end_time - self.start_time).total_seconds() if self.end_time and self.start_time else 0

        logger.info("=" * 60)
        logger.info("SCRAPING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
        logger.info(f"Folders processed: {self.folders_processed}")
        logger.info(f"Folders skipped (recently scanned): {self.folders_skipped}")
        unique_nav_failures = len(set(f['folder_id'] for f in self.navigation_failures))
        logger.info(f"Navigation failures: {len(self.navigation_failures)} ({unique_nav_failures} unique folders)")
        logger.info(f"Extraction failures: {len(self.extraction_failures)}")
        logger.info(f"Warnings: {len(self.warnings)}")
        logger.info(f"Errors: {len(self.errors)}")

        if self.navigation_failures:
            logger.info("-" * 40)
            logger.info("NAVIGATION FAILURES:")
            # Deduplicate by folder_id, keeping track of occurrence count
            seen = {}
            for fail in self.navigation_failures:
                fid = fail['folder_id']
                if fid not in seen:
                    seen[fid] = {'path': fail['path'], 'folder_id': fid, 'count': 1}
                else:
                    seen[fid]['count'] += 1
            unique_failures = list(seen.values())
            for fail in unique_failures[:20]:  # Limit to first 20 unique
                count_str = f" (x{fail['count']})" if fail['count'] > 1 else ""
                logger.info(f"  - {fail['path']} (id={fail['folder_id']}){count_str}")
            if len(unique_failures) > 20:
                logger.info(f"  ... and {len(unique_failures) - 20} more unique folders")

        if self.extraction_failures:
            logger.info("-" * 40)
            logger.info("EXTRACTION FAILURES:")
            for fail in self.extraction_failures[:20]:
                logger.info(f"  - {fail['path']}: {fail['exception']}")
            if len(self.extraction_failures) > 20:
                logger.info(f"  ... and {len(self.extraction_failures) - 20} more")

        if self.errors:
            logger.info("-" * 40)
            logger.info("ERRORS:")
            for err in self.errors[:20]:
                logger.info(f"  - {err['message']}")
            if len(self.errors) > 20:
                logger.info(f"  ... and {len(self.errors) - 20} more")

        logger.info("=" * 60)

    def to_dict(self) -> dict:
        """Export tracker state to dict for JSON serialization."""
        return {
            'duration_seconds': (self.end_time - self.start_time).total_seconds() if self.end_time and self.start_time else 0,
            'folders_processed': self.folders_processed,
            'folders_skipped': self.folders_skipped,
            'navigation_failure_count': len(self.navigation_failures),
            'extraction_failure_count': len(self.extraction_failures),
            'warning_count': len(self.warnings),
            'error_count': len(self.errors),
            'navigation_failures': self.navigation_failures,
            'extraction_failures': self.extraction_failures,
        }


def atomic_json_save(data: dict, output_file: Path):
    """Save JSON data atomically using a temp file and rename.

    This prevents corrupted files if the script crashes mid-write.
    """
    # Write to temp file in same directory (for atomic rename)
    temp_fd, temp_path = tempfile.mkstemp(
        suffix='.json',
        prefix='.tmp_',
        dir=output_file.parent
    )
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        # Atomic rename
        shutil.move(temp_path, output_file)
        logger.debug(f"Saved data to {output_file}")
    except Exception as e:
        # Clean up temp file on error
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise


class LibraryScraper:
    """Scraper for ProjectSight Library file structure."""

    BASE_CONNECT_URL = "https://web.connect.trimble.com"

    def __init__(self, session, skip_scanned_days: int = 0, existing_data: dict = None,
                 error_tracker: ErrorTracker = None):
        """Initialize with a ProjectSightSession instance.

        Args:
            session: ProjectSightSession instance
            skip_scanned_days: Skip folders scanned within this many days (0 = rescan all)
            existing_data: Existing extraction data for idempotent updates
            error_tracker: ErrorTracker instance for collecting errors/warnings
        """
        self.session = session
        self.page = session.page
        self.project = session.project
        self.all_items = []
        self.visited_folders = set()
        self.skip_scanned_days = skip_scanned_days
        self.existing_data = existing_data or {}
        self.scanned_folders = self._build_scanned_folders_index()
        self.error_tracker = error_tracker or ErrorTracker()

    def _build_scanned_folders_index(self) -> Dict[str, dict]:
        """Build an index of folder IDs to their scan info (lastScanned, navigationFailed)."""
        index = {}
        if not self.existing_data:
            return index

        for item in self.existing_data.get('items', []):
            if item.get('type') == 'folder' and item.get('lastScanned'):
                try:
                    last_scanned = datetime.fromisoformat(item['lastScanned'])
                    index[item['folderId']] = {
                        'lastScanned': last_scanned,
                        'navigationFailed': item.get('navigationFailed', False),
                    }
                except (ValueError, KeyError):
                    pass
        return index

    def should_skip_folder(self, folder_id: str) -> bool:
        """Check if a folder should be skipped based on lastScanned timestamp.

        Always retry folders that previously failed navigation.
        Only skip folders that were successfully scanned recently.
        """
        if self.skip_scanned_days <= 0:
            return False

        folder_info = self.scanned_folders.get(folder_id)
        if not folder_info:
            return False

        # Always retry folders that failed navigation
        if folder_info.get('navigationFailed', False):
            return False

        # Skip if successfully scanned within the cutoff period
        cutoff = datetime.now() - timedelta(days=self.skip_scanned_days)
        return folder_info['lastScanned'] > cutoff

    def get_library_frame(self):
        """Get the nested iframe locator for the library file explorer.

        Uses frame_locator for cross-origin iframe access.
        """
        # Use frame_locator chain - this works for cross-origin iframes
        return self.page.frame_locator('iframe[name="fraMenuContent"]').frame_locator('iframe')

    def navigate_to_folder_via_js(self, folder_id: str) -> bool:
        """Navigate to folder using JavaScript within the iframe."""
        try:
            # Get the inner iframe and navigate via JS
            outer_frame = self.page.frame(name="fraMenuContent")
            if not outer_frame:
                return False

            inner_frame = outer_frame.frame_locator('#fmm-tc-emb').first
            # Use evaluate to change location
            folder_path = f"/projects/{self.project.trimble_project_id}/data/folder/{folder_id}"

            # Try to find and click using JavaScript for better reliability
            script = f"""
                const links = document.querySelectorAll('a[href*="/data/folder/{folder_id}"]');
                if (links.length > 0) {{
                    links[0].click();
                    return true;
                }}
                return false;
            """
            # Execute in the inner frame context
            result = inner_frame.locator('body').evaluate(script)
            if result:
                time.sleep(2)
                return True
            return False
        except Exception as e:
            return False

    def scroll_grid_to_find_link(self, frame, folder_id: str, max_scrolls: int = 10) -> bool:
        """Scroll through the virtualized grid to find and click a folder link."""
        try:
            grid = frame.locator('[role="grid"]')
            if grid.count() == 0:
                return False

            # Get grid dimensions
            grid_info = grid.evaluate('''el => ({
                scrollHeight: el.scrollHeight,
                clientHeight: el.clientHeight,
                scrollTop: el.scrollTop
            })''')

            # Scroll to top first
            grid.evaluate('el => el.scrollTop = 0')
            time.sleep(0.3)

            # Check if link exists
            folder_link = frame.locator(f'a[href*="/data/folder/{folder_id}"]')
            if folder_link.count() > 0:
                folder_link.first.click(force=True)
                time.sleep(2)
                return True

            # Scroll through the grid in increments
            scroll_increment = grid_info['clientHeight'] * 0.8  # Scroll 80% of visible height
            current_scroll = 0

            for _ in range(max_scrolls):
                current_scroll += scroll_increment
                grid.evaluate(f'el => el.scrollTop = {current_scroll}')
                time.sleep(0.3)

                # Check if link is now visible
                folder_link = frame.locator(f'a[href*="/data/folder/{folder_id}"]')
                if folder_link.count() > 0:
                    folder_link.first.click(force=True)
                    time.sleep(2)
                    return True

                # Check if we've reached the bottom
                new_scroll = grid.evaluate('el => el.scrollTop')
                if new_scroll >= grid_info['scrollHeight'] - grid_info['clientHeight'] - 10:
                    break

            return False
        except Exception:
            return False

    def navigate_to_folder(self, folder_id: str, depth: int = 0, path: str = None,
                            max_retries: int = 2) -> bool:
        """Navigate to a specific folder by ID using multiple strategies.

        Args:
            folder_id: The folder ID to navigate to
            depth: Current depth in folder tree (for logging indentation)
            path: Folder path (for error tracking)
            max_retries: Number of retry attempts for transient failures
        """
        strategies_tried = []

        for attempt in range(max_retries + 1):
            try:
                frame = self.get_library_frame()

                # Strategy 1: Try breadcrumb links first (always visible)
                strategies_tried.append('breadcrumb')
                breadcrumb_link = frame.locator(f'nav a[href*="/data/folder/{folder_id}"], ol a[href*="/data/folder/{folder_id}"], .breadcrumb a[href*="/data/folder/{folder_id}"]')
                if breadcrumb_link.count() > 0 and breadcrumb_link.first.is_visible():
                    breadcrumb_link.first.click(force=True)
                    time.sleep(2)
                    logger.debug(f"{'  ' * depth}Navigated via breadcrumb to {folder_id}")
                    return True

                # Strategy 2: Try breadcrumb dropdown (parent links hidden in dropdown)
                strategies_tried.append('breadcrumb_dropdown')
                try:
                    # Click the breadcrumb dropdown button to reveal hidden parent links
                    # The button is in a list/listitem structure (ARIA roles, not ol/ul)
                    breadcrumb_btn = frame.get_by_role('list').get_by_role('button').first
                    if breadcrumb_btn.count() > 0 and breadcrumb_btn.is_visible():
                        breadcrumb_btn.click()
                        time.sleep(0.5)  # Wait for dropdown to appear
                        # Now try to find and click the link in the dropdown
                        dropdown_link = frame.locator(f'a[href*="/data/folder/{folder_id}"]')
                        if dropdown_link.count() > 0 and dropdown_link.first.is_visible():
                            dropdown_link.first.click()
                            time.sleep(2)
                            logger.debug(f"{'  ' * depth}Navigated via breadcrumb dropdown to {folder_id}")
                            return True
                except Exception:
                    pass  # Continue to next strategy

                # Strategy 3: Try any link with the folder ID (already visible in grid)
                strategies_tried.append('direct_link')
                folder_link = frame.locator(f'a[href*="/data/folder/{folder_id}"]')
                if folder_link.count() > 0:
                    folder_link.first.click(force=True)
                    time.sleep(2)
                    logger.debug(f"{'  ' * depth}Navigated via direct link to {folder_id}")
                    return True

                # Strategy 4: Scroll through the grid to find the link
                strategies_tried.append('scroll_grid')
                if self.scroll_grid_to_find_link(frame, folder_id):
                    logger.debug(f"{'  ' * depth}Navigated via scroll to {folder_id}")
                    return True

                # Strategy 5: Try JavaScript navigation
                strategies_tried.append('javascript')
                if self.navigate_to_folder_via_js(folder_id):
                    logger.debug(f"{'  ' * depth}Navigated via JS to {folder_id}")
                    return True

                # All strategies failed
                if attempt < max_retries:
                    logger.debug(f"{'  ' * depth}Retry {attempt + 1}/{max_retries} for folder {folder_id}")
                    time.sleep(2)  # Wait before retry
                    continue

                # Final failure - track it
                self.error_tracker.add_navigation_failure(folder_id, path or folder_id, strategies_tried)
                return False

            except Exception as e:
                if attempt < max_retries:
                    logger.debug(f"{'  ' * depth}Retry {attempt + 1}/{max_retries} after error: {e}")
                    time.sleep(2)
                    continue
                logger.warning(f"{'  ' * depth}Navigation error for {folder_id}: {e}")
                self.error_tracker.add_navigation_failure(folder_id, path or folder_id, strategies_tried)
                return False

        return False

    def navigate_to_root(self) -> bool:
        """Navigate back to the library root folder."""
        try:
            frame = self.get_library_frame()

            # Try breadcrumb "Explorer" or root folder link
            root_selectors = [
                f'a[href*="/data/folder/{self.project.root_folder_id}"]',
                'a:has-text("Explorer")',
                '.breadcrumb a:first-child',
                'nav a:first-child',
            ]

            for selector in root_selectors:
                try:
                    link = frame.locator(selector)
                    if link.count() > 0:
                        link.first.click(force=True)
                        time.sleep(2)
                        logger.debug("Navigated to root via selector")
                        return True
                except:
                    continue

            # Fallback: reload library page
            logger.debug("Navigating to root via page reload")
            self.session.navigate_to(self.project.library_url)
            time.sleep(5)
            return True
        except Exception as e:
            logger.warning(f"Error navigating to root: {e}")
            return False

    def extract_current_folder_items(self, folder_id: str = None, path: str = None) -> List[Dict]:
        """Extract all items (folders and files) from the current folder view.

        Args:
            folder_id: Current folder ID (for error tracking)
            path: Current folder path (for error tracking)
        """
        items = []
        try:
            frame = self.get_library_frame()

            # Wait for grid or empty state indicator
            grid = frame.locator('[role="grid"]')
            empty_indicator = frame.locator(':text("No items"), :text("This folder is empty"), .empty-state')

            try:
                # Try to wait for grid first (shorter timeout)
                grid.wait_for(state='attached', timeout=10000)
                time.sleep(1)
            except Exception:
                # Check if folder is empty
                if empty_indicator.count() > 0:
                    logger.debug(f"Empty folder: {path or folder_id}")
                    return []  # Empty folder, return no items
                # Wait a bit more and try again
                time.sleep(2)
                if grid.count() == 0:
                    logger.debug(f"No grid found, assuming empty: {path or folder_id}")
                    return []  # No grid found, assume empty

            # Get all links in the grid (use evaluate for reliability)
            links = frame.locator('[role="grid"] a[href]').all()

            for link in links:
                try:
                    href = link.get_attribute('href')
                    name = link.text_content().strip()

                    if not href or not name:
                        continue

                    # Determine type based on URL pattern
                    if '/data/folder/' in href:
                        item_type = 'folder'
                        # Extract folder ID from href
                        item_folder_id = href.split('/data/folder/')[-1].split('?')[0]
                        item = {
                            'name': name,
                            'type': item_type,
                            'folderId': item_folder_id,
                            'href': href,
                        }
                    elif '/detailviewer' in href:
                        item_type = 'file'
                        # Extract file ID from href
                        file_id = href.split('fileId=')[-1].split('&')[0] if 'fileId=' in href else None
                        item = {
                            'name': name,
                            'type': item_type,
                            'fileId': file_id,
                            'href': href,
                        }
                    else:
                        continue

                    items.append(item)
                except Exception as e:
                    logger.debug(f"Error extracting link: {e}")
                    continue

            logger.debug(f"Extracted {len(items)} items from {path or folder_id}")
            return items
        except Exception as e:
            self.error_tracker.add_extraction_failure(folder_id, path, e)
            return []

    def get_breadcrumb_path(self) -> str:
        """Get the current folder path from breadcrumbs."""
        try:
            frame = self.get_library_frame()
            breadcrumb_items = frame.locator('ol li, ul li').all()

            path_parts = []
            for item in breadcrumb_items:
                text = item.text_content().strip()
                if text and text not in ['', '>']:
                    path_parts.append(text)

            return '/'.join(path_parts) if path_parts else '/'
        except:
            return '/'

    def get_existing_folder_items(self, folder_id: str) -> List[Dict]:
        """Get existing items for a folder from previous extraction."""
        if not self.existing_data:
            return []

        # Find items that belong to this folder (direct children)
        items = []
        for item in self.existing_data.get('items', []):
            if item.get('parentFolderId') == folder_id:
                items.append(item)
        return items

    def extract_folder_recursive(self, folder_id: str, current_path: str, depth: int, max_depth: int,
                                   parent_folder_id: str = None) -> List[Dict]:
        """Recursively extract items from a folder and its subfolders.

        Uses breadth-first-like approach: collect all items first, then process subfolders.
        Navigation back uses folder link clicks instead of browser back.
        """
        if folder_id in self.visited_folders:
            logger.debug(f"{'  ' * depth}Already visited: {current_path}")
            return []

        if max_depth > 0 and depth > max_depth:
            logger.debug(f"{'  ' * depth}Max depth reached: {current_path}")
            return []

        self.visited_folders.add(folder_id)
        items = []

        # Check if folder should be skipped (already scanned recently)
        folder_info = self.scanned_folders.get(folder_id, {})
        is_retry = folder_info.get('navigationFailed', False)

        if self.should_skip_folder(folder_id):
            logger.info(f"{'  ' * depth}Skipping (recently scanned): {current_path}")
            self.error_tracker.folders_skipped += 1
            # Return existing items for this folder
            existing_items = self.get_existing_folder_items(folder_id)
            # Still need to process subfolders recursively (they might have failed)
            # Check if any subfolders need retry (have navigationFailed flag)
            needs_navigation = any(
                item.get('type') == 'folder' and item.get('navigationFailed', False)
                for item in existing_items
            )
            # Navigate into parent folder if any child needs retry
            navigated_into_folder = False
            if needs_navigation and folder_id != self.project.root_folder_id:
                logger.debug(f"{'  ' * depth}Navigating into skipped folder for retry: {current_path}")
                if self.navigate_to_folder(folder_id, depth, path=current_path):
                    navigated_into_folder = True
                else:
                    logger.warning(f"{'  ' * depth}Could not navigate into skipped folder: {current_path}")

            for item in existing_items:
                if item.get('type') == 'folder':
                    next_depth = depth + 1

                    # Check if next depth would exceed max_depth - if so, just skip
                    if max_depth > 0 and next_depth > max_depth:
                        logger.debug(f"{'  ' * next_depth}Max depth reached: {item['path']}")
                        continue

                    # Check if this child needs retry - if so, ensure we're in the parent folder first
                    child_needs_retry = item.get('navigationFailed', False)

                    # Check if child would cause navigation:
                    # 1. Child itself needs retry (has navigationFailed)
                    # 2. OR child is skipped but has grandchildren that need retry
                    child_would_navigate = child_needs_retry
                    if not child_would_navigate and self.should_skip_folder(item['folderId']):
                        # Child is skipped - check if any grandchild needs retry
                        grandchildren = self.get_existing_folder_items(item['folderId'])
                        child_would_navigate = any(
                            gc.get('type') == 'folder' and gc.get('navigationFailed', False)
                            for gc in grandchildren
                        )

                    if child_needs_retry and navigated_into_folder:
                        # Verify we're still in the parent folder, re-navigate if needed
                        # (previous sibling processing may have reset to root)
                        logger.debug(f"{'  ' * depth}Ensuring navigation to parent before retry: {current_path}")
                        if not self.navigate_to_folder(folder_id, depth, path=current_path):
                            logger.warning(f"{'  ' * depth}Could not re-navigate to parent folder: {current_path}")

                    subfolder_items = self.extract_folder_recursive(
                        item['folderId'],
                        item['path'],
                        next_depth,
                        max_depth,
                        parent_folder_id=folder_id
                    )
                    items.extend(subfolder_items)

                    # Navigate back to current folder after processing child
                    # Only if the child actually caused navigation
                    if navigated_into_folder and child_would_navigate:
                        if not self.navigate_to_folder(folder_id, depth, path=current_path):
                            logger.debug(f"{'  ' * depth}Resetting to root after failed back-navigation from skipped folder child")
                            self.navigate_to_root()
                            time.sleep(2)
                            if folder_id != self.project.root_folder_id:
                                self.navigate_to_folder(folder_id, depth, path=current_path)

            # Navigate back if we navigated into the folder
            if navigated_into_folder and parent_folder_id:
                self.navigate_to_folder(parent_folder_id, depth - 1)

            return existing_items + items

        # Log appropriate message
        if is_retry:
            logger.info(f"{'  ' * depth}Retrying (previously failed): {current_path}")
        else:
            logger.info(f"{'  ' * depth}Extracting: {current_path}")

        self.error_tracker.folders_processed += 1

        # Navigate to folder if not root
        if folder_id != self.project.root_folder_id:
            if not self.navigate_to_folder(folder_id, depth, path=current_path):
                logger.warning(f"{'  ' * depth}Could not navigate to folder: {current_path}")
                # Record the folder but skip its contents
                return [{
                    'name': current_path.split('/')[-1],
                    'type': 'folder',
                    'folderId': folder_id,
                    'path': current_path,
                    'depth': depth,
                    'parentFolderId': parent_folder_id,
                    'lastScanned': datetime.now().isoformat(),
                    'navigationFailed': True,
                }]
            time.sleep(1)

        # Extract items in current folder
        folder_items = self.extract_current_folder_items(folder_id=folder_id, path=current_path)
        scan_time = datetime.now().isoformat()

        # First, collect all items with paths
        subfolders = []
        for item in folder_items:
            item['path'] = f"{current_path}/{item['name']}"
            item['depth'] = depth
            item['parentFolderId'] = folder_id

            if item['type'] == 'folder':
                item['lastScanned'] = scan_time
                subfolders.append(item)

            items.append(item)

        logger.debug(f"{'  ' * depth}Found {len(subfolders)} subfolders, {len(folder_items) - len(subfolders)} files")

        # Update the current folder's own record to clear navigationFailed flag
        # This fixes the bug where successfully re-scanned folders kept their old failed status
        if folder_id != self.project.root_folder_id:
            for existing_item in self.existing_data.get('items', []):
                if existing_item.get('folderId') == folder_id and existing_item.get('type') == 'folder':
                    updated_folder = existing_item.copy()
                    updated_folder['lastScanned'] = scan_time
                    updated_folder.pop('navigationFailed', None)
                    items.append(updated_folder)
                    break

        # Now recursively process subfolders
        for subfolder in subfolders:
            next_depth = depth + 1

            # Check if next depth would exceed max_depth - if so, just log and skip
            # (no navigation needed since we won't actually enter the folder)
            if max_depth > 0 and next_depth > max_depth:
                logger.debug(f"{'  ' * next_depth}Max depth reached: {subfolder['path']}")
                continue

            # Determine if this subfolder will actually cause navigation
            # If it's skipped and has no failing grandchildren, we won't leave current folder
            subfolder_will_navigate = True
            if self.should_skip_folder(subfolder['folderId']):
                # Check if any grandchildren need retry
                grandchildren = self.get_existing_folder_items(subfolder['folderId'])
                has_failing_grandchildren = any(
                    gc.get('type') == 'folder' and gc.get('navigationFailed', False)
                    for gc in grandchildren
                )
                if not has_failing_grandchildren:
                    subfolder_will_navigate = False

            subfolder_items = self.extract_folder_recursive(
                subfolder['folderId'],
                subfolder['path'],
                next_depth,
                max_depth,
                parent_folder_id=folder_id
            )
            items.extend(subfolder_items)

            # After processing subfolder, navigate back to current folder
            # Only navigate back if the subfolder actually caused navigation
            if subfolder_will_navigate:
                if not self.navigate_to_folder(folder_id, depth, path=current_path):
                    # If we can't navigate back, try going to root and starting fresh
                    logger.debug(f"{'  ' * depth}Resetting to root after failed back-navigation")
                    self.navigate_to_root()
                    time.sleep(2)
                    # Re-navigate to current folder from root
                    if folder_id != self.project.root_folder_id:
                        self.navigate_to_folder(folder_id, depth, path=current_path)

        return items

    def extract_all(self, recursive: bool = False, max_depth: int = 0,
                     on_folder_complete: Callable[[List[Dict]], None] = None) -> List[Dict]:
        """Extract all items from the library.

        Args:
            recursive: If True, recursively extract all subfolders
            max_depth: Maximum depth to traverse (0 = unlimited)
            on_folder_complete: Callback called after each top-level folder with all items so far

        Returns:
            List of all items with path information
        """
        logger.info("Extracting library structure...")
        self.error_tracker.start()

        # Start at root
        items = self.extract_current_folder_items(
            folder_id=self.project.root_folder_id,
            path="/"
        )
        scan_time = datetime.now().isoformat()

        # Add root path and metadata to items
        for item in items:
            item['path'] = f"/{item['name']}"
            item['depth'] = 0
            item['parentFolderId'] = self.project.root_folder_id
            if item['type'] == 'folder':
                item['lastScanned'] = scan_time

        if recursive:
            # Get list of folders to process
            folders = [item for item in items if item['type'] == 'folder']
            logger.info(f"Found {len(folders)} folders at root level")

            all_items = list(items)

            for i, folder in enumerate(folders):
                subfolder_items = self.extract_folder_recursive(
                    folder['folderId'],
                    folder['path'],
                    1,
                    max_depth
                )
                all_items.extend(subfolder_items)

                # Call incremental save callback after each top-level folder
                if on_folder_complete:
                    on_folder_complete(all_items)
                    logger.info(f"Progress saved ({i + 1}/{len(folders)} top-level folders)")

                # Navigate back to root after each top-level folder
                self.navigate_to_root()
                time.sleep(1)

            self.error_tracker.stop()
            return all_items

        self.error_tracker.stop()
        return items


def load_existing_data(output_file: Path) -> dict:
    """Load existing extraction data if available."""
    if output_file.exists():
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def merge_items(existing_items: List[Dict], new_items: List[Dict]) -> List[Dict]:
    """Merge new items with existing items, updating by folderId/fileId."""
    # Create index of existing items by their unique ID
    item_index = {}
    for item in existing_items:
        if item['type'] == 'folder':
            key = f"folder:{item['folderId']}"
        else:
            key = f"file:{item.get('fileId', item['path'])}"
        item_index[key] = item

    # Update with new items
    for item in new_items:
        if item['type'] == 'folder':
            key = f"folder:{item['folderId']}"
        else:
            key = f"file:{item.get('fileId', item['path'])}"
        item_index[key] = item

    return list(item_index.values())


def main():
    """Main entry point."""
    from scripts.projectsight.utils.projectsight_session import PROJECTS, DEFAULT_PROJECT

    parser = argparse.ArgumentParser(description='Scrape ProjectSight Library structure')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    parser.add_argument('--recursive', action='store_true', help='Recursively extract all subfolders')
    parser.add_argument('--max-depth', type=int, default=0, help='Maximum depth to traverse (0 = unlimited)')
    parser.add_argument('--output', type=str, default=None, help='Output file path')
    parser.add_argument('--project', type=str, default=DEFAULT_PROJECT,
                        choices=list(PROJECTS.keys()),
                        help=f'Project to extract (default: {DEFAULT_PROJECT})')
    parser.add_argument('--skip-scanned-days', type=int, default=0,
                        help='Skip folders scanned within this many days (0 = rescan all)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Show DEBUG level output on console')
    args = parser.parse_args()

    headless = args.headless or os.getenv('PROJECTSIGHT_HEADLESS', 'false').lower() == 'true'

    # Create output directory
    try:
        from src.config.settings import Settings
        output_dir = Settings.PROJECTSIGHT_RAW_DIR / 'extracted'
    except ImportError:
        output_dir = project_root / 'data' / 'projectsight' / 'extracted'
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = Path(args.output) if args.output else output_dir / f'library_structure_{args.project}.json'
    log_file = output_dir / f'library_scraper_{args.project}.log'

    # Setup logging
    setup_logging(log_file, verbose=args.verbose)

    # Create error tracker
    error_tracker = ErrorTracker()

    logger.info("=" * 60)
    logger.info("ProjectSight Library Scraper")
    logger.info("=" * 60)
    logger.info(f"Project: {args.project}")
    logger.info(f"Headless mode: {headless}")
    logger.info(f"Recursive: {args.recursive}")
    logger.info(f"Max depth: {args.max_depth if args.max_depth > 0 else 'unlimited'}")
    logger.info(f"Skip scanned days: {args.skip_scanned_days if args.skip_scanned_days > 0 else 'disabled (rescan all)'}")
    logger.info(f"Log file: {log_file}")
    logger.info(f"Output file: {output_file}")

    # Load existing data for idempotent updates
    existing_data = load_existing_data(output_file)
    if existing_data:
        logger.info(f"Loaded existing data: {len(existing_data.get('items', []))} items")

    # Import session manager
    from scripts.projectsight.utils.projectsight_session import ProjectSightSession

    try:
        with ProjectSightSession(headless=headless, project=args.project) as session:
            if not session.login():
                logger.error("Login failed!")
                return 1

            # Debug: Show current URL
            logger.debug(f"Current URL after login: {session.page.url}")

            # Wait for library iframe element to appear in DOM
            logger.info("Waiting for library iframe to load...")
            try:
                # Wait for the outer iframe element
                session.page.wait_for_selector('iframe[name="fraMenuContent"]', timeout=30000)
                logger.debug("Outer iframe element found")

                # Wait for outer iframe content to load
                outer_frame_loc = session.page.frame_locator('iframe[name="fraMenuContent"]')
                outer_frame_loc.locator('body').wait_for(timeout=30000)
                logger.debug("Outer iframe body loaded")

                # Wait for inner iframe (Trimble Connect embed)
                inner_iframe = outer_frame_loc.locator('iframe')
                inner_iframe.wait_for(timeout=60000)
                inner_count = inner_iframe.count()
                logger.debug(f"Inner iframe(s) found: {inner_count}")

                # Wait for grid in the nested iframe
                inner_frame = outer_frame_loc.frame_locator('iframe')
                inner_frame.locator('[role="grid"]').wait_for(timeout=60000)
                logger.info("Library grid loaded successfully")

            except Exception as e:
                logger.warning(f"Failed to wait for iframe/grid: {e}")
                error_tracker.add_warning("Iframe/grid wait failed", exception=e)

            scraper = LibraryScraper(
                session,
                skip_scanned_days=args.skip_scanned_days,
                existing_data=existing_data,
                error_tracker=error_tracker
            )

            # Create incremental save callback using atomic saves
            def save_progress(all_items: List[Dict]):
                """Save progress after each top-level folder."""
                # Merge with existing items
                merged_items = merge_items(existing_data.get('items', []), all_items)

                folders = [i for i in merged_items if i['type'] == 'folder']
                files = [i for i in merged_items if i['type'] == 'file']

                output_data = {
                    'extractedAt': datetime.now().isoformat(),
                    'source': 'ProjectSight Library Scraper',
                    'project': session.project.to_dict(),
                    'recursive': args.recursive,
                    'maxDepth': args.max_depth,
                    'skipScannedDays': args.skip_scanned_days,
                    'summary': {
                        'totalFolders': len(folders),
                        'totalFiles': len(files),
                        'totalItems': len(merged_items),
                    },
                    'scrapeStats': error_tracker.to_dict(),
                    'items': merged_items
                }

                atomic_json_save(output_data, output_file)

            items = scraper.extract_all(
                recursive=args.recursive,
                max_depth=args.max_depth,
                on_folder_complete=save_progress if args.recursive else None
            )

            # Final merge with existing items
            merged_items = merge_items(existing_data.get('items', []), items)

            # Summarize
            folders = [i for i in merged_items if i['type'] == 'folder']
            files = [i for i in merged_items if i['type'] == 'file']

            logger.info(f"Extraction complete!")
            logger.info(f"  Folders: {len(folders)}")
            logger.info(f"  Files: {len(files)}")
            logger.info(f"  Total: {len(merged_items)}")

            # Log error summary
            scraper.error_tracker.log_summary()

            # Save final results with atomic write
            output_data = {
                'extractedAt': datetime.now().isoformat(),
                'source': 'ProjectSight Library Scraper',
                'project': session.project.to_dict(),
                'recursive': args.recursive,
                'maxDepth': args.max_depth,
                'skipScannedDays': args.skip_scanned_days,
                'summary': {
                    'totalFolders': len(folders),
                    'totalFiles': len(files),
                    'totalItems': len(merged_items),
                },
                'scrapeStats': scraper.error_tracker.to_dict(),
                'items': merged_items
            }

            atomic_json_save(output_data, output_file)

            logger.info(f"Output saved to: {output_file}")
            logger.info(f"Log saved to: {log_file}")
            return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == '__main__':
    sys.exit(main())
