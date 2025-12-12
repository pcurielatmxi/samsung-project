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
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Callable
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

load_dotenv(override=True)


class LibraryScraper:
    """Scraper for ProjectSight Library file structure."""

    BASE_CONNECT_URL = "https://web.connect.trimble.com"

    def __init__(self, session, skip_scanned_days: int = 0, existing_data: dict = None):
        """Initialize with a ProjectSightSession instance.

        Args:
            session: ProjectSightSession instance
            skip_scanned_days: Skip folders scanned within this many days (0 = rescan all)
            existing_data: Existing extraction data for idempotent updates
        """
        self.session = session
        self.page = session.page
        self.project = session.project
        self.all_items = []
        self.visited_folders = set()
        self.skip_scanned_days = skip_scanned_days
        self.existing_data = existing_data or {}
        self.scanned_folders = self._build_scanned_folders_index()

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

    def navigate_to_folder(self, folder_id: str, depth: int = 0) -> bool:
        """Navigate to a specific folder by ID using multiple strategies."""
        try:
            frame = self.get_library_frame()

            # Strategy 1: Try breadcrumb links first (always visible)
            breadcrumb_link = frame.locator(f'nav a[href*="/data/folder/{folder_id}"], ol a[href*="/data/folder/{folder_id}"], .breadcrumb a[href*="/data/folder/{folder_id}"]')
            if breadcrumb_link.count() > 0:
                breadcrumb_link.first.click(force=True)
                time.sleep(2)
                return True

            # Strategy 2: Try any link with the folder ID (already visible)
            folder_link = frame.locator(f'a[href*="/data/folder/{folder_id}"]')
            if folder_link.count() > 0:
                folder_link.first.click(force=True)
                time.sleep(2)
                return True

            # Strategy 3: Scroll through the grid to find the link
            if self.scroll_grid_to_find_link(frame, folder_id):
                return True

            # Strategy 4: Try JavaScript navigation
            if self.navigate_to_folder_via_js(folder_id):
                return True

            return False
        except Exception as e:
            print(f"    {'  ' * depth}Error navigating to folder {folder_id}: {e}")
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
                        return True
                except:
                    continue

            # Fallback: reload library page
            self.session.navigate_to(self.project.library_url)
            time.sleep(5)
            return True
        except Exception as e:
            print(f"    Error navigating to root: {e}")
            return False

    def extract_current_folder_items(self) -> List[Dict]:
        """Extract all items (folders and files) from the current folder view."""
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
                    return []  # Empty folder, return no items
                # Wait a bit more and try again
                time.sleep(2)
                if grid.count() == 0:
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
                        folder_id = href.split('/data/folder/')[-1].split('?')[0]
                        item = {
                            'name': name,
                            'type': item_type,
                            'folderId': folder_id,
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
                    continue

            return items
        except Exception as e:
            print(f"    Error extracting items: {e}")
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
            return []

        if max_depth > 0 and depth > max_depth:
            return []

        self.visited_folders.add(folder_id)
        items = []

        # Check if folder should be skipped (already scanned recently)
        folder_info = self.scanned_folders.get(folder_id, {})
        is_retry = folder_info.get('navigationFailed', False)

        if self.should_skip_folder(folder_id):
            print(f"  {'  ' * depth}Skipping (recently scanned): {current_path}")
            # Return existing items for this folder
            existing_items = self.get_existing_folder_items(folder_id)
            # Still need to process subfolders recursively (they might have failed)
            for item in existing_items:
                if item.get('type') == 'folder':
                    subfolder_items = self.extract_folder_recursive(
                        item['folderId'],
                        item['path'],
                        depth + 1,
                        max_depth,
                        parent_folder_id=folder_id
                    )
                    items.extend(subfolder_items)
            return existing_items + items

        # Print appropriate message
        if is_retry:
            print(f"  {'  ' * depth}Retrying (previously failed): {current_path}")
        else:
            print(f"  {'  ' * depth}Extracting: {current_path}")

        # Navigate to folder if not root
        if folder_id != self.project.root_folder_id:
            if not self.navigate_to_folder(folder_id, depth):
                print(f"    {'  ' * depth}Could not navigate to folder {folder_id}")
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
        folder_items = self.extract_current_folder_items()
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

        # Now recursively process subfolders
        for subfolder in subfolders:
            subfolder_items = self.extract_folder_recursive(
                subfolder['folderId'],
                subfolder['path'],
                depth + 1,
                max_depth,
                parent_folder_id=folder_id
            )
            items.extend(subfolder_items)

            # After processing subfolder, navigate back to current folder
            if subfolder_items:
                if not self.navigate_to_folder(folder_id, depth):
                    # If we can't navigate back, try going to root and starting fresh
                    print(f"    {'  ' * depth}Resetting to root after failed navigation")
                    self.navigate_to_root()
                    time.sleep(2)
                    # Re-navigate to current folder from root
                    if folder_id != self.project.root_folder_id:
                        self.navigate_to_folder(folder_id, depth)

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
        print("Extracting library structure...")

        # Start at root
        items = self.extract_current_folder_items()
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
            print(f"  Found {len(folders)} folders at root level")

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
                    print(f"  Progress saved ({i + 1}/{len(folders)} top-level folders)")

                # Navigate back to root after each top-level folder
                self.navigate_to_root()
                time.sleep(1)

            return all_items

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
    args = parser.parse_args()

    headless = args.headless or os.getenv('PROJECTSIGHT_HEADLESS', 'false').lower() == 'true'

    print("=" * 60)
    print("ProjectSight Library Scraper")
    print("=" * 60)
    print(f"Project: {args.project}")
    print(f"Headless mode: {headless}")
    print(f"Recursive: {args.recursive}")
    print(f"Max depth: {args.max_depth if args.max_depth > 0 else 'unlimited'}")
    print(f"Skip scanned days: {args.skip_scanned_days if args.skip_scanned_days > 0 else 'disabled (rescan all)'}")

    # Create output path
    try:
        from src.config.settings import Settings
        output_dir = Settings.PROJECTSIGHT_RAW_DIR / 'extracted'
    except ImportError:
        output_dir = project_root / 'data' / 'projectsight' / 'extracted'
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = Path(args.output) if args.output else output_dir / f'library_structure_{args.project}.json'

    # Load existing data for idempotent updates
    existing_data = load_existing_data(output_file)
    if existing_data:
        print(f"Loaded existing data: {len(existing_data.get('items', []))} items")

    # Import session manager
    from scripts.projectsight.utils.projectsight_session import ProjectSightSession

    try:
        with ProjectSightSession(headless=headless, project=args.project) as session:
            if not session.login():
                print("Login failed!")
                return 1

            # Debug: Show current URL
            print(f"Current URL after login: {session.page.url}")

            # Wait for library iframe element to appear in DOM
            print("Waiting for library iframe to load...")
            try:
                # Wait for the outer iframe element
                session.page.wait_for_selector('iframe[name="fraMenuContent"]', timeout=30000)
                print("  Outer iframe element found")

                # Wait for outer iframe content to load
                outer_frame_loc = session.page.frame_locator('iframe[name="fraMenuContent"]')
                outer_frame_loc.locator('body').wait_for(timeout=30000)
                print("  Outer iframe body loaded")

                # Wait for inner iframe (Trimble Connect embed)
                inner_iframe = outer_frame_loc.locator('iframe')
                inner_iframe.wait_for(timeout=60000)
                inner_count = inner_iframe.count()
                print(f"  Inner iframe(s) found: {inner_count}")

                # Wait for grid in the nested iframe
                inner_frame = outer_frame_loc.frame_locator('iframe')
                inner_frame.locator('[role="grid"]').wait_for(timeout=60000)
                print("  Grid loaded inside nested iframe")

            except Exception as e:
                print(f"  Warning: Failed to wait for iframe/grid: {e}")

            scraper = LibraryScraper(
                session,
                skip_scanned_days=args.skip_scanned_days,
                existing_data=existing_data
            )

            # Create incremental save callback
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
                    'items': merged_items
                }

                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, indent=2, ensure_ascii=False)

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

            print(f"\nExtraction complete!")
            print(f"  Folders: {len(folders)}")
            print(f"  Files: {len(files)}")
            print(f"  Total: {len(merged_items)}")

            # Save final results
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
                'items': merged_items
            }

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)

            print(f"Output saved to: {output_file}")
            return 0

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
