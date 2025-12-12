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
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

load_dotenv(override=True)


class LibraryScraper:
    """Scraper for ProjectSight Library file structure."""

    # Constants
    TRIMBLE_PROJECT_ID = "jFeM-GUk7QI"
    ROOT_FOLDER_ID = "GBVZIZkHPRc"
    BASE_CONNECT_URL = "https://web.connect.trimble.com"

    def __init__(self, session):
        """Initialize with a ProjectSightSession instance."""
        self.session = session
        self.page = session.page
        self.all_items = []
        self.visited_folders = set()

    def get_library_frame(self):
        """Get the nested iframe locator for the library file explorer."""
        return self.page.frame_locator('iframe[name="fraMenuContent"]').frame_locator('#fmm-tc-emb')

    def navigate_to_folder(self, folder_id: str) -> bool:
        """Navigate to a specific folder by ID."""
        try:
            # Construct the direct URL to the folder
            folder_url = f"{self.BASE_CONNECT_URL}/#/project/{self.TRIMBLE_PROJECT_ID}/data/folder/{folder_id}"

            # Navigate within the iframe by clicking the folder link
            frame = self.get_library_frame()

            # Try to find and click the folder link
            folder_link = frame.locator(f'a[href*="/data/folder/{folder_id}"]')
            if folder_link.count() > 0:
                folder_link.first.click()
                time.sleep(2)
                return True

            return False
        except Exception as e:
            print(f"    Error navigating to folder {folder_id}: {e}")
            return False

    def navigate_to_root(self) -> bool:
        """Navigate back to the library root folder."""
        try:
            frame = self.get_library_frame()

            # Click the Explorer breadcrumb link
            explorer_link = frame.locator(f'a[href*="/data/folder/{self.ROOT_FOLDER_ID}"]')
            if explorer_link.count() > 0:
                explorer_link.first.click()
                time.sleep(2)
                return True

            # Alternative: navigate to library URL
            self.session.navigate_to(self.session.LIBRARY_URL)
            time.sleep(3)
            return True
        except Exception as e:
            print(f"    Error navigating to root: {e}")
            return False

    def extract_current_folder_items(self) -> List[Dict]:
        """Extract all items (folders and files) from the current folder view."""
        items = []
        try:
            frame = self.get_library_frame()

            # Wait for grid to be attached (not visible - headless mode issue)
            frame.locator('[role="grid"]').wait_for(state='attached', timeout=30000)
            time.sleep(2)

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

    def extract_folder_recursive(self, folder_id: str, current_path: str, depth: int, max_depth: int) -> List[Dict]:
        """Recursively extract items from a folder and its subfolders."""
        if folder_id in self.visited_folders:
            return []

        if max_depth > 0 and depth > max_depth:
            return []

        self.visited_folders.add(folder_id)
        items = []

        print(f"  {'  ' * depth}Extracting: {current_path}")

        # Navigate to folder if not root
        if folder_id != self.ROOT_FOLDER_ID:
            frame = self.get_library_frame()
            folder_link = frame.locator(f'a[href*="/data/folder/{folder_id}"]')
            if folder_link.count() > 0:
                folder_link.first.click()
                time.sleep(2)
            else:
                print(f"    {'  ' * depth}Could not find folder link for {folder_id}")
                return []

        # Extract items in current folder
        folder_items = self.extract_current_folder_items()

        for item in folder_items:
            item['path'] = f"{current_path}/{item['name']}"
            item['depth'] = depth
            items.append(item)

            # Recursively extract subfolders
            if item['type'] == 'folder':
                subfolder_items = self.extract_folder_recursive(
                    item['folderId'],
                    item['path'],
                    depth + 1,
                    max_depth
                )
                items.extend(subfolder_items)

                # Navigate back to current folder after processing subfolder
                if subfolder_items:
                    self.page.go_back()
                    time.sleep(1)

        return items

    def extract_all(self, recursive: bool = False, max_depth: int = 0) -> List[Dict]:
        """Extract all items from the library.

        Args:
            recursive: If True, recursively extract all subfolders
            max_depth: Maximum depth to traverse (0 = unlimited)

        Returns:
            List of all items with path information
        """
        print("Extracting library structure...")

        # Start at root
        items = self.extract_current_folder_items()

        # Add root path to items
        for item in items:
            item['path'] = f"/{item['name']}"
            item['depth'] = 0

        if recursive:
            # Get list of folders to process
            folders = [item for item in items if item['type'] == 'folder']
            print(f"  Found {len(folders)} folders at root level")

            all_items = list(items)

            for folder in folders:
                subfolder_items = self.extract_folder_recursive(
                    folder['folderId'],
                    folder['path'],
                    1,
                    max_depth
                )
                all_items.extend(subfolder_items)

                # Navigate back to root after each top-level folder
                self.navigate_to_root()
                time.sleep(1)

            return all_items

        return items


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Scrape ProjectSight Library structure')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    parser.add_argument('--recursive', action='store_true', help='Recursively extract all subfolders')
    parser.add_argument('--max-depth', type=int, default=0, help='Maximum depth to traverse (0 = unlimited)')
    parser.add_argument('--output', type=str, default=None, help='Output file path')
    args = parser.parse_args()

    headless = args.headless or os.getenv('PROJECTSIGHT_HEADLESS', 'false').lower() == 'true'

    print("=" * 60)
    print("ProjectSight Library Scraper")
    print("=" * 60)
    print(f"Headless mode: {headless}")
    print(f"Recursive: {args.recursive}")
    print(f"Max depth: {args.max_depth if args.max_depth > 0 else 'unlimited'}")

    # Create output path
    try:
        from src.config.settings import Settings
        output_dir = Settings.PROJECTSIGHT_RAW_DIR / 'extracted'
    except ImportError:
        output_dir = project_root / 'data' / 'projectsight' / 'extracted'
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = Path(args.output) if args.output else output_dir / 'library_structure.json'

    # Import session manager
    from scripts.projectsight.utils.projectsight_session import ProjectSightSession

    try:
        with ProjectSightSession(headless=headless) as session:
            if not session.login(session.LIBRARY_URL):
                print("Login failed!")
                return 1

            # Wait for library to load (embedded Trimble Connect takes time)
            print("Waiting for library iframe to load...")
            time.sleep(10)

            scraper = LibraryScraper(session)
            items = scraper.extract_all(recursive=args.recursive, max_depth=args.max_depth)

            # Summarize
            folders = [i for i in items if i['type'] == 'folder']
            files = [i for i in items if i['type'] == 'file']

            print(f"\nExtraction complete!")
            print(f"  Folders: {len(folders)}")
            print(f"  Files: {len(files)}")
            print(f"  Total: {len(items)}")

            # Save results
            output_data = {
                'extractedAt': datetime.now().isoformat(),
                'source': 'ProjectSight Library Scraper',
                'project': 'Yates Construction Portfolio > Taylor Fab1 58202',
                'projectId': LibraryScraper.TRIMBLE_PROJECT_ID,
                'rootFolderId': LibraryScraper.ROOT_FOLDER_ID,
                'recursive': args.recursive,
                'maxDepth': args.max_depth,
                'summary': {
                    'totalFolders': len(folders),
                    'totalFiles': len(files),
                    'totalItems': len(items),
                },
                'items': items
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
