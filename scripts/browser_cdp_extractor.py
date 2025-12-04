#!/usr/bin/env python3
"""
ProjectSight CDP Extractor - Connect to existing browser session.

This script connects to your already-logged-in browser via Chrome DevTools Protocol,
allowing you to automate extraction without dealing with login/MFA issues.

SETUP:
1. Launch Chrome with remote debugging:

   # Linux
   google-chrome --remote-debugging-port=9222

   # macOS
   /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222

   # Windows
   chrome.exe --remote-debugging-port=9222

2. Manually log into ProjectSight in that browser

3. Run this script:
   python scripts/browser_cdp_extractor.py

The script will connect to your session and automate the extraction loop.
"""

import json
import time
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from playwright.sync_api import sync_playwright, Page, Browser

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CDPBrowserExtractor:
    """Extract data from ProjectSight using an existing browser session."""

    def __init__(self, cdp_url: str = "http://localhost:9222"):
        self.cdp_url = cdp_url
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.playwright = None
        self.extracted_data: List[Dict[str, Any]] = []

    def connect(self) -> bool:
        """Connect to existing browser via CDP."""
        try:
            logger.info(f"Connecting to browser at {self.cdp_url}...")
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.connect_over_cdp(self.cdp_url)

            # Get existing contexts and pages
            contexts = self.browser.contexts
            if not contexts:
                logger.error("No browser contexts found. Is the browser open?")
                return False

            pages = contexts[0].pages
            if not pages:
                logger.error("No pages found. Open ProjectSight in the browser first.")
                return False

            # Use the first page or find ProjectSight tab
            self.page = self._find_projectsight_page(pages)
            if not self.page:
                logger.warning("ProjectSight page not found. Using first available page.")
                self.page = pages[0]

            logger.info(f"Connected! Current URL: {self.page.url}")
            return True

        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            logger.error("\nMake sure Chrome is running with --remote-debugging-port=9222")
            return False

    def _find_projectsight_page(self, pages: List[Page]) -> Optional[Page]:
        """Find the ProjectSight tab among open pages."""
        for page in pages:
            try:
                url = page.url
                if 'projectsight' in url.lower() or 'trimble' in url.lower():
                    logger.info(f"Found ProjectSight page: {url}")
                    return page
            except:
                continue
        return None

    def navigate_to_section(self, section: str) -> bool:
        """Navigate to a specific section (RFIs, Submittals, etc.)."""
        try:
            logger.info(f"Navigating to {section}...")

            # Common section URLs in ProjectSight
            section_paths = {
                'projects': '/web/app/Projects',
                'rfis': '/web/app/rfis',
                'submittals': '/web/app/submittals',
                'documents': '/web/app/documents',
                'issues': '/web/app/issues',
                'daily-logs': '/web/app/daily-logs',
            }

            if section.lower() in section_paths:
                # Get base URL
                current_url = self.page.url
                base_url = current_url.split('/web/')[0] if '/web/' in current_url else current_url.rsplit('/', 1)[0]
                target_url = base_url + section_paths[section.lower()]

                self.page.goto(target_url, wait_until='networkidle')
                time.sleep(2)  # Wait for SPA to render
                return True
            else:
                # Try clicking on navigation
                nav_link = self.page.get_by_text(section, exact=False).first
                if nav_link:
                    nav_link.click()
                    time.sleep(2)
                    return True

            return False
        except Exception as e:
            logger.error(f"Navigation failed: {e}")
            return False

    def extract_table_data(self) -> List[Dict[str, Any]]:
        """
        Extract data from the current table/list view.

        This is a generic extraction that works with common table structures.
        Customize the selectors based on what you see in ProjectSight.
        """
        try:
            logger.info("Extracting table data from current page...")

            # Wait for content to load
            time.sleep(1)

            # Try to extract using JavaScript for better SPA handling
            data = self.page.evaluate("""
                () => {
                    const results = [];

                    // Try common table patterns
                    // Pattern 1: Standard HTML tables
                    const tables = document.querySelectorAll('table');
                    for (const table of tables) {
                        const headers = [...table.querySelectorAll('thead th, tr:first-child th')]
                            .map(th => th.textContent.trim());

                        const rows = table.querySelectorAll('tbody tr');
                        for (const row of rows) {
                            const cells = [...row.querySelectorAll('td')];
                            if (cells.length > 0) {
                                const rowData = {};
                                cells.forEach((cell, i) => {
                                    const key = headers[i] || `column_${i}`;
                                    rowData[key] = cell.textContent.trim();
                                });
                                results.push(rowData);
                            }
                        }
                    }

                    // Pattern 2: Grid/list with role attributes
                    if (results.length === 0) {
                        const gridRows = document.querySelectorAll('[role="row"], .grid-row, .list-item');
                        for (const row of gridRows) {
                            const cells = row.querySelectorAll('[role="cell"], .cell, td, .field');
                            if (cells.length > 0) {
                                const rowData = {};
                                cells.forEach((cell, i) => {
                                    // Try to get label from aria or data attributes
                                    const label = cell.getAttribute('aria-label') ||
                                                  cell.getAttribute('data-field') ||
                                                  `field_${i}`;
                                    rowData[label] = cell.textContent.trim();
                                });
                                results.push(rowData);
                            }
                        }
                    }

                    // Pattern 3: Card-based layouts
                    if (results.length === 0) {
                        const cards = document.querySelectorAll('.card, .item-card, [class*="card"]');
                        for (const card of cards) {
                            const cardData = {};
                            // Extract all text content with structure
                            const fields = card.querySelectorAll('label, .label, .field-label');
                            fields.forEach(field => {
                                const value = field.nextElementSibling?.textContent?.trim() ||
                                             field.parentElement?.textContent?.replace(field.textContent, '')?.trim();
                                cardData[field.textContent.trim()] = value || '';
                            });
                            if (Object.keys(cardData).length > 0) {
                                results.push(cardData);
                            }
                        }
                    }

                    return results;
                }
            """)

            logger.info(f"Extracted {len(data)} records from current page")
            return data

        except Exception as e:
            logger.error(f"Table extraction failed: {e}")
            return []

    def extract_with_pagination(self, max_pages: int = 100) -> List[Dict[str, Any]]:
        """Extract data across multiple pages."""
        all_data = []
        page_num = 1

        while page_num <= max_pages:
            logger.info(f"Processing page {page_num}...")

            # Extract current page
            page_data = self.extract_table_data()
            if not page_data:
                logger.info("No more data found, stopping pagination")
                break

            all_data.extend(page_data)
            logger.info(f"Total records so far: {len(all_data)}")

            # Try to go to next page
            if not self._go_to_next_page():
                logger.info("No next page, extraction complete")
                break

            page_num += 1
            time.sleep(1)  # Be nice to the server

        return all_data

    def _go_to_next_page(self) -> bool:
        """Click next page button if available."""
        try:
            # Common next page patterns
            next_selectors = [
                'button[aria-label="Next"]',
                'button[aria-label="Next page"]',
                '.pagination-next',
                'a.next',
                'button:has-text("Next")',
                '[class*="next"]',
                'button[title="Next"]',
            ]

            for selector in next_selectors:
                try:
                    next_btn = self.page.query_selector(selector)
                    if next_btn and next_btn.is_visible() and next_btn.is_enabled():
                        next_btn.click()
                        time.sleep(2)  # Wait for page to load
                        return True
                except:
                    continue

            return False

        except Exception as e:
            logger.debug(f"Next page navigation failed: {e}")
            return False

    def extract_modal_details(self, row_selector: str, fields_to_extract: List[str] = None) -> List[Dict[str, Any]]:
        """
        Click each row to open modal and extract detailed data.

        Args:
            row_selector: CSS selector for clickable rows
            fields_to_extract: Optional list of field names to extract from modal
        """
        all_data = []

        try:
            rows = self.page.query_selector_all(row_selector)
            total_rows = len(rows)
            logger.info(f"Found {total_rows} rows to process")

            for i in range(total_rows):
                try:
                    # Re-query rows each time (DOM might change after modal closes)
                    rows = self.page.query_selector_all(row_selector)
                    if i >= len(rows):
                        break

                    row = rows[i]
                    logger.info(f"Processing row {i+1}/{total_rows}")

                    # Click to open modal
                    row.click()
                    time.sleep(1.5)  # Wait for modal animation

                    # Extract modal data
                    modal_data = self._extract_current_modal(fields_to_extract)
                    if modal_data:
                        all_data.append(modal_data)
                        logger.info(f"Extracted: {list(modal_data.keys())[:3]}...")

                    # Close modal
                    self._close_modal()
                    time.sleep(0.5)

                except Exception as e:
                    logger.warning(f"Failed to process row {i+1}: {e}")
                    self._close_modal()  # Try to close modal if open
                    continue

            return all_data

        except Exception as e:
            logger.error(f"Modal extraction failed: {e}")
            return all_data

    def _extract_current_modal(self, fields: List[str] = None) -> Dict[str, Any]:
        """Extract data from currently open modal."""
        try:
            # Wait for modal to be visible
            modal_selectors = [
                '[role="dialog"]',
                '.modal',
                '.modal-content',
                '[class*="modal"]',
                '.drawer',
                '.panel',
            ]

            modal = None
            for selector in modal_selectors:
                try:
                    modal = self.page.query_selector(selector)
                    if modal and modal.is_visible():
                        break
                except:
                    continue

            if not modal:
                logger.warning("No modal found")
                return {}

            # Extract all visible text with labels
            data = self.page.evaluate("""
                (fields) => {
                    const modal = document.querySelector('[role="dialog"], .modal, .modal-content, [class*="modal"], .drawer, .panel');
                    if (!modal) return {};

                    const result = {};

                    // Method 1: Label + value pairs
                    const labels = modal.querySelectorAll('label, .label, .field-label, dt, [class*="label"]');
                    labels.forEach(label => {
                        const labelText = label.textContent.trim().replace(':', '');
                        if (!labelText) return;

                        // Find associated value
                        const valueEl = label.nextElementSibling ||
                                       label.querySelector('+ *') ||
                                       label.closest('.field, .form-group, .row')?.querySelector('input, select, textarea, .value, dd');

                        if (valueEl) {
                            result[labelText] = valueEl.value || valueEl.textContent?.trim() || '';
                        }
                    });

                    // Method 2: Form fields with name/id
                    const inputs = modal.querySelectorAll('input, select, textarea');
                    inputs.forEach(input => {
                        const name = input.name || input.id || input.getAttribute('aria-label');
                        if (name && input.value) {
                            result[name] = input.value;
                        }
                    });

                    // Method 3: Data attributes
                    const dataFields = modal.querySelectorAll('[data-field], [data-value]');
                    dataFields.forEach(el => {
                        const field = el.getAttribute('data-field');
                        if (field) {
                            result[field] = el.textContent.trim();
                        }
                    });

                    // Filter by requested fields if specified
                    if (fields && fields.length > 0) {
                        const filtered = {};
                        fields.forEach(f => {
                            const key = Object.keys(result).find(k =>
                                k.toLowerCase().includes(f.toLowerCase())
                            );
                            if (key) filtered[f] = result[key];
                        });
                        return filtered;
                    }

                    return result;
                }
            """, fields)

            return data

        except Exception as e:
            logger.error(f"Modal data extraction failed: {e}")
            return {}

    def _close_modal(self):
        """Close the currently open modal."""
        try:
            close_selectors = [
                'button[aria-label="Close"]',
                'button[aria-label="Cancel"]',
                '.modal-close',
                '.close-button',
                'button:has-text("Close")',
                'button:has-text("Cancel")',
                '[class*="close"]',
                'button.close',
                '.modal button[type="button"]',
            ]

            for selector in close_selectors:
                try:
                    close_btn = self.page.query_selector(selector)
                    if close_btn and close_btn.is_visible():
                        close_btn.click()
                        time.sleep(0.5)
                        return
                except:
                    continue

            # Fallback: Press Escape
            self.page.keyboard.press('Escape')
            time.sleep(0.5)

        except Exception as e:
            logger.debug(f"Modal close failed: {e}")

    def intercept_api_calls(self, duration_seconds: int = 30) -> List[Dict[str, Any]]:
        """
        Capture API calls made by the SPA.

        This is often more reliable than DOM scraping - capture what the app
        is already fetching from the server.
        """
        logger.info(f"Intercepting API calls for {duration_seconds} seconds...")
        logger.info("Navigate around the app to trigger data loads...")

        api_responses = []

        def handle_response(response):
            try:
                url = response.url
                # Filter for API-like responses
                if any(pattern in url for pattern in ['/api/', '/v1/', '/v2/', '/graphql', '/data/']):
                    content_type = response.headers.get('content-type', '')
                    if 'json' in content_type:
                        try:
                            body = response.json()
                            api_responses.append({
                                'url': url,
                                'status': response.status,
                                'data': body
                            })
                            logger.info(f"Captured API response: {url[:80]}...")
                        except:
                            pass
            except:
                pass

        # Attach listener
        self.page.on('response', handle_response)

        # Wait for specified duration
        time.sleep(duration_seconds)

        # Remove listener
        self.page.remove_listener('response', handle_response)

        logger.info(f"Captured {len(api_responses)} API responses")
        return api_responses

    def save_data(self, data: List[Dict[str, Any]], filename: str = None):
        """Save extracted data to JSON file."""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"data/extracted/projectsight_{timestamp}.json"

        output_path = Path(filename)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)

        logger.info(f"Saved {len(data)} records to {output_path}")
        return str(output_path)

    def disconnect(self):
        """Clean up connection."""
        try:
            if self.playwright:
                self.playwright.stop()
            logger.info("Disconnected from browser")
        except:
            pass


def main():
    parser = argparse.ArgumentParser(
        description='Extract data from ProjectSight using existing browser session',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract table data from current page
  python scripts/browser_cdp_extractor.py --mode table

  # Extract with pagination
  python scripts/browser_cdp_extractor.py --mode paginated --max-pages 10

  # Capture API calls (navigate around while this runs)
  python scripts/browser_cdp_extractor.py --mode api --duration 60

  # Extract modal details (click each row)
  python scripts/browser_cdp_extractor.py --mode modal --row-selector ".rfi-row"

Before running, start Chrome with:
  google-chrome --remote-debugging-port=9222
        """
    )

    parser.add_argument('--cdp-url', default='http://localhost:9222',
                        help='Chrome DevTools Protocol URL (default: http://localhost:9222)')
    parser.add_argument('--mode', choices=['table', 'paginated', 'modal', 'api'],
                        default='table', help='Extraction mode')
    parser.add_argument('--max-pages', type=int, default=100,
                        help='Max pages for paginated mode')
    parser.add_argument('--row-selector', default='tbody tr',
                        help='CSS selector for rows (modal mode)')
    parser.add_argument('--duration', type=int, default=30,
                        help='Duration in seconds for API capture mode')
    parser.add_argument('--output', '-o', help='Output file path')
    parser.add_argument('--section', help='Navigate to section first (rfis, submittals, etc.)')

    args = parser.parse_args()

    extractor = CDPBrowserExtractor(args.cdp_url)

    try:
        if not extractor.connect():
            logger.error("Failed to connect to browser")
            logger.error("\nSetup instructions:")
            logger.error("1. Start Chrome with: google-chrome --remote-debugging-port=9222")
            logger.error("2. Log into ProjectSight manually")
            logger.error("3. Run this script again")
            return 1

        # Navigate to section if specified
        if args.section:
            extractor.navigate_to_section(args.section)

        # Extract based on mode
        data = []

        if args.mode == 'table':
            data = extractor.extract_table_data()

        elif args.mode == 'paginated':
            data = extractor.extract_with_pagination(args.max_pages)

        elif args.mode == 'modal':
            data = extractor.extract_modal_details(args.row_selector)

        elif args.mode == 'api':
            data = extractor.intercept_api_calls(args.duration)

        # Save results
        if data:
            output_file = extractor.save_data(data, args.output)
            logger.info(f"Extraction complete! Data saved to: {output_file}")
        else:
            logger.warning("No data extracted")

        return 0

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 1
    finally:
        extractor.disconnect()


if __name__ == '__main__':
    exit(main())
