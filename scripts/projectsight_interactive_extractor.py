#!/usr/bin/env python3
"""
ProjectSight Interactive Extractor - Automated extraction with your browser session.

This script provides an interactive loop for extracting RFIs, Submittals, Issues, etc.
from ProjectSight while you're logged in.

WORKFLOW:
1. Start Chrome with debugging: google-chrome --remote-debugging-port=9222
2. Log into ProjectSight manually
3. Navigate to the section you want to extract (RFIs, Submittals, etc.)
4. Run this script
5. The script will loop through items and extract data
"""

import json
import time
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, asdict

from playwright.sync_api import sync_playwright, Page, Browser, ElementHandle

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ExtractionConfig:
    """Configuration for extraction behavior."""
    # Selectors - customize based on what you see in ProjectSight
    row_selector: str = "table tbody tr"
    modal_selector: str = "[role='dialog'], .modal, .drawer"
    close_button_selector: str = "button[aria-label='Close'], .close-button"
    next_page_selector: str = "button[aria-label='Next']"

    # Timing
    click_delay: float = 1.0
    modal_wait: float = 1.5
    page_load_wait: float = 2.0

    # Limits
    max_items: int = 1000
    max_pages: int = 100


class ProjectSightExtractor:
    """Interactive extractor for ProjectSight data."""

    def __init__(self, cdp_url: str = "http://localhost:9222"):
        self.cdp_url = cdp_url
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.playwright = None
        self.config = ExtractionConfig()
        self.extracted_items: List[Dict[str, Any]] = []

    def connect(self) -> bool:
        """Connect to existing browser via CDP."""
        try:
            logger.info(f"Connecting to browser at {self.cdp_url}...")
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.connect_over_cdp(self.cdp_url)

            contexts = self.browser.contexts
            if not contexts or not contexts[0].pages:
                logger.error("No browser pages found. Open ProjectSight first.")
                return False

            # Find ProjectSight page
            for page in contexts[0].pages:
                if 'projectsight' in page.url.lower():
                    self.page = page
                    break

            if not self.page:
                self.page = contexts[0].pages[0]

            logger.info(f"Connected to: {self.page.url}")
            return True

        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False

    def discover_page_structure(self) -> Dict[str, Any]:
        """
        Analyze current page to discover its structure.
        Run this first to understand what selectors to use.
        """
        logger.info("Analyzing page structure...")

        structure = self.page.evaluate("""
            () => {
                const result = {
                    tables: [],
                    grids: [],
                    lists: [],
                    clickableRows: [],
                    modals: [],
                    pagination: []
                };

                // Find tables
                document.querySelectorAll('table').forEach((table, i) => {
                    const rows = table.querySelectorAll('tbody tr');
                    result.tables.push({
                        index: i,
                        selector: `table:nth-of-type(${i+1})`,
                        rowCount: rows.length,
                        headers: [...table.querySelectorAll('thead th, tr:first-child th')]
                            .map(th => th.textContent.trim())
                    });
                });

                // Find grid patterns
                document.querySelectorAll('[role="grid"], [class*="grid"]').forEach((grid, i) => {
                    const rows = grid.querySelectorAll('[role="row"], [class*="row"]');
                    result.grids.push({
                        index: i,
                        className: grid.className,
                        rowCount: rows.length
                    });
                });

                // Find list patterns
                document.querySelectorAll('[role="list"], ul.list, .list-view').forEach((list, i) => {
                    const items = list.querySelectorAll('[role="listitem"], li');
                    result.lists.push({
                        index: i,
                        className: list.className,
                        itemCount: items.length
                    });
                });

                // Find clickable rows (have click handlers or links)
                document.querySelectorAll('tr[onclick], tr[data-id], tr.clickable, [role="row"][tabindex]').forEach((row, i) => {
                    if (i < 5) {  // Sample first 5
                        result.clickableRows.push({
                            tag: row.tagName,
                            className: row.className,
                            text: row.textContent.substring(0, 100).trim()
                        });
                    }
                });

                // Find modal triggers
                document.querySelectorAll('[role="dialog"], .modal, .drawer, [class*="modal"]').forEach((modal, i) => {
                    result.modals.push({
                        role: modal.getAttribute('role'),
                        className: modal.className,
                        visible: modal.offsetParent !== null
                    });
                });

                // Find pagination
                document.querySelectorAll('.pagination, [class*="pagination"], nav[aria-label*="page"]').forEach((pag, i) => {
                    result.pagination.push({
                        className: pag.className,
                        buttons: [...pag.querySelectorAll('button, a')].map(b => b.textContent.trim()).slice(0, 10)
                    });
                });

                return result;
            }
        """)

        logger.info(f"Found: {len(structure['tables'])} tables, {len(structure['grids'])} grids")
        return structure

    def extract_current_view(self) -> List[Dict[str, Any]]:
        """Extract all visible data from current page/view."""
        logger.info("Extracting current view...")

        data = self.page.evaluate("""
            () => {
                const results = [];

                // Strategy 1: Table with headers
                const tables = document.querySelectorAll('table');
                for (const table of tables) {
                    const headers = [...table.querySelectorAll('thead th, tr:first-child th')]
                        .map(th => th.textContent.trim())
                        .filter(h => h.length > 0);

                    if (headers.length === 0) continue;

                    const rows = table.querySelectorAll('tbody tr');
                    for (const row of rows) {
                        const cells = [...row.querySelectorAll('td')];
                        if (cells.length === 0) continue;

                        const rowData = { _source: 'table' };
                        cells.forEach((cell, i) => {
                            const key = headers[i] || `col_${i}`;
                            rowData[key] = cell.textContent.trim();

                            // Also capture links
                            const link = cell.querySelector('a');
                            if (link) {
                                rowData[`${key}_link`] = link.href;
                            }
                        });
                        results.push(rowData);
                    }
                }

                // Strategy 2: Grid rows with cells
                if (results.length === 0) {
                    const rows = document.querySelectorAll('[role="row"]');
                    for (const row of rows) {
                        const cells = row.querySelectorAll('[role="cell"], [role="gridcell"]');
                        if (cells.length === 0) continue;

                        const rowData = { _source: 'grid' };
                        cells.forEach((cell, i) => {
                            const label = cell.getAttribute('aria-label') ||
                                         cell.getAttribute('data-label') ||
                                         `field_${i}`;
                            rowData[label] = cell.textContent.trim();
                        });
                        results.push(rowData);
                    }
                }

                return results;
            }
        """)

        logger.info(f"Extracted {len(data)} records")
        return data

    def extract_with_modal_loop(
        self,
        row_selector: str = None,
        extract_fn: Callable[[Page], Dict] = None
    ) -> List[Dict[str, Any]]:
        """
        Click each row, extract from modal, close, repeat.

        Args:
            row_selector: CSS selector for clickable rows
            extract_fn: Optional custom extraction function
        """
        row_selector = row_selector or self.config.row_selector
        results = []

        try:
            # Get initial row count
            rows = self.page.query_selector_all(row_selector)
            total_rows = len(rows)
            logger.info(f"Found {total_rows} rows to process")

            if total_rows == 0:
                logger.warning("No rows found. Try running discover_page_structure() first")
                return []

            for i in range(min(total_rows, self.config.max_items)):
                try:
                    # Re-query rows (DOM changes after modal interactions)
                    rows = self.page.query_selector_all(row_selector)
                    if i >= len(rows):
                        logger.info("No more rows available")
                        break

                    row = rows[i]

                    # Get row preview for logging
                    preview = row.text_content()[:50] if row.text_content() else "..."
                    logger.info(f"[{i+1}/{total_rows}] Processing: {preview}...")

                    # Click to open modal/detail view
                    row.click()
                    time.sleep(self.config.modal_wait)

                    # Extract data
                    if extract_fn:
                        item_data = extract_fn(self.page)
                    else:
                        item_data = self._extract_modal_content()

                    item_data['_index'] = i
                    item_data['_extracted_at'] = datetime.now().isoformat()
                    results.append(item_data)

                    # Close modal
                    self._close_current_modal()
                    time.sleep(self.config.click_delay)

                except Exception as e:
                    logger.warning(f"Failed to process row {i}: {e}")
                    self._close_current_modal()
                    time.sleep(0.5)
                    continue

            logger.info(f"Extracted {len(results)} items")
            return results

        except Exception as e:
            logger.error(f"Modal loop extraction failed: {e}")
            return results

    def _extract_modal_content(self) -> Dict[str, Any]:
        """Extract all data from the currently open modal/detail view."""
        return self.page.evaluate("""
            () => {
                const data = {};

                // Find the modal/detail container
                const container = document.querySelector(
                    '[role="dialog"], .modal-content, .detail-view, .drawer-content, ' +
                    '[class*="modal"], [class*="detail"], [class*="panel"]'
                );

                if (!container) {
                    // No modal, try extracting from main content area that changed
                    return { _error: 'No modal/detail container found' };
                }

                // Extract title/header
                const title = container.querySelector('h1, h2, h3, .title, .header, [class*="title"]');
                if (title) {
                    data['title'] = title.textContent.trim();
                }

                // Extract all label-value pairs
                const labelPatterns = [
                    'label', '.label', '.field-label', 'dt', '[class*="label"]',
                    '.form-label', '.key', '[class*="key"]'
                ];

                labelPatterns.forEach(pattern => {
                    container.querySelectorAll(pattern).forEach(label => {
                        const labelText = label.textContent.trim().replace(/[:*]/g, '').trim();
                        if (!labelText || labelText.length > 50) return;

                        // Find the value
                        let value = '';

                        // Check sibling
                        const sibling = label.nextElementSibling;
                        if (sibling) {
                            value = sibling.value || sibling.textContent?.trim();
                        }

                        // Check parent container
                        if (!value) {
                            const parent = label.closest('.field, .form-group, .row, dd');
                            if (parent) {
                                const valueEl = parent.querySelector('input, select, textarea, .value, span');
                                if (valueEl && valueEl !== label) {
                                    value = valueEl.value || valueEl.textContent?.trim();
                                }
                            }
                        }

                        if (value && !data[labelText]) {
                            data[labelText] = value;
                        }
                    });
                });

                // Extract form inputs directly
                container.querySelectorAll('input, select, textarea').forEach(input => {
                    const name = input.name || input.id || input.getAttribute('aria-label');
                    if (name && input.value && !data[name]) {
                        data[name] = input.value;
                    }
                });

                // Extract any data attributes
                container.querySelectorAll('[data-field], [data-value], [data-id]').forEach(el => {
                    const field = el.getAttribute('data-field') || el.getAttribute('data-id');
                    if (field && !data[field]) {
                        data[field] = el.textContent.trim() || el.getAttribute('data-value');
                    }
                });

                // Extract description/notes (usually longer text blocks)
                const descPatterns = ['.description', '.notes', '.comments', '[class*="description"]', 'textarea'];
                descPatterns.forEach(pattern => {
                    const el = container.querySelector(pattern);
                    if (el) {
                        const text = el.value || el.textContent?.trim();
                        if (text && text.length > 20) {
                            data['description'] = text;
                        }
                    }
                });

                return data;
            }
        """)

    def _close_current_modal(self):
        """Close any open modal/drawer/panel."""
        try:
            # Try close button patterns
            close_patterns = [
                'button[aria-label="Close"]',
                'button[aria-label="Cancel"]',
                '.modal-close',
                '.close-button',
                '.drawer-close',
                'button.close',
                '[class*="close"]:not([class*="closed"])',
                'button:has-text("Close")',
                'button:has-text("Cancel")',
            ]

            for pattern in close_patterns:
                try:
                    btn = self.page.query_selector(pattern)
                    if btn and btn.is_visible():
                        btn.click()
                        time.sleep(0.3)
                        return
                except:
                    continue

            # Fallback: click backdrop or press Escape
            try:
                backdrop = self.page.query_selector('.modal-backdrop, .overlay, [class*="backdrop"]')
                if backdrop and backdrop.is_visible():
                    backdrop.click()
                    return
            except:
                pass

            self.page.keyboard.press('Escape')

        except Exception as e:
            logger.debug(f"Modal close attempt: {e}")

    def extract_all_pages(self, per_page_fn: Callable = None) -> List[Dict[str, Any]]:
        """Extract data across all pages using pagination."""
        all_data = []
        page_num = 1

        while page_num <= self.config.max_pages:
            logger.info(f"=== Page {page_num} ===")

            # Extract current page
            if per_page_fn:
                page_data = per_page_fn()
            else:
                page_data = self.extract_current_view()

            if not page_data:
                logger.info("No data on this page, stopping")
                break

            all_data.extend(page_data)
            logger.info(f"Total records: {len(all_data)}")

            # Try next page
            if not self._go_next_page():
                logger.info("No more pages")
                break

            page_num += 1
            time.sleep(self.config.page_load_wait)

        return all_data

    def _go_next_page(self) -> bool:
        """Navigate to next page if available."""
        try:
            next_patterns = [
                'button[aria-label="Next"]',
                'button[aria-label="Next page"]',
                'a[aria-label="Next"]',
                '.pagination-next:not([disabled])',
                'button:has-text("Next"):not([disabled])',
                '[class*="next"]:not([disabled])',
            ]

            for pattern in next_patterns:
                try:
                    btn = self.page.query_selector(pattern)
                    if btn and btn.is_visible() and btn.is_enabled():
                        btn.click()
                        time.sleep(self.config.page_load_wait)
                        return True
                except:
                    continue

            return False

        except Exception as e:
            logger.debug(f"Next page navigation: {e}")
            return False

    def save_results(self, data: List[Dict], filename: str = None) -> str:
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

    def interactive_mode(self):
        """Run in interactive mode with menu options."""
        print("\n" + "="*60)
        print("ProjectSight Interactive Extractor")
        print("="*60)
        print(f"Connected to: {self.page.url}")
        print("\nCommands:")
        print("  1. discover  - Analyze page structure")
        print("  2. extract   - Extract current view (table/list)")
        print("  3. modals    - Extract by clicking each row (modal loop)")
        print("  4. pages     - Extract all pages")
        print("  5. capture   - Capture API calls (30 seconds)")
        print("  6. save      - Save extracted data")
        print("  7. quit      - Exit")
        print("="*60 + "\n")

        while True:
            try:
                cmd = input("\nCommand> ").strip().lower()

                if cmd in ['1', 'discover']:
                    structure = self.discover_page_structure()
                    print(json.dumps(structure, indent=2))

                elif cmd in ['2', 'extract']:
                    data = self.extract_current_view()
                    self.extracted_items.extend(data)
                    print(f"Extracted {len(data)} items (total: {len(self.extracted_items)})")
                    if data:
                        print(f"Sample: {json.dumps(data[0], indent=2)[:500]}")

                elif cmd in ['3', 'modals']:
                    selector = input("Row selector (Enter for default 'tbody tr'): ").strip()
                    selector = selector or 'tbody tr'
                    data = self.extract_with_modal_loop(selector)
                    self.extracted_items.extend(data)
                    print(f"Extracted {len(data)} items (total: {len(self.extracted_items)})")

                elif cmd in ['4', 'pages']:
                    data = self.extract_all_pages()
                    self.extracted_items.extend(data)
                    print(f"Extracted {len(data)} items (total: {len(self.extracted_items)})")

                elif cmd in ['5', 'capture']:
                    print("Capturing API calls for 30 seconds...")
                    print("Navigate around ProjectSight to trigger data loads...")
                    # This would need the intercept logic from the other script
                    print("(API capture not yet implemented in interactive mode)")

                elif cmd in ['6', 'save']:
                    if self.extracted_items:
                        filename = input("Filename (Enter for auto): ").strip() or None
                        self.save_results(self.extracted_items, filename)
                    else:
                        print("No data to save. Run extract first.")

                elif cmd in ['7', 'quit', 'q', 'exit']:
                    if self.extracted_items:
                        save = input(f"Save {len(self.extracted_items)} items before exit? (y/n): ")
                        if save.lower() == 'y':
                            self.save_results(self.extracted_items)
                    print("Goodbye!")
                    break

                else:
                    print("Unknown command. Try: discover, extract, modals, pages, save, quit")

            except KeyboardInterrupt:
                print("\nInterrupted. Type 'quit' to exit.")
            except Exception as e:
                print(f"Error: {e}")

    def disconnect(self):
        """Clean up connection."""
        try:
            if self.playwright:
                self.playwright.stop()
        except:
            pass


def main():
    parser = argparse.ArgumentParser(description='ProjectSight Interactive Extractor')
    parser.add_argument('--cdp-url', default='http://localhost:9222',
                        help='Chrome DevTools Protocol URL')
    parser.add_argument('--interactive', '-i', action='store_true',
                        help='Run in interactive mode')
    parser.add_argument('--discover', action='store_true',
                        help='Discover page structure and exit')
    parser.add_argument('--extract', action='store_true',
                        help='Extract current view and exit')
    parser.add_argument('--modal-loop', metavar='SELECTOR',
                        help='Extract using modal loop with given row selector')
    parser.add_argument('--output', '-o', help='Output file')

    args = parser.parse_args()

    extractor = ProjectSightExtractor(args.cdp_url)

    try:
        if not extractor.connect():
            print("\nFailed to connect. Make sure Chrome is running with:")
            print("  google-chrome --remote-debugging-port=9222")
            print("\nThen log into ProjectSight and run this script again.")
            return 1

        if args.discover:
            structure = extractor.discover_page_structure()
            print(json.dumps(structure, indent=2))

        elif args.extract:
            data = extractor.extract_current_view()
            if data:
                extractor.save_results(data, args.output)

        elif args.modal_loop:
            data = extractor.extract_with_modal_loop(args.modal_loop)
            if data:
                extractor.save_results(data, args.output)

        elif args.interactive:
            extractor.interactive_mode()

        else:
            # Default to interactive
            extractor.interactive_mode()

        return 0

    except KeyboardInterrupt:
        print("\nInterrupted")
        return 1
    finally:
        extractor.disconnect()


if __name__ == '__main__':
    exit(main())
