#!/usr/bin/env python3
"""
ProjectSight NCR/QOR/SOR/SWN/VR Scraper

Extracts quality non-conformance records from ProjectSight including:
- Main record fields (Number, Status, Type, Dates, Discipline, etc.)
- Additional Info fields (Cost, Root Cause, Disposition, etc.)
- Full comments thread with author, company, date, and linked items
- Attachments (downloaded to attachments/{record-id}/)

Each record is saved as an individual JSON file ({TYPE}-{NUMBER}.json) to:
- Enable incremental/resumable extraction
- Prevent data loss from truncation
- Allow easy re-processing of individual records

Output: raw/projectsight/ncr/records/{TYPE}-{NUMBER}.json
        raw/projectsight/ncr/attachments/{TYPE}-{NUMBER}/
        raw/projectsight/ncr/manifest.json

Usage:
    # Full extraction (all 275 records)
    python scripts/projectsight/process/scrape_projectsight_ncr.py

    # Process specific date range (by Required Correction Date)
    python scripts/projectsight/process/scrape_projectsight_ncr.py --start-date 2024-01-01 --end-date 2024-12-31

    # Skip already processed records (idempotent mode)
    python scripts/projectsight/process/scrape_projectsight_ncr.py --skip-existing

    # Force re-process existing records
    python scripts/projectsight/process/scrape_projectsight_ncr.py --force

    # Limit for testing
    python scripts/projectsight/process/scrape_projectsight_ncr.py --limit 10

    # Headless mode
    python scripts/projectsight/process/scrape_projectsight_ncr.py --headless

Environment Variables (from .env):
    PROJECTSIGHT_USERNAME - Login email
    PROJECTSIGHT_PASSWORD - Login password
    PROJECTSIGHT_HEADLESS - Set to 'true' for headless mode (default: false)
    WINDOWS_DATA_DIR - Base data directory

Installation:
    pip install playwright python-dotenv
    playwright install chromium
"""

import os
import sys
import json
import time
import argparse
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from scripts.projectsight.utils.projectsight_session import ProjectSightSession, PROJECTS

# Load environment variables
load_dotenv(override=True)


class NCRScraper:
    """Scraper for ProjectSight NCR/QOR/SOR/SWN/VR records."""

    def __init__(self, headless: bool = False, output_dir: Path = None,
                 skip_existing: bool = False, force: bool = False,
                 limit: int = 0, start_date: str = None, end_date: str = None):
        self.headless = headless
        self.skip_existing = skip_existing
        self.force = force
        self.limit = limit
        self.start_date = start_date
        self.end_date = end_date

        # Output directories
        windows_data_dir = os.getenv('WINDOWS_DATA_DIR', '')
        if windows_data_dir:
            base_dir = Path(windows_data_dir.replace('\\', '/'))
            if not base_dir.exists():
                # Try WSL path conversion
                if windows_data_dir.startswith('C:'):
                    base_dir = Path('/mnt/c' + windows_data_dir[2:].replace('\\', '/'))
        else:
            base_dir = Path(__file__).resolve().parent.parent.parent.parent / 'data'

        self.output_dir = output_dir or base_dir / 'raw' / 'projectsight' / 'ncr'
        self.records_dir = self.output_dir / 'records'
        self.attachments_dir = self.output_dir / 'attachments'
        self.manifest_file = self.output_dir / 'manifest.json'

        # Create directories
        self.records_dir.mkdir(parents=True, exist_ok=True)
        self.attachments_dir.mkdir(parents=True, exist_ok=True)

        # Session and state
        self.session: Optional[ProjectSightSession] = None
        self.manifest = self._load_manifest()
        self.records_processed = 0
        self.records_skipped = 0
        self.errors = []

    def _load_manifest(self) -> Dict:
        """Load or initialize manifest file."""
        if self.manifest_file.exists():
            try:
                with open(self.manifest_file) as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading manifest: {e}")

        return {
            "last_updated": None,
            "total_records": 0,
            "processed_records": {},
            "processed_date_ranges": [],
            "errors": []
        }

    def _save_manifest(self):
        """Save manifest file."""
        self.manifest["last_updated"] = datetime.now().isoformat()
        with open(self.manifest_file, 'w') as f:
            json.dump(self.manifest, f, indent=2)

    def _is_record_processed(self, record_id: str) -> bool:
        """Check if record has been processed."""
        if self.force:
            return False
        return record_id in self.manifest.get("processed_records", {})

    def start(self):
        """Start the scraper session."""
        print(f"Starting NCR Scraper...")
        print(f"  Output directory: {self.output_dir}")
        print(f"  Skip existing: {self.skip_existing}")
        print(f"  Force: {self.force}")
        print(f"  Limit: {self.limit or 'unlimited'}")
        if self.start_date:
            print(f"  Start date: {self.start_date}")
        if self.end_date:
            print(f"  End date: {self.end_date}")

        # Use tpjt_fab1 project which has the NCR list
        self.session = ProjectSightSession(headless=self.headless, project='tpjt_fab1')
        self.session.start()

    def stop(self):
        """Stop the scraper session."""
        if self.session:
            self.session.stop()
        self._save_manifest()
        print(f"\nScraper stopped.")
        print(f"  Records processed: {self.records_processed}")
        print(f"  Records skipped: {self.records_skipped}")
        print(f"  Errors: {len(self.errors)}")

    def login(self) -> bool:
        """Login to ProjectSight."""
        ncr_url = self.session.project.ncr_list_url
        return self.session.login(target_url=ncr_url)

    def apply_date_filter(self) -> bool:
        """Apply date filter to the NCR list."""
        if not self.start_date and not self.end_date:
            return True

        try:
            page = self.session.page
            frame = page.frame_locator('iframe[name="fraMenuContent"]')

            print(f"Applying date filter: {self.start_date} to {self.end_date}")

            # Find the Required Correction Date filter section
            if self.start_date:
                # Click first date picker button
                start_picker = frame.locator('[title="Required Correction Date"]').locator('..').locator('button').first
                start_picker.click()
                time.sleep(0.5)

                # Enter start date
                start_input = frame.locator('input[placeholder*="Date"]').first
                start_input.fill(self.start_date)
                start_input.press('Enter')
                time.sleep(1)

            if self.end_date:
                # Click second date picker button
                end_picker = frame.locator('[title="Required Correction Date"]').locator('..').locator('button').last
                end_picker.click()
                time.sleep(0.5)

                # Enter end date
                end_input = frame.locator('input[placeholder*="Date"]').last
                end_input.fill(self.end_date)
                end_input.press('Enter')
                time.sleep(1)

            # Wait for filter to apply
            time.sleep(2)
            return True

        except Exception as e:
            print(f"Error applying date filter: {e}")
            return False

    def get_visible_records(self) -> List[Dict]:
        """Get list of visible records from the grid."""
        records = []
        try:
            page = self.session.page
            frame = page.frame_locator('iframe[name="fraMenuContent"]')

            # Expand all type groups first
            expand_buttons = frame.locator('[title="Expand Grouped Row"]')
            count = expand_buttons.count()
            print(f"  Found {count} collapsed groups, expanding...")

            for i in range(count):
                try:
                    expand_buttons.nth(i).click()
                    time.sleep(0.5)
                except:
                    pass

            time.sleep(2)

            # Get all data rows (not group headers)
            rows = frame.locator('tr[role="row"]:has(td[role="gridcell"]:nth-child(4))')
            row_count = rows.count()
            print(f"  Found {row_count} data rows")

            for i in range(row_count):
                try:
                    row = rows.nth(i)
                    cells = row.locator('td[role="gridcell"]')
                    cell_count = cells.count()

                    if cell_count >= 5:
                        # Grid columns (0-indexed): 0=checkbox/select, 1=Number, 2=Description, 3=Status, 4=Name of Issuer, 5=Required Correction Date
                        # The first column may be a selection column
                        number = cells.nth(1).text_content().strip()
                        description = cells.nth(2).text_content().strip()
                        status = cells.nth(3).text_content().strip()
                        name_of_issuer = cells.nth(4).text_content().strip() if cell_count > 4 else ""
                        required_correction_date = cells.nth(5).text_content().strip() if cell_count > 5 else ""

                        # Skip group header rows (they only have the type name)
                        if not number or number in ['NCR', 'QOR', 'SOR', 'SWN', 'Variance Request']:
                            continue

                        # Determine type from description pattern or number pattern
                        record_type = "UNKNOWN"
                        if "-NCR-" in description or number.startswith('NCR'):
                            record_type = "NCR"
                        elif "-QOR-" in description or number.startswith('QOR'):
                            record_type = "QOR"
                        elif "-SOR-" in description or number.startswith('SOR'):
                            record_type = "SOR"
                        elif "-SWN-" in description or number.startswith('SWN'):
                            record_type = "SWN"
                        elif "Variance" in description or "VR" in number:
                            record_type = "VR"

                        # Parse the record number (e.g., "0828" from full number)
                        # Number field may contain just the numeric part or a formatted ID
                        record_num = number
                        record_id = f"{record_type}-{number}"

                        records.append({
                            "id": record_id,
                            "number": record_num,
                            "type": record_type,
                            "description": description,
                            "status": status,
                            "name_of_issuer": name_of_issuer,
                            "required_correction_date": required_correction_date,
                            "row_index": i
                        })
                except Exception as e:
                    print(f"  Error parsing row {i}: {e}")

            # If no records found with shifted indices, try without offset
            if not records and row_count > 0:
                print("  Retrying with alternative column mapping...")
                for i in range(row_count):
                    try:
                        row = rows.nth(i)
                        cells = row.locator('td[role="gridcell"]')
                        cell_count = cells.count()

                        if cell_count >= 4:
                            # Try 0-based: 0=Number, 1=Description, 2=Status, 3=Name of Issuer
                            number = cells.nth(0).text_content().strip()
                            description = cells.nth(1).text_content().strip()
                            status = cells.nth(2).text_content().strip()
                            name_of_issuer = cells.nth(3).text_content().strip() if cell_count > 3 else ""

                            if not number or number in ['NCR', 'QOR', 'SOR', 'SWN', 'Variance Request']:
                                continue

                            record_type = "UNKNOWN"
                            if "-NCR-" in description:
                                record_type = "NCR"
                            elif "-QOR-" in description:
                                record_type = "QOR"
                            elif "-SOR-" in description:
                                record_type = "SOR"
                            elif "-SWN-" in description:
                                record_type = "SWN"
                            elif "Variance" in description:
                                record_type = "VR"

                            record_id = f"{record_type}-{number}"

                            records.append({
                                "id": record_id,
                                "number": number,
                                "type": record_type,
                                "description": description,
                                "status": status,
                                "name_of_issuer": name_of_issuer,
                                "row_index": i
                            })
                    except Exception as e:
                        pass

        except Exception as e:
            print(f"Error getting visible records: {e}")

        return records

    def scroll_to_load_all_records(self):
        """Scroll the virtualized grid to load all records."""
        try:
            page = self.session.page
            frame = page.frame_locator('iframe[name="fraMenuContent"]')

            # Find the grid container
            grid = frame.locator('.dx-datagrid-rowsview')

            # Scroll down repeatedly to load all rows
            for scroll in range(50):  # Max 50 scroll attempts
                grid.evaluate('el => el.scrollTop = el.scrollHeight')
                time.sleep(0.3)

                # Check if we've reached the end
                current_rows = frame.locator('tr[role="row"]').count()
                if scroll > 0 and scroll % 10 == 0:
                    print(f"    Scrolled {scroll} times, {current_rows} rows loaded")

        except Exception as e:
            print(f"Error scrolling grid: {e}")

    def click_record(self, record: Dict) -> bool:
        """Click on a record to open the detail panel."""
        try:
            page = self.session.page
            frame = page.frame_locator('iframe[name="fraMenuContent"]')

            # Find the record row by number
            number_cell = frame.locator(f'td[role="gridcell"]:has-text("{record["number"]}")').first

            # Double-click to open the full detail view directly
            # This is the standard way to open records in grid-based apps
            number_cell.dblclick()
            time.sleep(3)

            return True
        except Exception as e:
            print(f"  Error clicking record {record['id']}: {e}")
            return False

    def expand_detail_view(self) -> bool:
        """Click expand button to open full detail view."""
        try:
            page = self.session.page
            frame = page.frame_locator('iframe[name="fraMenuContent"]')

            # Wait for the expand button to become visible after clicking a record
            # The button appears in the quick preview panel
            # Try multiple selectors with visibility wait
            selectors = [
                '#tlbEdit_divForbtnOpenDetailView',  # Edit mode expand button
                '#tlbCreate_divForbtnOpenDetailView',  # Create mode expand button
                '[title="Expand"]:visible',  # Any visible expand button
                'div.regularTooltip[title="Expand"]',  # Div-based expand tooltip
            ]

            for selector in selectors:
                try:
                    expand_btn = frame.locator(selector).first
                    # Wait up to 10 seconds for button to be visible
                    expand_btn.wait_for(state='visible', timeout=10000)
                    expand_btn.click()
                    time.sleep(3)
                    return True
                except:
                    continue

            # If no expand button found/clicked, the preview panel might be sufficient
            print("    No expand button visible, using preview panel")
            return False

        except Exception as e:
            print(f"  Error expanding detail view: {e}")
            return False

    def extract_main_fields(self) -> Dict:
        """Extract main fields from the detail panel."""
        fields = {}
        try:
            page = self.session.page

            # Check for detail frame (fraDef)
            detail_frame = page.frame_locator('iframe[name="fraDef"]')

            # Try to extract from detail frame first, fallback to main content frame
            for frame in [detail_frame, page.frame_locator('iframe[name="fraMenuContent"]')]:
                try:
                    # Number
                    number_field = frame.locator('input[placeholder*="Number"], input[aria-label*="Number"]').first
                    if number_field.count() > 0:
                        fields["number"] = number_field.input_value()

                    # Status
                    status_field = frame.locator('input[placeholder*="Status"], [aria-label*="Status"]').first
                    if status_field.count() > 0:
                        fields["status"] = status_field.input_value() or status_field.text_content()

                    # Description
                    desc_field = frame.locator('input[placeholder*="Description"]').first
                    if desc_field.count() > 0:
                        fields["description"] = desc_field.input_value()

                    # Created on
                    created_field = frame.locator('input[aria-label*="Created on"], [placeholder*="Created"]').first
                    if created_field.count() > 0:
                        fields["created_on"] = created_field.input_value()

                    # Type
                    type_field = frame.locator('select:has-text("NCR"), [aria-label*="Type"]').first
                    if type_field.count() > 0:
                        fields["type"] = type_field.input_value() or type_field.text_content()

                    # Required Correction Date
                    rcd_field = frame.locator('input[aria-label*="Required Correction"]').first
                    if rcd_field.count() > 0:
                        fields["required_correction_date"] = rcd_field.input_value()

                    # Discipline
                    disc_field = frame.locator('[aria-label*="Discipline"]').first
                    if disc_field.count() > 0:
                        fields["discipline"] = disc_field.input_value() or disc_field.text_content()

                    # Building Type
                    bldg_field = frame.locator('[aria-label*="Building Type"]').first
                    if bldg_field.count() > 0:
                        fields["building_type"] = bldg_field.input_value() or bldg_field.text_content()

                    # Name of Issuer (from list view data)
                    issuer_field = frame.locator('[aria-label*="Name of Issuer"], [title*="Name of Issuer"]').first
                    if issuer_field.count() > 0:
                        fields["name_of_issuer"] = issuer_field.text_content()

                    if fields:
                        break

                except Exception as e:
                    continue

        except Exception as e:
            print(f"  Error extracting main fields: {e}")

        return fields

    def extract_assigned_to(self) -> List[Dict]:
        """Extract assignee list from detail panel."""
        assignees = []
        try:
            page = self.session.page
            detail_frame = page.frame_locator('iframe[name="fraDef"]')

            # Look for assigned to section
            assigned_section = detail_frame.locator('[aria-label*="Assigned to"], :has-text("Assigned to")')

            # Extract individual assignees
            assignee_items = detail_frame.locator('.assignee-item, [class*="assignment"]')
            for i in range(assignee_items.count()):
                try:
                    item = assignee_items.nth(i)
                    name = item.locator('.name, [class*="name"]').text_content()
                    due = item.locator('.due, [class*="due"]').text_content()
                    assignees.append({"name": name.strip(), "due_on": due.strip() if due else ""})
                except:
                    pass

        except Exception as e:
            print(f"  Error extracting assignees: {e}")

        return assignees

    def extract_additional_info(self) -> Dict:
        """Extract Additional Info tab fields."""
        info = {}
        try:
            page = self.session.page
            detail_frame = page.frame_locator('iframe[name="fraDef"]')

            # Click Additional Info tab
            tab = detail_frame.locator('text="Additional Info"').first
            if tab.count() > 0:
                tab.click()
                time.sleep(1)

            # Extract all text inputs in the Additional Info section
            field_mappings = [
                ("unique_identifier", "Unique Identifier"),
                ("issued_to_company", "Issued To Company"),
                ("work_step", "Work Step"),
                ("grade", "Grade"),
                ("work_description", "Work Description"),
                ("contractor", "Contractor"),
                ("project_manager", "Project Manager"),
                ("disposition", "Disposition"),
                ("required_action_item", "Required Action Item"),
                ("quality_standard", "Quality Standard"),
                ("root_cause", "RootCause"),
                ("car_no", "CAR No"),
                ("cost_of_loss_direct", "Cost of Loss (Direct"),
                ("cost_of_loss_indirect", "Cost of Loss (Indirect"),
                ("cause_type", "Cause Type"),
                ("date_resolved", "Date resolved"),
            ]

            for key, label in field_mappings:
                try:
                    field = detail_frame.locator(f'input[placeholder*="{label}"], [aria-label*="{label}"]').first
                    if field.count() > 0:
                        info[key] = field.input_value() or ""
                except:
                    info[key] = ""

            # CAR Required (checkbox)
            try:
                car_checkbox = detail_frame.locator('[aria-label*="CAR Required"], :has-text("CAR Required") input[type="checkbox"]')
                if car_checkbox.count() > 0:
                    info["car_required"] = car_checkbox.is_checked()
                else:
                    info["car_required"] = False
            except:
                info["car_required"] = False

        except Exception as e:
            print(f"  Error extracting additional info: {e}")

        return info

    def extract_comments(self) -> tuple:
        """Extract Comments tab content including Cause of Issue and comment thread."""
        cause_of_issue = {}
        comments = []
        attachments = []

        try:
            page = self.session.page
            detail_frame = page.frame_locator('iframe[name="fraDef"]')

            # Click Comments tab
            tab = detail_frame.locator('text="Comments"').first
            if tab.count() > 0:
                tab.click()
                time.sleep(1)

            # Extract Cause of Issue
            cause_section = detail_frame.locator(':has-text("Cause of Issue")').first
            if cause_section.count() > 0:
                # Try to get author, date, and content
                try:
                    author_elem = cause_section.locator('.author, [class*="author"]').first
                    cause_of_issue["author"] = author_elem.text_content().strip() if author_elem.count() > 0 else ""

                    date_elem = cause_section.locator('.date, [class*="date"]').first
                    cause_of_issue["date"] = date_elem.text_content().strip() if date_elem.count() > 0 else ""

                    content_elem = cause_section.locator('.content, [class*="content"]').first
                    cause_of_issue["content"] = content_elem.text_content().strip() if content_elem.count() > 0 else ""
                except:
                    pass

            # Extract comment thread
            comment_items = detail_frame.locator('[class*="comment"], [class*="forum"]')
            for i in range(comment_items.count()):
                try:
                    item = comment_items.nth(i)
                    comment = {
                        "author": "",
                        "company": "",
                        "date": "",
                        "content": "",
                        "linked_items": []
                    }

                    # Extract author
                    author = item.locator('.author, [class*="author"]').first
                    if author.count() > 0:
                        comment["author"] = author.text_content().strip()

                    # Extract date
                    date = item.locator('.date, [class*="date"]').first
                    if date.count() > 0:
                        comment["date"] = date.text_content().strip()

                    # Extract content
                    content = item.locator('.content, [class*="content"]').first
                    if content.count() > 0:
                        comment["content"] = content.text_content().strip()

                    if comment["content"]:
                        comments.append(comment)

                except Exception as e:
                    continue

            # Extract attachment references
            attachment_items = detail_frame.locator('[class*="attachment"], [class*="file"]')
            for i in range(attachment_items.count()):
                try:
                    item = attachment_items.nth(i)
                    filename = item.text_content().strip()
                    if filename and not filename.startswith(('RFI', 'forum')):
                        attachments.append({
                            "filename": filename,
                            "type": "File",
                            "local_path": "",
                            "downloaded_at": ""
                        })
                except:
                    pass

        except Exception as e:
            print(f"  Error extracting comments: {e}")

        return cause_of_issue, comments, attachments

    def download_attachments(self, record_id: str, attachments: List[Dict]) -> List[Dict]:
        """Download attachments for a record."""
        if not attachments:
            return attachments

        record_attachments_dir = self.attachments_dir / record_id
        record_attachments_dir.mkdir(parents=True, exist_ok=True)

        try:
            page = self.session.page
            detail_frame = page.frame_locator('iframe[name="fraDef"]')

            for i, attachment in enumerate(attachments):
                try:
                    # Find and click the attachment link
                    filename = attachment["filename"]
                    attachment_link = detail_frame.locator(f'a:has-text("{filename}"), [title*="{filename}"]').first

                    if attachment_link.count() > 0:
                        # Set up download handler
                        with page.expect_download() as download_info:
                            attachment_link.click()

                        download = download_info.value
                        save_path = record_attachments_dir / download.suggested_filename

                        download.save_as(str(save_path))

                        attachment["local_path"] = str(save_path.relative_to(self.output_dir))
                        attachment["downloaded_at"] = datetime.now().isoformat()
                        print(f"    Downloaded: {save_path.name}")

                except Exception as e:
                    print(f"    Error downloading {attachment.get('filename', 'unknown')}: {e}")

        except Exception as e:
            print(f"  Error in attachment download: {e}")

        return attachments

    def close_detail_view(self):
        """Close the detail view to go back to the list."""
        try:
            page = self.session.page

            # Try to find close button in fraDef iframe
            detail_frame = page.frame_locator('iframe[name="fraDef"]')
            close_btn = detail_frame.locator('[title="Close"], [aria-label*="Close"], button:has-text("Done")').first

            if close_btn.count() > 0:
                close_btn.click()
                time.sleep(1)
                return

            # Fallback - press Escape
            page.keyboard.press('Escape')
            time.sleep(1)

        except Exception as e:
            print(f"  Error closing detail view: {e}")

    def extract_record(self, record: Dict) -> Optional[Dict]:
        """Extract full details for a single record."""
        record_id = record["id"]
        print(f"  Extracting {record_id}...")

        try:
            # Click on the record
            if not self.click_record(record):
                return None

            # Expand to full view
            self.expand_detail_view()
            time.sleep(2)

            # Extract all data
            main_fields = self.extract_main_fields()
            assigned_to = self.extract_assigned_to()
            additional_info = self.extract_additional_info()
            cause_of_issue, comments, attachments = self.extract_comments()

            # Download attachments
            attachments = self.download_attachments(record_id, attachments)

            # Build complete record
            full_record = {
                "id": record_id,
                "number": record.get("number") or main_fields.get("number", ""),
                "type": record.get("type") or main_fields.get("type", ""),
                "status": main_fields.get("status", record.get("status", "")),
                "description": main_fields.get("description", record.get("description", "")),
                "created_on": main_fields.get("created_on", ""),
                "required_correction_date": main_fields.get("required_correction_date", ""),
                "discipline": main_fields.get("discipline", ""),
                "building_type": main_fields.get("building_type", ""),
                "name_of_issuer": main_fields.get("name_of_issuer", ""),
                "assigned_to": assigned_to,
                "additional_info": additional_info,
                "cause_of_issue": cause_of_issue,
                "comments": comments,
                "attachments": attachments,
                "metadata": {
                    "scraped_at": datetime.now().isoformat(),
                    "source_url": self.session.project.ncr_list_url
                }
            }

            # Close detail view
            self.close_detail_view()

            return full_record

        except Exception as e:
            print(f"  Error extracting {record_id}: {e}")
            self.errors.append({"record_id": record_id, "error": str(e)})
            return None

    def save_record(self, record: Dict):
        """Save a record to individual JSON file."""
        record_id = record["id"]
        record_file = self.records_dir / f"{record_id}.json"

        with open(record_file, 'w') as f:
            json.dump(record, f, indent=2)

        # Update manifest
        self.manifest["processed_records"][record_id] = {
            "processed_at": datetime.now().isoformat(),
            "created_on": record.get("created_on", ""),
            "required_correction_date": record.get("required_correction_date", "")
        }

        print(f"  Saved: {record_file.name}")

    def run(self):
        """Run the full scraping process."""
        try:
            self.start()

            if not self.login():
                print("Login failed!")
                return

            print("\nGetting record list...")

            page = self.session.page

            # Navigate to NCR list URL to ensure we're on the right page
            print(f"  Navigating to NCR list...")
            page.goto(self.session.project.ncr_list_url, wait_until='networkidle', timeout=60000)
            time.sleep(5)

            # Wait for the fraMenuContent iframe to load
            frame = page.frame_locator('iframe[name="fraMenuContent"]')
            print("  Waiting for list iframe to load...")
            try:
                frame.locator('.dx-datagrid').wait_for(timeout=30000)
            except:
                print("  Warning: Grid may not have loaded completely")
            time.sleep(3)

            # Expand all type groups first
            expand_buttons = frame.locator('[title="Expand Grouped Row"]')
            count = expand_buttons.count()
            if count > 0:
                print(f"  Found {count} collapsed groups, expanding...")
                for i in range(count):
                    try:
                        expand_buttons.nth(i).click()
                        time.sleep(0.5)
                    except:
                        pass
                time.sleep(2)

            # Process records by clicking each row directly
            # The grid is virtualized - work with visible rows and scroll as needed
            processed_numbers = set()  # Track by number to avoid duplicates

            while True:
                if self.limit > 0 and self.records_processed >= self.limit:
                    print(f"\nReached limit of {self.limit} records")
                    break

                # Get currently visible data rows (try multiple selectors)
                rows = frame.locator('tr[role="row"]:has(td[role="gridcell"]:nth-child(3))')
                row_count = rows.count()
                print(f"  Found {row_count} visible rows")

                if row_count == 0:
                    print("No more rows visible")
                    break

                # Find first unprocessed row
                found_unprocessed = False
                for i in range(row_count):
                    if self.limit > 0 and self.records_processed >= self.limit:
                        break

                    try:
                        row = rows.nth(i)
                        cells = row.locator('td[role="gridcell"]')
                        cell_count = cells.count()

                        if cell_count < 4:
                            continue

                        # Get number from cell (try different indices)
                        number = cells.nth(1).text_content().strip()

                        # Skip headers and empty rows
                        if not number or number in ['NCR', 'QOR', 'SOR', 'SWN', 'Variance Request']:
                            continue

                        # Skip if already processed this number
                        if number in processed_numbers:
                            continue

                        # Get other fields
                        description = cells.nth(2).text_content().strip()
                        status = cells.nth(3).text_content().strip()

                        # Determine type
                        record_type = "UNKNOWN"
                        if "-NCR-" in description or number.startswith('NCR'):
                            record_type = "NCR"
                        elif "-QOR-" in description or number.startswith('QOR'):
                            record_type = "QOR"
                        elif "-SOR-" in description or number.startswith('SOR'):
                            record_type = "SOR"
                        elif "-SWN-" in description or number.startswith('SWN'):
                            record_type = "SWN"
                        elif "Variance" in description or "VR" in number:
                            record_type = "VR"

                        record_id = f"{record_type}-{number}"

                        # Check if should skip
                        if self.skip_existing and self._is_record_processed(record_id):
                            print(f"  Skipping {record_id} (already processed)")
                            processed_numbers.add(number)
                            self.records_skipped += 1
                            continue

                        print(f"  Extracting {record_id}...")
                        found_unprocessed = True

                        # Double-click to open detail view
                        row.dblclick()
                        time.sleep(3)

                        # Wait for detail iframe to load
                        try:
                            detail_frame = page.frame_locator('iframe[name="fraDef"]')
                            detail_frame.locator('body').wait_for(timeout=10000)
                            time.sleep(2)

                            # Extract fields from detail view
                            main_fields = self.extract_main_fields()
                            assigned_to = self.extract_assigned_to()
                            additional_info = self.extract_additional_info()
                            cause_of_issue, comments, attachments = self.extract_comments()

                            # Download attachments
                            attachments = self.download_attachments(record_id, attachments)

                            # Build complete record
                            full_record = {
                                "id": record_id,
                                "number": number,
                                "type": record_type,
                                "status": main_fields.get("status", status),
                                "description": main_fields.get("description", description),
                                "created_on": main_fields.get("created_on", ""),
                                "required_correction_date": main_fields.get("required_correction_date", ""),
                                "discipline": main_fields.get("discipline", ""),
                                "building_type": main_fields.get("building_type", ""),
                                "name_of_issuer": main_fields.get("name_of_issuer", ""),
                                "assigned_to": assigned_to,
                                "additional_info": additional_info,
                                "cause_of_issue": cause_of_issue,
                                "comments": comments,
                                "attachments": attachments,
                                "metadata": {
                                    "scraped_at": datetime.now().isoformat(),
                                    "source_url": self.session.project.ncr_list_url
                                }
                            }

                            # Save record
                            self.save_record(full_record)
                            self.records_processed += 1
                            processed_numbers.add(number)

                            # Close detail view
                            self.close_detail_view()
                            time.sleep(2)

                        except Exception as e:
                            print(f"    Error extracting: {e}")
                            processed_numbers.add(number)  # Mark as seen to avoid retry loop
                            # Try to close any open detail view
                            try:
                                page.keyboard.press('Escape')
                                time.sleep(1)
                            except:
                                pass

                        break  # Process one record at a time, then re-scan visible rows

                    except Exception as e:
                        print(f"  Error processing row {i}: {e}")
                        continue

                # If no unprocessed rows found, try scrolling
                if not found_unprocessed:
                    print("  Scrolling to load more records...")
                    try:
                        grid = frame.locator('.dx-datagrid-rowsview')
                        # Scroll down
                        grid.evaluate('el => el.scrollTop = el.scrollTop + 500')
                        time.sleep(1)

                        # Check if scroll had any effect (more rows loaded)
                        new_rows = frame.locator('tr[role="row"]:has(td[role="gridcell"]:nth-child(3))')
                        if new_rows.count() == row_count:
                            # Try one more big scroll
                            grid.evaluate('el => el.scrollTop = el.scrollHeight')
                            time.sleep(2)
                            final_rows = frame.locator('tr[role="row"]:has(td[role="gridcell"]:nth-child(3))')
                            if final_rows.count() == row_count:
                                print("  No more records to load")
                                break
                    except Exception as e:
                        print(f"  Scroll error: {e}")
                        break

                # Progress update
                if self.records_processed > 0 and self.records_processed % 10 == 0:
                    print(f"\nProgress: {self.records_processed} records processed")
                    self._save_manifest()

        except KeyboardInterrupt:
            print("\n\nInterrupted by user")
        except Exception as e:
            print(f"\nError: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.stop()


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Scrape NCR/QOR/SOR/SWN/VR records from ProjectSight'
    )
    parser.add_argument('--headless', action='store_true',
                        help='Run in headless mode')
    parser.add_argument('--skip-existing', action='store_true',
                        help='Skip already processed records')
    parser.add_argument('--force', action='store_true',
                        help='Force re-process existing records')
    parser.add_argument('--limit', type=int, default=0,
                        help='Limit number of records to process (0 = unlimited)')
    parser.add_argument('--start-date', type=str,
                        help='Start date for filtering (YYYY-MM-DD or MM/DD/YYYY)')
    parser.add_argument('--end-date', type=str,
                        help='End date for filtering (YYYY-MM-DD or MM/DD/YYYY)')
    parser.add_argument('--output-dir', type=str,
                        help='Custom output directory')
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else None

    scraper = NCRScraper(
        headless=args.headless,
        output_dir=output_dir,
        skip_existing=args.skip_existing,
        force=args.force,
        limit=args.limit,
        start_date=args.start_date,
        end_date=args.end_date
    )

    scraper.run()


if __name__ == '__main__':
    main()
