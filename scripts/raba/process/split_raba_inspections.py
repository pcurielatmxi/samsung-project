#!/usr/bin/env python3
"""
Split RABA daily batch PDFs into individual inspection files.

RABA daily batch PDFs contain multiple reports, each with multiple test sets.
This script splits them into individual PDFs for cleaner processing, where each
output file contains a single inspection set.

Output naming convention:
  {date}_{assignment_number}_set{XX}.pdf
  Example: 2022-06-08_A22-017046_set01.pdf

The assignment number is extracted from the RABA document (ASSIGNMENT NO field).
The set number identifies which test set within that assignment.

Usage:
    python split_raba_inspections.py \\
        --input-dir /path/to/raba/daily \\
        --output-dir /path/to/raba/split \\
        --skip-existing
"""

import argparse
import re
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional, Tuple
import json

try:
    import fitz  # PyMuPDF
except ImportError:
    print("PyMuPDF is required. Install with: pip install pymupdf")
    exit(1)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class InspectionPage:
    """Represents a single page belonging to an inspection."""
    page_num: int  # 0-indexed
    sheet_num: int
    sheet_total: int
    set_num: Optional[int]
    set_total: Optional[int]
    assignment_number: Optional[str]
    sample_location: Optional[str]
    test_type: Optional[str]


@dataclass
class InspectionGroup:
    """A group of pages belonging to the same inspection/set."""
    pages: List[int]  # 0-indexed page numbers
    assignment_number: str
    set_num: int
    set_total: int
    sample_location: str
    test_type: str
    source_file: str


def extract_page_metadata(page: fitz.Page) -> InspectionPage:
    """Extract metadata from a single PDF page."""
    text = page.get_text()

    # Extract sheet number
    sheet_match = re.search(r'SHEET NUMBER:\s*(\d+)\s+of\s+(\d+)', text)
    sheet_num = int(sheet_match.group(1)) if sheet_match else 0
    sheet_total = int(sheet_match.group(2)) if sheet_match else 0

    # Extract set index
    set_match = re.search(r'SET INDEX:\s*Set\s+(\d+)\s+of\s+(\d+)', text, re.IGNORECASE)
    if not set_match:
        set_match = re.search(r'Set\s+(\d+)\s+of\s+(\d+)', text)
    set_num = int(set_match.group(1)) if set_match else None
    set_total = int(set_match.group(2)) if set_match else None

    # Extract assignment number - look for A##-###### pattern anywhere on page
    # The format is typically A22-016871 (letter, 2 digits, dash, 6 digits)
    assignment_match = re.search(r'\b([A-Z]\d{2}-\d{6})\b', text)
    assignment_number = assignment_match.group(1) if assignment_match else None

    # Extract sample location
    location_match = re.search(r'SAMPLE LOCATION:\s*([^\n]+)', text)
    sample_location = location_match.group(1).strip() if location_match else None

    # Extract test type (from title)
    test_type = None
    if 'Compressive Strength' in text:
        test_type = 'Compressive_Strength'
    elif 'Soil' in text or 'Compaction' in text:
        test_type = 'Soil_Compaction'
    elif 'Fireproof' in text:
        test_type = 'Fireproofing'
    elif 'Weld' in text:
        test_type = 'Welding'
    elif 'Rebar' in text or 'Reinforc' in text:
        test_type = 'Rebar'
    else:
        test_type = 'Unknown'

    return InspectionPage(
        page_num=page.number,
        sheet_num=sheet_num,
        sheet_total=sheet_total,
        set_num=set_num,
        set_total=set_total,
        assignment_number=assignment_number,
        sample_location=sample_location,
        test_type=test_type
    )


def identify_report_boundaries(pages: List[InspectionPage]) -> List[Tuple[int, int]]:
    """
    Identify where different reports start/end within the PDF.

    Reports are separated by:
    - Sheet number resetting to 1
    - Different assignment numbers

    Returns list of (start_page, end_page) tuples (inclusive, 0-indexed).
    """
    if not pages:
        return []

    boundaries = []
    report_start = 0

    for i in range(1, len(pages)):
        # Check if this is a new report
        is_new_report = False

        # Sheet number reset to 1 (and previous wasn't also 1)
        if pages[i].sheet_num == 1 and pages[i-1].sheet_num > 1:
            is_new_report = True

        # Different assignment number
        if (pages[i].assignment_number and pages[i-1].assignment_number and
            pages[i].assignment_number != pages[i-1].assignment_number):
            is_new_report = True

        if is_new_report:
            boundaries.append((report_start, i - 1))
            report_start = i

    # Add final report
    boundaries.append((report_start, len(pages) - 1))

    return boundaries


def group_pages_by_set(pages: List[InspectionPage], start: int, end: int) -> List[InspectionGroup]:
    """
    Group pages within a report by their set number.

    Most sets are single-page, but some reports may have multi-page sets.
    """
    groups = []

    # Get pages in this report range
    report_pages = pages[start:end + 1]

    # Get assignment number from first page that has one
    assignment = None
    for p in report_pages:
        if p.assignment_number:
            assignment = p.assignment_number
            break
    assignment = assignment or "UNKNOWN"

    # Group by set number
    sets_seen = {}
    for p in report_pages:
        if p.set_num is not None:
            key = p.set_num
            if key not in sets_seen:
                sets_seen[key] = {
                    'pages': [],
                    'set_total': p.set_total,
                    'location': p.sample_location,
                    'test_type': p.test_type
                }
            sets_seen[key]['pages'].append(p.page_num)
            # Update location if we find a better one
            if p.sample_location and not sets_seen[key]['location']:
                sets_seen[key]['location'] = p.sample_location

    # Convert to InspectionGroup objects
    for set_num, data in sorted(sets_seen.items()):
        groups.append(InspectionGroup(
            pages=sorted(data['pages']),
            assignment_number=assignment,
            set_num=set_num,
            set_total=data['set_total'] or len(sets_seen),
            sample_location=data['location'] or 'Unknown',
            test_type=data['test_type'] or 'Unknown',
            source_file=""  # Will be set later
        ))

    # Handle pages without set numbers (create individual groups with unique numbering)
    # Find the max set_num already used to avoid collisions
    max_set_used = max((g.set_num for g in groups), default=0)
    next_set_num = max_set_used + 1

    for p in report_pages:
        if p.set_num is None:
            # Check if this page is already in a group
            already_grouped = any(p.page_num in g.pages for g in groups)
            if not already_grouped:
                # Use sheet_num if valid, otherwise use sequential counter
                if p.sheet_num and p.sheet_num > 0:
                    set_num_to_use = p.sheet_num + 100  # Offset to avoid collision with real sets
                else:
                    set_num_to_use = next_set_num
                    next_set_num += 1

                groups.append(InspectionGroup(
                    pages=[p.page_num],
                    assignment_number=assignment,
                    set_num=set_num_to_use,
                    set_total=p.sheet_total,
                    sample_location=p.sample_location or 'Unknown',
                    test_type=p.test_type or 'Unknown',
                    source_file=""
                ))

    return groups


def split_pdf(input_path: Path, output_dir: Path, skip_existing: bool = False) -> List[Path]:
    """
    Split a RABA daily batch PDF into individual inspection PDFs.

    Returns list of output file paths created.
    """
    doc = fitz.open(input_path)
    date_str = input_path.stem  # e.g., "2022-06-08"

    # Extract metadata from all pages
    pages = []
    for i in range(doc.page_count):
        page = doc[i]
        metadata = extract_page_metadata(page)
        pages.append(metadata)

    # Identify report boundaries
    boundaries = identify_report_boundaries(pages)

    # Group pages by set within each report
    all_groups = []
    for start, end in boundaries:
        groups = group_pages_by_set(pages, start, end)
        for g in groups:
            g.source_file = str(input_path)
        all_groups.extend(groups)

    # Create output files
    output_files = []
    for group in all_groups:
        # Generate output filename using RABA assignment number: {date}_{assignment}_set{XX}.pdf
        # Example: 2022-06-08_A22-017046_set01.pdf
        assignment_clean = re.sub(r'[^A-Za-z0-9-]', '', group.assignment_number)
        output_name = f"{date_str}_{assignment_clean}_set{group.set_num:02d}.pdf"
        output_path = output_dir / output_name

        # Skip if exists and skip_existing is True
        if skip_existing and output_path.exists():
            logger.debug(f"Skipping existing: {output_name}")
            output_files.append(output_path)
            continue

        # Create new PDF with just these pages
        new_doc = fitz.open()
        for page_num in group.pages:
            new_doc.insert_pdf(doc, from_page=page_num, to_page=page_num)

        # Save
        new_doc.save(output_path)
        new_doc.close()

        logger.info(f"Created: {output_name} ({len(group.pages)} page(s)) - {group.sample_location[:50]}")
        output_files.append(output_path)

    doc.close()

    return output_files


def create_manifest(output_dir: Path, all_files: List[Path], source_files: List[Path]):
    """Create a manifest file documenting the split operation."""
    manifest = {
        "source_files": [str(f) for f in source_files],
        "output_files": [str(f) for f in all_files],
        "total_source_files": len(source_files),
        "total_output_files": len(all_files),
    }

    manifest_path = output_dir / "split_manifest.json"
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)

    logger.info(f"Manifest written to: {manifest_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Split RABA daily batch PDFs into individual inspection files"
    )
    parser.add_argument(
        "-i", "--input-dir",
        required=True,
        help="Input directory containing RABA daily batch PDFs"
    )
    parser.add_argument(
        "-o", "--output-dir",
        required=True,
        help="Output directory for split inspection PDFs"
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip files that already exist in output directory"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of source files to process (for testing)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all PDF files
    pdf_files = sorted(input_dir.glob("*.pdf"))
    if args.limit:
        pdf_files = pdf_files[:args.limit]

    logger.info(f"Found {len(pdf_files)} PDF files to process")
    logger.info(f"Output directory: {output_dir}")
    logger.info("")

    all_output_files = []

    for i, pdf_path in enumerate(pdf_files, 1):
        logger.info(f"[{i}/{len(pdf_files)}] Processing: {pdf_path.name}")
        try:
            output_files = split_pdf(pdf_path, output_dir, args.skip_existing)
            all_output_files.extend(output_files)
        except Exception as e:
            logger.error(f"Error processing {pdf_path.name}: {e}")

    logger.info("")
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Source files processed: {len(pdf_files)}")
    logger.info(f"Output files created: {len(all_output_files)}")

    # Create manifest
    create_manifest(output_dir, all_output_files, pdf_files)

    return 0


if __name__ == "__main__":
    exit(main())
