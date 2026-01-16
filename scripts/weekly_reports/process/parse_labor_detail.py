#!/usr/bin/env python3
"""
Parse labor detail from Weekly Report PDF addendums using bounding boxes.

Uses fitz (PyMuPDF) to extract text with position info, then groups by row
to reconstruct table structure.
"""

import re
import sys
from pathlib import Path
from collections import defaultdict
import pandas as pd

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from schemas.validator import validated_df_to_csv

try:
    import fitz
except ImportError:
    print("ERROR: PyMuPDF not installed. Run: pip install pymupdf")
    exit(1)


# Classification keywords
CLASSIFICATIONS = [
    'Superintendent', 'Assistant Superintendent', 'General Foreman', 'Foreman',
    'Project Manager', 'Assistant PM', 'Project Executive',
    'Engineer', 'Intern', 'Labor', 'Operator', 'Rodbuster',
    'Safety Manager', 'Safety Personnel', 'QC Manager', 'Quality Control',
    'Journeyman', 'Installer', 'Worker'
]


def extract_rows_from_page(page) -> list:
    """Extract text grouped by row (Y coordinate) from a PDF page."""
    blocks = page.get_text("dict")["blocks"]

    # Collect all text spans with positions
    spans = []
    for block in blocks:
        if "lines" in block:
            for line in block["lines"]:
                for span in line["spans"]:
                    y = round(span["bbox"][1], 0)
                    x = round(span["bbox"][0], 0)
                    text = span["text"].strip()
                    if text:
                        spans.append((y, x, text))

    # Sort by Y, then X
    spans.sort(key=lambda s: (s[0], s[1]))

    # Group by Y coordinate (same row) - allow 3px tolerance
    rows = defaultdict(list)
    for y, x, text in spans:
        # Find existing row within tolerance
        matched_y = None
        for existing_y in rows.keys():
            if abs(existing_y - y) <= 3:
                matched_y = existing_y
                break
        if matched_y is not None:
            rows[matched_y].append((x, text))
        else:
            rows[y].append((x, text))

    # Convert to sorted list of row texts
    result = []
    for y in sorted(rows.keys()):
        row_items = sorted(rows[y], key=lambda r: r[0])
        row_text = [t for x, t in row_items]
        result.append((y, row_text))

    return result


def parse_labor_page(page, file_id: int, report_date: str) -> list:
    """Parse labor entries from a single page."""
    entries = []
    rows = extract_rows_from_page(page)

    current_company = None
    current_status = None
    in_labor_section = False

    for y, row_items in rows:
        row_text = ' '.join(row_items)

        # Detect company header: "COMPANY NAME (details)" followed by "Status:"
        if '(' in row_text and 'Status:' in row_text:
            # Extract company name (before parenthesis)
            company_match = re.match(r'^([A-Z][A-Z\s&.,\'\-]+)\s*\(', row_text)
            if company_match:
                current_company = company_match.group(1).strip()
            # Extract status
            status_match = re.search(r'Status:\s*(\w+)', row_text)
            if status_match:
                current_status = status_match.group(1)
            in_labor_section = True
            continue

        # Detect header row
        if 'Name' in row_items and 'Classification' in row_items and 'Trade' in row_items:
            in_labor_section = True
            continue

        # Detect total row - end of company section
        if 'Total Workers:' in row_text:
            in_labor_section = False
            continue

        # Skip non-labor sections
        if not in_labor_section or not current_company:
            continue

        # Try to parse worker row
        # Pattern: Name | Classification | Trade | ... | Hours (last number)
        # Find classification in row
        classification = None
        class_idx = None
        for i, item in enumerate(row_items):
            if item in CLASSIFICATIONS:
                classification = item
                class_idx = i
                break

        if classification and class_idx is not None and class_idx > 0:
            # Name is everything before classification
            name = ' '.join(row_items[:class_idx])

            # Hours is the last decimal number
            hours = None
            for item in reversed(row_items):
                try:
                    hours = float(item)
                    break
                except ValueError:
                    continue

            # Trade is between classification and hours
            trade_parts = []
            for i in range(class_idx + 1, len(row_items)):
                item = row_items[i]
                # Stop at numbers (Start, End, Break, Hours)
                if re.match(r'^\d+\.?\d*$', item) or ':' in item:
                    break
                trade_parts.append(item)
            trade = ' '.join(trade_parts) if trade_parts else None

            if name and hours is not None:
                entries.append({
                    'file_id': file_id,
                    'source_section': 'labor_detail',
                    'report_date': report_date,
                    'company': current_company,
                    'status': current_status,
                    'name': name[:60],
                    'classification': classification,
                    'trade': trade[:50] if trade else None,
                    'hours': hours,
                })

    return entries


def find_labor_pages(doc) -> list:
    """Find page numbers containing labor detail."""
    labor_pages = []
    for i, page in enumerate(doc):
        text = page.get_text()[:2500]  # Search more text for header detection
        # Check for explicit DETAILED LABOR marker
        if 'DETAILED LABOR' in text:
            labor_pages.append(i)
        # Check for labor table structure (header row + Status pattern)
        elif ('Classification' in text and 'Trade' in text and 'Hours' in text):
            labor_pages.append(i)
        # Check for company status pattern that appears in labor sections
        elif 'Status: Pending' in text and ('Name' in text or 'Total Workers' in text):
            labor_pages.append(i)
    return labor_pages


def extract_report_date(doc, filename: str) -> str:
    """Extract report date from document or filename."""
    # Try to find in PDF content first
    for page in doc[:5]:
        text = page.get_text()
        match = re.search(r'Daily Report for (\d{1,2}/\d{1,2}/\d{4})', text)
        if match:
            return match.group(1)

    # Fall back to filename patterns (order matters - try most specific first)
    # MM.DD.YYYY format (e.g., 12.26.2022)
    m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', filename)
    if m:
        return f"{m.group(3)}-{m.group(1)}-{m.group(2)}"

    # YYYYMMDD format (e.g., 20230306)
    m = re.search(r'(\d{8})', filename)
    if m:
        d = m.group(1)
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}"

    # MMDD format at end before .pdf (e.g., "Week of 0123.pdf" -> 2023-01-23)
    m = re.search(r'(\d{4})\.pdf$', filename)
    if m:
        d = m.group(1)
        return f"2023-{d[:2]}-{d[2:4]}"

    return None


def parse_pdf(pdf_path: Path, file_id: int) -> list:
    """Parse all labor entries from a PDF."""
    entries = []

    try:
        doc = fitz.open(pdf_path)

        # Find labor pages
        labor_pages = find_labor_pages(doc)

        if not labor_pages:
            doc.close()
            return entries

        # Get report date from early pages or filename
        report_date = extract_report_date(doc, pdf_path.name)

        # Parse each labor page
        for page_idx in labor_pages:
            page = doc[page_idx]
            page_entries = parse_labor_page(page, file_id, report_date)
            entries.extend(page_entries)

        doc.close()

    except Exception as e:
        print(f"Error parsing {pdf_path.name}: {e}")

    return entries


def main():
    """Main processing function."""
    input_dir = Path('data/raw/weekly_reports')
    output_dir = Path('data/weekly_reports/tables')
    output_dir.mkdir(parents=True, exist_ok=True)

    pdf_files = sorted(input_dir.glob('*.pdf'))
    print(f"Found {len(pdf_files)} weekly report PDFs")

    all_entries = []

    import time
    total_start = time.time()

    for file_id, pdf_path in enumerate(pdf_files, 1):
        start = time.time()
        print(f"Processing {file_id}/{len(pdf_files)}: {pdf_path.name[:50]}...", end=' ', flush=True)

        entries = parse_pdf(pdf_path, file_id)
        all_entries.extend(entries)

        elapsed = time.time() - start
        print(f"{len(entries)} workers ({elapsed:.1f}s)")

    total_elapsed = time.time() - total_start
    print(f"\nTotal processing time: {total_elapsed:.1f}s")

    # Save output (with schema validation)
    print("\n=== Saving outputs ===")

    if all_entries:
        df = pd.DataFrame(all_entries)
        validated_df_to_csv(df, output_dir / 'labor_detail.csv', index=False)
        print(f"labor_detail.csv: {len(df)} entries (validated)")

        # Summary by company
        summary = df.groupby('company').agg({
            'hours': 'sum',
            'name': 'nunique'
        }).rename(columns={'name': 'unique_workers'}).sort_values('hours', ascending=False)
        validated_df_to_csv(summary, output_dir / 'labor_detail_by_company.csv')
        print(f"labor_detail_by_company.csv: {len(summary)} companies (validated)")

        # Summary by classification
        class_summary = df.groupby('classification').agg({
            'hours': 'sum',
            'name': 'nunique'
        }).rename(columns={'name': 'unique_workers'}).sort_values('hours', ascending=False)
        validated_df_to_csv(class_summary, output_dir / 'labor_detail_by_classification.csv')
        print(f"labor_detail_by_classification.csv: {len(class_summary)} classifications (validated)")

    print("\nDone!")


if __name__ == '__main__':
    main()
