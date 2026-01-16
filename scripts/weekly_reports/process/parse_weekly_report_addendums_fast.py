#!/usr/bin/env python3
"""
Fast parser for Weekly Report PDF addendums using PyMuPDF (fitz).

Extracts:
- RFI Log
- Submittal Log
- Manpower/Labor Report

Uses fitz for fast text extraction, then regex parsing.

NOTE: This script updates the existing weekly_reports.csv (created by
parse_weekly_reports.py) with addendum counts (rfi_count, submittal_count,
manpower_count). Run parse_weekly_reports.py first to create the base file.
"""

import re
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from schemas.validator import validated_df_to_csv

try:
    import fitz  # PyMuPDF
except ImportError:
    print("ERROR: PyMuPDF not installed. Run: pip install pymupdf")
    exit(1)


def extract_text_fast(pdf_path: Path) -> str:
    """Extract all text from PDF using PyMuPDF."""
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text()
    doc.close()
    return text


def parse_rfi_entries(text: str, file_id: int) -> list:
    """Parse RFI entries from text."""
    entries = []

    # Pattern: [Y_RFI#NNNN] followed by subject, then dates at end
    # Format: [Y_RFI#0003] Subject text ... Author Company Importance Date Date
    pattern = r'\[Y_RFI#(\d+)\]\s*([^\[]+?)(\d{1,2}/\d{1,2}/\d{4})\s*(\d{1,2}/\d{1,2}/\d{4})?'
    matches = re.findall(pattern, text, re.DOTALL)

    for rfi_num, content, created, due in matches:
        # Clean content
        content = re.sub(r'\s+', ' ', content).strip()[:300]

        entries.append({
            'file_id': file_id,
            'source_section': 'rfi_log',
            'rfi_number': rfi_num,
            'subject': content,
            'created_date': created if created else None,
            'due_date': due if due else None,
        })

    return entries


def parse_submittal_entries(text: str, file_id: int) -> list:
    """Parse Submittal entries from text."""
    entries = []

    # Find SUBMITTAL LOG section
    sub_match = re.search(r'SUBMITTAL LOG(.+?)(?:CHANGE ORDER|THANK YOU|$)', text, re.DOTALL)
    if not sub_match:
        return entries

    sub_text = sub_match.group(1)

    # Pattern: [Y_SBMT#N] followed by subject
    pattern = r'\[Y_SBMT#(\d+)\]\s*(.+?)(?=\[Y_SBMT#|\Z)'
    matches = re.findall(pattern, sub_text, re.DOTALL)

    for sub_num, content in matches:
        content = re.sub(r'\s+', ' ', content).strip()[:300]
        dates = re.findall(r'(\d{1,2}/\d{1,2}/\d{4})', content)

        entries.append({
            'file_id': file_id,
            'source_section': 'submittal_log',
            'submittal_number': sub_num,
            'content': content,
            'created_date': dates[0] if dates else None,
            'due_date': dates[1] if len(dates) > 1 else None,
        })

    return entries


def parse_manpower_entries(text: str, file_id: int) -> list:
    """Parse manpower/labor summary entries from ProjectSight daily reports.

    Strategy: Find each "Total Workers: N" line, look backwards for hours and company name.
    Format in PDF text:
        COMPANY NAME (Tier 1/Subcontractor)
        ... worker entries ...
        HHH.HH
        Total Workers: N
    """
    entries = []

    # Track current report date as we scan
    current_date = None

    # Find all "Total Workers:" occurrences with their positions
    for match in re.finditer(r'([\d.]+)\s*\n\s*Total Workers:\s*(\d+)', text):
        hours = float(match.group(1))
        workers = int(match.group(2))

        if workers == 0 and hours == 0:
            continue

        # Look backwards from this match to find the company name
        # Search in the 2500 chars before this match
        start = max(0, match.start() - 2500)
        context = text[start:match.start()]

        # Find most recent date in context
        date_matches = list(re.finditer(r'Daily Report for (\d{1,2}/\d{1,2}/\d{4})', context))
        if date_matches:
            current_date = date_matches[-1].group(1)

        # Find last company name pattern in context
        # Common patterns: "NAME INC", "NAME LLC", "NAME CO", etc.
        company_pattern = r'([A-Z][A-Z\s&.,\'\-]{2,}(?:INC\.?|LLC\.?|CO\.?|COMPANY|CORP\.?|CONSTRUCTION|CONTRACTORS|ELECTRIC|CONCRETE|STEEL|SERVICES|MECHANICAL|GLAZING|PAINTING|PLUMBING|WELDING|ERECTORS|FABRICATORS))'
        company_matches = list(re.finditer(company_pattern, context))

        company = None
        if company_matches:
            company = company_matches[-1].group(1).strip()
            company = re.sub(r'\s+', ' ', company)

        if company and current_date:
            entries.append({
                'file_id': file_id,
                'source_section': 'manpower',
                'report_date': current_date,
                'company': company[:60],
                'total_workers': workers,
                'total_hours': hours,
            })

    return entries


def parse_date_from_filename(filename: str) -> str:
    """Extract date from filename."""
    patterns = [
        r'(\d{8})',  # YYYYMMDD
        r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
    ]
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            date_str = match.group(1)
            if len(date_str) == 8:
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            return date_str
    return None


def main():
    """Main processing function."""
    input_dir = Path('data/raw/weekly_reports')
    output_dir = Path('data/weekly_reports/tables')
    output_dir.mkdir(parents=True, exist_ok=True)

    weekly_reports_path = output_dir / 'weekly_reports.csv'

    # Load existing weekly_reports.csv to get file_id mapping
    if weekly_reports_path.exists():
        weekly_df = pd.read_csv(weekly_reports_path)
        filename_to_id = dict(zip(weekly_df['filename'], weekly_df['file_id']))
        print(f"Loaded {len(filename_to_id)} file mappings from weekly_reports.csv")
    else:
        weekly_df = None
        filename_to_id = {}
        print("No existing weekly_reports.csv found - will create file_ids")

    pdf_files = sorted(input_dir.glob('*.pdf'))
    print(f"Found {len(pdf_files)} weekly report PDFs")

    all_rfi = []
    all_submittal = []
    all_manpower = []
    addendum_counts = {}  # file_id -> counts

    import time
    total_start = time.time()

    for i, pdf_path in enumerate(pdf_files, 1):
        start = time.time()

        # Get file_id from existing mapping or create new one
        if pdf_path.name in filename_to_id:
            file_id = filename_to_id[pdf_path.name]
        else:
            file_id = i  # Fallback if not in existing mapping

        print(f"Processing {i}/{len(pdf_files)}: {pdf_path.name[:50]}...", end=' ', flush=True)

        # Extract text with fitz (fast)
        text = extract_text_fast(pdf_path)

        # Parse each section
        rfi = parse_rfi_entries(text, file_id)
        submittal = parse_submittal_entries(text, file_id)
        manpower = parse_manpower_entries(text, file_id)

        all_rfi.extend(rfi)
        all_submittal.extend(submittal)
        all_manpower.extend(manpower)

        # Track counts for updating weekly_reports.csv
        addendum_counts[file_id] = {
            'rfi_count': len(rfi),
            'submittal_count': len(submittal),
            'manpower_count': len(manpower),
        }

        elapsed = time.time() - start
        print(f"{len(rfi)} RFIs, {len(submittal)} submittals, {len(manpower)} labor ({elapsed:.1f}s)")

    total_elapsed = time.time() - total_start
    print(f"\nTotal processing time: {total_elapsed:.1f}s")

    # Save outputs (with schema validation)
    print("\n=== Saving outputs ===")

    # Update weekly_reports.csv with addendum counts
    if weekly_df is not None and addendum_counts:
        # Add/update count columns
        weekly_df['rfi_count'] = weekly_df['file_id'].map(lambda x: addendum_counts.get(x, {}).get('rfi_count', 0))
        weekly_df['submittal_count'] = weekly_df['file_id'].map(lambda x: addendum_counts.get(x, {}).get('submittal_count', 0))
        weekly_df['manpower_count'] = weekly_df['file_id'].map(lambda x: addendum_counts.get(x, {}).get('manpower_count', 0))
        validated_df_to_csv(weekly_df, weekly_reports_path, index=False)
        print(f"Updated weekly_reports.csv with addendum counts ({len(weekly_df)} files, validated)")
    else:
        print("Warning: weekly_reports.csv not found - run parse_weekly_reports.py first")

    if all_rfi:
        df = pd.DataFrame(all_rfi)
        validated_df_to_csv(df, output_dir / 'addendum_rfi_log.csv', index=False)
        print(f"addendum_rfi_log.csv: {len(df)} entries (validated)")

    if all_submittal:
        df = pd.DataFrame(all_submittal)
        validated_df_to_csv(df, output_dir / 'addendum_submittal_log.csv', index=False)
        print(f"addendum_submittal_log.csv: {len(df)} entries (validated)")

    if all_manpower:
        df = pd.DataFrame(all_manpower)
        validated_df_to_csv(df, output_dir / 'addendum_manpower.csv', index=False)
        print(f"addendum_manpower.csv: {len(df)} entries (validated)")

    print("\nDone!")


if __name__ == '__main__':
    main()
