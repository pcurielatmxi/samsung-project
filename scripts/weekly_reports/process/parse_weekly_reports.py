#!/usr/bin/env python3
"""
Parse Weekly Report PDFs from Taylor Fab1 project.

Extracts narrative sections (Executive Summary, Issues, Procurement) from
the first ~25 pages. Later pages are data dumps from other systems.

Input: data/raw/weekly_reports/*.pdf
Output: data/weekly_reports/tables/*.csv
"""

import pdfplumber
import pandas as pd
import re
import sys
import warnings
from pathlib import Path
from datetime import datetime

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from schemas.validator import validated_df_to_csv

warnings.filterwarnings('ignore')


def extract_date_from_filename(filename: str) -> str:
    """Extract report date from filename."""
    name = Path(filename).stem

    # Pattern: Week of YYYYMMDD
    match = re.search(r'(\d{8})', name)
    if match:
        date_str = match.group(1)
        try:
            dt = datetime.strptime(date_str, '%Y%m%d')
            return dt.strftime('%Y-%m-%d')
        except:
            pass

    # Pattern: Week of MMDD (assume 2023)
    match = re.search(r'Week of (\d{4})(?!\d)', name)
    if match:
        mmdd = match.group(1)
        try:
            dt = datetime.strptime(f'2023{mmdd}', '%Y%m%d')
            return dt.strftime('%Y-%m-%d')
        except:
            pass

    # Pattern: MM.DD.YYYY or MM-DD-YYYY
    match = re.search(r'(\d{1,2})[.-](\d{1,2})[.-](\d{4})', name)
    if match:
        month, day, year = match.groups()
        return f'{year}-{int(month):02d}-{int(day):02d}'

    return None


def extract_date_from_content(text: str) -> str:
    """Extract date from page content."""
    # Pattern: MM/DD/YYYY
    match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', text)
    if match:
        month, day, year = match.groups()
        return f'{year}-{int(month):02d}-{int(day):02d}'

    # Pattern: WEEK OF MM/DD/YYYY
    match = re.search(r'WEEK OF\s+(\d{1,2})/(\d{1,2})/(\d{4})', text, re.IGNORECASE)
    if match:
        month, day, year = match.groups()
        return f'{year}-{int(month):02d}-{int(day):02d}'

    return None


def extract_author(text: str) -> tuple:
    """Extract author name and role from Executive Summary."""
    # Pattern: Written by NAME (ROLE)
    match = re.search(r'Written by\s+([A-Z][A-Z\s]+)\s*\(([^)]+)\)', text)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return None, None


def parse_numbered_items(text: str, section_name: str) -> list:
    """Parse numbered items from a section."""
    items = []

    # Find section start - multiple patterns for different report formats
    section_patterns = [
        rf'{section_name}\s*\n',
        rf'{section_name}:?\s*\n',
        rf'\d+\.\s*{section_name}\s*\n',  # "1. Key Issues"
        rf'{section_name}\s*–',  # "Key Issues –"
    ]

    section_start = None
    for pattern in section_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            section_start = match.end()
            break

    if section_start is None:
        return items

    # Extract text until next major section
    section_text = text[section_start:section_start + 4000]

    # Find numbered items (1. or 1) patterns)
    item_pattern = r'(\d+)[.)]\s*(.+?)(?=\n\d+[.)]|\n[A-Z][a-z]+\s+[A-Z]|\n\n\n|$)'
    matches = re.findall(item_pattern, section_text, re.DOTALL)

    for num, content in matches:
        # Clean up content
        content = re.sub(r'\s+', ' ', content).strip()
        if len(content) > 10:  # Skip very short items
            items.append({
                'number': int(num),
                'content': content[:500]  # Limit length
            })

    return items


def parse_open_issues(text: str) -> list:
    """Parse OPEN ISSUES section (common in later reports)."""
    items = []

    match = re.search(r'OPEN ISSUES\s*\n(.+?)(?=MILESTONES|TAYLOR FAB|Written by|\n\n\n)', text, re.DOTALL | re.IGNORECASE)
    if not match:
        return items

    section_text = match.group(1)

    # Find numbered items or bullet points
    item_pattern = r'(\d+)[.)]\s*(.+?)(?=\n\d+[.)]|\no\s|\n•|\n$|$)'
    matches = re.findall(item_pattern, section_text, re.DOTALL)

    for num, content in matches:
        content = re.sub(r'\s+', ' ', content).strip()
        if len(content) > 10:
            items.append({
                'number': int(num),
                'content': content[:500]
            })

    return items


def extract_narrative_text(pdf_path: Path, max_pages: int = 25) -> str:
    """Extract narrative text from first N pages of PDF."""
    text_parts = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages[:max_pages]):
                page_text = page.extract_text() or ""

                # Skip pages that look like data dumps
                if 'ProjectSight' in page_text and 'Daily Report' in page_text:
                    break
                if 'Activity ID' in page_text and 'Original Duration' in page_text:
                    break  # Primavera printout

                text_parts.append(page_text)
    except Exception as e:
        print(f"  Error reading {pdf_path.name}: {e}")
        return ""

    return '\n\n'.join(text_parts)


def parse_weekly_report(pdf_path: Path) -> dict:
    """Parse a single weekly report PDF."""
    result = {
        'filename': pdf_path.name,
        'report_date': None,
        'author_name': None,
        'author_role': None,
        'narrative_text': None,
        'work_progressing': [],
        'key_issues': [],
        'procurement': [],
        'page_count': 0,
    }

    try:
        with pdfplumber.open(pdf_path) as pdf:
            result['page_count'] = len(pdf.pages)
    except:
        return result

    # Extract narrative from first 25 pages
    narrative = extract_narrative_text(pdf_path, max_pages=25)
    if not narrative:
        return result

    result['narrative_text'] = narrative

    # Extract date
    result['report_date'] = extract_date_from_filename(pdf_path.name)
    if not result['report_date']:
        result['report_date'] = extract_date_from_content(narrative[:500])

    # Extract author
    result['author_name'], result['author_role'] = extract_author(narrative)

    # Parse structured sections - try multiple section name variations
    result['work_progressing'] = parse_numbered_items(narrative, 'Work Progressing')
    if not result['work_progressing']:
        result['work_progressing'] = parse_numbered_items(narrative, 'Last Week Events')

    result['key_issues'] = parse_numbered_items(narrative, 'Key Open Issues')
    if not result['key_issues']:
        result['key_issues'] = parse_numbered_items(narrative, 'Key Issues')
    if not result['key_issues']:
        result['key_issues'] = parse_open_issues(narrative)

    result['procurement'] = parse_numbered_items(narrative, 'Procurement')
    if not result['procurement']:
        result['procurement'] = parse_numbered_items(narrative, 'Buyout')

    return result


def main():
    """Main processing function."""
    input_dir = Path('data/raw/weekly_reports')
    output_dir = Path('data/weekly_reports/tables')
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create .gitkeep
    (output_dir / '.gitkeep').touch()

    pdf_files = sorted(input_dir.glob('*.pdf'))
    print(f"Found {len(pdf_files)} weekly report PDFs")

    all_reports = []
    all_issues = []
    all_work_items = []
    all_procurement = []

    for i, pdf_path in enumerate(pdf_files):
        print(f"Processing {i+1}/{len(pdf_files)}: {pdf_path.name[:50]}...")

        report = parse_weekly_report(pdf_path)

        if report['report_date']:
            # File-level info
            all_reports.append({
                'file_id': i + 1,
                'filename': report['filename'],
                'report_date': report['report_date'],
                'author_name': report['author_name'],
                'author_role': report['author_role'],
                'page_count': report['page_count'],
                'narrative_length': len(report['narrative_text'] or ''),
                'work_items_count': len(report['work_progressing']),
                'issues_count': len(report['key_issues']),
                'procurement_count': len(report['procurement']),
            })

            file_id = i + 1

            # Work progressing items
            for item in report['work_progressing']:
                all_work_items.append({
                    'file_id': file_id,
                    'report_date': report['report_date'],
                    'item_number': item['number'],
                    'content': item['content'],
                })

            # Key issues
            for item in report['key_issues']:
                all_issues.append({
                    'file_id': file_id,
                    'report_date': report['report_date'],
                    'item_number': item['number'],
                    'content': item['content'],
                })

            # Procurement
            for item in report['procurement']:
                all_procurement.append({
                    'file_id': file_id,
                    'report_date': report['report_date'],
                    'item_number': item['number'],
                    'content': item['content'],
                })

    # Save outputs (with schema validation)
    print("\n=== Saving outputs ===")

    if all_reports:
        df = pd.DataFrame(all_reports)
        validated_df_to_csv(df, output_dir / 'weekly_reports.csv', index=False)
        print(f"weekly_reports.csv: {len(df)} reports (validated)")

    if all_work_items:
        df = pd.DataFrame(all_work_items)
        validated_df_to_csv(df, output_dir / 'work_progressing.csv', index=False)
        print(f"work_progressing.csv: {len(df)} items (validated)")

    if all_issues:
        df = pd.DataFrame(all_issues)
        validated_df_to_csv(df, output_dir / 'key_issues.csv', index=False)
        print(f"key_issues.csv: {len(df)} issues (validated)")

    if all_procurement:
        df = pd.DataFrame(all_procurement)
        validated_df_to_csv(df, output_dir / 'procurement.csv', index=False)
        print(f"procurement.csv: {len(df)} items (validated)")

    print("\nDone!")


if __name__ == '__main__':
    main()
