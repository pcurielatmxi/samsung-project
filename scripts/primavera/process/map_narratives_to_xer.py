#!/usr/bin/env python3
"""
Map narrative documents to XER schedule files by date.

This script:
1. Extracts dates from narrative document filenames
2. Matches them to XER files from the manifest by date proximity
3. Outputs a mapping CSV with confidence scores

Confidence Levels:
- HIGH: Exact match or 1-day difference
- MEDIUM: 2-3 day difference
- LOW: 4-7 day difference
- NONE: Date extracted but no XER match within 7 days
- NO_DATE: Could not extract date from filename

Document Types:
- schedule_narrative: Direct schedule narratives (most reliable dates)
- milestone_variance: Variance reports (date usually 1 day after schedule)
- review_response: SECAI review/response docs (date may be review date)
- schedule_export: Full schedule exports by area/trade
- expert_report: Litigation expert reports
- other: Uncategorized documents

Date Reliability:
- RELIABLE: Filename date = Schedule data date
- OFFSET: Filename date offset from schedule (e.g., variance reports)
- REVIEW_NEEDED: Date semantics unclear, may need manual verification

Usage:
    python map_narratives_to_xer.py [--max-days N]
"""

import re
import json
import csv
import argparse
from pathlib import Path
from datetime import datetime
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.config.settings import Settings

# Directories
NARRATIVES_RAW_DIR = Settings.PRIMAVERA_RAW_DIR.parent / "primavera_narratives"
NARRATIVES_PROCESSED_DIR = Settings.PRIMAVERA_PROCESSED_DIR.parent / "primavera_narratives"
MANIFEST_PATH = Settings.PRIMAVERA_RAW_DIR / "manifest.json"

# Date extraction patterns - ordered by specificity
DATE_PATTERNS = [
    # YYYY.MM.DD (e.g., 2025.03.27)
    (r'(\d{4})\.(\d{1,2})\.(\d{1,2})',
     lambda m: (int(m.group(1)), int(m.group(2)), int(m.group(3)))),

    # YYMMDD at start of filename (e.g., 250228)
    (r'^(\d{2})(\d{2})(\d{2})',
     lambda m: (2000 + int(m.group(1)), int(m.group(2)), int(m.group(3)))),

    # MM-DD-YYYY (e.g., 04-04-2025)
    (r'(\d{1,2})-(\d{1,2})-(\d{4})',
     lambda m: (int(m.group(3)), int(m.group(1)), int(m.group(2)))),

    # M-D-YY or MM-DD-YY (e.g., 2-7-25, 10-29-23)
    (r'(\d{1,2})-(\d{1,2})-(\d{2})(?!\d)',
     lambda m: (2000 + int(m.group(3)) if int(m.group(3)) < 50 else 1900 + int(m.group(3)),
                int(m.group(1)), int(m.group(2)))),

    # M.D.YY at start (e.g., 2.7.25)
    (r'^(\d{1,2})\.(\d{1,2})\.(\d{2})(?!\d)',
     lambda m: (2000 + int(m.group(3)), int(m.group(1)), int(m.group(2)))),
]


def get_all_files_recursive(directory: Path) -> list[Path]:
    """Get all files recursively from directory and subdirectories."""
    all_files = []
    for item in sorted(directory.iterdir()):
        if item.is_file():
            all_files.append(item)
        elif item.is_dir():
            all_files.extend(get_all_files_recursive(item))
    return all_files


def classify_document(filename: str) -> tuple[str, str, str]:
    """Classify document type and determine date reliability.

    Args:
        filename: The filename to classify

    Returns:
        Tuple of (document_type, date_reliability, notes)
    """
    fname_lower = filename.lower()

    if 'expert report' in fname_lower or 'preliminary expert' in fname_lower:
        return 'expert_report', 'REVIEW_NEEDED', 'Litigation document - verify schedule references'
    elif 'variance report' in fname_lower or 'milestone variance' in fname_lower:
        return 'milestone_variance', 'OFFSET', 'Report date usually 1 day after schedule DD'
    elif 'review' in fname_lower or 'response' in fname_lower or 'comment' in fname_lower:
        return 'review_response', 'REVIEW_NEEDED', 'Review/response date - schedule DD may differ'
    elif 'full schedule' in fname_lower:
        return 'schedule_export', 'RELIABLE', 'Schedule data export'
    elif 'narrative' in fname_lower:
        return 'schedule_narrative', 'RELIABLE', 'Filename date = Schedule DD'
    else:
        return 'other', 'REVIEW_NEEDED', 'Document type unclear'


def extract_date_from_filename(filename: str) -> datetime | None:
    """Extract date from filename using various patterns.

    Args:
        filename: The filename to parse

    Returns:
        datetime object if date found, None otherwise
    """
    for pattern, converter in DATE_PATTERNS:
        match = re.search(pattern, filename)
        if match:
            try:
                year, month, day = converter(match)
                return datetime(year, month, day)
            except ValueError:
                # Invalid date (e.g., month > 12), try next pattern
                continue
    return None


def load_xer_dates(manifest_path: Path) -> dict[str, datetime]:
    """Load XER file dates from manifest.

    Args:
        manifest_path: Path to manifest.json

    Returns:
        Dict mapping XER filename to datetime
    """
    manifest = json.loads(manifest_path.read_text())
    xer_dates = {}

    for fname, meta in manifest.get('files', {}).items():
        date_str = meta.get('date')
        if date_str:
            try:
                xer_dates[fname] = datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                pass

    return xer_dates


def find_closest_xer(
    narrative_date: datetime,
    xer_dates: dict[str, datetime],
    max_days: int = 7
) -> tuple[str | None, int | None, str]:
    """Find closest XER file by date.

    Args:
        narrative_date: Date extracted from narrative filename
        xer_dates: Dict of XER filename to date
        max_days: Maximum days difference for a match

    Returns:
        Tuple of (xer_filename, days_diff, confidence)
    """
    best_match = None
    best_diff = None

    for xer_name, xer_date in xer_dates.items():
        diff = abs((narrative_date - xer_date).days)
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best_match = xer_name

    if best_diff is not None and best_diff <= max_days:
        if best_diff <= 1:
            confidence = 'HIGH'
        elif best_diff <= 3:
            confidence = 'MEDIUM'
        else:
            confidence = 'LOW'
        return best_match, best_diff, confidence

    return None, best_diff, 'NONE'


def map_narratives_to_xer(max_days: int = 7) -> list[dict]:
    """Map all narrative files to XER files.

    Args:
        max_days: Maximum days difference for matching

    Returns:
        List of mapping dictionaries
    """
    # Load XER dates
    xer_dates = load_xer_dates(MANIFEST_PATH)
    print(f"Loaded {len(xer_dates)} XER files with dates")

    # Process narratives recursively
    all_files = get_all_files_recursive(NARRATIVES_RAW_DIR)
    print(f"Found {len(all_files)} files (including subfolders)")

    mappings = []

    for f in all_files:
        # Calculate relative path for subfolder tracking
        rel_path = f.relative_to(NARRATIVES_RAW_DIR)
        subfolder = str(rel_path.parent) if rel_path.parent != Path('.') else ''

        narrative_date = extract_date_from_filename(f.name)
        doc_type, date_reliability, notes = classify_document(f.name)

        if narrative_date:
            xer_match, days_diff, confidence = find_closest_xer(
                narrative_date, xer_dates, max_days
            )
            mappings.append({
                'subfolder': subfolder,
                'narrative_file': f.name,
                'narrative_date': narrative_date.strftime('%Y-%m-%d'),
                'xer_file': xer_match or '',
                'xer_date': xer_dates.get(xer_match, datetime(1900,1,1)).strftime('%Y-%m-%d') if xer_match else '',
                'days_diff': days_diff if days_diff is not None else '',
                'confidence': confidence,
                'document_type': doc_type,
                'date_reliability': date_reliability,
                'notes': notes
            })
        else:
            mappings.append({
                'subfolder': subfolder,
                'narrative_file': f.name,
                'narrative_date': '',
                'xer_file': '',
                'xer_date': '',
                'days_diff': '',
                'confidence': 'NO_DATE',
                'document_type': doc_type,
                'date_reliability': date_reliability,
                'notes': notes
            })

    return mappings


def write_mapping_csv(mappings: list[dict], output_path: Path) -> None:
    """Write mappings to CSV file.

    Args:
        mappings: List of mapping dictionaries
        output_path: Path to output CSV
    """
    fieldnames = [
        'subfolder', 'narrative_file', 'narrative_date', 'xer_file', 'xer_date',
        'days_diff', 'confidence', 'document_type', 'date_reliability', 'notes'
    ]

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(mappings)


def print_summary(mappings: list[dict]) -> None:
    """Print mapping summary statistics."""
    by_conf = {}
    by_type = {}
    by_reliability = {}

    for m in mappings:
        conf = m['confidence']
        by_conf[conf] = by_conf.get(conf, 0) + 1

        doc_type = m.get('document_type', 'unknown')
        by_type[doc_type] = by_type.get(doc_type, 0) + 1

        reliability = m.get('date_reliability', 'unknown')
        by_reliability[reliability] = by_reliability.get(reliability, 0) + 1

    print("\nMapping Summary:")
    print("-" * 40)
    print("By Confidence:")
    for conf in ['HIGH', 'MEDIUM', 'LOW', 'NONE', 'NO_DATE']:
        count = by_conf.get(conf, 0)
        pct = count / len(mappings) * 100
        print(f"  {conf:8}: {count:3} ({pct:5.1f}%)")

    matched = by_conf.get('HIGH', 0) + by_conf.get('MEDIUM', 0) + by_conf.get('LOW', 0)
    print("-" * 40)
    print(f"  Total narratives: {len(mappings)}")
    print(f"  Matched:          {matched}")
    print(f"  Match rate:       {matched/len(mappings)*100:.1f}%")

    print("\nBy Document Type:")
    print("-" * 40)
    for dtype in ['schedule_narrative', 'milestone_variance', 'review_response',
                  'schedule_export', 'expert_report', 'other']:
        count = by_type.get(dtype, 0)
        if count > 0:
            print(f"  {dtype:20}: {count:3}")

    print("\nBy Date Reliability:")
    print("-" * 40)
    for rel in ['RELIABLE', 'OFFSET', 'REVIEW_NEEDED']:
        count = by_reliability.get(rel, 0)
        print(f"  {rel:15}: {count:3}")


def print_unmatched(mappings: list[dict]) -> None:
    """Print details of unmatched narratives."""
    print("\n=== NONE (date extracted but no XER match within threshold) ===\n")
    for m in mappings:
        if m['confidence'] == 'NONE':
            print(f"  {m['narrative_date']}: {m['narrative_file'][:70]}")
            print(f"      Closest XER: {m['days_diff']} days away")

    print("\n=== NO_DATE (no date extracted from filename) ===\n")
    for m in mappings:
        if m['confidence'] == 'NO_DATE':
            print(f"  {m['narrative_file']}")


def main():
    parser = argparse.ArgumentParser(
        description='Map narrative documents to XER schedule files by date'
    )
    parser.add_argument(
        '--max-days', type=int, default=7,
        help='Maximum days difference for matching (default: 7)'
    )
    parser.add_argument(
        '--show-unmatched', action='store_true',
        help='Show details of unmatched narratives'
    )
    args = parser.parse_args()

    # Ensure output directory exists
    NARRATIVES_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Generate mappings
    print(f"Mapping narratives from: {NARRATIVES_RAW_DIR}")
    print(f"Using XER manifest: {MANIFEST_PATH}")
    print(f"Max days threshold: {args.max_days}")

    mappings = map_narratives_to_xer(max_days=args.max_days)

    # Write output
    output_path = NARRATIVES_PROCESSED_DIR / 'narrative_xer_mapping.csv'
    write_mapping_csv(mappings, output_path)
    print(f"\nWrote mappings to: {output_path}")

    # Print summary
    print_summary(mappings)

    if args.show_unmatched:
        print_unmatched(mappings)


if __name__ == '__main__':
    main()
