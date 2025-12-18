#!/usr/bin/env python3
"""
Get Next Narrative for Analysis

Returns the next unanalyzed narrative file in chronological order.
Tracks progress against narrative_findings.csv to skip already-analyzed files.

Usage:
    python get_next_narrative.py           # Show next file info
    python get_next_narrative.py --show    # Also print markdown content
    python get_next_narrative.py --all     # List all files in order
"""

import argparse
import csv
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.config.settings import Settings

# Paths
MAPPING_CSV = Settings.PROCESSED_DATA_DIR / "primavera_narratives" / "narrative_xer_mapping.csv"
FINDINGS_CSV = Settings.DERIVED_DATA_DIR / "primavera_narratives" / "narrative_findings.csv"
MARKDOWN_DIR = Settings.PROCESSED_DATA_DIR / "primavera_narratives"

# Categories for reference
CATEGORIES = [
    "DESIGN",        # Drawing issues, design changes, design busts
    "COORDINATION",  # Clash, sequencing, access conflicts
    "FABRICATION",   # Shop drawing delays, fab lead times
    "DELIVERY",      # Material delivery, procurement delays
    "QUALITY",       # NCRs, rework, inspection failures
    "RFI",           # Pending RFIs blocking work
    "SCOPE_CHANGE",  # Added scope, change orders
    "RESOURCE",      # Manpower, crane access, laydown
    "WEATHER",       # Weather/environmental impacts
    "OWNER_DIRECTION", # SECAI directives, decisions
]

IMPACT_TYPES = ["delay", "cost", "quality", "scope"]
RESPONSIBLE_PARTIES = ["YATES", "SECAI", "Owner", "Design", "Subcontractor", "Other"]


def load_mapping() -> list[dict]:
    """Load narrative-XER mapping sorted by date."""
    if not MAPPING_CSV.exists():
        print(f"ERROR: Mapping CSV not found: {MAPPING_CSV}")
        sys.exit(1)

    with open(MAPPING_CSV, encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    # Filter to files with dates and sort chronologically
    dated = [r for r in rows if r['narrative_date']]
    dated.sort(key=lambda r: r['narrative_date'])

    # Add undated files at the end
    undated = [r for r in rows if not r['narrative_date']]

    return dated + undated


def load_analyzed_files() -> set[str]:
    """Load set of files that have at least one finding."""
    if not FINDINGS_CSV.exists():
        return set()

    analyzed = set()
    with open(FINDINGS_CSV, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('source_file'):
                analyzed.add(row['source_file'])

    return analyzed


def get_markdown_path(mapping_row: dict) -> Path:
    """Get path to markdown file for a mapping row."""
    subfolder = mapping_row.get('subfolder', '')
    filename = mapping_row['narrative_file']
    stem = Path(filename).stem

    if subfolder:
        return MARKDOWN_DIR / subfolder / f"{stem}.md"
    return MARKDOWN_DIR / f"{stem}.md"


def print_file_info(row: dict, index: int, total: int) -> None:
    """Print information about a narrative file."""
    print("=" * 70)
    print(f"NEXT FILE TO ANALYZE ({index + 1} of {total} with dates)")
    print("=" * 70)
    print()
    print(f"File:      {row['narrative_file']}")
    if row.get('subfolder'):
        print(f"Folder:    {row['subfolder']}")
    print(f"Date:      {row['narrative_date']}")
    print(f"Doc Type:  {row['document_type']}")
    print()

    if row.get('xer_file'):
        print(f"XER Match: {row['xer_file']}")
        print(f"XER Date:  {row['xer_date']}")
        print(f"Match:     {row['confidence']} ({row['days_diff']} days diff)")
    else:
        print("XER Match: None")
    print()

    md_path = get_markdown_path(row)
    if md_path.exists():
        print(f"Markdown:  {md_path}")
    else:
        print(f"Markdown:  NOT FOUND - {md_path}")
    print()

    print("Categories:", ", ".join(CATEGORIES))
    print("Impact Types:", ", ".join(IMPACT_TYPES))
    print("Parties:", ", ".join(RESPONSIBLE_PARTIES))
    print("=" * 70)


def print_markdown_content(row: dict) -> None:
    """Print the markdown content of a file."""
    md_path = get_markdown_path(row)
    if not md_path.exists():
        print(f"\nERROR: Markdown file not found: {md_path}")
        return

    print("\n" + "=" * 70)
    print("DOCUMENT CONTENT")
    print("=" * 70 + "\n")

    content = md_path.read_text(encoding='utf-8')
    print(content)


def list_all_files(mappings: list[dict], analyzed: set[str]) -> None:
    """List all files with their analysis status."""
    print("=" * 90)
    print(f"{'#':>3}  {'Date':10}  {'Status':8}  {'Type':20}  File")
    print("=" * 90)

    for i, row in enumerate(mappings):
        date = row['narrative_date'] or '(no date)'
        status = "DONE" if row['narrative_file'] in analyzed else "pending"
        doc_type = row['document_type'][:20]
        filename = row['narrative_file'][:50]

        marker = "*" if status == "pending" else " "
        print(f"{i+1:>3}{marker} {date:10}  {status:8}  {doc_type:20}  {filename}")

    print("=" * 90)
    done = len(analyzed)
    total = len(mappings)
    print(f"Progress: {done}/{total} files analyzed ({done/total*100:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description='Get next narrative file to analyze')
    parser.add_argument('--show', action='store_true', help='Show markdown content')
    parser.add_argument('--all', action='store_true', help='List all files with status')
    parser.add_argument('--skip', type=int, default=0, help='Skip N files from current position')
    parser.add_argument('--type', '-t', type=str, help='Filter by document type (schedule_narrative, milestone_variance, etc)')
    parser.add_argument('--docx-only', action='store_true', help='Only show .docx files (skip PDF duplicates)')
    args = parser.parse_args()

    mappings = load_mapping()
    analyzed = load_analyzed_files()

    # Apply filters
    if args.type:
        mappings = [m for m in mappings if m.get('document_type') == args.type]
    if args.docx_only:
        mappings = [m for m in mappings if m['narrative_file'].lower().endswith('.docx')]

    if args.all:
        list_all_files(mappings, analyzed)
        return

    # Find next unanalyzed file
    dated_mappings = [m for m in mappings if m['narrative_date']]

    next_index = None
    skipped = 0
    for i, row in enumerate(dated_mappings):
        if row['narrative_file'] not in analyzed:
            if skipped >= args.skip:
                next_index = i
                break
            skipped += 1

    if next_index is None:
        print("All dated files have been analyzed!")

        # Check for undated files
        undated = [m for m in mappings if not m['narrative_date'] and m['narrative_file'] not in analyzed]
        if undated:
            print(f"\n{len(undated)} undated files remaining.")
        return

    row = dated_mappings[next_index]
    print_file_info(row, next_index, len(dated_mappings))

    if args.show:
        print_markdown_content(row)


if __name__ == '__main__':
    main()
