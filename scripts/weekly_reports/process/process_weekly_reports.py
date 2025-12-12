#!/usr/bin/env python3
"""
Master script to process all Weekly Report PDFs.

Runs all weekly report parsing scripts in the correct order:
1. parse_weekly_reports.py - Extract narrative content, create weekly_reports.csv
2. parse_weekly_report_addendums_fast.py - Extract RFI/submittal/manpower logs
3. parse_labor_detail.py - Extract detailed labor tables

Input: data/raw/weekly_reports/*.pdf
Output: data/weekly_reports/tables/*.csv

Usage:
    python scripts/process_weekly_reports.py
    python scripts/process_weekly_reports.py --skip-narratives  # Skip step 1
"""

import subprocess
import sys
import time
from pathlib import Path


SCRIPTS_DIR = Path(__file__).parent
SCRIPTS = [
    ("parse_weekly_reports.py", "Extracting narratives and key issues"),
    ("parse_weekly_report_addendums_fast.py", "Extracting RFI/submittal/manpower logs"),
    ("parse_labor_detail.py", "Extracting detailed labor tables"),
]


def run_script(script_name: str, description: str) -> bool:
    """Run a script and return success status."""
    script_path = SCRIPTS_DIR / script_name

    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        return False

    print(f"\n{'='*60}")
    print(f"STEP: {description}")
    print(f"Running: {script_name}")
    print('='*60)

    start = time.time()
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=Path.cwd(),
    )
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"\nERROR: {script_name} failed with code {result.returncode}")
        return False

    print(f"\nCompleted in {elapsed:.1f}s")
    return True


def main():
    """Run all weekly report processing scripts."""
    print("="*60)
    print("WEEKLY REPORT PROCESSING PIPELINE")
    print("="*60)

    # Check for input files
    input_dir = Path('data/raw/weekly_reports')
    if not input_dir.exists():
        print(f"ERROR: Input directory not found: {input_dir}")
        sys.exit(1)

    pdf_count = len(list(input_dir.glob('*.pdf')))
    print(f"Found {pdf_count} PDF files in {input_dir}")

    if pdf_count == 0:
        print("No PDFs to process. Exiting.")
        sys.exit(0)

    # Parse arguments
    skip_narratives = '--skip-narratives' in sys.argv

    total_start = time.time()
    scripts_to_run = SCRIPTS[1:] if skip_narratives else SCRIPTS

    if skip_narratives:
        print("\nSkipping narrative extraction (--skip-narratives)")

    # Run each script
    for script_name, description in scripts_to_run:
        if not run_script(script_name, description):
            print("\nPipeline failed. Fix errors and retry.")
            sys.exit(1)

    total_elapsed = time.time() - total_start

    # Summary
    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print(f"Total time: {total_elapsed:.1f}s")
    print(f"\nOutput files in: data/weekly_reports/tables/")

    # List output files
    output_dir = Path('data/weekly_reports/tables')
    if output_dir.exists():
        csv_files = sorted(output_dir.glob('*.csv'))
        for f in csv_files:
            size_kb = f.stat().st_size / 1024
            print(f"  {f.name}: {size_kb:.1f} KB")


if __name__ == '__main__':
    main()
