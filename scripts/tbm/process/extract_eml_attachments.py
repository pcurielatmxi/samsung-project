#!/usr/bin/env python3
"""
Extract Excel attachments from TBM EML files.

This script processes EML email files containing TBM daily work plans as Excel
attachments. It extracts all Excel files (.xlsx, .xls) from the emails and saves
them to the raw/tbm/ directory for processing by the existing TBM pipeline.

Input:  ${WINDOWS_DATA_DIR}/raw/tbm/TBM EML Files/*.eml
Output: ${WINDOWS_DATA_DIR}/raw/tbm/eml_YYYYMMDD_N_*.xlsx

The script maintains a manifest to track extracted files and avoid re-extraction.
"""

import email
from email import policy
from pathlib import Path
import json
import re
from datetime import datetime
from typing import Dict, List, Tuple
import argparse
import sys

# Add project root to path
project_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(project_root))

from src.config.settings import settings


def is_excel_file(data: bytes) -> Tuple[bool, str]:
    """
    Check if binary data is an Excel file.

    Returns:
        (is_excel, extension) where extension is 'xlsx' or 'xls'
    """
    if not data:
        return False, ""

    # Check file signatures
    magic = data[:8]

    # XLSX (Office Open XML) - ZIP signature
    if magic[:4] == b'PK\x03\x04':
        return True, 'xlsx'

    # XLS (Office 97-2003) - OLE2 signature
    if magic[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':
        return True, 'xls'

    return False, ""


def extract_date_from_filename(filename: str) -> str:
    """
    Extract date from EML filename.

    Expected format: YYYYMMDD_*.eml
    Returns: YYYYMMDD or empty string if not found
    """
    match = re.match(r'^(\d{8})_', filename)
    return match.group(1) if match else ""


def load_manifest(manifest_path: Path) -> Dict:
    """Load extraction manifest."""
    if manifest_path.exists():
        with open(manifest_path, 'r') as f:
            return json.load(f)
    return {"extracted_files": {}, "extraction_stats": {}}


def save_manifest(manifest_path: Path, manifest: Dict):
    """Save extraction manifest."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)


def extract_attachments_from_eml(
    eml_path: Path,
    output_dir: Path,
    manifest: Dict,
    force: bool = False,
    dry_run: bool = False
) -> Tuple[int, int]:
    """
    Extract Excel attachments from a single EML file.

    Returns:
        (extracted_count, skipped_count)
    """
    eml_key = eml_path.name

    # Check if already processed
    if eml_key in manifest["extracted_files"] and not force:
        existing_count = len(manifest["extracted_files"][eml_key])
        return 0, existing_count

    # Parse EML
    try:
        with open(eml_path, 'rb') as f:
            msg = email.message_from_binary_file(f, policy=policy.default)
    except Exception as e:
        print(f"ERROR: Failed to parse {eml_path.name}: {e}")
        return 0, 0

    # Extract date from filename
    date_str = extract_date_from_filename(eml_path.name)
    if not date_str:
        print(f"WARNING: No date found in filename: {eml_path.name}")
        date_str = "unknown"

    # Extract attachments
    extracted_files = []
    attachment_num = 0

    for part in msg.walk():
        if part.get_content_disposition() == 'attachment':
            data = part.get_payload(decode=True)

            # Check if it's an Excel file
            is_excel, ext = is_excel_file(data)
            if not is_excel:
                continue

            attachment_num += 1

            # Generate output filename
            output_filename = f"eml_{date_str}_{attachment_num:02d}.{ext}"
            output_path = output_dir / output_filename

            if dry_run:
                print(f"  [DRY RUN] Would extract: {output_filename} ({len(data)/1024:.1f} KB)")
            else:
                # Save attachment
                output_path.write_bytes(data)
                print(f"  Extracted: {output_filename} ({len(data)/1024:.1f} KB)")

            extracted_files.append({
                "filename": output_filename,
                "size_bytes": len(data),
                "extension": ext
            })

    # Update manifest
    if not dry_run:
        manifest["extracted_files"][eml_key] = {
            "date": date_str,
            "extracted_at": datetime.now().isoformat(),
            "attachments": extracted_files
        }

    return len(extracted_files), 0


def main():
    parser = argparse.ArgumentParser(
        description="Extract Excel attachments from TBM EML files"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-extract files even if already processed"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be extracted without actually extracting"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of EML files to process (for testing)"
    )

    args = parser.parse_args()

    # Setup paths
    eml_dir = settings.DATA_DIR / "raw" / "tbm" / "TBM EML Files"
    output_dir = settings.DATA_DIR / "raw" / "tbm"
    manifest_path = output_dir / "eml_extraction_manifest.json"

    # Validate input directory
    if not eml_dir.exists():
        print(f"ERROR: EML directory not found: {eml_dir}")
        print("Expected: ${WINDOWS_DATA_DIR}/raw/tbm/TBM EML Files/")
        return 1

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load manifest
    manifest = load_manifest(manifest_path)

    # Find EML files
    eml_files = sorted(eml_dir.glob("*.eml"))

    if not eml_files:
        print(f"No EML files found in {eml_dir}")
        return 1

    print("=" * 80)
    print("TBM EML ATTACHMENT EXTRACTION")
    print("=" * 80)
    print(f"Input directory:  {eml_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Total EML files:  {len(eml_files)}")

    if args.limit:
        eml_files = eml_files[:args.limit]
        print(f"Limiting to:      {args.limit} files (--limit)")

    if args.dry_run:
        print("Mode:             DRY RUN (no files will be written)")

    if args.force:
        print("Mode:             FORCE (re-extract all)")

    print("=" * 80)
    print()

    # Process EML files
    total_extracted = 0
    total_skipped = 0
    total_errors = 0

    for i, eml_path in enumerate(eml_files, 1):
        print(f"[{i}/{len(eml_files)}] {eml_path.name}")

        try:
            extracted, skipped = extract_attachments_from_eml(
                eml_path,
                output_dir,
                manifest,
                force=args.force,
                dry_run=args.dry_run
            )

            total_extracted += extracted
            total_skipped += skipped

            if skipped > 0:
                print(f"  Skipped: {skipped} attachments (already extracted)")

        except Exception as e:
            print(f"  ERROR: {e}")
            total_errors += 1

    # Save manifest
    if not args.dry_run:
        save_manifest(manifest_path, manifest)

    # Summary
    print()
    print("=" * 80)
    print("EXTRACTION SUMMARY")
    print("=" * 80)
    print(f"EML files processed:  {len(eml_files)}")
    print(f"Attachments extracted: {total_extracted}")
    print(f"Attachments skipped:   {total_skipped}")
    print(f"Errors:                {total_errors}")

    if not args.dry_run:
        print(f"Manifest saved to:     {manifest_path}")
        print(f"Excel files ready for: ./run.sh parse")

    print("=" * 80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
