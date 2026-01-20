#!/usr/bin/env python3
"""
Sync Daily Work Plan files from the field team's TBM folder to raw/tbm/.

The field team maintains TBM files in a nested folder structure:
  Field Tracking/TBM Analysis/
  ├── Axios/
  │   ├── 1-12-26/
  │   │   └── AXIOS Daily Work Plan - 01-13-2026 Planned.xlsx
  │   └── December 2025/
  │       └── 12-15-25/
  │           └── AXIOS Daily Work Plan 12.15.2025.xlsx
  ├── Berg & MK Marlow/
  │   └── ...
  └── ...

This script:
1. Recursively finds Daily Work Plan files
2. Copies them to raw/tbm/ with flattened names
3. Tracks synced files in a manifest to avoid duplicates

Usage:
    python sync_field_tbm.py           # Sync new files only
    python sync_field_tbm.py --dry-run # Preview without copying
    python sync_field_tbm.py --force   # Re-copy all files
    python sync_field_tbm.py --status  # Show sync status
"""

import argparse
import hashlib
import json
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from src.config.settings import Settings


# Subcontractor folder name → prefix for output filename
SUBCONTRACTOR_PREFIXES = {
    'Axios': 'Axios',
    'Berg & MK Marlow': 'Berg_MKMarlow',
}

# File patterns to include (case-insensitive)
INCLUDE_PATTERNS = [
    r'daily.*work.*plan',
    r'secai.*daily.*work.*plan',
    r'yates.*secai.*daily',
]

# Folders to exclude from scanning
EXCLUDE_FOLDERS = {
    'Weekly Report',
    'TCO Plan',
    'Fieldwire Data Dump',
    'Combined - Cover Sheet',
}


def get_file_hash(filepath: Path) -> str:
    """Calculate MD5 hash of file for duplicate detection."""
    hasher = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            hasher.update(chunk)
    return hasher.hexdigest()


def matches_include_pattern(filename: str) -> bool:
    """Check if filename matches any include pattern."""
    name_lower = filename.lower()
    return any(re.search(pattern, name_lower) for pattern in INCLUDE_PATTERNS)


def get_subcontractor_from_path(filepath: Path, field_root: Path) -> str:
    """Extract subcontractor name from the file's relative path."""
    try:
        rel_path = filepath.relative_to(field_root)
        # First component is the subcontractor folder
        if rel_path.parts:
            folder = rel_path.parts[0]
            return SUBCONTRACTOR_PREFIXES.get(folder, folder.replace(' ', '_'))
    except ValueError:
        pass
    return 'Unknown'


def generate_output_filename(filepath: Path, field_root: Path) -> str:
    """
    Generate output filename from the folder structure.

    Uses the relative path from field_root with underscores as separators.
    Example: Axios/1-2-26/file.xlsx → Axios_1-2-26_file.xlsx
    """
    try:
        rel_path = filepath.relative_to(field_root)
    except ValueError:
        # Fallback if relative_to fails
        return filepath.name

    # Get all path components and join with underscores
    # Replace spaces and special chars in folder names
    parts = []
    for part in rel_path.parts:
        # Replace spaces and & with underscores, collapse multiple underscores
        cleaned = part.replace(' & ', '_').replace(' ', '_').replace('&', '_')
        cleaned = '_'.join(filter(None, cleaned.split('_')))  # Collapse multiple underscores
        parts.append(cleaned)

    return '_'.join(parts)


def load_manifest(manifest_path: Path) -> dict:
    """Load sync manifest from file."""
    if manifest_path.exists():
        with open(manifest_path) as f:
            return json.load(f)
    return {'synced_files': {}, 'last_sync': None}


def save_manifest(manifest_path: Path, manifest: dict):
    """Save sync manifest to file."""
    manifest['last_sync'] = datetime.now().isoformat()
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)


def find_daily_work_plans(field_dir: Path) -> list[tuple[Path, str]]:
    """
    Find all Daily Work Plan files in the field directory.

    Returns:
        List of (filepath, subcontractor) tuples
    """
    results = []

    for subcontractor_folder in field_dir.iterdir():
        if not subcontractor_folder.is_dir():
            continue
        if subcontractor_folder.name in EXCLUDE_FOLDERS:
            continue

        subcontractor = SUBCONTRACTOR_PREFIXES.get(
            subcontractor_folder.name,
            subcontractor_folder.name.replace(' ', '_')
        )

        # Recursively find Excel files
        for filepath in subcontractor_folder.rglob('*.xlsx'):
            if matches_include_pattern(filepath.name):
                results.append((filepath, subcontractor))

        for filepath in subcontractor_folder.rglob('*.xlsm'):
            if matches_include_pattern(filepath.name):
                results.append((filepath, subcontractor))

    return sorted(results, key=lambda x: x[0].name)


def sync_files(
    field_dir: Path,
    raw_dir: Path,
    dry_run: bool = False,
    force: bool = False
) -> dict:
    """
    Sync Daily Work Plan files from field folder to raw/tbm/.

    Args:
        field_dir: Source directory (field team's folder)
        raw_dir: Destination directory (raw/tbm/)
        dry_run: If True, preview without copying
        force: If True, re-copy existing files

    Returns:
        Dict with sync statistics
    """
    manifest_path = raw_dir / '.field_sync_manifest.json'
    manifest = load_manifest(manifest_path)

    stats = {
        'found': 0,
        'new': 0,
        'skipped': 0,
        'updated': 0,
        'errors': 0,
        'files': []
    }

    # Find all Daily Work Plan files
    files = find_daily_work_plans(field_dir)
    stats['found'] = len(files)

    for filepath, subcontractor in files:
        output_name = generate_output_filename(filepath, field_dir)
        output_path = raw_dir / output_name

        # Calculate source file hash
        source_hash = get_file_hash(filepath)

        # Check if already synced
        rel_path = str(filepath)
        synced_info = manifest['synced_files'].get(rel_path, {})

        if output_path.exists() and not force:
            if synced_info.get('hash') == source_hash:
                stats['skipped'] += 1
                continue
            else:
                # File changed, update it
                action = 'update'
                stats['updated'] += 1
        else:
            action = 'new'
            stats['new'] += 1

        stats['files'].append({
            'source': str(filepath),
            'dest': output_name,
            'subcontractor': subcontractor,
            'action': action
        })

        if not dry_run:
            try:
                shutil.copy2(filepath, output_path)
                manifest['synced_files'][rel_path] = {
                    'hash': source_hash,
                    'output': output_name,
                    'synced_at': datetime.now().isoformat()
                }
            except Exception as e:
                print(f"Error copying {filepath.name}: {e}")
                stats['errors'] += 1

    if not dry_run:
        save_manifest(manifest_path, manifest)

    return stats


def show_status(field_dir: Path, raw_dir: Path):
    """Show current sync status."""
    print("=" * 70)
    print("Field TBM Sync Status")
    print("=" * 70)

    print(f"\nSource: {field_dir}")
    print(f"Destination: {raw_dir}")

    # Count files in field folder
    files = find_daily_work_plans(field_dir)
    print(f"\nDaily Work Plan files in field folder: {len(files)}")

    # Group by subcontractor
    by_sub = {}
    for filepath, sub in files:
        by_sub[sub] = by_sub.get(sub, 0) + 1

    for sub, count in sorted(by_sub.items()):
        print(f"  {sub}: {count}")

    # Check manifest
    manifest_path = raw_dir / '.field_sync_manifest.json'
    manifest = load_manifest(manifest_path)

    synced_count = len(manifest.get('synced_files', {}))
    last_sync = manifest.get('last_sync', 'Never')

    print(f"\nSynced files: {synced_count}")
    print(f"Last sync: {last_sync}")

    # Count files in raw folder
    raw_files = list(raw_dir.glob('*.xlsx')) + list(raw_dir.glob('*.xlsm'))
    print(f"\nTotal files in raw/tbm/: {len(raw_files)}")

    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description='Sync Daily Work Plan files from field team folder'
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Preview changes without copying files'
    )
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Re-copy all files even if already synced'
    )
    parser.add_argument(
        '--status', '-s',
        action='store_true',
        help='Show current sync status'
    )

    args = parser.parse_args()

    # Validate paths
    if Settings.FIELD_TBM_DIR is None:
        print("Error: FIELD_TBM_FILES not set in .env")
        print("Add: FIELD_TBM_FILES=C:\\path\\to\\Field Tracking\\TBM Analysis")
        sys.exit(1)

    field_dir = Settings.FIELD_TBM_DIR
    raw_dir = Settings.TBM_RAW_DIR

    if not field_dir.exists():
        print(f"Error: Field TBM folder not found: {field_dir}")
        sys.exit(1)

    if not raw_dir.exists():
        print(f"Error: Raw TBM folder not found: {raw_dir}")
        sys.exit(1)

    if args.status:
        show_status(field_dir, raw_dir)
        return

    # Run sync
    mode = "DRY RUN" if args.dry_run else "SYNC"
    print(f"\n{'='*70}")
    print(f"Field TBM Sync - {mode}")
    print(f"{'='*70}")
    print(f"Source: {field_dir}")
    print(f"Destination: {raw_dir}")

    stats = sync_files(field_dir, raw_dir, dry_run=args.dry_run, force=args.force)

    print(f"\nResults:")
    print(f"  Found: {stats['found']} Daily Work Plan files")
    print(f"  New: {stats['new']}")
    print(f"  Updated: {stats['updated']}")
    print(f"  Skipped (unchanged): {stats['skipped']}")
    if stats['errors']:
        print(f"  Errors: {stats['errors']}")

    if stats['files']:
        print(f"\nFiles {'to copy' if args.dry_run else 'copied'}:")
        for f in stats['files'][:20]:
            action = f['action'].upper()
            print(f"  [{action}] {f['subcontractor']}: {f['dest']}")
        if len(stats['files']) > 20:
            print(f"  ... and {len(stats['files']) - 20} more")

    if args.dry_run:
        print(f"\nRun without --dry-run to copy files")

    print(f"{'='*70}\n")


if __name__ == '__main__':
    main()
