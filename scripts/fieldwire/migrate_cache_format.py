"""
Migrate AI enrichment cache files from derived/ to processed/ and update format.

This script performs two migrations:
1. LOCATION: Move from derived/fieldwire/ to processed/fieldwire/
2. FORMAT: Convert old format to new format with metadata

Old format: Direct result object
    {"tags": ["passive", "waiting"]}

New format: Wrapped with metadata
    {
        "_cache_key": "TBM-12345",
        "_cached_at": "2026-01-20T16:44:54",
        "_migrated_from": "old_format",
        "result": {"tags": ["passive", "waiting"]}
    }

Usage:
    # Dry run (show what would be migrated)
    python -m scripts.fieldwire.migrate_cache_format --dry-run

    # Run migration (creates backup first, then moves and converts)
    python -m scripts.fieldwire.migrate_cache_format

    # Skip backup (not recommended)
    python -m scripts.fieldwire.migrate_cache_format --no-backup
"""

import argparse
import json
import logging
import shutil
from datetime import datetime
from pathlib import Path

from src.config.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_old_cache_dir() -> Path:
    """Get the old (deprecated) Fieldwire AI cache directory in derived/."""
    return settings.FIELDWIRE_DERIVED_DIR / "ai_cache"


def get_new_cache_dir() -> Path:
    """Get the new Fieldwire AI cache directory in processed/."""
    return settings.FIELDWIRE_PROCESSED_DIR / "ai_cache"


def get_backup_dir() -> Path:
    """Get backup directory path with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return settings.FIELDWIRE_PROCESSED_DIR / f"ai_cache_backup_{timestamp}"


def get_old_derived_dir() -> Path:
    """Get the old (deprecated) derived/fieldwire directory."""
    return settings.FIELDWIRE_DERIVED_DIR


def get_new_processed_dir() -> Path:
    """Get the new processed/fieldwire directory."""
    return settings.FIELDWIRE_PROCESSED_DIR


def is_old_format(data: dict) -> bool:
    """Check if cache file is in old format (no _cache_key wrapper)."""
    return "_cache_key" not in data or "result" not in data


def migrate_file(cache_file: Path, dry_run: bool = False) -> bool:
    """
    Migrate a single cache file to new format.

    Returns True if file was migrated, False if already in new format.
    """
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Failed to read {cache_file.name}: {e}")
        return False

    if not is_old_format(data):
        return False  # Already in new format

    # Extract cache key from filename (remove .json extension)
    # Note: This is the sanitized key, which may differ from original
    cache_key = cache_file.stem

    # Get file modification time for _cached_at
    mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)

    # Create new format
    new_data = {
        "_cache_key": cache_key,
        "_cached_at": mtime.isoformat(),
        "_migrated_from": "old_format",
        "_migration_date": datetime.now().isoformat(),
        "result": data,
    }

    if dry_run:
        logger.info(f"Would migrate: {cache_file.name}")
        return True

    # Write migrated data
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(new_data, f, indent=2, ensure_ascii=False)

    return True


def create_backup(cache_dir: Path, backup_dir: Path) -> bool:
    """Create a backup of the cache directory."""
    if not cache_dir.exists():
        logger.error(f"Cache directory does not exist: {cache_dir}")
        return False

    logger.info(f"Creating backup: {backup_dir}")
    try:
        shutil.copytree(cache_dir, backup_dir)

        # Count files in backup
        backup_files = list(backup_dir.glob("*.json"))
        logger.info(f"Backup created with {len(backup_files)} files")
        return True
    except Exception as e:
        logger.error(f"Failed to create backup: {e}")
        return False


def run_migration(dry_run: bool = False, skip_backup: bool = False) -> dict:
    """
    Run the full migration: move from derived/ to processed/ and update format.

    Steps:
    1. Create backup of old cache in derived/
    2. Move cache directory from derived/ to processed/
    3. Convert all files to new format
    4. Move other derived files (tbm_content.csv, tbm_content_enriched.csv)

    Returns dict with migration statistics.
    """
    old_cache_dir = get_old_cache_dir()
    new_cache_dir = get_new_cache_dir()
    old_derived = get_old_derived_dir()
    new_processed = get_new_processed_dir()

    # Check if there's anything to migrate
    if not old_cache_dir.exists() and not new_cache_dir.exists():
        logger.error("No cache directory found in derived/ or processed/")
        return {"error": "No cache directory found"}

    # Determine source directory
    if old_cache_dir.exists():
        source_dir = old_cache_dir
        needs_move = True
        logger.info(f"Found cache in deprecated location: {old_cache_dir}")
    else:
        source_dir = new_cache_dir
        needs_move = False
        logger.info(f"Cache already in correct location: {new_cache_dir}")

    # Count files
    cache_files = list(source_dir.glob("*.json"))
    logger.info(f"Found {len(cache_files)} cache files")

    if not cache_files:
        logger.info("No cache files to migrate")
        return {"total": 0, "migrated": 0, "skipped": 0, "moved": False}

    # Create backup (unless dry run or explicitly skipped)
    backup_dir = None
    if not dry_run and not skip_backup:
        backup_dir = get_backup_dir()
        if not create_backup(source_dir, backup_dir):
            logger.error("Backup failed, aborting migration")
            return {"error": "Backup failed"}
        logger.info(f"Backup saved to: {backup_dir}")

    # Move cache directory if needed
    if needs_move and not dry_run:
        logger.info(f"Moving cache from {old_cache_dir} to {new_cache_dir}")
        new_cache_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(old_cache_dir), str(new_cache_dir))
        source_dir = new_cache_dir
        cache_files = list(source_dir.glob("*.json"))
        logger.info(f"Moved {len(cache_files)} files to {new_cache_dir}")
    elif needs_move and dry_run:
        logger.info(f"Would move cache from {old_cache_dir} to {new_cache_dir}")

    # Move other derived files
    derived_files = ["tbm_content.csv", "tbm_content_enriched.csv"]
    for filename in derived_files:
        old_file = old_derived / filename
        new_file = new_processed / filename
        if old_file.exists():
            if dry_run:
                logger.info(f"Would move {old_file} to {new_file}")
            else:
                new_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(old_file), str(new_file))
                logger.info(f"Moved {filename} to {new_processed}")

    # Migrate file formats (only if not dry run, since files were moved)
    migrated = 0
    skipped = 0
    errors = 0

    if dry_run:
        # In dry run, check files in original location
        for cache_file in cache_files:
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if is_old_format(data):
                    logger.info(f"Would convert format: {cache_file.name}")
                    migrated += 1
                else:
                    skipped += 1
            except Exception as e:
                logger.error(f"Error reading {cache_file.name}: {e}")
                errors += 1
    else:
        # Actually migrate files
        for cache_file in cache_files:
            try:
                if migrate_file(cache_file, dry_run=False):
                    migrated += 1
                else:
                    skipped += 1
            except Exception as e:
                logger.error(f"Error migrating {cache_file.name}: {e}")
                errors += 1

    # Summary
    action = "Would migrate" if dry_run else "Migrated"
    logger.info(f"\n=== Migration Summary ===")
    logger.info(f"Total files:    {len(cache_files)}")
    logger.info(f"Location move:  {'Yes' if needs_move else 'No'}")
    logger.info(f"Format {action.lower()}: {migrated}")
    logger.info(f"Already new:    {skipped}")
    if errors:
        logger.info(f"Errors:         {errors}")
    if backup_dir:
        logger.info(f"Backup:         {backup_dir}")

    return {
        "total": len(cache_files),
        "migrated": migrated,
        "skipped": skipped,
        "errors": errors,
        "moved": needs_move,
        "backup_dir": str(backup_dir) if backup_dir else None,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Migrate Fieldwire AI cache from derived/ to processed/ and update format"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be migrated without making changes",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip creating backup (not recommended)",
    )

    args = parser.parse_args()

    if args.dry_run:
        logger.info("=== DRY RUN MODE ===")

    result = run_migration(dry_run=args.dry_run, skip_backup=args.no_backup)

    if "error" in result:
        exit(1)


if __name__ == "__main__":
    main()
