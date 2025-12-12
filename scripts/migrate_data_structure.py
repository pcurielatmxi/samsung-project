#!/usr/bin/env python3
"""
Migrate data files to new folder structure.

New structure:
    WINDOWS_DATA_DIR/
    ├── raw/{source}/           # Source files (XER, PDF, Excel, CSV dumps)
    └── processed/{source}/     # Parsed/transformed data (CSV tables)

    PROJECT_ROOT/data/analysis/{source}/  # Analysis outputs (tracked by git)

Usage:
    python scripts/migrate_data_structure.py --dry-run   # Preview changes
    python scripts/migrate_data_structure.py             # Execute migration
"""
import sys
import shutil
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import settings


def get_migration_plan(windows_data_dir: Path) -> list[tuple[Path, Path, str]]:
    """
    Generate migration plan based on current file locations.

    Returns list of (source, destination, description) tuples.
    """
    plan = []

    # =========================================================================
    # Primavera migrations
    # =========================================================================
    # xer_exports/ -> processed/primavera/ (these are parsed CSV tables)
    xer_exports = windows_data_dir / 'xer_exports'
    if xer_exports.exists():
        for f in xer_exports.glob('*.csv'):
            if f.name != 'wbs_taxonomy_enriched.csv':  # Skip generated analysis
                plan.append((f, settings.PRIMAVERA_PROCESSED_DIR / f.name,
                            "Primavera parsed table"))
        # Generated analysis file goes to analysis dir
        enriched = xer_exports / 'generated' / 'wbs_taxonomy_enriched.csv'
        if enriched.exists():
            plan.append((enriched, settings.PRIMAVERA_ANALYSIS_DIR / 'wbs_taxonomy_enriched.csv',
                        "Primavera analysis output"))

    # =========================================================================
    # Weekly Reports migrations
    # =========================================================================
    # weekly_reports/tables/ -> processed/weekly_reports/
    wr_tables = windows_data_dir / 'weekly_reports' / 'tables'
    if wr_tables.exists():
        for f in wr_tables.glob('*.csv'):
            plan.append((f, settings.WEEKLY_REPORTS_PROCESSED_DIR / f.name,
                        "Weekly reports parsed table"))

    # weekly_reports/analysis/ -> repo analysis (will be handled separately)
    wr_analysis = windows_data_dir / 'weekly_reports' / 'analysis'
    if wr_analysis.exists():
        for f in wr_analysis.glob('*'):
            if f.is_file():
                plan.append((f, settings.WEEKLY_REPORTS_ANALYSIS_DIR / f.name,
                            "Weekly reports analysis"))

    # =========================================================================
    # TBM migrations
    # =========================================================================
    # tbm/tables/ -> processed/tbm/
    tbm_tables = windows_data_dir / 'tbm' / 'tables'
    if tbm_tables.exists():
        for f in tbm_tables.glob('*.csv'):
            plan.append((f, settings.TBM_PROCESSED_DIR / f.name,
                        "TBM parsed table"))

    # =========================================================================
    # Fieldwire migrations
    # =========================================================================
    # fieldwire/ -> raw/fieldwire/ (these are raw CSV dumps)
    fieldwire = windows_data_dir / 'fieldwire'
    if fieldwire.exists():
        for f in fieldwire.glob('*.csv'):
            plan.append((f, settings.FIELDWIRE_RAW_DIR / f.name,
                        "Fieldwire raw dump"))

    # =========================================================================
    # ProjectSight migrations
    # =========================================================================
    # projectsight/ -> processed/projectsight/
    projectsight = windows_data_dir / 'projectsight'
    if projectsight.exists():
        for f in projectsight.glob('*.csv'):
            plan.append((f, settings.PROJECTSIGHT_PROCESSED_DIR / f.name,
                        "ProjectSight export"))

    # =========================================================================
    # Objective migrations (to analysis)
    # =========================================================================
    objective = windows_data_dir / 'objective'
    if objective.exists():
        for f in objective.glob('*.csv'):
            # These are analysis tracking files - keep in repo
            plan.append((f, settings.ANALYSIS_DIR / f.name,
                        "Analysis tracking"))

    return plan


def execute_migration(plan: list[tuple[Path, Path, str]], dry_run: bool = True):
    """Execute or preview the migration plan."""
    print("=" * 70)
    print("Data Migration Plan")
    print("=" * 70)
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'EXECUTING'}")
    print()

    moved = 0
    skipped = 0
    errors = 0

    for source, dest, desc in plan:
        if not source.exists():
            print(f"[SKIP] Source not found: {source}")
            skipped += 1
            continue

        if dest.exists():
            print(f"[SKIP] Destination exists: {dest}")
            skipped += 1
            continue

        print(f"[{'WOULD MOVE' if dry_run else 'MOVING'}] {desc}")
        print(f"  FROM: {source}")
        print(f"  TO:   {dest}")

        if not dry_run:
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, dest)
                moved += 1
                print(f"  [OK]")
            except Exception as e:
                print(f"  [ERROR] {e}")
                errors += 1
        else:
            moved += 1

        print()

    print("=" * 70)
    print(f"Summary: {moved} files to move, {skipped} skipped, {errors} errors")
    if dry_run:
        print("\nRun without --dry-run to execute migration.")
    print("=" * 70)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Migrate data to new folder structure")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without executing")
    args = parser.parse_args()

    # First ensure directories exist
    print("Ensuring directories exist...")
    settings.ensure_directories()
    print()

    # Generate and execute plan
    plan = get_migration_plan(settings.DATA_DIR)

    if not plan:
        print("No files to migrate.")
        return

    execute_migration(plan, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
