#!/usr/bin/env python3
"""
Daily Data Refresh Script - Phase-Based Pipeline Orchestrator.

Runs all data pipelines through defined phases:
  PREFLIGHT  - Verify dimension tables exist (or rebuild with --rebuild-dimensions)
  PARSE      - File-based incremental parsing
  SCRAPE     - Web-based manifest-tracked scraping
  CONSOLIDATE - Dimension enrichment + fact/quality table split
  VALIDATE   - Schema validation of staged outputs
  COMMIT     - Move successful sources from staging to final location

All outputs go to a staging directory first. Only validated sources are committed.
Failed sources don't block successful ones (partial commits).

Usage:
    python -m scripts.shared.daily_refresh
    python -m scripts.shared.daily_refresh --dry-run
    python -m scripts.shared.daily_refresh --rebuild-dimensions
    python -m scripts.shared.daily_refresh --skip-scrapers
    python -m scripts.shared.daily_refresh --phase consolidate
    python -m scripts.shared.daily_refresh --source tbm
    python -m scripts.shared.daily_refresh --verbose
"""

import argparse
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import settings
from scripts.shared.pipeline_registry import (
    Phase,
    SourceConfig,
    SOURCES,
    DIMENSION_BUILDERS,
    REQUIRED_DIMENSIONS,
    POST_CONSOLIDATION_STEPS,
    get_source_by_name,
    get_sources_for_phase,
)
from scripts.shared.pipeline_utils import (
    StagingContext,
    ensure_dimension_tables_exist,
    rebuild_dimension_tables,
)


def run_module(
    module: str,
    args: list[str],
    description: str,
    dry_run: bool = False,
    verbose: bool = False,
    staging_dir: Optional[Path] = None,
) -> tuple[bool, str]:
    """
    Run a Python module with arguments.

    Args:
        module: Module path (e.g., 'scripts.tbm.process.consolidate_tbm')
        args: Additional command-line arguments
        description: Human-readable description for logging
        dry_run: If True, show what would be run without executing
        verbose: If True, show full command output
        staging_dir: If provided, pass --staging-dir to the module

    Returns:
        Tuple of (success, output/error message)
    """
    print(f"\n{'─' * 60}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {description}")
    print(f"{'─' * 60}")

    # Build command
    cmd = [sys.executable, "-m", module] + args

    # Add staging dir if provided
    if staging_dir:
        cmd.extend(["--staging-dir", str(staging_dir)])

    if dry_run:
        print(f"  [DRY RUN] Would run: {' '.join(cmd)}")
        return True, ""

    if verbose:
        print(f"  Command: {' '.join(cmd)}")

    # Set PYTHONPATH for module imports
    env = os.environ.copy()
    env['PYTHONPATH'] = str(project_root)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(project_root),
            env=env,
        )

        if result.returncode == 0:
            print(f"  ✓ Success")
            if verbose and result.stdout:
                print(result.stdout)
            return True, result.stdout
        else:
            print(f"  ✗ Failed (exit code {result.returncode})")
            if result.stderr:
                # Show first 500 chars of error
                print(f"  Error: {result.stderr[:500]}")
            return False, result.stderr

    except Exception as e:
        print(f"  ✗ Exception: {e}")
        return False, str(e)


def run_preflight(
    rebuild_dimensions: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
) -> bool:
    """
    Run PREFLIGHT phase: verify dimension tables exist.

    Args:
        rebuild_dimensions: If True, rebuild dimensions even if they exist
        dry_run: If True, show what would be done
        verbose: If True, show detailed output

    Returns:
        True if all dimensions exist (or were rebuilt), False otherwise
    """
    print("\n" + "=" * 60)
    print("PHASE: PREFLIGHT")
    print("=" * 60)

    if rebuild_dimensions:
        print("\nRebuilding dimension tables...")
        results = rebuild_dimension_tables(dry_run=dry_run)
        all_success = all(results.values())

        for name, success in results.items():
            status = "✓" if success else "✗"
            print(f"  {status} {name}")

        if not all_success:
            print("\n✗ Failed to rebuild some dimension tables")
            return False

        print("\n✓ Dimension tables rebuilt")
        return True

    # Check if all required dimensions exist
    all_exist, missing = ensure_dimension_tables_exist()

    if all_exist:
        print("\n✓ All required dimension tables exist:")
        for dim in REQUIRED_DIMENSIONS:
            print(f"    - {dim}")
        return True
    else:
        print("\n✗ Missing required dimension tables:")
        for dim in missing:
            print(f"    - {dim}")
        print("\nRun with --rebuild-dimensions to generate them")
        return False


def run_parse_phase(
    sources: list[SourceConfig],
    dry_run: bool = False,
    verbose: bool = False,
) -> dict[str, bool]:
    """
    Run PARSE phase for specified sources.

    Returns:
        Dict of {source_name: success}
    """
    print("\n" + "=" * 60)
    print("PHASE: PARSE")
    print("=" * 60)

    results = {}
    parse_sources = [s for s in sources if s.parse_module]

    if not parse_sources:
        print("\n  No sources to parse")
        return results

    for source in parse_sources:
        success, _ = run_module(
            module=source.parse_module,
            args=source.parse_args,
            description=f"Parse {source.name}: {source.description}",
            dry_run=dry_run,
            verbose=verbose,
        )
        results[source.name] = success

    return results


def run_scrape_phase(
    sources: list[SourceConfig],
    dry_run: bool = False,
    verbose: bool = False,
) -> dict[str, bool]:
    """
    Run SCRAPE phase for specified sources.

    Returns:
        Dict of {source_name: success}
    """
    print("\n" + "=" * 60)
    print("PHASE: SCRAPE")
    print("=" * 60)

    results = {}
    scrape_sources = [s for s in sources if s.scrape_module]

    if not scrape_sources:
        print("\n  No sources to scrape")
        return results

    for source in scrape_sources:
        success, _ = run_module(
            module=source.scrape_module,
            args=source.scrape_args,
            description=f"Scrape {source.name}: {source.description}",
            dry_run=dry_run,
            verbose=verbose,
        )
        results[source.name] = success

    return results


def run_consolidate_phase(
    sources: list[SourceConfig],
    staging_dir: Path,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict[str, bool]:
    """
    Run CONSOLIDATE phase for specified sources.

    All outputs go to staging directory.

    Returns:
        Dict of {source_name: success}
    """
    print("\n" + "=" * 60)
    print("PHASE: CONSOLIDATE")
    print("=" * 60)

    results = {}
    consolidate_sources = [s for s in sources if s.consolidate_module]

    if not consolidate_sources:
        print("\n  No sources to consolidate")
        return results

    for source in consolidate_sources:
        success, _ = run_module(
            module=source.consolidate_module,
            args=source.consolidate_args,
            description=f"Consolidate {source.name}: {source.description}",
            dry_run=dry_run,
            verbose=verbose,
            staging_dir=staging_dir,
        )
        results[source.name] = success

    # Run post-consolidation steps
    print("\n--- Post-consolidation steps ---")
    for step in POST_CONSOLIDATION_STEPS:
        # Check if dependencies are satisfied
        deps = step.get('depends_on', [])
        deps_ok = all(results.get(d, False) for d in deps)

        if not deps_ok:
            print(f"  ○ Skipping {step['name']} (dependencies not met)")
            continue

        success, _ = run_module(
            module=step['module'],
            args=[],
            description=step['description'],
            dry_run=dry_run,
            verbose=verbose,
            staging_dir=staging_dir,
        )
        results[f"post:{step['name']}"] = success

    return results


def run_validate_phase(
    sources: list[SourceConfig],
    staging_dir: Path,
    dry_run: bool = False,
) -> dict[str, bool]:
    """
    Run VALIDATE phase: check all staged files against schemas.

    Returns:
        Dict of {source_name: valid}
    """
    print("\n" + "=" * 60)
    print("PHASE: VALIDATE")
    print("=" * 60)

    if dry_run:
        print("\n  [DRY RUN] Would validate staged files")
        return {s.name: True for s in sources if s.fact_table}

    from schemas.validator import validate_output_file
    from schemas.registry import get_schema_for_file

    results = {}

    for source in sources:
        if not source.fact_table:
            continue

        errors = []

        # Check fact table
        fact_path = staging_dir / source.fact_table
        if not fact_path.exists():
            errors.append(f"Fact table not found: {source.fact_table}")
        else:
            schema = get_schema_for_file(fact_path.name)
            if schema:
                file_errors = validate_output_file(fact_path, schema)
                errors.extend(file_errors)

        # Check data quality table (if expected)
        if source.data_quality_table:
            quality_path = staging_dir / source.data_quality_table
            if not quality_path.exists():
                # Data quality table missing is a warning, not an error
                print(f"  ⚠ {source.name}: Data quality table not found (optional)")

        # Check additional outputs
        for additional in source.additional_outputs:
            add_path = staging_dir / additional
            if not add_path.exists():
                errors.append(f"Additional output not found: {additional}")

        is_valid = len(errors) == 0
        results[source.name] = is_valid

        if is_valid:
            print(f"  ✓ {source.name}: valid")
        else:
            print(f"  ✗ {source.name}:")
            for err in errors[:3]:  # Show first 3 errors
                print(f"      - {err}")

    return results


def run_commit_phase(
    sources: list[SourceConfig],
    staging_dir: Path,
    validation_results: dict[str, bool],
    dry_run: bool = False,
) -> dict[str, str]:
    """
    Run COMMIT phase: move validated files from staging to final location.

    Only commits sources that passed validation.

    Returns:
        Dict of {source_name: status}
    """
    print("\n" + "=" * 60)
    print("PHASE: COMMIT")
    print("=" * 60)

    results = {}

    for source in sources:
        if not source.fact_table:
            continue

        # Skip sources that failed validation
        if not validation_results.get(source.name, False):
            results[source.name] = "skipped (validation failed)"
            print(f"  ○ {source.name}: skipped (validation failed)")
            continue

        if dry_run:
            results[source.name] = "would commit"
            print(f"  [DRY RUN] {source.name}: would commit")
            continue

        try:
            # Collect all files for this source
            files_to_commit = [source.fact_table]
            if source.data_quality_table:
                files_to_commit.append(source.data_quality_table)
            files_to_commit.extend(source.additional_outputs)

            committed = 0
            for rel_path in files_to_commit:
                src = staging_dir / rel_path
                dst = settings.PROCESSED_DATA_DIR / rel_path

                if src.exists():
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(src), str(dst))
                    committed += 1

            results[source.name] = f"committed ({committed} files)"
            print(f"  ✓ {source.name}: committed ({committed} files)")

        except Exception as e:
            results[source.name] = f"error: {e}"
            print(f"  ✗ {source.name}: {e}")

    return results


def print_summary(
    parse_results: dict[str, bool],
    scrape_results: dict[str, bool],
    consolidate_results: dict[str, bool],
    validation_results: dict[str, bool],
    commit_results: dict[str, str],
) -> int:
    """Print final summary and return exit code."""
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    # Combine all results
    all_sources = set()
    all_sources.update(parse_results.keys())
    all_sources.update(scrape_results.keys())
    all_sources.update(consolidate_results.keys())
    all_sources.update(validation_results.keys())
    all_sources.update(commit_results.keys())

    # Remove post-consolidation steps from source list
    all_sources = {s for s in all_sources if not s.startswith('post:')}

    committed = 0
    failed = 0

    for source in sorted(all_sources):
        commit_status = commit_results.get(source, "")
        if "committed" in commit_status:
            print(f"  ✓ {source}")
            committed += 1
        elif "skipped" in commit_status or "validation failed" in commit_status:
            print(f"  ✗ {source}: validation failed")
            failed += 1
        elif "would commit" in commit_status:
            print(f"  ○ {source}: would commit")
        else:
            # Check earlier phases
            parse_ok = parse_results.get(source, True)
            scrape_ok = scrape_results.get(source, True)
            consolidate_ok = consolidate_results.get(source, True)

            if not consolidate_ok:
                print(f"  ✗ {source}: consolidation failed")
                failed += 1
            elif not scrape_ok:
                print(f"  ✗ {source}: scrape failed")
                failed += 1
            elif not parse_ok:
                print(f"  ✗ {source}: parse failed")
                failed += 1
            else:
                print(f"  ? {source}: unknown status")

    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Results: {committed} committed, {failed} failed")

    # Print Power BI output summary
    if committed > 0:
        print("\n" + "-" * 60)
        print("POWER BI DATA SOURCES (processed/)")
        print("-" * 60)
        print("  Fact Tables:")
        print("    - tbm/work_entries.csv")
        print("    - raba/raba_psi_consolidated.csv")
        print("    - projectsight/labor_entries.csv")
        print("    - projectsight/ncr_consolidated.csv")
        print("    - primavera/p6_task_taxonomy.csv")
        print("    - fieldwire/fieldwire_combined.csv")
        print("    - quality/qc_inspections_enriched.csv")
        print("  Data Quality (hide in Power BI):")
        print("    - *_data_quality.csv (1:1 with fact tables)")
        print("  Dimensions:")
        print("    - integrated_analysis/dim_*.csv")

    return 1 if failed > 0 else 0


def main():
    parser = argparse.ArgumentParser(
        description="Run daily data refresh for all pipelines"
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Show what would be run without executing"
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help="Show detailed output"
    )
    parser.add_argument(
        '--skip-scrapers',
        action='store_true',
        help="Skip web scrapers (RABA, PSI)"
    )
    parser.add_argument(
        '--rebuild-dimensions',
        action='store_true',
        help="Rebuild dimension tables before processing"
    )
    parser.add_argument(
        '--phase',
        choices=['preflight', 'parse', 'scrape', 'consolidate', 'validate', 'commit'],
        default=None,
        help="Run only a specific phase (for debugging)"
    )
    parser.add_argument(
        '--source',
        type=str,
        default=None,
        help="Run only a specific source (for debugging)"
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("DAILY DATA REFRESH")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Determine which sources to run
    if args.source:
        source = get_source_by_name(args.source)
        if not source:
            print(f"\nUnknown source: {args.source}")
            print(f"Available: {', '.join(s.name for s in SOURCES)}")
            return 1
        sources = [source]
    else:
        sources = SOURCES

    # Note: --skip-scrapers skips the SCRAPE phase, not the sources
    if args.skip_scrapers:
        print("\n[SKIP] Web scrapers (--skip-scrapers flag)")

    # Initialize result trackers
    parse_results = {}
    scrape_results = {}
    consolidate_results = {}
    validation_results = {}
    commit_results = {}

    # PREFLIGHT phase
    if args.phase is None or args.phase == 'preflight':
        if not run_preflight(
            rebuild_dimensions=args.rebuild_dimensions,
            dry_run=args.dry_run,
            verbose=args.verbose,
        ):
            if not args.rebuild_dimensions:
                print("\n✗ PREFLIGHT failed - dimension tables missing")
                return 1

    if args.phase == 'preflight':
        return 0

    # PARSE phase
    if args.phase is None or args.phase == 'parse':
        parse_results = run_parse_phase(
            sources=sources,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )

    if args.phase == 'parse':
        return 0 if all(parse_results.values()) else 1

    # SCRAPE phase
    if args.phase is None or args.phase == 'scrape':
        if not args.skip_scrapers:
            scrape_results = run_scrape_phase(
                sources=sources,
                dry_run=args.dry_run,
                verbose=args.verbose,
            )

    if args.phase == 'scrape':
        return 0 if all(scrape_results.values()) else 1

    # Set up staging directory for consolidate/validate/commit
    staging_dir = settings.PROCESSED_DATA_DIR / '.staging'

    if not args.dry_run:
        # Clear staging directory
        if staging_dir.exists():
            shutil.rmtree(staging_dir)
        staging_dir.mkdir(parents=True, exist_ok=True)

    try:
        # CONSOLIDATE phase
        if args.phase is None or args.phase == 'consolidate':
            consolidate_results = run_consolidate_phase(
                sources=sources,
                staging_dir=staging_dir,
                dry_run=args.dry_run,
                verbose=args.verbose,
            )

        if args.phase == 'consolidate':
            return 0 if all(consolidate_results.values()) else 1

        # VALIDATE phase
        if args.phase is None or args.phase == 'validate':
            validation_results = run_validate_phase(
                sources=sources,
                staging_dir=staging_dir,
                dry_run=args.dry_run,
            )

        if args.phase == 'validate':
            return 0 if all(validation_results.values()) else 1

        # COMMIT phase
        if args.phase is None or args.phase == 'commit':
            commit_results = run_commit_phase(
                sources=sources,
                staging_dir=staging_dir,
                validation_results=validation_results,
                dry_run=args.dry_run,
            )

    finally:
        # Clean up staging directory
        if not args.dry_run and staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)

    # Print summary
    return print_summary(
        parse_results=parse_results,
        scrape_results=scrape_results,
        consolidate_results=consolidate_results,
        validation_results=validation_results,
        commit_results=commit_results,
    )


if __name__ == "__main__":
    sys.exit(main())
