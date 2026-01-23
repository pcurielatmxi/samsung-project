#!/usr/bin/env python3
"""
Daily Data Refresh Script

Runs all incremental data pipelines to check for and process new data.
Each pipeline is idempotent and will skip already-processed files.

Usage:
    python -m scripts.shared.daily_refresh
    python -m scripts.shared.daily_refresh --dry-run
    python -m scripts.shared.daily_refresh --verbose
"""

import subprocess
import sys
import argparse
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


def run_command(cmd: list[str], description: str, dry_run: bool = False, verbose: bool = False) -> tuple[bool, str]:
    """Run a command and return (success, output)."""
    print(f"\n{'=' * 60}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {description}")
    print(f"{'=' * 60}")

    if dry_run:
        print(f"  DRY-RUN: Would run: {' '.join(cmd)}")
        return True, ""

    if verbose:
        print(f"  Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(project_root)
        )

        if result.returncode == 0:
            print(f"  ✓ Success")
            if verbose and result.stdout:
                print(result.stdout)
            return True, result.stdout
        else:
            print(f"  ✗ Failed (exit code {result.returncode})")
            if result.stderr:
                print(f"  Error: {result.stderr[:500]}")
            return False, result.stderr

    except Exception as e:
        print(f"  ✗ Exception: {e}")
        return False, str(e)


def main():
    parser = argparse.ArgumentParser(description="Run daily data refresh for all pipelines")
    parser.add_argument('--dry-run', action='store_true', help="Show what would be run without executing")
    parser.add_argument('--verbose', '-v', action='store_true', help="Show detailed output")
    parser.add_argument('--skip-scrapers', action='store_true', help="Skip web scrapers (RABA, PSI)")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("DAILY DATA REFRESH")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    results = []
    python = sys.executable

    # =========================================================================
    # PARSERS (file-based, incremental)
    # =========================================================================

    # 1. TBM Daily Plans - incremental parsing
    success, _ = run_command(
        [python, "-m", "scripts.tbm.process.parse_tbm_daily_plans", "--incremental"],
        "TBM Daily Plans (incremental)",
        dry_run=args.dry_run,
        verbose=args.verbose
    )
    results.append(("TBM Daily Plans", success))

    # 2. Primavera XER - incremental processing
    success, _ = run_command(
        [python, "-m", "scripts.primavera.process.batch_process_xer", "--incremental"],
        "Primavera XER Files (incremental)",
        dry_run=args.dry_run,
        verbose=args.verbose
    )
    results.append(("Primavera XER", success))

    # 3. ProjectSight Labor - incremental parsing
    success, _ = run_command(
        [python, "-m", "scripts.projectsight.process.parse_labor_from_json", "--incremental"],
        "ProjectSight Labor (incremental)",
        dry_run=args.dry_run,
        verbose=args.verbose
    )
    results.append(("ProjectSight Labor", success))

    # =========================================================================
    # SCRAPERS (web-based, manifest-tracked)
    # =========================================================================

    if not args.skip_scrapers:
        # 4. RABA Individual Reports - manifest-based idempotency
        success, _ = run_command(
            [python, "-m", "scripts.raba.process.scrape_raba_individual", "--headless"],
            "RABA Reports (scraper)",
            dry_run=args.dry_run,
            verbose=args.verbose
        )
        results.append(("RABA Scraper", success))

        # 5. PSI Reports - manifest-based idempotency
        success, _ = run_command(
            [python, "-m", "scripts.psi.process.scrape_psi_reports", "--headless"],
            "PSI Reports (scraper)",
            dry_run=args.dry_run,
            verbose=args.verbose
        )
        results.append(("PSI Scraper", success))
    else:
        print("\n[SKIPPED] Web scrapers (--skip-scrapers flag)")

    # =========================================================================
    # PROJECTSIGHT DAILY REPORTS - NOT READY
    # =========================================================================
    # TODO: The ProjectSight daily reports scraper needs work:
    #
    # 1. Date Filter Issue: The scraper cannot efficiently navigate to a target
    #    date range. The Infragistics grid's filter API returns "Grid not found"
    #    and fallback approaches only sort, not filter. This means the scraper
    #    would iterate through all 1000+ reports instead of jumping to recent ones.
    #
    # 2. Session Storage: ProjectSight uses Trimble Identity with 2FA (email code).
    #    To avoid repeated logins, we should:
    #    - Use Playwright's browser context storage_state() to save session cookies
    #    - Load saved session on subsequent runs
    #    - Only prompt for 2FA when session expires
    #    See: https://playwright.dev/python/docs/auth#reuse-signed-in-state
    #
    # When ready, uncomment the following:
    #
    # success, _ = run_command(
    #     [python, "-m", "scripts.projectsight.process.scrape_projectsight_daily_reports",
    #      "--redownload-days", "14", "--headless"],
    #     "ProjectSight Daily Reports (scraper)",
    #     dry_run=args.dry_run,
    #     verbose=args.verbose
    # )
    # results.append(("ProjectSight Daily Reports", success))

    # =========================================================================
    # SUMMARY
    # =========================================================================

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    succeeded = sum(1 for _, s in results if s)
    failed = sum(1 for _, s in results if not s)

    for name, success in results:
        status = "✓" if success else "✗"
        print(f"  {status} {name}")

    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Results: {succeeded} succeeded, {failed} failed")

    if failed > 0:
        print("\nNote: Check sync_log.csv for detailed pipeline logs")
        sys.exit(1)

    return 0


if __name__ == "__main__":
    sys.exit(main())
