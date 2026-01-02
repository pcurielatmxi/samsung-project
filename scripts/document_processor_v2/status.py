"""
Pipeline status analysis tool.

Scans file system to determine processing status without requiring a manifest.

Usage:
    python status.py <config_dir> [options]

Options:
    --json          Output as JSON
    --errors        Show error details
    --verbose       Show per-file status
"""

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from config import load_config, PipelineConfig, ConfigValidationError


@dataclass
class FileStatus:
    """Status of a single file."""
    relative_path: Path
    source_path: Path
    stage1: str  # completed, failed, pending
    stage2: str  # completed, failed, pending, blocked
    stage1_error: Optional[str] = None
    stage2_error: Optional[str] = None


@dataclass
class PipelineStatus:
    """Overall pipeline status."""
    total_files: int = 0

    # Stage 1 counts
    stage1_completed: int = 0
    stage1_failed: int = 0
    stage1_pending: int = 0

    # Stage 2 counts
    stage2_completed: int = 0
    stage2_failed: int = 0
    stage2_pending: int = 0
    stage2_blocked: int = 0

    # Error tracking
    stage1_errors: List[tuple] = field(default_factory=list)  # (path, error)
    stage2_errors: List[tuple] = field(default_factory=list)  # (path, error)

    # Per-file status
    files: List[FileStatus] = field(default_factory=list)


def analyze_status(config: PipelineConfig) -> PipelineStatus:
    """
    Analyze pipeline status by scanning file system.

    Args:
        config: Pipeline configuration

    Returns:
        PipelineStatus with counts and details
    """
    status = PipelineStatus()

    # Discover all input files
    for ext in config.file_extensions:
        for source_path in config.input_dir.rglob(f"*{ext}"):
            if not source_path.is_file():
                continue

            status.total_files += 1

            relative_path = source_path.relative_to(config.input_dir)
            output_dir = config.output_dir / relative_path.parent
            stem = source_path.stem

            # Check Stage 1 status
            extract_output = output_dir / f"{stem}.extract.json"
            extract_error = output_dir / f"{stem}.extract.error.json"

            if extract_output.exists():
                s1_status = "completed"
                status.stage1_completed += 1
            elif extract_error.exists():
                s1_status = "failed"
                status.stage1_failed += 1
                # Read error details
                try:
                    with open(extract_error, "r") as f:
                        err_data = json.load(f)
                    status.stage1_errors.append((relative_path, err_data.get("error", "Unknown")))
                except:
                    status.stage1_errors.append((relative_path, "Unknown error"))
            else:
                s1_status = "pending"
                status.stage1_pending += 1

            # Check Stage 2 status
            format_output = output_dir / f"{stem}.format.json"
            format_error = output_dir / f"{stem}.format.error.json"

            s1_error = None
            s2_error = None

            if format_output.exists():
                s2_status = "completed"
                status.stage2_completed += 1
            elif format_error.exists():
                s2_status = "failed"
                status.stage2_failed += 1
                # Read error details
                try:
                    with open(format_error, "r") as f:
                        err_data = json.load(f)
                    s2_error = err_data.get("error", "Unknown")
                    status.stage2_errors.append((relative_path, s2_error))
                except:
                    status.stage2_errors.append((relative_path, "Unknown error"))
            elif s1_status != "completed":
                s2_status = "blocked"
                status.stage2_blocked += 1
            else:
                s2_status = "pending"
                status.stage2_pending += 1

            # Store per-file status
            status.files.append(FileStatus(
                relative_path=relative_path,
                source_path=source_path,
                stage1=s1_status,
                stage2=s2_status,
                stage1_error=s1_error,
                stage2_error=s2_error,
            ))

    return status


def print_status(status: PipelineStatus, show_errors: bool = False, verbose: bool = False) -> None:
    """Print status summary."""
    print("=" * 60)
    print("Pipeline Status")
    print("=" * 60)
    print(f"Total input files: {status.total_files}")
    print()

    # Stage 1
    print("Stage 1 (Extract):")
    if status.total_files > 0:
        pct_complete = status.stage1_completed / status.total_files * 100
        pct_failed = status.stage1_failed / status.total_files * 100
        pct_pending = status.stage1_pending / status.total_files * 100
    else:
        pct_complete = pct_failed = pct_pending = 0

    print(f"  Completed: {status.stage1_completed:>6} ({pct_complete:>5.1f}%)")
    print(f"  Failed:    {status.stage1_failed:>6} ({pct_failed:>5.1f}%)")
    print(f"  Pending:   {status.stage1_pending:>6} ({pct_pending:>5.1f}%)")
    print()

    # Stage 2
    print("Stage 2 (Format):")
    if status.total_files > 0:
        pct_complete = status.stage2_completed / status.total_files * 100
        pct_failed = status.stage2_failed / status.total_files * 100
        pct_pending = status.stage2_pending / status.total_files * 100
        pct_blocked = status.stage2_blocked / status.total_files * 100
    else:
        pct_complete = pct_failed = pct_pending = pct_blocked = 0

    print(f"  Completed: {status.stage2_completed:>6} ({pct_complete:>5.1f}%)")
    print(f"  Failed:    {status.stage2_failed:>6} ({pct_failed:>5.1f}%)")
    print(f"  Pending:   {status.stage2_pending:>6} ({pct_pending:>5.1f}%)")
    print(f"  Blocked:   {status.stage2_blocked:>6} ({pct_blocked:>5.1f}%)")
    print()

    # Progress bar
    if status.total_files > 0:
        s1_pct = status.stage1_completed / status.total_files
        s2_pct = status.stage2_completed / status.total_files

        bar_width = 40
        s1_filled = int(bar_width * s1_pct)
        s2_filled = int(bar_width * s2_pct)

        print("Progress:")
        print(f"  Stage 1: [{'#' * s1_filled}{'-' * (bar_width - s1_filled)}] {s1_pct*100:.1f}%")
        print(f"  Stage 2: [{'#' * s2_filled}{'-' * (bar_width - s2_filled)}] {s2_pct*100:.1f}%")

    # Show errors
    if show_errors:
        if status.stage1_errors:
            print()
            print(f"Stage 1 Errors ({len(status.stage1_errors)}):")
            for path, error in status.stage1_errors[:20]:
                print(f"  {path}: {error[:60]}...")
            if len(status.stage1_errors) > 20:
                print(f"  ... and {len(status.stage1_errors) - 20} more")

        if status.stage2_errors:
            print()
            print(f"Stage 2 Errors ({len(status.stage2_errors)}):")
            for path, error in status.stage2_errors[:20]:
                print(f"  {path}: {error[:60]}...")
            if len(status.stage2_errors) > 20:
                print(f"  ... and {len(status.stage2_errors) - 20} more")

    # Verbose per-file status
    if verbose:
        print()
        print("Per-file Status:")
        print("-" * 60)
        for f in status.files:
            s1_icon = {"completed": "+", "failed": "X", "pending": "."}[f.stage1]
            s2_icon = {"completed": "+", "failed": "X", "pending": ".", "blocked": "B"}[f.stage2]
            print(f"  [{s1_icon}][{s2_icon}] {f.relative_path}")

    print("=" * 60)


def status_to_dict(status: PipelineStatus) -> dict:
    """Convert status to dictionary for JSON output."""
    return {
        "total_files": status.total_files,
        "stage1": {
            "completed": status.stage1_completed,
            "failed": status.stage1_failed,
            "pending": status.stage1_pending,
        },
        "stage2": {
            "completed": status.stage2_completed,
            "failed": status.stage2_failed,
            "pending": status.stage2_pending,
            "blocked": status.stage2_blocked,
        },
        "errors": {
            "stage1": [{"path": str(p), "error": e} for p, e in status.stage1_errors],
            "stage2": [{"path": str(p), "error": e} for p, e in status.stage2_errors],
        },
    }


def main():
    parser = argparse.ArgumentParser(
        description="Analyze pipeline processing status",
    )
    parser.add_argument(
        "config_dir",
        help="Path to config folder",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON",
    )
    parser.add_argument(
        "--errors",
        action="store_true",
        help="Show error details",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show per-file status",
    )

    args = parser.parse_args()

    # Load config
    try:
        config = load_config(args.config_dir)
    except (FileNotFoundError, ConfigValidationError) as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    # Analyze status
    status = analyze_status(config)

    # Output
    if args.json:
        print(json.dumps(status_to_dict(status), indent=2))
    else:
        print_status(status, show_errors=args.errors, verbose=args.verbose)


if __name__ == "__main__":
    main()
