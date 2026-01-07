"""
Pipeline status analysis tool.

Scans file system to determine processing status for N-stage pipelines.
"""

import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from ..config import PipelineConfig, StageConfig
from ..stages.base import FileTask


@dataclass
class StageStatus:
    """Status counts for a single stage."""
    name: str
    completed: int = 0
    failed: int = 0
    pending: int = 0
    blocked: int = 0
    errors: List[tuple] = field(default_factory=list)  # (path, error)


@dataclass
class PipelineStatus:
    """Overall pipeline status."""
    total_files: int = 0
    stages: Dict[str, StageStatus] = field(default_factory=dict)
    files: List[dict] = field(default_factory=list)


def analyze_status(config: PipelineConfig) -> PipelineStatus:
    """
    Analyze pipeline status by scanning file system.

    Args:
        config: Pipeline configuration

    Returns:
        PipelineStatus with counts and details per stage
    """
    from .file_utils import discover_source_files

    status = PipelineStatus()

    # Initialize stage status objects
    for stage in config.stages:
        status.stages[stage.name] = StageStatus(name=stage.name)

    # Discover all input files (with duplicate handling)
    resolved_files, conflicts = discover_source_files(
        config.input_dir, config.file_extensions
    )

    # Note conflicts but don't raise - just report in status
    # (could add conflicts to PipelineStatus if needed for reporting)

    for source_path in resolved_files:
        status.total_files += 1

        relative_path = source_path.relative_to(config.input_dir)
        relative_subdir = relative_path.parent

        task = FileTask(
            source_path=source_path,
            relative_path=relative_path,
            output_base=config.output_dir,
            stem=source_path.stem,
            relative_subdir=relative_subdir,
        )

        file_status = {
            "relative_path": str(relative_path),
            "source_path": str(source_path),
            "stages": {},
        }

        # Check status for each stage
        for stage in config.stages:
            prior_stage = config.get_prior_stage(stage)
            stage_stat = task.stage_status(stage, prior_stage)

            file_status["stages"][stage.name] = stage_stat

            # Update counts
            ss = status.stages[stage.name]
            if stage_stat == "completed":
                ss.completed += 1
            elif stage_stat == "failed":
                ss.failed += 1
                # Read error details
                error_path = task.get_stage_error(stage)
                try:
                    with open(error_path, "r") as f:
                        err_data = json.load(f)
                    ss.errors.append((relative_path, err_data.get("error", "Unknown")))
                except:
                    ss.errors.append((relative_path, "Unknown error"))
            elif stage_stat == "blocked":
                ss.blocked += 1
            else:
                ss.pending += 1

        status.files.append(file_status)

    return status


def print_status(
    status: PipelineStatus,
    show_errors: bool = False,
    verbose: bool = False,
) -> None:
    """Print status summary."""
    print("=" * 60)
    print("Pipeline Status")
    print("=" * 60)
    print(f"Total input files: {status.total_files}")
    print()

    # Print each stage
    for stage_name, ss in status.stages.items():
        print(f"Stage: {stage_name}")
        if status.total_files > 0:
            pct_complete = ss.completed / status.total_files * 100
            pct_failed = ss.failed / status.total_files * 100
            pct_pending = ss.pending / status.total_files * 100
            pct_blocked = ss.blocked / status.total_files * 100
        else:
            pct_complete = pct_failed = pct_pending = pct_blocked = 0

        print(f"  Completed: {ss.completed:>6} ({pct_complete:>5.1f}%)")
        print(f"  Failed:    {ss.failed:>6} ({pct_failed:>5.1f}%)")
        print(f"  Pending:   {ss.pending:>6} ({pct_pending:>5.1f}%)")
        if ss.blocked > 0:
            print(f"  Blocked:   {ss.blocked:>6} ({pct_blocked:>5.1f}%)")
        print()

    # Progress bars
    if status.total_files > 0:
        print("Progress:")
        bar_width = 40
        for stage_name, ss in status.stages.items():
            pct = ss.completed / status.total_files
            filled = int(bar_width * pct)
            print(f"  {stage_name}: [{'#' * filled}{'-' * (bar_width - filled)}] {pct*100:.1f}%")

    # Show errors
    if show_errors:
        for stage_name, ss in status.stages.items():
            if ss.errors:
                print()
                print(f"{stage_name} Errors ({len(ss.errors)}):")
                for path, error in ss.errors[:20]:
                    error_preview = str(error)[:60]
                    print(f"  {path}: {error_preview}...")
                if len(ss.errors) > 20:
                    print(f"  ... and {len(ss.errors) - 20} more")

    # Verbose per-file status
    if verbose:
        print()
        print("Per-file Status:")
        print("-" * 60)
        for f in status.files[:50]:
            icons = []
            for stage_name in status.stages.keys():
                stage_status = f["stages"].get(stage_name, "?")
                icon = {
                    "completed": "+",
                    "failed": "X",
                    "pending": ".",
                    "blocked": "B",
                }.get(stage_status, "?")
                icons.append(icon)
            icon_str = "][".join(icons)
            print(f"  [{icon_str}] {f['relative_path']}")
        if len(status.files) > 50:
            print(f"  ... and {len(status.files) - 50} more files")

    print("=" * 60)


def status_to_dict(status: PipelineStatus) -> dict:
    """Convert status to dictionary for JSON output."""
    return {
        "total_files": status.total_files,
        "stages": {
            name: {
                "completed": ss.completed,
                "failed": ss.failed,
                "pending": ss.pending,
                "blocked": ss.blocked,
            }
            for name, ss in status.stages.items()
        },
        "errors": {
            name: [{"path": str(p), "error": e} for p, e in ss.errors]
            for name, ss in status.stages.items()
            if ss.errors
        },
    }
