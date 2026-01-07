"""
N-stage document processing pipeline with quality checking.

Processes documents through a configurable sequence of LLM and script stages,
with optional quality checking and automatic halt on high failure rates.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Set

from .config import PipelineConfig, StageConfig
from .stages.base import BaseStage, FileTask, StageResult
from .stages.registry import create_stage
from .quality_check import (
    QCTracker,
    check_qc_halt,
    write_qc_halt,
    write_qc_result_file,
    run_quality_check,
)
from .utils.file_utils import (
    write_json_atomic,
    write_error_file,
    write_stage_output,
    format_time,
)
from .utils.progress import ProgressDisplay

logger = logging.getLogger(__name__)

# Global progress display (set in run_pipeline)
_progress: Optional[ProgressDisplay] = None


@dataclass
class ProcessingStats:
    """Statistics for a pipeline run."""
    stage_name: str
    total_files: int = 0           # Files to process this run
    processed: int = 0             # Successfully processed
    errors: int = 0                # Failed during processing
    # Pre-run counts (for logging)
    count_completed: int = 0       # Already done (skipped)
    count_failed: int = 0          # Had errors (will retry)
    count_blocked: int = 0         # Waiting for prior stage
    count_pending: int = 0         # New files to process
    # Other stats
    total_tokens: int = 0
    start_time: float = 0
    qc_samples: int = 0
    qc_failures: int = 0


def discover_files(config: PipelineConfig) -> List[FileTask]:
    """Discover all input files matching configured extensions."""
    tasks = []

    for ext in config.file_extensions:
        for source_path in config.input_dir.rglob(f"*{ext}"):
            if source_path.is_file():
                relative_path = source_path.relative_to(config.input_dir)
                output_dir = config.output_dir / relative_path.parent

                tasks.append(FileTask(
                    source_path=source_path,
                    relative_path=relative_path,
                    output_dir=output_dir,
                    stem=source_path.stem,
                ))

    # Sort for consistent ordering
    tasks.sort(key=lambda t: t.relative_path)
    return tasks


async def process_single_file(
    stage_impl: BaseStage,
    stage_config: StageConfig,
    prior_stage: Optional[StageConfig],
    task: FileTask,
    stats: ProcessingStats,
    enable_enhance: bool = False,
) -> bool:
    """
    Process a single file through a stage.

    Returns:
        True if processing succeeded, False otherwise
    """
    global _progress

    input_path = task.get_stage_input(stage_config, prior_stage)
    output_path = task.get_stage_output(stage_config)
    error_path = task.get_stage_error(stage_config)

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Report file start
    if _progress:
        _progress.file_start(task.stem)

    try:
        result = await stage_impl.process(task, input_path, enable_enhance=enable_enhance)

        if result.success:
            # Write successful output
            extra_metadata = {}
            if stage_config.type == "llm":
                extra_metadata["model"] = stage_config.model

            write_stage_output(
                path=output_path,
                content=result.result,
                source_file=task.source_path,
                stage=stage_config.name,
                model=stage_config.model if stage_config.type == "llm" else None,
                usage=result.usage,
            )

            # Remove error file if it exists (from previous failed attempt)
            if error_path.exists():
                error_path.unlink()

            stats.processed += 1
            input_tokens = 0
            output_tokens = 0
            if result.usage:
                input_tokens = result.usage.get("prompt_tokens", 0) or 0
                output_tokens = result.usage.get("output_tokens", 0) or 0
                stats.total_tokens += input_tokens + output_tokens

            # Report success
            if _progress:
                _progress.file_complete(
                    task.stem,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    duration_ms=result.duration_ms or 0,
                )

            return True
        else:
            # Write error file
            write_error_file(
                path=error_path,
                source_file=task.source_path,
                stage=stage_config.name,
                error=result.error or "Unknown error",
                retryable=result.retryable,
            )
            stats.errors += 1

            # Report error
            if _progress:
                _progress.file_error(
                    task.stem,
                    error=result.error or "Unknown error",
                    retryable=result.retryable,
                    duration_ms=result.duration_ms or 0,
                )

            return False

    except Exception as e:
        # Unexpected error
        write_error_file(
            path=error_path,
            source_file=task.source_path,
            stage=stage_config.name,
            error=str(e),
            retryable=True,
        )
        stats.errors += 1

        # Report error
        if _progress:
            _progress.file_error(task.stem, error=str(e), retryable=True)

        return False


async def run_stage(
    config: PipelineConfig,
    stage_config: StageConfig,
    tasks: List[FileTask],
    force: bool = False,
    retry_errors: bool = False,
    limit: Optional[int] = None,
    dry_run: bool = False,
    disable_qc: bool = False,
    enable_enhance: bool = False,
) -> tuple[ProcessingStats, Optional[QCTracker]]:
    """
    Run a single stage of the pipeline.

    Returns:
        Tuple of (stats, qc_tracker or None)
    """
    global _progress

    stats = ProcessingStats(stage_name=stage_config.name)
    stats.start_time = time.time()

    prior_stage = config.get_prior_stage(stage_config)
    stage_impl = create_stage(stage_config, config.config_dir)

    # Initialize QC tracker if stage has QC
    qc_tracker = None
    if stage_config.has_qc and not disable_qc:
        qc_tracker = QCTracker(stage_name=stage_config.name)

    # First pass: count all statuses
    for task in tasks:
        status = task.stage_status(stage_config, prior_stage)
        if status == "completed":
            stats.count_completed += 1
        elif status == "failed":
            stats.count_failed += 1
        elif status == "blocked":
            stats.count_blocked += 1
        elif status == "pending":
            stats.count_pending += 1

    # Second pass: determine which tasks to process
    eligible_tasks = []
    for task in tasks:
        status = task.stage_status(stage_config, prior_stage)

        if force:
            # Include if prior stage complete (or no prior stage)
            if prior_stage is None:
                eligible_tasks.append(task)
            elif status != "blocked":
                eligible_tasks.append(task)
        elif retry_errors:
            # Only retry failed files
            if status == "failed":
                eligible_tasks.append(task)
        elif status == "pending":
            eligible_tasks.append(task)
        elif status == "failed":
            # Retry failed files by default
            eligible_tasks.append(task)

    # Apply limit
    if limit and len(eligible_tasks) > limit:
        eligible_tasks = eligible_tasks[:limit]

    stats.total_files = len(eligible_tasks)

    if dry_run:
        print(f"Stage '{stage_config.name}': Would process {stats.total_files} files")
        return stats, qc_tracker

    # Report stage start
    if _progress:
        _progress.stage_start(
            stage_config.name,
            total_files=len(tasks),
            to_process=stats.total_files,
            completed=stats.count_completed,
            failed=stats.count_failed,
            blocked=stats.count_blocked,
            pending=stats.count_pending,
            model=stage_config.model if stage_config.type == "llm" else None,
        )

    if stats.total_files == 0:
        return stats, qc_tracker

    # Process with concurrency control
    semaphore = asyncio.Semaphore(config.concurrency)

    async def process_with_semaphore(task: FileTask) -> tuple[FileTask, bool]:
        async with semaphore:
            success = await process_single_file(
                stage_impl=stage_impl,
                stage_config=stage_config,
                prior_stage=prior_stage,
                task=task,
                stats=stats,
                enable_enhance=enable_enhance,
            )
            return task, success

    # Process in batches for QC sampling
    batch_size = config.qc_batch_size

    for i in range(0, len(eligible_tasks), batch_size):
        batch = eligible_tasks[i:i + batch_size]

        # Process batch concurrently
        results = await asyncio.gather(
            *[process_with_semaphore(task) for task in batch]
        )

        # Collect successful tasks for potential QC sampling
        successful_tasks = [task for task, success in results if success]

        # QC sampling: check 1 file per batch
        if qc_tracker and successful_tasks and stage_config.has_qc:
            # Sample the first successful file in batch
            sample_task = successful_tasks[0]
            input_path = sample_task.get_stage_input(stage_config, prior_stage)
            output_path = sample_task.get_stage_output(stage_config)

            try:
                qc_result = await run_quality_check(
                    stage=stage_config,
                    input_path=input_path,
                    output_path=output_path,
                    model=stage_config.model,
                )
                qc_tracker.add_result(qc_result)
                stats.qc_samples += 1

                # Persist QC result to file
                write_qc_result_file(
                    output_path=output_path,
                    stage_name=stage_config.name,
                    result=qc_result,
                )

                if not qc_result.passed:
                    stats.qc_failures += 1
                    if _progress:
                        _progress.qc_result(False, sample_task.stem, qc_result.reason)
                else:
                    if _progress:
                        _progress.qc_result(True, sample_task.stem)

                # Check if we should halt
                if qc_tracker.should_halt(
                    config.qc_failure_threshold,
                    config.qc_min_samples,
                ):
                    if _progress:
                        _progress.qc_halt(qc_tracker.failure_rate, config.qc_failure_threshold)
                    write_qc_halt(config.output_dir, qc_tracker, config.qc_failure_threshold)
                    break

            except Exception as e:
                logger.warning(f"QC check failed: {e}")

    # Report stage complete
    if _progress:
        _progress.stage_complete({
            "processed": stats.processed,
            "errors": stats.errors,
            "total_tokens": stats.total_tokens,
        })

    return stats, qc_tracker


async def run_pipeline(
    config: PipelineConfig,
    stages: Optional[List[str]] = None,
    force: bool = False,
    retry_errors: bool = False,
    limit: Optional[int] = None,
    dry_run: bool = False,
    bypass_qc_halt: bool = False,
    disable_qc: bool = False,
    enable_enhance: bool = False,
    verbose: bool = False,
) -> dict:
    """
    Run the N-stage processing pipeline.

    Args:
        config: Pipeline configuration
        stages: List of stage names to run (None = all)
        force: Reprocess completed files
        retry_errors: Only retry failed files
        limit: Maximum files to process per stage
        dry_run: Show what would be processed without processing
        bypass_qc_halt: Continue despite QC halt file
        disable_qc: Skip quality checks entirely
        enable_enhance: Enable enhancement pass for LLM stages
        verbose: Show per-file error messages

    Returns:
        Dictionary with results and statistics
    """
    global _progress

    # Initialize progress display
    log_file = config.output_dir / "pipeline.log" if not dry_run else None
    _progress = ProgressDisplay(log_file=log_file, verbose=verbose)

    # Check for QC halt
    if not bypass_qc_halt:
        halt_data = check_qc_halt(config.output_dir)
        if halt_data:
            print("=" * 60)
            print("PIPELINE HALTED")
            print("=" * 60)
            print(f"Stage: {halt_data.get('stage', 'unknown')}")
            print(f"Failure rate: {halt_data.get('failure_rate', 0)*100:.1f}%")
            print(f"Message: {halt_data.get('message', 'QC failure threshold exceeded')}")
            print()
            print("To continue:")
            print("  1. Review and fix the prompt, then delete .qc_halt.json")
            print("  2. Or run with --bypass-qc-halt to continue despite failures")
            print("=" * 60)
            return {"halted": True, "halt_data": halt_data}

    # Discover files
    all_tasks = discover_files(config)

    # Determine which stages to run
    if stages:
        stages_to_run = [s for s in config.stages if s.name in stages]
    else:
        stages_to_run = config.stages

    if not stages_to_run:
        print("No stages to run")
        return {"stages": []}

    # Report pipeline start
    if not dry_run:
        _progress.pipeline_start(len(all_tasks), stages_to_run)
    else:
        print(f"Discovered {len(all_tasks)} input files")
        print(f"Running {len(stages_to_run)} stage(s): {[s.name for s in stages_to_run]}")

    results = {"stages": [], "halted": False}

    # Run each stage
    for stage_config in stages_to_run:
        stats, qc_tracker = await run_stage(
            config=config,
            stage_config=stage_config,
            tasks=all_tasks,
            force=force,
            retry_errors=retry_errors,
            limit=limit,
            dry_run=dry_run,
            disable_qc=disable_qc,
            enable_enhance=enable_enhance,
        )

        elapsed = time.time() - stats.start_time if stats.start_time else 0

        stage_result = {
            "name": stage_config.name,
            "total_files": stats.total_files,
            "processed": stats.processed,
            "errors": stats.errors,
            "count_completed": stats.count_completed,
            "count_failed": stats.count_failed,
            "count_blocked": stats.count_blocked,
            "count_pending": stats.count_pending,
            "total_tokens": stats.total_tokens,
            "elapsed_seconds": elapsed,
            "qc_samples": stats.qc_samples,
            "qc_failures": stats.qc_failures,
        }
        results["stages"].append(stage_result)

        # Check if we should halt due to QC
        if qc_tracker and qc_tracker.should_halt(
            config.qc_failure_threshold,
            config.qc_min_samples,
        ):
            results["halted"] = True
            results["halt_stage"] = stage_config.name
            break

    # Report pipeline complete
    if not dry_run:
        _progress.pipeline_complete(results)

    return results
