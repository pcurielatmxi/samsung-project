"""
Two-stage document processing pipeline.

Stage 1: Gemini extraction (PDF → free text)
Stage 2: Claude formatting (free text → schema-validated JSON)

Usage:
    python pipeline.py <config_dir> [options]

Options:
    --stage 1|2|both    Run specific stage (default: both)
    --force             Reprocess existing files
    --retry-errors      Retry files that previously failed
    --limit N           Process only N files (for testing)
    --dry-run           Show what would be processed without processing
"""

import argparse
import asyncio
import json
import os
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional, Literal

from config import load_config, print_config, PipelineConfig, ConfigValidationError
from gemini_client import process_document as gemini_process, get_document_info

# Add parent for claude imports
sys.path.insert(0, str(Path(__file__).parent.parent / "document_processor"))


@dataclass
class FileTask:
    """A file to be processed."""
    source_path: Path
    relative_path: Path  # Relative to input_dir
    output_dir: Path     # Full output directory for this file
    stem: str            # Filename without extension

    @property
    def extract_output(self) -> Path:
        """Stage 1 output path (in 1.extract/ subdirectory)."""
        return self.output_dir / "1.extract" / f"{self.stem}.extract.json"

    @property
    def extract_error(self) -> Path:
        """Stage 1 error path (in 1.extract/ subdirectory)."""
        return self.output_dir / "1.extract" / f"{self.stem}.extract.error.json"

    @property
    def format_output(self) -> Path:
        """Stage 2 output path (in 2.format/ subdirectory)."""
        return self.output_dir / "2.format" / f"{self.stem}.format.json"

    @property
    def format_error(self) -> Path:
        """Stage 2 error path (in 2.format/ subdirectory)."""
        return self.output_dir / "2.format" / f"{self.stem}.format.error.json"

    def stage1_status(self) -> Literal["completed", "failed", "pending"]:
        """Get Stage 1 status from file system."""
        if self.extract_output.exists():
            return "completed"
        if self.extract_error.exists():
            return "failed"
        return "pending"

    def stage2_status(self) -> Literal["completed", "failed", "pending", "blocked"]:
        """Get Stage 2 status from file system."""
        if self.format_output.exists():
            return "completed"
        if self.format_error.exists():
            return "failed"
        if self.stage1_status() != "completed":
            return "blocked"
        return "pending"


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


def write_json_atomic(path: Path, data: dict) -> None:
    """Write JSON file atomically (write to temp, then rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)

    # Write to temp file in same directory (for atomic rename)
    fd, tmp_path = tempfile.mkstemp(
        suffix=".tmp",
        prefix=path.stem,
        dir=path.parent,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        # Atomic rename
        os.replace(tmp_path, path)
    except Exception:
        # Clean up temp file on error
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def write_error_file(path: Path, source_file: Path, stage: str, error: str, retryable: bool = True) -> None:
    """Write an error marker file."""
    data = {
        "source_file": str(source_file),
        "stage": stage,
        "error": error,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "retryable": retryable,
    }
    write_json_atomic(path, data)


def format_time(seconds: float) -> str:
    """Format seconds to human-readable time."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def calculate_gemini_cost(prompt_tokens: int, output_tokens: int, model: str = "gemini-3-flash-preview") -> float:
    """
    Calculate cost for Gemini API usage.

    Pricing (as of 2024):
    - gemini-3-flash-preview: $0.50 per 1M input tokens, $3.00 per 1M output tokens
    """
    if model == "gemini-3-flash-preview":
        input_cost = (prompt_tokens / 1_000_000) * 0.50
        output_cost = (output_tokens / 1_000_000) * 3.00
    else:
        # Default to flash pricing if unknown
        input_cost = (prompt_tokens / 1_000_000) * 0.50
        output_cost = (output_tokens / 1_000_000) * 3.00

    return input_cost + output_cost


def print_progress(stage: str, processed: int, total: int, success: int, failed: int, elapsed: float,
                   prompt_tokens: int = 0, output_tokens: int = 0, model: str = "gemini-3-flash-preview") -> None:
    """Print progress bar and statistics."""
    pct = (processed / total * 100) if total > 0 else 0
    error_rate = (failed / processed * 100) if processed > 0 else 0
    speed = processed / elapsed if elapsed > 0 else 0

    # Calculate ETA
    remaining = total - processed
    if speed > 0:
        eta_seconds = remaining / speed
        eta_str = format_time(eta_seconds)
    else:
        eta_str = "calculating..."

    # Progress bar (40 chars)
    filled = int(pct / 2.5)
    bar = "█" * filled + "░" * (40 - filled)

    # Calculate cost
    cost = calculate_gemini_cost(prompt_tokens, output_tokens, model)

    print(f"[{stage}] {bar} {pct:6.1f}% | {processed:5d}/{total:5d} | "
          f"✓{success:5d} ✗{failed:4d} ({error_rate:5.1f}%) | "
          f"Tokens: {prompt_tokens:8d}p {output_tokens:8d}o | Cost: ${cost:7.4f} | "
          f"Speed: {speed:5.1f}f/s | ETA: {eta_str:>8s} | Elapsed: {format_time(elapsed)}", flush=True)


async def process_stage1(task: FileTask, config: PipelineConfig) -> bool:
    """
    Process Stage 1: Gemini extraction.

    Returns:
        True if successful, False if failed
    """
    try:
        # Get document info first
        doc_info = get_document_info(task.source_path)
        if not doc_info.is_valid:
            write_error_file(
                task.extract_error,
                task.source_path,
                "extract",
                doc_info.error,
                retryable=False,
            )
            return False

        # Process with Gemini
        response = gemini_process(
            filepath=task.source_path,
            prompt=config.stage1.prompt,
            model=config.stage1.model,
        )

        if not response.success:
            write_error_file(
                task.extract_error,
                task.source_path,
                "extract",
                response.error,
                retryable=True,
            )
            return False

        # Write successful output
        output = {
            "metadata": {
                "source_file": str(task.source_path),
                "relative_path": str(task.relative_path),
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "model": response.model,
                "stage": "extract",
                "usage": response.usage,
                "document": {
                    "size_mb": response.doc_info.file_size_mb if response.doc_info else None,
                    "pages": response.doc_info.page_count if response.doc_info else None,
                },
            },
            "content": response.result,
        }

        write_json_atomic(task.extract_output, output)

        # Remove error file if it exists (successful retry)
        if task.extract_error.exists():
            task.extract_error.unlink()

        return True

    except Exception as e:
        write_error_file(
            task.extract_error,
            task.source_path,
            "extract",
            str(e),
            retryable=True,
        )
        return False


def convert_schema_to_gemini(schema: dict) -> dict:
    """Convert JSON Schema to Gemini-compatible format."""
    def convert_type(t):
        if isinstance(t, list):
            # Handle ["string", "null"] -> STRING with nullable
            non_null = [x for x in t if x != "null"]
            return non_null[0].upper() if non_null else "STRING"
        return t.upper()

    def convert_prop(prop):
        result = {}
        if "type" in prop:
            t = prop["type"]
            if isinstance(t, list):
                result["type"] = convert_type(t)
                if "null" in t:
                    result["nullable"] = True
            else:
                result["type"] = t.upper()
        if "description" in prop:
            result["description"] = prop["description"]
        if "properties" in prop:
            result["properties"] = {k: convert_prop(v) for k, v in prop["properties"].items()}
        if "items" in prop:
            result["items"] = convert_prop(prop["items"])
        return result

    result = {"type": "OBJECT"}
    if "properties" in schema:
        result["properties"] = {k: convert_prop(v) for k, v in schema["properties"].items()}
    if "required" in schema:
        result["required"] = schema["required"]
    return result


async def process_stage2(task: FileTask, config: PipelineConfig) -> bool:
    """
    Process Stage 2: Formatting with Gemini or Claude.

    Returns:
        True if successful, False if failed
    """
    try:
        # Read Stage 1 output
        with open(task.extract_output, "r", encoding="utf-8") as f:
            extract_data = json.load(f)

        extracted_content = extract_data.get("content", "")

        # Build prompt
        full_prompt = f"{config.stage2.prompt}\n\nExtracted content:\n---\n{extracted_content}\n---"

        # Check if using Gemini or Claude
        model = config.stage2.model
        is_gemini = model.startswith("gemini")

        if is_gemini:
            # Use Gemini with structured output
            from gemini_client import process_document_text

            gemini_schema = convert_schema_to_gemini(config.stage2.schema)
            response = process_document_text(
                text=extracted_content,
                prompt=config.stage2.prompt,
                schema=gemini_schema,
                model=model,
            )

            if not response.success:
                write_error_file(
                    task.format_error,
                    task.source_path,
                    "format",
                    response.error,
                    retryable=True,
                )
                return False

            result = response.result
            usage = response.usage
            duration_ms = 0
            cost_usd = 0.0
        else:
            # Use Claude
            from claude_client import ClaudeClient

            client = ClaudeClient(model=model)
            response = await client.analyze_document(
                content=extracted_content,
                prompt=config.stage2.prompt,
                schema=config.stage2.schema,
            )

            if not response.success:
                write_error_file(
                    task.format_error,
                    task.source_path,
                    "format",
                    response.error,
                    retryable=True,
                )
                return False

            result = response.result
            usage = None
            duration_ms = response.duration_ms
            cost_usd = response.cost_usd

        # Write successful output
        output = {
            "metadata": {
                "source_file": str(task.source_path),
                "extract_file": str(task.extract_output),
                "relative_path": str(task.relative_path),
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "model": config.stage2.model,
                "stage": "format",
                "usage": usage,
                "duration_ms": duration_ms,
                "cost_usd": cost_usd,
            },
            "content": result,
        }

        write_json_atomic(task.format_output, output)

        # Remove error file if it exists (successful retry)
        if task.format_error.exists():
            task.format_error.unlink()

        return True

    except Exception as e:
        write_error_file(
            task.format_error,
            task.source_path,
            "format",
            str(e),
            retryable=True,
        )
        return False


async def run_pipeline(
    config: PipelineConfig,
    stage: Literal["1", "2", "both"] = "both",
    force: bool = False,
    retry_errors: bool = False,
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> dict:
    """
    Run the processing pipeline.

    Args:
        config: Pipeline configuration
        stage: Which stage(s) to run
        force: Reprocess even if output exists
        retry_errors: Retry files that previously failed
        limit: Max files to process (for testing)
        dry_run: Show what would be processed without processing

    Returns:
        Summary statistics dict
    """
    # Discover files
    tasks = discover_files(config)
    print(f"Discovered {len(tasks)} input files", flush=True)

    if limit:
        tasks = tasks[:limit]
        print(f"Limited to {limit} files", flush=True)

    # Collect tasks for each stage
    stage1_tasks = []
    stage2_tasks = []

    for task in tasks:
        s1_status = task.stage1_status()
        s2_status = task.stage2_status()

        # Stage 1 eligibility
        if stage in ("1", "both"):
            if force:
                stage1_tasks.append(task)
            elif retry_errors and s1_status == "failed":
                stage1_tasks.append(task)
            elif s1_status == "pending":
                stage1_tasks.append(task)

        # Stage 2 eligibility
        if stage in ("2", "both"):
            if force and s1_status == "completed":
                stage2_tasks.append(task)
            elif retry_errors and s2_status == "failed":
                stage2_tasks.append(task)
            elif s2_status == "pending":
                stage2_tasks.append(task)

    print(f"Stage 1 tasks: {len(stage1_tasks)}", flush=True)
    print(f"Stage 2 tasks: {len(stage2_tasks)}", flush=True)

    if dry_run:
        print("\n[DRY RUN] Would process:", flush=True)
        if stage1_tasks:
            print("\nStage 1:", flush=True)
            for t in stage1_tasks[:10]:
                print(f"  {t.relative_path}", flush=True)
            if len(stage1_tasks) > 10:
                print(f"  ... and {len(stage1_tasks) - 10} more", flush=True)
        if stage2_tasks:
            print("\nStage 2:", flush=True)
            for t in stage2_tasks[:10]:
                print(f"  {t.relative_path}", flush=True)
            if len(stage2_tasks) > 10:
                print(f"  ... and {len(stage2_tasks) - 10} more", flush=True)
        return {"dry_run": True}

    stats = {
        "stage1": {"processed": 0, "success": 0, "failed": 0, "prompt_tokens": 0, "output_tokens": 0, "cost": 0.0},
        "stage2": {"processed": 0, "success": 0, "failed": 0, "prompt_tokens": 0, "output_tokens": 0, "cost": 0.0},
    }

    # Process Stage 1
    if stage1_tasks:
        print(f"\n{'=' * 60}", flush=True)
        print("STAGE 1: Gemini Extraction", flush=True)
        print(f"{'=' * 60}", flush=True)
        print(flush=True)

        stage1_start_time = time.time()
        semaphore = asyncio.Semaphore(config.concurrency)
        stage1_results = {}

        async def process_with_semaphore(idx: int, task: FileTask) -> tuple:
            async with semaphore:
                result = await process_stage1(task, config)
                stage1_results[idx] = result

                # Extract token usage from output file if available
                if result and task.extract_output.exists():
                    try:
                        with open(task.extract_output, "r", encoding="utf-8") as f:
                            extract_data = json.load(f)
                            extract_usage = extract_data.get("metadata", {}).get("usage", {})
                            if extract_usage:
                                stats["stage1"]["prompt_tokens"] += extract_usage.get("prompt_tokens", 0)
                                stats["stage1"]["output_tokens"] += extract_usage.get("output_tokens", 0)
                    except Exception:
                        pass  # Silently skip if we can't extract tokens

                # Print progress immediately when task completes
                elapsed = time.time() - stage1_start_time
                success_count = sum(1 for r in stage1_results.values() if r is True)
                failed_count = sum(1 for r in stage1_results.values() if r is False)

                stats["stage1"]["cost"] = calculate_gemini_cost(
                    stats["stage1"]["prompt_tokens"],
                    stats["stage1"]["output_tokens"],
                    config.stage1.model
                )

                print_progress(
                    "S1",
                    len(stage1_results),
                    len(stage1_tasks),
                    success_count,
                    failed_count,
                    elapsed,
                    stats["stage1"]["prompt_tokens"],
                    stats["stage1"]["output_tokens"],
                    config.stage1.model,
                )
                return idx, result

        results = await asyncio.gather(
            *[process_with_semaphore(i, t) for i, t in enumerate(stage1_tasks)],
            return_exceptions=True,
        )

        for idx, result in results:
            if not isinstance(result, tuple):
                continue  # Skip exception cases
            _, task_result = result
            if task_result:
                stats["stage1"]["success"] += 1
                # Add to stage2 tasks if running both stages
                if stage == "both" and stage1_tasks[idx] not in stage2_tasks:
                    stage2_tasks.append(stage1_tasks[idx])
            else:
                stats["stage1"]["failed"] += 1
            stats["stage1"]["processed"] += 1

        print(flush=True)  # Blank line after stage

    # Process Stage 2
    if stage2_tasks:
        print(f"{'=' * 60}", flush=True)
        print("STAGE 2: Gemini Formatting", flush=True)
        print(f"{'=' * 60}", flush=True)
        print(flush=True)

        stage2_start_time = time.time()
        semaphore = asyncio.Semaphore(config.concurrency)
        stage2_results = {}

        async def process_with_semaphore(idx: int, task: FileTask) -> tuple:
            async with semaphore:
                # Double-check stage 1 is complete
                if task.stage1_status() != "completed":
                    stage2_results[idx] = False
                    return idx, False
                result = await process_stage2(task, config)
                stage2_results[idx] = result

                # Extract token usage from format output file if available
                if result and task.format_output.exists():
                    try:
                        with open(task.format_output, "r", encoding="utf-8") as f:
                            format_data = json.load(f)
                            format_usage = format_data.get("metadata", {}).get("usage", {})
                            if format_usage:
                                stats["stage2"]["prompt_tokens"] += format_usage.get("prompt_tokens", 0)
                                stats["stage2"]["output_tokens"] += format_usage.get("output_tokens", 0)
                    except Exception:
                        pass  # Silently skip if we can't extract tokens

                # Print progress immediately when task completes
                elapsed = time.time() - stage2_start_time
                success_count = sum(1 for r in stage2_results.values() if r is True)
                failed_count = sum(1 for r in stage2_results.values() if r is False)

                stats["stage2"]["cost"] = calculate_gemini_cost(
                    stats["stage2"]["prompt_tokens"],
                    stats["stage2"]["output_tokens"],
                    config.stage2.model
                )

                print_progress(
                    "S2",
                    len(stage2_results),
                    len(stage2_tasks),
                    success_count,
                    failed_count,
                    elapsed,
                    stats["stage2"]["prompt_tokens"],
                    stats["stage2"]["output_tokens"],
                    config.stage2.model,
                )
                return idx, result

        results = await asyncio.gather(
            *[process_with_semaphore(i, t) for i, t in enumerate(stage2_tasks)],
            return_exceptions=True,
        )

        for idx, result in results:
            if not isinstance(result, tuple):
                continue  # Skip exception cases
            _, task_result = result
            if task_result:
                stats["stage2"]["success"] += 1
            else:
                stats["stage2"]["failed"] += 1
            stats["stage2"]["processed"] += 1

        print(flush=True)  # Blank line after stage

    # Print summary
    print(f"{'=' * 60}", flush=True)
    print("FINAL SUMMARY", flush=True)
    print(f"{'=' * 60}", flush=True)

    total_cost = 0.0

    if stats['stage1']['processed'] > 0:
        s1_success_rate = stats['stage1']['success'] / stats['stage1']['processed'] * 100
        print(f"Stage 1 (Extraction): {stats['stage1']['success']}/{stats['stage1']['processed']} succeeded ({s1_success_rate:.1f}%)", flush=True)
        print(f"  └─ Failures: {stats['stage1']['failed']}", flush=True)
        print(f"  └─ Tokens: {stats['stage1']['prompt_tokens']:,} prompt + {stats['stage1']['output_tokens']:,} output = {stats['stage1']['prompt_tokens'] + stats['stage1']['output_tokens']:,} total", flush=True)
        print(f"  └─ Cost: ${stats['stage1']['cost']:.4f}", flush=True)
        total_cost += stats['stage1']['cost']

    if stats['stage2']['processed'] > 0:
        s2_success_rate = stats['stage2']['success'] / stats['stage2']['processed'] * 100
        print(f"Stage 2 (Formatting): {stats['stage2']['success']}/{stats['stage2']['processed']} succeeded ({s2_success_rate:.1f}%)", flush=True)
        print(f"  └─ Failures: {stats['stage2']['failed']}", flush=True)
        print(f"  └─ Tokens: {stats['stage2']['prompt_tokens']:,} prompt + {stats['stage2']['output_tokens']:,} output = {stats['stage2']['prompt_tokens'] + stats['stage2']['output_tokens']:,} total", flush=True)
        print(f"  └─ Cost: ${stats['stage2']['cost']:.4f}", flush=True)
        total_cost += stats['stage2']['cost']

    total_files = stats['stage1']['processed'] + stats['stage2']['processed']
    if total_files == 0:
        print("No files processed", flush=True)
    else:
        print(flush=True)
        print(f"Total Cost: ${total_cost:.4f}", flush=True)

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Two-stage document processing pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "config_dir",
        help="Path to config folder containing config.json, prompts, and schema",
    )
    parser.add_argument(
        "--stage",
        choices=["1", "2", "both"],
        default="both",
        help="Which stage(s) to run (default: both)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess files even if output exists",
    )
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="Retry files that previously failed",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Process only N files (for testing)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without processing",
    )

    args = parser.parse_args()
    main_start_time = time.time()

    # Load and validate config
    try:
        config = load_config(args.config_dir)
    except (FileNotFoundError, ConfigValidationError) as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    print_config(config)
    print()

    # Run pipeline
    try:
        stats = asyncio.run(run_pipeline(
            config=config,
            stage=args.stage,
            force=args.force,
            retry_errors=args.retry_errors,
            limit=args.limit,
            dry_run=args.dry_run,
        ))

        # Print total elapsed time
        if not stats.get("dry_run"):
            total_elapsed = time.time() - main_start_time
            print(f"Total elapsed time: {format_time(total_elapsed)}", flush=True)

    except KeyboardInterrupt:
        print("\nInterrupted by user", flush=True)
        sys.exit(130)
    except Exception as e:
        print(f"ERROR: {e}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
