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
from dataclasses import dataclass
from datetime import datetime, timezone
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
        """Stage 1 output path."""
        return self.output_dir / f"{self.stem}.extract.json"

    @property
    def extract_error(self) -> Path:
        """Stage 1 error path."""
        return self.output_dir / f"{self.stem}.extract.error.json"

    @property
    def format_output(self) -> Path:
        """Stage 2 output path."""
        return self.output_dir / f"{self.stem}.format.json"

    @property
    def format_error(self) -> Path:
        """Stage 2 error path."""
        return self.output_dir / f"{self.stem}.format.error.json"

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
    print(f"Discovered {len(tasks)} input files")

    if limit:
        tasks = tasks[:limit]
        print(f"Limited to {limit} files")

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

    print(f"Stage 1 tasks: {len(stage1_tasks)}")
    print(f"Stage 2 tasks: {len(stage2_tasks)}")

    if dry_run:
        print("\n[DRY RUN] Would process:")
        if stage1_tasks:
            print("\nStage 1:")
            for t in stage1_tasks[:10]:
                print(f"  {t.relative_path}")
            if len(stage1_tasks) > 10:
                print(f"  ... and {len(stage1_tasks) - 10} more")
        if stage2_tasks:
            print("\nStage 2:")
            for t in stage2_tasks[:10]:
                print(f"  {t.relative_path}")
            if len(stage2_tasks) > 10:
                print(f"  ... and {len(stage2_tasks) - 10} more")
        return {"dry_run": True}

    stats = {
        "stage1": {"processed": 0, "success": 0, "failed": 0},
        "stage2": {"processed": 0, "success": 0, "failed": 0},
    }

    # Process Stage 1
    if stage1_tasks:
        print(f"\n{'=' * 60}")
        print("STAGE 1: Gemini Extraction")
        print(f"{'=' * 60}")

        semaphore = asyncio.Semaphore(config.concurrency)

        async def process_with_semaphore(task: FileTask) -> bool:
            async with semaphore:
                print(f"[S1] Processing: {task.relative_path}")
                return await process_stage1(task, config)

        results = await asyncio.gather(
            *[process_with_semaphore(t) for t in stage1_tasks],
            return_exceptions=True,
        )

        for task, result in zip(stage1_tasks, results):
            stats["stage1"]["processed"] += 1
            if isinstance(result, Exception):
                print(f"[S1] ERROR: {task.relative_path} - {result}")
                stats["stage1"]["failed"] += 1
            elif result:
                print(f"[S1] OK: {task.relative_path}")
                stats["stage1"]["success"] += 1
                # Add to stage2 tasks if running both stages
                if stage == "both" and task not in stage2_tasks:
                    stage2_tasks.append(task)
            else:
                print(f"[S1] FAILED: {task.relative_path}")
                stats["stage1"]["failed"] += 1

    # Process Stage 2
    if stage2_tasks:
        print(f"\n{'=' * 60}")
        print("STAGE 2: Claude Formatting")
        print(f"{'=' * 60}")

        semaphore = asyncio.Semaphore(config.concurrency)

        async def process_with_semaphore(task: FileTask) -> bool:
            async with semaphore:
                # Double-check stage 1 is complete
                if task.stage1_status() != "completed":
                    print(f"[S2] BLOCKED: {task.relative_path} (Stage 1 not complete)")
                    return False
                print(f"[S2] Processing: {task.relative_path}")
                return await process_stage2(task, config)

        results = await asyncio.gather(
            *[process_with_semaphore(t) for t in stage2_tasks],
            return_exceptions=True,
        )

        for task, result in zip(stage2_tasks, results):
            stats["stage2"]["processed"] += 1
            if isinstance(result, Exception):
                print(f"[S2] ERROR: {task.relative_path} - {result}")
                stats["stage2"]["failed"] += 1
            elif result:
                print(f"[S2] OK: {task.relative_path}")
                stats["stage2"]["success"] += 1
            else:
                print(f"[S2] FAILED: {task.relative_path}")
                stats["stage2"]["failed"] += 1

    # Print summary
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    print(f"Stage 1: {stats['stage1']['success']}/{stats['stage1']['processed']} succeeded")
    print(f"Stage 2: {stats['stage2']['success']}/{stats['stage2']['processed']} succeeded")

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
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
