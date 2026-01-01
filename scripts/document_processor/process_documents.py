#!/usr/bin/env python3
"""
Document Processor - Convert unstructured documents to structured JSON using Claude Code.

This script processes Word documents, PDFs, and text files, analyzing them with Claude
and outputting structured JSON results.

Usage:
    python process_documents.py \
        --input-dir /path/to/documents \
        --output-dir /path/to/output \
        --prompt "Extract key information..." \
        --schema schema.json \
        --concurrency 5 \
        --skip-existing
"""

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from parsers import parse_pdf, parse_pdf_pages, parse_docx, parse_text
from claude_client import ClaudeClient, ClaudeResponse
from chunked_processor import ChunkedDocumentProcessor, result_to_dict
from chunking import analyze_pages, compute_chunks
from utils.logging_config import setup_logging
from utils.tokens import estimate_tokens, is_document_too_large, get_token_stats

logger = logging.getLogger(__name__)

# Supported file extensions
SUPPORTED_EXTENSIONS = {
    ".pdf": parse_pdf,
    ".docx": parse_docx,
    ".doc": parse_docx,  # May not work for old .doc format
    ".txt": parse_text,
    ".md": parse_text,
    ".text": parse_text,
}


@dataclass
class ProcessingStats:
    """Track processing statistics."""
    total_files: int = 0
    processed: int = 0
    skipped_existing: int = 0
    skipped_too_large: int = 0
    skipped_unsupported: int = 0
    errors: int = 0
    successful: int = 0
    total_cost_usd: float = 0.0
    total_duration_ms: int = 0

    # Track files for error rate calculation
    recent_results: List[bool] = field(default_factory=list)

    def record_result(self, success: bool) -> None:
        """Record a processing result."""
        self.recent_results.append(success)
        if success:
            self.successful += 1
        else:
            self.errors += 1

    def get_error_rate(self) -> float:
        """Get error rate for processed files."""
        if not self.recent_results:
            return 0.0
        return self.recent_results.count(False) / len(self.recent_results)

    def should_abort(self, min_files: int = 10, max_error_rate: float = 0.5) -> bool:
        """Check if processing should abort due to high error rate."""
        if len(self.recent_results) < min_files:
            return False
        return self.get_error_rate() > max_error_rate


@dataclass
class FileTask:
    """Represents a file to be processed."""
    source_path: Path
    output_path: Path
    relative_path: Path  # Relative to input directory


class DocumentProcessor:
    """Main document processing orchestrator."""

    def __init__(
        self,
        input_dir: Path,
        output_dir: Path,
        prompt: str,
        schema: Optional[dict] = None,
        model: str = "sonnet",
        concurrency: int = 5,
        skip_existing: bool = False,
        max_tokens: int = 100_000,
        timeout: int = 300,
        max_retries: int = 5,
    ):
        self.input_dir = Path(input_dir).resolve()
        self.output_dir = Path(output_dir).resolve()
        self.prompt = prompt
        self.schema = schema
        self.model = model
        self.concurrency = concurrency
        self.skip_existing = skip_existing
        self.max_tokens = max_tokens
        self.timeout = timeout
        self.max_retries = max_retries

        self.stats = ProcessingStats()
        self.semaphore = asyncio.Semaphore(concurrency)
        self.client = ClaudeClient(
            model=model,
            timeout=timeout,
            max_retries=max_retries,
        )

        # Chunked processor for large PDFs
        self.chunked_processor = ChunkedDocumentProcessor(
            client=self.client,
            user_prompt=prompt,
            user_schema=schema,
            max_tokens=max_tokens,
            target_chunk_tokens=30_000,  # Target ~30K tokens per chunk
            overlap_pages=2,
        )

        # Track skipped files for user notification
        self.skipped_large_files: List[Path] = []
        self.error_files: List[tuple[Path, str]] = []

    def discover_files(self) -> List[FileTask]:
        """Discover all processable files in input directory."""
        tasks = []

        for ext in SUPPORTED_EXTENSIONS:
            for file_path in self.input_dir.rglob(f"*{ext}"):
                if file_path.is_file():
                    relative = file_path.relative_to(self.input_dir)
                    output_path = self.output_dir / relative.with_suffix(".json")
                    tasks.append(FileTask(
                        source_path=file_path,
                        output_path=output_path,
                        relative_path=relative,
                    ))

        # Sort by path for consistent ordering
        tasks.sort(key=lambda t: t.relative_path)
        return tasks

    def should_skip(self, task: FileTask) -> Optional[str]:
        """Check if a file should be skipped."""
        # Check idempotency
        if self.skip_existing and task.output_path.exists():
            return "already_processed"

        # Check extension
        ext = task.source_path.suffix.lower()
        if ext not in SUPPORTED_EXTENSIONS:
            return "unsupported_format"

        return None

    async def process_file(self, task: FileTask) -> bool:
        """
        Process a single file.

        Returns:
            True if successful, False if error
        """
        async with self.semaphore:
            file_path = task.source_path
            logger.info(f"Processing: {task.relative_path}")

            try:
                ext = file_path.suffix.lower()

                # For PDFs, use chunked processing
                if ext == ".pdf":
                    return await self._process_pdf(task)

                # For other formats, use simple processing
                parser = SUPPORTED_EXTENSIONS.get(ext)
                if not parser:
                    logger.warning(f"Unsupported format: {file_path}")
                    self.stats.skipped_unsupported += 1
                    return True  # Not an error, just skipped

                content = parser(file_path)

                # Check token limit for non-PDF files
                token_count = estimate_tokens(content)
                if token_count > self.max_tokens:
                    logger.warning(
                        f"SKIPPED (too large): {task.relative_path} "
                        f"(~{token_count:,} tokens > {self.max_tokens:,} limit)"
                    )
                    self.skipped_large_files.append(file_path)
                    self.stats.skipped_too_large += 1
                    return True

                # Analyze with Claude (non-PDF)
                response = await self.client.analyze_document(
                    content=content,
                    prompt=self.prompt,
                    schema=self.schema,
                    file_path=file_path,
                )

                if not response.success:
                    logger.error(f"Analysis failed: {task.relative_path} - {response.error}")
                    self.error_files.append((file_path, response.error or "Unknown error"))
                    return False

                # Build output (legacy format for non-PDF)
                output = self._build_output(task, response, content)

                # Write output
                task.output_path.parent.mkdir(parents=True, exist_ok=True)
                with open(task.output_path, "w", encoding="utf-8") as f:
                    json.dump(output, f, indent=2, ensure_ascii=False)

                # Update stats
                self.stats.total_cost_usd += response.cost_usd
                self.stats.total_duration_ms += response.duration_ms

                logger.info(
                    f"Completed: {task.relative_path} "
                    f"(${response.cost_usd:.4f}, {response.duration_ms}ms)"
                )
                return True

            except Exception as e:
                logger.error(f"Error processing {task.relative_path}: {e}")
                self.error_files.append((file_path, str(e)))
                return False

    async def _process_pdf(self, task: FileTask) -> bool:
        """
        Process a PDF file using chunked processing.

        Returns:
            True if successful, False if error
        """
        file_path = task.source_path

        try:
            # Parse PDF into pages
            pages = parse_pdf_pages(file_path)

            if not pages:
                logger.warning(f"No pages extracted from {task.relative_path}")
                self.stats.skipped_unsupported += 1
                return True

            # Process with chunked processor
            result = await self.chunked_processor.process_document(
                pages=pages,
                file_path=file_path,
            )

            if not result.success:
                logger.error(f"Analysis failed: {task.relative_path} - {result.error}")
                self.error_files.append((file_path, result.error or "Unknown error"))
                return False

            # Convert to output format
            output = result_to_dict(result)

            # Write output
            task.output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(task.output_path, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, ensure_ascii=False)

            # Update stats from all chunks
            total_cost = sum(
                c.chunk_metadata.get("cost_usd", 0) for c in result.chunks
            )
            total_duration = sum(
                c.chunk_metadata.get("duration_ms", 0) for c in result.chunks
            )
            self.stats.total_cost_usd += total_cost
            self.stats.total_duration_ms += total_duration

            chunks_info = f"{len(result.chunks)} chunk{'s' if len(result.chunks) > 1 else ''}"
            logger.info(
                f"Completed: {task.relative_path} "
                f"({chunks_info}, ${total_cost:.4f}, {total_duration}ms)"
            )
            return True

        except Exception as e:
            logger.error(f"Error processing PDF {task.relative_path}: {e}")
            self.error_files.append((file_path, str(e)))
            return False

    def _build_output(
        self,
        task: FileTask,
        response: ClaudeResponse,
        content: str,
    ) -> dict:
        """Build the output JSON structure."""
        token_stats = get_token_stats(content)

        return {
            "metadata": {
                "source_file": str(task.source_path),
                "relative_path": str(task.relative_path),
                "filename": task.source_path.name,
                "processed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "model": self.model,
                "prompt": self.prompt,
                "duration_ms": response.duration_ms,
                "cost_usd": response.cost_usd,
                "session_id": response.session_id,
                "document_stats": token_stats,
            },
            "content": response.result,
        }

    async def run(self) -> ProcessingStats:
        """Run the document processing pipeline."""
        logger.info(f"Input directory: {self.input_dir}")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Concurrency: {self.concurrency}")
        logger.info(f"Model: {self.model}")
        logger.info(f"Max tokens: {self.max_tokens:,}")
        logger.info(f"Skip existing: {self.skip_existing}")
        logger.info("")

        # Discover files
        tasks = self.discover_files()
        self.stats.total_files = len(tasks)
        logger.info(f"Discovered {len(tasks)} files to process")

        if not tasks:
            logger.warning("No files found to process")
            return self.stats

        # Filter tasks
        pending_tasks = []
        for task in tasks:
            skip_reason = self.should_skip(task)
            if skip_reason == "already_processed":
                self.stats.skipped_existing += 1
                logger.debug(f"Skipping (already exists): {task.relative_path}")
            elif skip_reason == "unsupported_format":
                self.stats.skipped_unsupported += 1
            else:
                pending_tasks.append(task)

        if self.stats.skipped_existing > 0:
            logger.info(f"Skipping {self.stats.skipped_existing} already processed files")

        if not pending_tasks:
            logger.info("All files already processed")
            return self.stats

        logger.info(f"Processing {len(pending_tasks)} files...")
        logger.info("")

        # Process files with progress tracking
        for i, task in enumerate(pending_tasks, 1):
            # Check abort condition
            if self.stats.should_abort():
                error_rate = self.stats.get_error_rate()
                logger.error(
                    f"ABORTING: Error rate {error_rate:.0%} exceeds 50% threshold "
                    f"after {len(self.stats.recent_results)} files"
                )
                break

            # Progress indicator
            logger.info(f"[{i}/{len(pending_tasks)}] {task.relative_path}")

            # Process file
            success = await self.process_file(task)
            self.stats.record_result(success)
            self.stats.processed += 1

        # Print summary
        self._print_summary()

        return self.stats

    def _print_summary(self) -> None:
        """Print processing summary."""
        logger.info("")
        logger.info("=" * 60)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total files discovered: {self.stats.total_files}")
        logger.info(f"Successfully processed: {self.stats.successful}")
        logger.info(f"Errors: {self.stats.errors}")
        logger.info(f"Skipped (already exists): {self.stats.skipped_existing}")
        logger.info(f"Skipped (too large): {self.stats.skipped_too_large}")
        logger.info(f"Skipped (unsupported): {self.stats.skipped_unsupported}")
        logger.info(f"Total cost: ${self.stats.total_cost_usd:.4f}")
        logger.info(f"Total API time: {self.stats.total_duration_ms / 1000:.1f}s")

        # Notify about skipped large files
        if self.skipped_large_files:
            logger.warning("")
            logger.warning("FILES SKIPPED DUE TO SIZE (require manual splitting):")
            for f in self.skipped_large_files:
                logger.warning(f"  - {f}")

        # Notify about errors
        if self.error_files:
            logger.error("")
            logger.error("FILES WITH ERRORS:")
            for f, err in self.error_files:
                logger.error(f"  - {f}: {err}")


def load_schema(schema_path: str) -> dict:
    """Load JSON schema from file."""
    path = Path(schema_path)
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_prompt(prompt_path: str) -> str:
    """Load prompt from file."""
    path = Path(prompt_path)
    if not path.exists():
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")

    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def main():
    parser = argparse.ArgumentParser(
        description="Process documents with Claude Code for structured extraction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python process_documents.py -i ./docs -o ./output \\
      --prompt-file prompt.txt --schema schema.json

  # With concurrency and skip existing
  python process_documents.py -i ./docs -o ./output \\
      --prompt-file prompt.txt --schema schema.json \\
      --concurrency 10 --skip-existing

  # With specific model
  python process_documents.py -i ./docs -o ./output \\
      --prompt-file prompt.txt --schema schema.json \\
      --model opus --timeout 600
        """,
    )

    parser.add_argument(
        "-i", "--input-dir",
        required=True,
        help="Input directory containing documents",
    )
    parser.add_argument(
        "-o", "--output-dir",
        required=True,
        help="Output directory for JSON results",
    )
    parser.add_argument(
        "--prompt-file",
        required=True,
        help="Path to prompt file for document analysis",
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="Path to JSON schema file for expected output structure",
    )
    parser.add_argument(
        "--model",
        default="sonnet",
        choices=["sonnet", "opus", "haiku"],
        help="Claude model to use (default: sonnet)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Maximum concurrent file processing (default: 5)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip files that already have output JSON",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=100_000,
        help="Maximum tokens per document before skipping (default: 100000)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Timeout in seconds per document (default: 300)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Maximum retries for rate limits (default: 5)",
    )
    parser.add_argument(
        "--log-file",
        help="Path to log file",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Setup logging
    log_path = Path(args.log_file) if args.log_file else None
    setup_logging(log_file=log_path, verbose=args.verbose)

    # Load prompt from file
    try:
        prompt = load_prompt(args.prompt_file)
    except Exception as e:
        logger.error(f"Failed to load prompt file: {e}")
        sys.exit(1)

    if not prompt:
        logger.error("Prompt file is empty.")
        sys.exit(1)

    # Load schema from file
    try:
        schema = load_schema(args.schema)
    except Exception as e:
        logger.error(f"Failed to load schema: {e}")
        sys.exit(1)

    if not schema:
        logger.error("Schema file is empty or invalid.")
        sys.exit(1)

    # Validate directories
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        sys.exit(1)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create processor and run
    processor = DocumentProcessor(
        input_dir=input_dir,
        output_dir=output_dir,
        prompt=prompt,
        schema=schema,
        model=args.model,
        concurrency=args.concurrency,
        skip_existing=args.skip_existing,
        max_tokens=args.max_tokens,
        timeout=args.timeout,
        max_retries=args.max_retries,
    )

    try:
        stats = asyncio.run(processor.run())
        # Exit with error code if there were failures
        if stats.errors > 0:
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
