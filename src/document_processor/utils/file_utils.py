"""
File utility functions for document processing pipeline.
"""

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


def write_json_atomic(path: Path, data: dict) -> None:
    """
    Write JSON file atomically (write to temp, then rename).

    Args:
        path: Target file path
        data: Data to write as JSON
    """
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


def write_error_file(
    path: Path,
    source_file: Path,
    stage: str,
    error: str,
    retryable: bool = True,
) -> None:
    """
    Write an error marker file.

    Args:
        path: Error file path
        source_file: Original source file
        stage: Stage name where error occurred
        error: Error message
        retryable: Whether the error is retryable
    """
    data = {
        "source_file": str(source_file),
        "stage": stage,
        "error": error,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "retryable": retryable,
    }
    write_json_atomic(path, data)


def write_stage_output(
    path: Path,
    content: Any,
    source_file: Path,
    stage: str,
    model: str = None,
    usage: dict = None,
    extra_metadata: dict = None,
) -> None:
    """
    Write stage output with standardized metadata.

    Args:
        path: Output file path
        content: Stage output content
        source_file: Original source file
        stage: Stage name
        model: Model used (for LLM stages)
        usage: Token usage (for LLM stages)
        extra_metadata: Additional metadata to include
    """
    metadata = {
        "source_file": str(source_file),
        "stage": stage,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }

    if model:
        metadata["model"] = model
    if usage:
        metadata["usage"] = usage
    if extra_metadata:
        metadata.update(extra_metadata)

    data = {
        "metadata": metadata,
        "content": content,
    }

    write_json_atomic(path, data)


def read_error_file(path: Path) -> dict:
    """
    Read an error file.

    Args:
        path: Error file path

    Returns:
        Error data dictionary
    """
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


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


def format_size(bytes_count: int) -> str:
    """Format bytes to human-readable size."""
    if bytes_count < 1024:
        return f"{bytes_count}B"
    elif bytes_count < 1024 * 1024:
        return f"{bytes_count / 1024:.1f}KB"
    elif bytes_count < 1024 * 1024 * 1024:
        return f"{bytes_count / (1024 * 1024):.1f}MB"
    else:
        return f"{bytes_count / (1024 * 1024 * 1024):.1f}GB"


def discover_source_files(
    input_dir: Path,
    file_extensions: List[str],
) -> Tuple[List[Path], List[Tuple[str, List[str]]]]:
    """
    Discover source files with duplicate handling.

    Handles duplicate filenames (same stem, different extensions):
    - .pdf + .docx/.doc: Keep .pdf only (preferred format)
    - Other duplicates: Return as conflicts for manual resolution

    Args:
        input_dir: Directory to scan for files
        file_extensions: List of extensions to match (e.g., [".pdf", ".docx"])

    Returns:
        Tuple of (resolved_files, conflicts):
        - resolved_files: List of Paths to process
        - conflicts: List of (stem, [file_paths]) for unresolved duplicates
    """
    # Collect all matching files grouped by stem
    files_by_stem: Dict[str, List[Path]] = {}

    for ext in file_extensions:
        for source_path in input_dir.rglob(f"*{ext}"):
            if source_path.is_file():
                # Key by parent + stem to handle subdirectories
                key = str(source_path.parent / source_path.stem)
                if key not in files_by_stem:
                    files_by_stem[key] = []
                files_by_stem[key].append(source_path)

    # Resolve duplicates
    resolved = []
    conflicts = []

    for key, paths in files_by_stem.items():
        if len(paths) == 1:
            # No duplicate
            resolved.append(paths[0])
        else:
            # Multiple files with same stem - check for .pdf + .docx/.doc pattern
            extensions = {p.suffix.lower() for p in paths}

            if extensions == {'.pdf', '.docx'} or extensions == {'.pdf', '.doc'}:
                # Prefer PDF over Word docs
                resolved.append(next(p for p in paths if p.suffix.lower() == '.pdf'))
            else:
                # Unresolved conflict - flag for manual resolution
                conflicts.append((key, [str(p) for p in paths]))

    return resolved, conflicts


def report_conflicts_and_raise(conflicts: List[Tuple[str, List[str]]]) -> None:
    """
    Report file conflicts and raise ValueError.

    Args:
        conflicts: List of (stem, [file_paths]) tuples

    Raises:
        ValueError: Always raises with conflict details
    """
    print("\n⚠️  DUPLICATE FILES REQUIRING MANUAL RESOLUTION:")
    print("   Remove duplicates to continue (keep one version per document)")
    print()
    for stem, file_list in conflicts:
        print(f"   {Path(stem).name}:")
        for f in file_list:
            print(f"      - {f}")
    print()
    raise ValueError(
        f"Found {len(conflicts)} file(s) with unresolved duplicates. "
        "Remove duplicates and retry."
    )
