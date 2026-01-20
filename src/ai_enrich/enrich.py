"""
Core enrichment logic for AI-generated columns.

Processes DataFrame rows in batches, caches results per primary key,
and merges AI output back into the DataFrame.
"""

import json
import logging
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, Union
from datetime import datetime

import pandas as pd

from src.document_processor.clients.gemini_client import (
    process_document_text,
    GeminiResponse,
)

logger = logging.getLogger(__name__)

# Token pricing per 1M tokens (input, output) - as of Jan 2026
MODEL_PRICING = {
    "gemini-3-flash-preview": (0.50, 3.00),
    "gemini-2.5-flash-lite": (0.10, 0.40),
    "gemini-2.0-flash": (0.10, 0.40),
    "gemini-1.5-flash": (0.075, 0.30),
}


@dataclass
class EnrichConfig:
    """Configuration for AI enrichment."""
    batch_size: int = 20
    model: str = "gemini-3-flash-preview"
    retry_errors: bool = False
    force: bool = False


@dataclass
class EnrichResult:
    """Result of enrichment operation."""
    total_rows: int
    cached_rows: int
    processed_rows: int
    error_rows: int
    skipped_rows: int  # rows with missing primary key
    total_tokens: int = 0
    total_cost: float = 0.0
    errors: list = field(default_factory=list)


def _get_cache_key(row: dict, pk_cols: list[str], delimiter: str = "|") -> str:
    """
    Generate cache key from primary key columns.

    Args:
        row: Row data as dict
        pk_cols: Primary key column names
        delimiter: Separator for composite keys

    Returns:
        Cache key string (e.g., "FAB|1F" for composite, "ISS-001" for single)
    """
    values = [str(row.get(col, "")) for col in pk_cols]
    return delimiter.join(values)


def _sanitize_filename(key: str) -> str:
    """
    Sanitize cache key for use as filename.

    Handles special characters that are invalid in filenames.
    """
    # Replace problematic characters
    sanitized = key.replace("/", "_").replace("\\", "_").replace(":", "_")
    sanitized = sanitized.replace("<", "_").replace(">", "_").replace('"', "_")
    sanitized = sanitized.replace("?", "_").replace("*", "_")

    # If key is too long, hash it
    if len(sanitized) > 200:
        hash_suffix = hashlib.md5(key.encode()).hexdigest()[:8]
        sanitized = sanitized[:190] + "_" + hash_suffix

    return sanitized


def _load_cached_result(cache_dir: Path, cache_key: str) -> Optional[dict]:
    """Load cached result for a primary key if it exists."""
    filename = _sanitize_filename(cache_key)
    cache_file = cache_dir / f"{filename}.json"

    if cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            # Handle both old format (direct result) and new format (wrapped)
            if "_cache_key" in data and "result" in data:
                return data["result"]
            return data  # Old format: direct result
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load cache file {cache_file}: {e}")

    return None


def _has_error(cache_dir: Path, cache_key: str) -> bool:
    """Check if a primary key has an error file."""
    filename = _sanitize_filename(cache_key)
    error_dir = cache_dir / "_errors"
    error_file = error_dir / f"{filename}.json"
    return error_file.exists()


def _save_cached_result(cache_dir: Path, cache_key: str, result: dict) -> None:
    """Save result to cache file with original key preserved."""
    filename = _sanitize_filename(cache_key)
    cache_file = cache_dir / f"{filename}.json"

    cache_dir.mkdir(parents=True, exist_ok=True)

    # Store original key alongside result for round-trip fidelity
    cache_data = {
        "_cache_key": cache_key,
        "_cached_at": datetime.now().isoformat(),
        "result": result,
    }

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(cache_data, f, indent=2, ensure_ascii=False)


def _save_error(cache_dir: Path, cache_key: str, error: str, row: dict) -> None:
    """Save error information for a failed row."""
    filename = _sanitize_filename(cache_key)
    error_dir = cache_dir / "_errors"
    error_dir.mkdir(parents=True, exist_ok=True)
    error_file = error_dir / f"{filename}.json"

    error_data = {
        "cache_key": cache_key,
        "error": error,
        "timestamp": datetime.now().isoformat(),
        "input_row": row,
    }

    with open(error_file, "w", encoding="utf-8") as f:
        json.dump(error_data, f, indent=2, ensure_ascii=False)


def _clear_error(cache_dir: Path, cache_key: str) -> None:
    """Remove error file after successful retry."""
    filename = _sanitize_filename(cache_key)
    error_dir = cache_dir / "_errors"
    error_file = error_dir / f"{filename}.json"

    if error_file.exists():
        error_file.unlink()


def _sanitize_property_name(key: str) -> str:
    """
    Sanitize a primary key value for use as a JSON property name.

    Gemini schema property names must be valid identifiers.
    We replace problematic characters with underscores.
    """
    # Replace characters that might cause issues in property names
    sanitized = key.replace("-", "_").replace(" ", "_").replace(".", "_")
    sanitized = sanitized.replace("/", "_").replace("|", "_")
    # Ensure it starts with a letter or underscore
    if sanitized and sanitized[0].isdigit():
        sanitized = "_" + sanitized
    return sanitized


def _build_batch_schema(row_schema: dict, batch_keys: list[str]) -> dict:
    """
    Build output schema for a batch request.

    The schema expects a mapping from primary key values to row outputs.
    Creates explicit properties for each key since Gemini doesn't support
    additionalProperties well.

    Args:
        row_schema: JSON schema for each row's AI output
        batch_keys: List of primary key values in this batch

    Returns:
        Schema with explicit properties for each batch key
    """
    # Create properties dict with each key mapped to row_schema
    properties = {}
    for key in batch_keys:
        # Use sanitized key as property name
        prop_name = _sanitize_property_name(key)
        properties[prop_name] = row_schema.copy()

    return {
        "type": "object",
        "properties": properties,
        "required": [_sanitize_property_name(k) for k in batch_keys],
    }


@dataclass
class BatchResult:
    """Result from processing a single batch."""
    results: dict[str, dict]  # key -> AI output
    errors: dict[str, str]    # key -> error message
    input_tokens: int = 0
    output_tokens: int = 0


def _process_batch(
    batch_rows: list[dict],
    batch_keys: list[str],
    pk_cols: list[str],
    prompt_fn: Callable[[list[dict], list[str]], str],
    row_schema: dict,
    model: str,
) -> BatchResult:
    """
    Process a batch of rows through the LLM.

    Args:
        batch_rows: List of row dicts to process
        batch_keys: Corresponding primary key values
        pk_cols: Primary key column names
        prompt_fn: Function to generate prompt from rows
        row_schema: JSON schema for each row's output
        model: Gemini model to use

    Returns:
        BatchResult with results, errors, and token counts
    """
    results = {}
    errors = {}

    # Generate prompt
    prompt = prompt_fn(batch_rows, pk_cols)

    # Build batch schema
    batch_schema = _build_batch_schema(row_schema, batch_keys)

    # Call Gemini
    response: GeminiResponse = process_document_text(
        text="",  # Prompt contains all context
        prompt=prompt,
        schema=batch_schema,
        model=model,
    )

    # Extract token usage
    input_tokens = 0
    output_tokens = 0
    if response.usage:
        input_tokens = response.usage.get("prompt_tokens", 0) or 0
        output_tokens = response.usage.get("output_tokens", 0) or 0

    if not response.success:
        # Batch failed - mark all rows as errors
        for key, row in zip(batch_keys, batch_rows):
            errors[key] = response.error or "Unknown error"
        return BatchResult(results, errors, input_tokens, output_tokens)

    # Parse response - should be a dict mapping sanitized keys to outputs
    if isinstance(response.result, dict):
        for key in batch_keys:
            # Look up using sanitized property name
            prop_name = _sanitize_property_name(key)
            if prop_name in response.result:
                results[key] = response.result[prop_name]
            elif key in response.result:
                # Fallback: check original key in case LLM used it
                results[key] = response.result[key]
            else:
                errors[key] = f"Key '{key}' (property '{prop_name}') not found in LLM response"
    else:
        # Unexpected response format
        for key in batch_keys:
            errors[key] = f"Unexpected response format: {type(response.result)}"

    return BatchResult(results, errors, input_tokens, output_tokens)


def _calculate_cost(input_tokens: int, output_tokens: int, model: str) -> float:
    """Calculate cost based on token usage and model pricing."""
    pricing = MODEL_PRICING.get(model, (0.10, 0.40))  # Default to flash pricing
    input_cost = (input_tokens / 1_000_000) * pricing[0]
    output_cost = (output_tokens / 1_000_000) * pricing[1]
    return input_cost + output_cost


def enrich_dataframe(
    df: pd.DataFrame,
    prompt_fn: Callable[[list[dict], list[str]], str],
    row_schema: dict,
    primary_key: Union[str, list[str]],
    cache_dir: Union[str, Path],
    config: Optional[EnrichConfig] = None,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> tuple[pd.DataFrame, EnrichResult]:
    """
    Enrich a DataFrame with AI-generated columns.

    Processes rows in batches, caches results per primary key, and merges
    AI output back into the DataFrame as a new 'ai_output' column.

    Args:
        df: Input DataFrame
        prompt_fn: Function(rows, pk_cols) -> prompt string
        row_schema: JSON schema for each row's AI output
        primary_key: Column name(s) for cache key
        cache_dir: Directory to store cached results
        config: Optional configuration (batch_size, model, etc.)
        progress_callback: Optional callback(processed, total, message)

    Returns:
        Tuple of (enriched DataFrame, EnrichResult with statistics)
    """
    config = config or EnrichConfig()
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Normalize primary key to list
    pk_cols = [primary_key] if isinstance(primary_key, str) else list(primary_key)

    # Validate primary key columns exist
    missing_cols = [col for col in pk_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Primary key columns not found in DataFrame: {missing_cols}")

    # Initialize result tracking
    result = EnrichResult(
        total_rows=len(df),
        cached_rows=0,
        processed_rows=0,
        error_rows=0,
        skipped_rows=0,
    )

    # Build list of rows to process
    rows_to_process = []
    cached_results = {}

    for idx, row in df.iterrows():
        row_dict = row.to_dict()

        # Check for missing primary key values
        if any(pd.isna(row_dict.get(col)) for col in pk_cols):
            result.skipped_rows += 1
            continue

        cache_key = _get_cache_key(row_dict, pk_cols)

        # Check cache
        if not config.force:
            cached = _load_cached_result(cache_dir, cache_key)
            if cached is not None:
                cached_results[cache_key] = cached
                result.cached_rows += 1
                continue

        # Check error status
        if _has_error(cache_dir, cache_key) and not config.retry_errors:
            result.error_rows += 1
            continue

        rows_to_process.append((idx, cache_key, row_dict))

    logger.info(
        f"Enrichment: {result.total_rows} total, {result.cached_rows} cached, "
        f"{len(rows_to_process)} to process, {result.skipped_rows} skipped"
    )

    # Process in batches
    total_batches = (len(rows_to_process) + config.batch_size - 1) // config.batch_size

    for batch_idx in range(0, len(rows_to_process), config.batch_size):
        batch = rows_to_process[batch_idx:batch_idx + config.batch_size]
        batch_num = batch_idx // config.batch_size + 1

        batch_indices = [item[0] for item in batch]
        batch_keys = [item[1] for item in batch]
        batch_rows = [item[2] for item in batch]

        if progress_callback:
            progress_callback(
                batch_idx,
                len(rows_to_process),
                f"Processing batch {batch_num}/{total_batches}"
            )

        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} rows)")

        # Process batch
        batch_result = _process_batch(
            batch_rows=batch_rows,
            batch_keys=batch_keys,
            pk_cols=pk_cols,
            prompt_fn=prompt_fn,
            row_schema=row_schema,
            model=config.model,
        )

        # Track token usage
        result.total_tokens += batch_result.input_tokens + batch_result.output_tokens
        result.total_cost += _calculate_cost(
            batch_result.input_tokens,
            batch_result.output_tokens,
            config.model
        )

        # Save results to cache
        for key in batch_keys:
            row = batch_rows[batch_keys.index(key)]

            if key in batch_result.results:
                _save_cached_result(cache_dir, key, batch_result.results[key])
                _clear_error(cache_dir, key)
                cached_results[key] = batch_result.results[key]
                result.processed_rows += 1
            elif key in batch_result.errors:
                _save_error(cache_dir, key, batch_result.errors[key], row)
                result.error_rows += 1
                result.errors.append({"key": key, "error": batch_result.errors[key]})

    if progress_callback:
        progress_callback(len(rows_to_process), len(rows_to_process), "Complete")

    # Merge results back into DataFrame
    def get_ai_output(row):
        if any(pd.isna(row.get(col)) for col in pk_cols):
            return None
        cache_key = _get_cache_key(row.to_dict(), pk_cols)
        return cached_results.get(cache_key)

    df_result = df.copy()
    df_result["ai_output"] = df_result.apply(get_ai_output, axis=1)

    logger.info(
        f"Enrichment complete: {result.processed_rows} processed, "
        f"{result.cached_rows} from cache, {result.error_rows} errors, "
        f"{result.total_tokens:,} tokens, ${result.total_cost:.4f}"
    )

    return df_result, result


def load_cached_results(
    cache_dir: Union[str, Path],
) -> dict[str, dict]:
    """
    Load all cached results from a cache directory.

    Args:
        cache_dir: Directory containing cached JSON files

    Returns:
        Dict mapping original cache keys to their AI outputs
    """
    cache_dir = Path(cache_dir)
    results = {}

    if not cache_dir.exists():
        return results

    for cache_file in cache_dir.glob("*.json"):
        if cache_file.name.startswith("_"):
            continue

        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Handle both new format (with _cache_key) and old format
            if "_cache_key" in data and "result" in data:
                key = data["_cache_key"]
                results[key] = data["result"]
            else:
                # Old format: filename is sanitized key, data is result
                key = cache_file.stem
                results[key] = data
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load {cache_file}: {e}")

    return results


def get_error_summary(cache_dir: Union[str, Path]) -> list[dict]:
    """
    Get summary of all errors in a cache directory.

    Args:
        cache_dir: Directory containing cached results

    Returns:
        List of error dicts with key, error message, and timestamp
    """
    cache_dir = Path(cache_dir)
    error_dir = cache_dir / "_errors"
    errors = []

    if not error_dir.exists():
        return errors

    for error_file in error_dir.glob("*.json"):
        try:
            with open(error_file, "r", encoding="utf-8") as f:
                error_data = json.load(f)
            errors.append({
                "key": error_data.get("cache_key", error_file.stem),
                "error": error_data.get("error", "Unknown"),
                "timestamp": error_data.get("timestamp"),
            })
        except (json.JSONDecodeError, IOError):
            errors.append({
                "key": error_file.stem,
                "error": "Failed to read error file",
                "timestamp": None,
            })

    return errors
