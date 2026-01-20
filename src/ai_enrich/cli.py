"""
CLI interface for AI enrichment.

Provides command-line access to enrich CSVs with AI-generated columns.
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

from .enrich import enrich_dataframe, EnrichConfig, EnrichResult, get_error_summary


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )


def load_prompt_template(prompt_file: Path) -> str:
    """Load prompt template from file."""
    with open(prompt_file, "r", encoding="utf-8") as f:
        return f.read()


def load_schema(schema_file: Path) -> dict:
    """Load JSON schema from file."""
    with open(schema_file, "r", encoding="utf-8") as f:
        return json.load(f)


def create_prompt_fn(
    prompt_template: str,
    pk_cols: list[str],
    row_columns: list[str],
):
    """
    Create a prompt function from a template.

    The template can use {rows} placeholder which will be replaced with
    formatted row data showing primary keys and specified columns.

    Args:
        prompt_template: Template string with {rows} placeholder
        pk_cols: Primary key column names
        row_columns: Columns to include in row representation

    Returns:
        Function(rows, pk_cols) -> prompt string
    """
    def prompt_fn(rows: list[dict], pk_cols_arg: list[str]) -> str:
        # Build key string for each row
        pk_delimiter = "|"

        rows_text_parts = []
        for row in rows:
            pk_value = pk_delimiter.join(str(row.get(col, "")) for col in pk_cols)

            # Build row content from specified columns
            row_content_parts = []
            for col in row_columns:
                if col in row and row[col] is not None and str(row[col]).strip():
                    value = str(row[col])
                    # Truncate long values
                    if len(value) > 500:
                        value = value[:500] + "..."
                    row_content_parts.append(f"{col}: {value}")

            row_content = " | ".join(row_content_parts)
            rows_text_parts.append(f"[{pk_value}] {row_content}")

        rows_text = "\n".join(rows_text_parts)

        return prompt_template.replace("{rows}", rows_text)

    return prompt_fn


def run_enrich(
    input_csv: Path,
    prompt_file: Path,
    schema_file: Path,
    primary_key: list[str],
    cache_dir: Path,
    output_csv: Path | None = None,
    row_columns: list[str] | None = None,
    batch_size: int = 20,
    model: str = "gemini-2.0-flash",
    concurrency: int = 5,
    force: bool = False,
    retry_errors: bool = False,
    limit: int | None = None,
    dry_run: bool = False,
) -> EnrichResult:
    """
    Run AI enrichment on a CSV file.

    Args:
        input_csv: Path to input CSV
        prompt_file: Path to prompt template file
        schema_file: Path to JSON schema file
        primary_key: Primary key column name(s)
        cache_dir: Directory for caching results
        output_csv: Path to output CSV (default: input with _enriched suffix)
        row_columns: Columns to include in prompt (default: all non-PK columns)
        batch_size: Rows per batch
        model: Gemini model name
        concurrency: Max concurrent batches
        force: Reprocess cached rows
        retry_errors: Retry previously failed rows
        limit: Max rows to process
        dry_run: Preview without processing

    Returns:
        EnrichResult with statistics
    """
    logger = logging.getLogger(__name__)

    # Load input data
    logger.info(f"Loading {input_csv}")
    df = pd.read_csv(input_csv)
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")

    # Apply limit if specified
    if limit:
        df = df.head(limit)
        logger.info(f"Limited to {len(df)} rows")

    # Validate primary key columns
    missing_pk = [col for col in primary_key if col not in df.columns]
    if missing_pk:
        raise ValueError(f"Primary key columns not found: {missing_pk}")

    # Determine row columns to include in prompt
    if row_columns is None:
        row_columns = [col for col in df.columns if col not in primary_key]
    else:
        missing_cols = [col for col in row_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Row columns not found: {missing_cols}")

    logger.info(f"Primary key: {primary_key}")
    logger.info(f"Row columns for prompt: {row_columns[:5]}{'...' if len(row_columns) > 5 else ''}")

    # Load prompt and schema
    prompt_template = load_prompt_template(prompt_file)
    row_schema = load_schema(schema_file)

    logger.info(f"Prompt template: {len(prompt_template)} chars")
    logger.info(f"Schema properties: {list(row_schema.get('properties', {}).keys())}")

    # Create prompt function
    prompt_fn = create_prompt_fn(prompt_template, primary_key, row_columns)

    # Preview a sample prompt in dry run mode
    if dry_run:
        logger.info("=== DRY RUN MODE ===")
        sample_rows = df.head(3).to_dict("records")
        sample_prompt = prompt_fn(sample_rows, primary_key)
        print("\n--- Sample Prompt (3 rows) ---")
        print(sample_prompt)
        print("\n--- Schema ---")
        print(json.dumps(row_schema, indent=2))
        print("\n--- Would process ---")
        print(f"  Input: {input_csv}")
        print(f"  Rows: {len(df)}")
        print(f"  Cache: {cache_dir}")
        print(f"  Batch size: {batch_size}")
        print(f"  Model: {model}")
        return EnrichResult(
            total_rows=len(df),
            cached_rows=0,
            processed_rows=0,
            error_rows=0,
            skipped_rows=0,
        )

    # Create config
    config = EnrichConfig(
        batch_size=batch_size,
        model=model,
        concurrency=concurrency,
        force=force,
        retry_errors=retry_errors,
    )

    # Run enrichment
    df_enriched, result = enrich_dataframe(
        df=df,
        prompt_fn=prompt_fn,
        row_schema=row_schema,
        primary_key=primary_key,
        cache_dir=cache_dir,
        config=config,
    )

    # Determine output path
    if output_csv is None:
        output_csv = input_csv.parent / f"{input_csv.stem}_enriched.csv"

    # Save enriched DataFrame
    df_enriched.to_csv(output_csv, index=False)
    logger.info(f"Saved enriched data to {output_csv}")

    # Print summary
    print("\n=== Enrichment Summary ===")
    print(f"Total rows:     {result.total_rows}")
    print(f"From cache:     {result.cached_rows}")
    print(f"Processed:      {result.processed_rows}")
    print(f"Errors:         {result.error_rows}")
    print(f"Skipped (no PK):{result.skipped_rows}")
    print(f"Output:         {output_csv}")

    if result.errors:
        print(f"\nFirst 5 errors:")
        for err in result.errors[:5]:
            print(f"  [{err['key']}]: {err['error'][:100]}")

    return result


def run_status(cache_dir: Path) -> None:
    """Show status of a cache directory."""
    cache_dir = Path(cache_dir)

    if not cache_dir.exists():
        print(f"Cache directory does not exist: {cache_dir}")
        return

    # Count cached results
    cached_files = list(cache_dir.glob("*.json"))
    cached_count = len(cached_files)

    # Count errors
    error_dir = cache_dir / "_errors"
    error_files = list(error_dir.glob("*.json")) if error_dir.exists() else []
    error_count = len(error_files)

    print(f"=== Cache Status: {cache_dir} ===")
    print(f"Cached results: {cached_count}")
    print(f"Errors:         {error_count}")

    if error_count > 0:
        print(f"\nError summary:")
        errors = get_error_summary(cache_dir)
        for err in errors[:10]:
            print(f"  [{err['key']}]: {err['error'][:80]}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Enrich CSV files with AI-generated columns",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python -m src.ai_enrich input.csv --prompt prompt.txt --schema schema.json \\
      --primary-key issue_id --cache-dir cache/issues

  # With specific columns and custom output
  python -m src.ai_enrich input.csv --prompt prompt.txt --schema schema.json \\
      --primary-key id --columns title,description --output enriched.csv

  # Composite primary key
  python -m src.ai_enrich input.csv --prompt prompt.txt --schema schema.json \\
      --primary-key building,level --cache-dir cache/locations

  # Dry run to preview prompt
  python -m src.ai_enrich input.csv --prompt prompt.txt --schema schema.json \\
      --primary-key id --dry-run

  # Check cache status
  python -m src.ai_enrich --status --cache-dir cache/issues
""",
    )

    parser.add_argument(
        "input_csv",
        type=Path,
        nargs="?",
        help="Input CSV file to enrich",
    )
    parser.add_argument(
        "--prompt",
        type=Path,
        dest="prompt_file",
        help="Path to prompt template file (must contain {rows} placeholder)",
    )
    parser.add_argument(
        "--schema",
        type=Path,
        dest="schema_file",
        help="Path to JSON schema file for row output",
    )
    parser.add_argument(
        "--primary-key",
        type=str,
        nargs="+",
        dest="primary_key",
        help="Primary key column(s) for caching",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        dest="cache_dir",
        help="Directory to store cached results",
    )
    parser.add_argument(
        "--output",
        type=Path,
        dest="output_csv",
        help="Output CSV path (default: input_enriched.csv)",
    )
    parser.add_argument(
        "--columns",
        type=str,
        dest="row_columns",
        help="Comma-separated list of columns to include in prompt (default: all)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="Rows per batch (default: 20)",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="gemini-2.0-flash",
        help="Gemini model name (default: gemini-2.0-flash)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Max concurrent batches (default: 5)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess cached rows",
    )
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="Retry previously failed rows",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Max rows to process",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview prompt without processing",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show cache status instead of processing",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)

    # Status mode
    if args.status:
        if not args.cache_dir:
            parser.error("--cache-dir required with --status")
        run_status(args.cache_dir)
        return

    # Enrich mode - validate required args
    if not args.input_csv:
        parser.error("input_csv is required")
    if not args.prompt_file:
        parser.error("--prompt is required")
    if not args.schema_file:
        parser.error("--schema is required")
    if not args.primary_key:
        parser.error("--primary-key is required")
    if not args.cache_dir:
        parser.error("--cache-dir is required")

    # Parse row columns
    row_columns = None
    if args.row_columns:
        row_columns = [col.strip() for col in args.row_columns.split(",")]

    try:
        run_enrich(
            input_csv=args.input_csv,
            prompt_file=args.prompt_file,
            schema_file=args.schema_file,
            primary_key=args.primary_key,
            cache_dir=args.cache_dir,
            output_csv=args.output_csv,
            row_columns=row_columns,
            batch_size=args.batch_size,
            model=args.model,
            concurrency=args.concurrency,
            force=args.force,
            retry_errors=args.retry_errors,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    except Exception as e:
        logging.error(f"Enrichment failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
