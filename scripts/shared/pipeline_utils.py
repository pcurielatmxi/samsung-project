#!/usr/bin/env python3
"""
Pipeline Utilities - Shared functions for staging and fact/quality table splitting.

Provides:
- get_output_path(): Resolve output paths (staging or final)
- write_fact_and_quality(): Split DataFrame and write both tables
- StagingContext: Context manager for staging directory operations
"""

import os
import shutil
from pathlib import Path
from typing import Optional

import pandas as pd

from src.config.settings import settings


# Environment variable for staging directory override
STAGING_DIR_ENV = 'PIPELINE_STAGING_DIR'


def get_staging_dir() -> Optional[Path]:
    """
    Get the staging directory from environment or return None.

    When set, all output writes go to staging first.
    """
    staging = os.environ.get(STAGING_DIR_ENV)
    if staging:
        return Path(staging)
    return None


def get_output_path(
    relative_path: str,
    staging_dir: Optional[Path] = None,
) -> Path:
    """
    Get the output path for a file, respecting staging directory if set.

    Args:
        relative_path: Path relative to processed/ (e.g., 'tbm/work_entries.csv')
        staging_dir: Explicit staging directory (overrides env var)

    Returns:
        Full path to write the file
    """
    # Check explicit staging dir first, then env var
    staging = staging_dir or get_staging_dir()

    if staging:
        output_path = staging / relative_path
    else:
        output_path = settings.PROCESSED_DATA_DIR / relative_path

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    return output_path


def write_fact_and_quality(
    df: pd.DataFrame,
    primary_key: str,
    quality_columns: list[str],
    fact_path: Path,
    quality_path: Optional[Path] = None,
    column_renames: Optional[dict[str, str]] = None,
) -> tuple[int, int]:
    """
    Split DataFrame into fact and data quality tables and write both.

    The fact table contains business-relevant columns for Power BI.
    The quality table contains metadata columns (inference sources, raw values, etc.)
    that can be hidden in Power BI but are useful for debugging.

    Args:
        df: DataFrame to split
        primary_key: Column name used as the join key (must exist in df)
        quality_columns: Columns to move to quality table
        fact_path: Path to write fact table
        quality_path: Path to write quality table (if None, quality columns are kept in fact)
        column_renames: Optional dict of {old_name: new_name} for fact table

    Returns:
        Tuple of (fact_row_count, quality_column_count)
    """
    # Find which quality columns actually exist
    existing_quality_cols = [c for c in quality_columns if c in df.columns]

    if quality_path and existing_quality_cols:
        # Quality table: PK + quality columns
        quality_cols_with_pk = [primary_key] + existing_quality_cols
        df_quality = df[quality_cols_with_pk].copy()

        # Fact table: everything except quality columns
        fact_cols = [c for c in df.columns if c not in existing_quality_cols]
        df_fact = df[fact_cols].copy()

        # Apply column renames to fact table
        if column_renames:
            cols_to_rename = {k: v for k, v in column_renames.items() if k in df_fact.columns}
            if cols_to_rename:
                df_fact = df_fact.rename(columns=cols_to_rename)

        # Ensure parent directories exist
        fact_path.parent.mkdir(parents=True, exist_ok=True)
        quality_path.parent.mkdir(parents=True, exist_ok=True)

        # Write both tables
        df_fact.to_csv(fact_path, index=False)
        df_quality.to_csv(quality_path, index=False)

        return len(df_fact), len(existing_quality_cols)
    else:
        # No quality table - write all to fact
        df_fact = df.copy()

        # Apply column renames
        if column_renames:
            cols_to_rename = {k: v for k, v in column_renames.items() if k in df_fact.columns}
            if cols_to_rename:
                df_fact = df_fact.rename(columns=cols_to_rename)

        fact_path.parent.mkdir(parents=True, exist_ok=True)
        df_fact.to_csv(fact_path, index=False)

        return len(df_fact), 0


class StagingContext:
    """
    Context manager for staging directory operations.

    Sets environment variable so all writes go to staging,
    then provides methods for validation and commit.

    Usage:
        with StagingContext() as staging:
            # Run consolidation scripts - they write to staging.staging_dir
            ...

            # Validate
            if staging.validate():
                staging.commit()
    """

    def __init__(self, base_dir: Optional[Path] = None):
        """
        Initialize staging context.

        Args:
            base_dir: Base directory for staging (default: processed/.staging/)
        """
        if base_dir is None:
            base_dir = settings.PROCESSED_DATA_DIR / '.staging'

        self.staging_dir = base_dir
        self._previous_env = None

    def __enter__(self) -> 'StagingContext':
        """Set up staging directory and environment."""
        # Clear any existing staging
        if self.staging_dir.exists():
            shutil.rmtree(self.staging_dir)

        self.staging_dir.mkdir(parents=True, exist_ok=True)

        # Set environment variable
        self._previous_env = os.environ.get(STAGING_DIR_ENV)
        os.environ[STAGING_DIR_ENV] = str(self.staging_dir)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore environment."""
        if self._previous_env is None:
            os.environ.pop(STAGING_DIR_ENV, None)
        else:
            os.environ[STAGING_DIR_ENV] = self._previous_env

        return False  # Don't suppress exceptions

    def get_staged_files(self) -> list[Path]:
        """Get all files in the staging directory."""
        if not self.staging_dir.exists():
            return []
        return list(self.staging_dir.rglob('*.csv'))

    def validate_file(self, relative_path: str) -> tuple[bool, list[str]]:
        """
        Validate a staged file against its schema.

        Args:
            relative_path: Path relative to staging dir

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        from schemas.validator import validate_output_file
        from schemas.registry import get_schema_for_file

        staged_path = self.staging_dir / relative_path

        if not staged_path.exists():
            return False, [f"File not found: {relative_path}"]

        schema = get_schema_for_file(staged_path.name)
        if schema is None:
            # No schema - consider valid
            return True, []

        errors = validate_output_file(staged_path, schema)
        return len(errors) == 0, errors

    def commit_file(self, relative_path: str) -> bool:
        """
        Move a staged file to its final location.

        Args:
            relative_path: Path relative to staging/final dirs

        Returns:
            True if successful
        """
        staged_path = self.staging_dir / relative_path
        final_path = settings.PROCESSED_DATA_DIR / relative_path

        if not staged_path.exists():
            return False

        # Ensure parent directory exists
        final_path.parent.mkdir(parents=True, exist_ok=True)

        # Move file
        shutil.move(str(staged_path), str(final_path))
        return True

    def commit_source(self, source_name: str, files: list[str]) -> dict[str, str]:
        """
        Commit all files for a source.

        Args:
            source_name: Name of the source (for logging)
            files: List of relative paths to commit

        Returns:
            Dict of {relative_path: status} where status is 'committed' or error message
        """
        results = {}
        for relative_path in files:
            try:
                if self.commit_file(relative_path):
                    results[relative_path] = 'committed'
                else:
                    results[relative_path] = 'not found in staging'
            except Exception as e:
                results[relative_path] = f'error: {e}'
        return results

    def cleanup(self):
        """Remove staging directory."""
        if self.staging_dir.exists():
            shutil.rmtree(self.staging_dir)


def ensure_dimension_tables_exist() -> tuple[bool, list[str]]:
    """
    Check that all required dimension tables exist.

    Returns:
        Tuple of (all_exist, list of missing files)
    """
    from scripts.shared.pipeline_registry import REQUIRED_DIMENSIONS

    missing = []
    for rel_path in REQUIRED_DIMENSIONS:
        full_path = settings.PROCESSED_DATA_DIR / rel_path
        if not full_path.exists():
            missing.append(rel_path)

    return len(missing) == 0, missing


def rebuild_dimension_tables(dry_run: bool = False) -> dict[str, bool]:
    """
    Rebuild all dimension tables.

    Args:
        dry_run: If True, show what would be done without executing

    Returns:
        Dict of {dim_name: success}
    """
    import subprocess
    import sys

    from scripts.shared.pipeline_registry import DIMENSION_BUILDERS

    results = {}
    python = sys.executable

    for builder in DIMENSION_BUILDERS:
        name = builder['name']
        module = builder['module']

        if dry_run:
            print(f"  [DRY RUN] Would run: {module}")
            results[name] = True
            continue

        try:
            result = subprocess.run(
                [python, '-m', module],
                capture_output=True,
                text=True,
                cwd=str(Path(__file__).parent.parent.parent),
            )
            results[name] = result.returncode == 0
            if result.returncode != 0:
                print(f"  Error building {name}: {result.stderr[:200]}")
        except Exception as e:
            results[name] = False
            print(f"  Exception building {name}: {e}")

    return results
