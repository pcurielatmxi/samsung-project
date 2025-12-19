#!/usr/bin/env python3
"""
Add Task Code Versions to P6 Task Table

This script enhances the task.csv file with version tracking for each task code.
Version 1 represents the first time a task code appears (ordered by file date),
and increments sequentially for each new appearance across files.

This enables tracking of:
- Original target duration (version 1)
- Evolution of task scope and durations
- Task code changes over time in BI/PowerBI

Usage:
    python scripts/primavera/process/add_task_versions.py

Output:
    - Modifies data/processed/primavera/task.csv in place
    - Adds 'task_code_version' column
    - Preserves all other columns
"""

import sys
import pandas as pd
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import settings


def add_task_code_versions():
    """Add version numbers to task codes based on chronological file order."""

    print("Loading data files...")

    # Load file manifest with dates
    xer_files_path = settings.PRIMAVERA_PROCESSED_DIR / 'xer_files.csv'
    xer_files = pd.read_csv(xer_files_path, dtype={'file_id': int})
    print(f"  ✓ Loaded {len(xer_files)} XER files")

    # Sort by date to establish chronological order
    xer_files['date'] = pd.to_datetime(xer_files['date'])
    xer_files = xer_files.sort_values('date').reset_index(drop=True)
    print(f"  ✓ Sorted files by date: {xer_files['date'].min()} to {xer_files['date'].max()}")

    # Load task data
    task_path = settings.PRIMAVERA_PROCESSED_DIR / 'task.csv'
    task = pd.read_csv(task_path, dtype={'file_id': int, 'task_code': str})
    print(f"  ✓ Loaded {len(task)} task records")

    # Merge task with file metadata to get dates
    print("\nMerging task data with file dates...")
    task_with_dates = task.merge(
        xer_files[['file_id', 'date']],
        on='file_id',
        how='left'
    )

    # Create version numbers
    print("Assigning version numbers...")

    # Sort by task_code, then by date
    task_with_dates_sorted = task_with_dates.sort_values(['task_code', 'date'])

    # Assign version: group by task_code and count unique file_id (chronologically)
    # For each task code, we need to get the version based on which appearance of that
    # task_code in chronological order this file represents

    def assign_versions(group):
        """Assign version numbers to each appearance of a task code."""
        # Get unique file dates in chronological order for this task code
        unique_files = group[['file_id', 'date']].drop_duplicates().sort_values('date')
        file_to_version = {fid: vid + 1 for vid, fid in enumerate(unique_files['file_id'].values)}

        # Assign versions based on file_id mapping
        group['task_code_version'] = group['file_id'].map(file_to_version)
        return group

    task_with_versions = task_with_dates_sorted.groupby('task_code', group_keys=False).apply(
        assign_versions
    )

    # Sort back to original order (by file_id, task_id)
    task_with_versions = task_with_versions.sort_values(['file_id', 'task_id']).reset_index(drop=True)

    # Print statistics
    print(f"\n✓ Version assignments complete:")
    print(f"  Unique task codes: {task_with_versions['task_code'].nunique()}")
    print(f"  Version range: {task_with_versions['task_code_version'].min()} to {task_with_versions['task_code_version'].max()}")
    print(f"  Average versions per task code: {task_with_versions.groupby('task_code')['task_code_version'].max().mean():.1f}")

    # Show sample data
    print(f"\nSample task code versions:")
    sample_codes = task_with_versions['task_code'].dropna().unique()[:5]
    for task_code in sample_codes:
        versions = task_with_versions[task_with_versions['task_code'] == task_code][
            ['file_id', 'task_code', 'task_code_version', 'target_drtn_hr_cnt', 'task_name', 'date']
        ].drop_duplicates(subset=['file_id']).sort_values('date')
        print(f"\n  {task_code}:")
        for _, row in versions.iterrows():
            print(f"    v{row['task_code_version']} (file_id={row['file_id']:2d}, {row['date'].strftime('%Y-%m-%d')}): "
                  f"duration={row['target_drtn_hr_cnt']:.0f}h - {row['task_name'][:50]}")

    # Remove helper columns and save
    print(f"\nSaving enhanced task data...")
    task_output = task_with_versions.drop(columns=['date'])

    # Reorder columns: move task_code_version right after task_code
    cols = list(task_output.columns)
    if 'task_code_version' in cols:
        cols.remove('task_code_version')
        task_code_idx = cols.index('task_code')
        cols.insert(task_code_idx + 1, 'task_code_version')
        task_output = task_output[cols]

    task_output.to_csv(task_path, index=False)
    print(f"  ✓ Saved to {task_path}")
    print(f"\nEnhancement complete!")
    print(f"New column 'task_code_version' added to task table for BI analysis")


if __name__ == '__main__':
    add_task_code_versions()
