#!/usr/bin/env python3
"""
Extract Grid Coordinates from P6 Task Names

Enriches dim_location with grid bounds by parsing P6 task descriptions.

Problem:
--------
Many stairs and elevators use Yates' internal numbering (STR-23, ELV-24) which
doesn't match architectural drawing codes (FAB1-ST01). Without grid bounds,
we cannot spatially join quality inspection data to these locations.

Solution:
---------
Task names contain embedded grid info:
  - "STAIR #62 - SUW - 3-K-L - LEVEL 04" -> Grid col 3, rows K-L
  - "STAIR #49 - SUW - 18-19-L"          -> Grid cols 18-19, row L

This script extracts these coordinates, enabling spatial joins like:
  Quality issue at SUW-4F grid L/18 -> Matches STR-65 (grid L-L / 18-19)

Impact on dim_location:
-----------------------
Before: 11/66 stairs have grid bounds (17%)
After:  66/66 stairs have grid bounds (100%)

This enables cross-source analysis between P6 tasks and quality data (RABA/PSI).

Usage:
------
    python scripts/shared/extract_location_grids.py
    python scripts/shared/extract_location_grids.py --dry-run
    python scripts/shared/extract_location_grids.py --location-type STAIR

Output:
-------
- Updates: raw/location_mappings/location_master.csv (Action_Status -> EXTRACTED)
- Creates: processed/location_mappings/extracted_grids.csv (for review)

Pipeline Integration:
--------------------
Run after generate_location_master.py and before build_dim_location.py:
    1. generate_location_master.py  -> Creates location_master.csv from taxonomy
    2. extract_location_grids.py    -> Adds grid coordinates from task names
    3. build_dim_location.py        -> Builds dim_location.csv
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings


# =============================================================================
# Grid Extraction Patterns
# =============================================================================

# Pattern 1: Standard format - "BUILDING - GRID-ROW - LEVEL"
# Examples:
#   SUW - 3-K-L - LEVEL 04     -> SUW, col 3, rows K-L
#   SUW - 18-19-L - LEVEL 01   -> SUW, cols 18-19, row L
#   FIZ - 1-L - LEVEL 02       -> FIZ, col 1, row L
#   SUW - 30-32-N - LEVEL 02   -> SUW, cols 30-32, row N
#   SUW - 29.M.5 - LEVEL U1    -> SUW, col 29, row M (dot separator)
PATTERN_STANDARD = re.compile(
    r'(SUW|SUE|FIZ|FAB)\s*[-\.]\s*'      # Building code (dash or dot separator)
    r'(\d+)(?:[/\-](\d+))?'               # Column(s): single or range (e.g., 18 or 18-19)
    r'\s*[-\.]\s*'                        # Separator
    r'([A-N])(?:\.5)?'                    # Row (with optional .5 suffix)
    r'(?:[/\-]([A-N])(?:\.5)?)?',         # Optional second row for range
    re.IGNORECASE
)

# Pattern 2: GL (Gridline) format - "GL XX-YY"
# Examples:
#   SEB3 L1 GL 25-28           -> cols 25-28
#   GL 5-7 - L1 - C-A          -> cols 5-7
PATTERN_GL = re.compile(
    r'GL\s*(\d+)(?:\s*-\s*(\d+))?',       # GL followed by column(s)
    re.IGNORECASE
)

# Pattern 3: Row pattern for GL format - "L1 - ROW" or "- ROW-ROW"
# Used in combination with PATTERN_GL
PATTERN_ROW = re.compile(
    r'[-\s]([A-N])(?:\.5)?(?:\s*-\s*([A-N])(?:\.5)?)?(?:\s|$|\))',
    re.IGNORECASE
)


def extract_grid_from_task_name(task_name: str) -> dict | None:
    """
    Extract grid coordinates from a task name.

    Args:
        task_name: The P6 task name string

    Returns:
        Dict with keys: building, col_min, col_max, row_min, row_max
        or None if no grid info found
    """
    if not task_name or pd.isna(task_name):
        return None

    name = str(task_name).upper()

    # Try Pattern 1: Standard format (most common)
    match = PATTERN_STANDARD.search(name)
    if match:
        return {
            'building': match.group(1),
            'col_min': int(match.group(2)),
            'col_max': int(match.group(3)) if match.group(3) else int(match.group(2)),
            'row_min': match.group(4),
            'row_max': match.group(5) if match.group(5) else match.group(4),
        }

    # Try Pattern 2: GL format
    gl_match = PATTERN_GL.search(name)
    if gl_match:
        col_min = int(gl_match.group(1))
        col_max = int(gl_match.group(2)) if gl_match.group(2) else col_min

        # Try to find row info
        row_match = PATTERN_ROW.search(name)
        if row_match:
            row_min = row_match.group(1)
            row_max = row_match.group(2) if row_match.group(2) else row_min
            # Normalize row order (A < C, so swap if reversed)
            if row_min > row_max:
                row_min, row_max = row_max, row_min

            # Try to infer building from area codes in name
            building = None
            if 'SEA' in name or 'SEB' in name:
                building = 'SUE'
            elif 'SWA' in name or 'SWB' in name:
                building = 'SUW'
            elif 'FIZ' in name:
                building = 'FIZ'

            return {
                'building': building,
                'col_min': col_min,
                'col_max': col_max,
                'row_min': row_min,
                'row_max': row_max,
            }

    return None


def extract_grids_for_location_type(
    taxonomy_df: pd.DataFrame,
    tasks_df: pd.DataFrame,
    location_type: str,
    sample_size: int = 30
) -> pd.DataFrame:
    """
    Extract grid coordinates for all locations of a given type.

    Args:
        taxonomy_df: Task taxonomy DataFrame with location_code column
        tasks_df: Task DataFrame with task_id and task_name columns
        location_type: 'STAIR' or 'ELEVATOR'
        sample_size: Number of task names to sample per location code

    Returns:
        DataFrame with extraction results
    """
    # Filter to location type and merge with task names
    loc_tasks = taxonomy_df[taxonomy_df['location_type'] == location_type].copy()
    loc_tasks = loc_tasks.merge(tasks_df[['task_id', 'task_name']], on='task_id', how='left')

    results = []
    location_codes = loc_tasks['location_code'].unique()

    for code in sorted(location_codes):
        code_tasks = loc_tasks[loc_tasks['location_code'] == code]
        task_count = len(code_tasks)

        # Get building/level from taxonomy (most common values)
        bldg_from_tax = code_tasks['building'].mode()
        bldg_from_tax = bldg_from_tax.iloc[0] if len(bldg_from_tax) > 0 else None
        lvl_from_tax = code_tasks['level'].mode()
        lvl_from_tax = lvl_from_tax.iloc[0] if len(lvl_from_tax) > 0 else None

        # Try to extract grid from task names
        grid_matches = []
        sample_names = []

        for name in code_tasks['task_name'].dropna().head(sample_size):
            grid = extract_grid_from_task_name(name)
            if grid:
                grid_matches.append(grid)
                sample_names.append(str(name)[:100])

        if grid_matches:
            # Use first match (most common pattern)
            first = grid_matches[0]
            results.append({
                'location_code': code,
                'location_type': location_type,
                'task_count': task_count,
                'building': first['building'] or bldg_from_tax,
                'level': lvl_from_tax,
                'grid_col_min': first['col_min'],
                'grid_col_max': first['col_max'],
                'grid_row_min': first['row_min'],
                'grid_row_max': first['row_max'],
                'match_count': len(grid_matches),
                'extracted': True,
                'sample_task': sample_names[0] if sample_names else None,
            })
        else:
            results.append({
                'location_code': code,
                'location_type': location_type,
                'task_count': task_count,
                'building': bldg_from_tax,
                'level': lvl_from_tax,
                'grid_col_min': None,
                'grid_col_max': None,
                'grid_row_min': None,
                'grid_row_max': None,
                'match_count': 0,
                'extracted': False,
                'sample_task': None,
            })

    return pd.DataFrame(results)


def update_location_master(
    master_df: pd.DataFrame,
    extracted_df: pd.DataFrame,
    dry_run: bool = False
) -> tuple[pd.DataFrame, int]:
    """
    Update location_master with extracted grid coordinates.

    Only updates entries that are currently missing grid data.
    Sets Action_Status to 'EXTRACTED' for updated entries.

    Args:
        master_df: Location master DataFrame
        extracted_df: Extracted grid coordinates DataFrame
        dry_run: If True, don't modify the DataFrame

    Returns:
        Tuple of (updated DataFrame, count of updated entries)
    """
    if dry_run:
        master_df = master_df.copy()

    updated_count = 0

    for _, row in extracted_df[extracted_df['extracted'] == True].iterrows():
        code = row['location_code']
        mask = master_df['Code'] == code

        if not mask.any():
            continue

        # Only update if currently missing grid data
        current = master_df.loc[mask].iloc[0]
        if pd.isna(current['Row_Min']) or pd.isna(current['Col_Min']):
            master_df.loc[mask, 'Row_Min'] = row['grid_row_min']
            master_df.loc[mask, 'Row_Max'] = row['grid_row_max']
            master_df.loc[mask, 'Col_Min'] = int(row['grid_col_min'])
            master_df.loc[mask, 'Col_Max'] = int(row['grid_col_max'])
            master_df.loc[mask, 'Action_Status'] = 'EXTRACTED'
            updated_count += 1

    return master_df, updated_count


def print_summary(extracted_df: pd.DataFrame, updated_count: int) -> None:
    """Print extraction summary."""
    total = len(extracted_df)
    extracted = len(extracted_df[extracted_df['extracted'] == True])

    print("\n" + "=" * 60)
    print("EXTRACTION SUMMARY")
    print("=" * 60)

    print(f"\nTotal location codes processed: {total}")
    print(f"Successfully extracted: {extracted} ({100*extracted/total:.1f}%)")
    print(f"Could not extract: {total - extracted}")
    print(f"Location master entries updated: {updated_count}")

    # By location type
    print("\nBy location type:")
    for loc_type in extracted_df['location_type'].unique():
        subset = extracted_df[extracted_df['location_type'] == loc_type]
        ext = len(subset[subset['extracted'] == True])
        print(f"  {loc_type}: {ext}/{len(subset)} extracted")

    # Show entries that couldn't be extracted
    not_extracted = extracted_df[extracted_df['extracted'] == False]
    if len(not_extracted) > 0:
        print(f"\nCould not extract ({len(not_extracted)} entries):")
        for _, row in not_extracted.iterrows():
            print(f"  {row['location_code']}: {row['task_count']} tasks")


def main():
    parser = argparse.ArgumentParser(
        description='Extract grid coordinates from P6 task names',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                        # Extract and update location_master
  %(prog)s --dry-run              # Preview without saving
  %(prog)s --location-type STAIR  # Only extract stairs
  %(prog)s --export-only          # Export extraction results without updating
        """
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without modifying files'
    )
    parser.add_argument(
        '--location-type',
        choices=['STAIR', 'ELEVATOR', 'ALL'],
        default='ALL',
        help='Location type to process (default: ALL)'
    )
    parser.add_argument(
        '--export-only',
        action='store_true',
        help='Only export extraction results, do not update location_master'
    )
    parser.add_argument(
        '--sample-size',
        type=int,
        default=30,
        help='Number of task names to sample per location (default: 30)'
    )
    args = parser.parse_args()

    print("=" * 60)
    print("EXTRACT LOCATION GRIDS FROM P6 TASK NAMES")
    print("=" * 60)

    # Load taxonomy
    taxonomy_path = Settings.PRIMAVERA_PROCESSED_DIR / 'p6_task_taxonomy.csv'
    print(f"\nLoading taxonomy from: {taxonomy_path}")
    taxonomy_df = pd.read_csv(taxonomy_path, low_memory=False)
    print(f"  Total tasks: {len(taxonomy_df):,}")

    # Load task names
    task_path = Settings.PRIMAVERA_PROCESSED_DIR / 'task.csv'
    print(f"Loading tasks from: {task_path}")
    tasks_df = pd.read_csv(task_path, low_memory=False)
    print(f"  Total tasks: {len(tasks_df):,}")

    # Determine which location types to process
    if args.location_type == 'ALL':
        location_types = ['STAIR', 'ELEVATOR']
    else:
        location_types = [args.location_type]

    # Extract grids
    all_results = []
    for loc_type in location_types:
        print(f"\nExtracting grids for {loc_type}...")
        results = extract_grids_for_location_type(
            taxonomy_df, tasks_df, loc_type, args.sample_size
        )
        all_results.append(results)
        extracted = len(results[results['extracted'] == True])
        print(f"  Extracted: {extracted}/{len(results)}")

    extracted_df = pd.concat(all_results, ignore_index=True)

    # Export extraction results
    export_path = Settings.PROCESSED_DATA_DIR / 'location_mappings' / 'extracted_grids.csv'
    export_path.parent.mkdir(parents=True, exist_ok=True)
    extracted_df.to_csv(export_path, index=False)
    print(f"\nExported extraction results to: {export_path}")

    if args.export_only:
        print("\n[EXPORT ONLY] Skipping location_master update")
        print_summary(extracted_df, 0)
        return

    # Load and update location_master
    master_path = Settings.RAW_DATA_DIR / 'location_mappings' / 'location_master.csv'
    print(f"\nLoading location_master from: {master_path}")
    master_df = pd.read_csv(master_path)
    print(f"  Total entries: {len(master_df)}")

    # Update
    updated_df, updated_count = update_location_master(
        master_df, extracted_df, dry_run=args.dry_run
    )

    if args.dry_run:
        print(f"\n[DRY RUN] Would update {updated_count} entries")
    else:
        updated_df.to_csv(master_path, index=False)
        print(f"\nSaved updated location_master to: {master_path}")

    print_summary(extracted_df, updated_count)

    if args.dry_run:
        print("\n[DRY RUN] No files were modified. Run without --dry-run to apply changes.")


if __name__ == '__main__':
    main()
