#!/usr/bin/env python3
"""
Batch XER Processor - Process all XER files with auto-discovery and file tracking

This script auto-discovers XER files in the input directory, extracts dates from
filenames, and exports ALL tables from each XER file to CSV format. Each record
includes a file_id to track which XER file it came from.

IMPORTANT: All ID columns are prefixed with file_id to maintain referential integrity
across multiple XER files. Format: "{file_id}_{original_id}"

For example, if file_id=48 and task_id=715090, the output will be task_id="48_715090"

This ensures:
- Primary keys are unique across all files
- Foreign key relationships are preserved within each file
- PowerBI and other tools can build proper data models

Schedule Filtering:
- By default, only YATES (General Contractor) schedules are processed
- Use --all to process all schedules (YATES + SECAI)
- Use --schedule-type to explicitly filter by schedule type

WBS Hierarchy Enhancement:
- projwbs.csv is automatically enhanced with tier columns (depth, tier_1 through tier_6)
- Each tier represents a level in the WBS hierarchy for easy filtering/grouping

Task Taxonomy Generation:
- task_taxonomy.csv is automatically generated with phase, scope, location classifications
- Saved to derived/primavera/ directory (separate from processed data)

Output Tables (all with file_id and prefixed IDs):
- xer_files.csv        - Metadata about each XER file (auto-discovered)
- task.csv             - Tasks (activities)
- taskpred.csv         - Task predecessors/dependencies
- taskrsrc.csv         - Task resource assignments
- taskactv.csv         - Task activity code assignments
- taskmemo.csv         - Task notes/memos
- projwbs.csv          - WBS (Work Breakdown Structure)
- actvcode.csv         - Activity code values
- actvtype.csv         - Activity code types
- calendar.csv         - Calendars
- rsrc.csv             - Resources
- rsrcrate.csv         - Resource rates
- udftype.csv          - User-defined field types
- udfvalue.csv         - User-defined field values
- project.csv          - Project metadata
- ... and all other tables in the XER files

Usage:
    python scripts/batch_process_xer.py                 # YATES schedules only (default)
    python scripts/batch_process_xer.py --all           # All schedules
    python scripts/batch_process_xer.py --schedule-type SECAI  # SECAI schedules only
    python scripts/batch_process_xer.py --current-only  # Only process current file
    python scripts/batch_process_xer.py --output-dir data/primavera/processed
"""

import sys
import json
from pathlib import Path
from datetime import datetime
import pandas as pd

# Add project root to path (scripts/primavera/process -> scripts/primavera -> scripts -> project_root)
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Add derive directory for task_taxonomy module
derive_dir = Path(__file__).parent.parent / 'derive'
sys.path.insert(0, str(derive_dir))

from src.utils.xer_parser import XERParser
from src.config.settings import Settings
from src.classifiers.task_classifier import TaskClassifier
from task_taxonomy import build_task_context, infer_all_fields, get_default_mapping

# Paths - use Settings for proper WINDOWS_DATA_DIR support
XER_DIR = Settings.PRIMAVERA_RAW_DIR
DEFAULT_OUTPUT_DIR = Settings.PRIMAVERA_PROCESSED_DIR

# Schedule type constants
SCHEDULE_YATES = 'YATES'
SCHEDULE_SECAI = 'SECAI'
SCHEDULE_UNKNOWN = 'UNKNOWN'
DEFAULT_SCHEDULE_TYPE = SCHEDULE_YATES

# WBS hierarchy configuration
DEFAULT_NUM_TIERS = 6


def classify_schedule_from_filename(filename: str) -> str:
    """
    Classify schedule type from filename.

    This is used for pre-filtering before parsing. For more accurate
    classification after parsing, use classify_schedules.py which also
    checks proj_short_name from the PROJECT table.

    Args:
        filename: XER filename

    Returns:
        Schedule type: 'YATES', 'SECAI', or 'UNKNOWN'
    """
    fname = str(filename).upper()

    # YATES (General Contractor) patterns
    # Most YATES files contain 'SAMSUNG-FAB' or 'SAMSUNG-TFAB' in the filename
    if any(pattern in fname for pattern in ['YATES', 'SAMSUNG-FAB', 'SAMSUNG-TFAB']):
        return SCHEDULE_YATES

    # SECAI (Owner) patterns
    if any(pattern in fname for pattern in ['SECAI', 'T1 PROJECT', 'T1P1']):
        return SCHEDULE_SECAI

    return SCHEDULE_UNKNOWN


def extract_date_from_filename(filename: str) -> str | None:
    """
    Extract schedule date from XER filename.

    Supports patterns like:
        - "10-10-22" -> "2022-10-10"
        - "11-20-25" -> "2025-11-20"
        - "12-31-23" -> "2023-12-31"
        - "11.29.24" -> "2024-11-29"

    Args:
        filename: XER filename

    Returns:
        ISO date string (YYYY-MM-DD) or None if no date found
    """
    import re

    # Pattern: MM-DD-YY or MM.DD.YY (with various separators)
    # Look for date patterns in the filename
    patterns = [
        r'(\d{1,2})[-.](\d{1,2})[-.](\d{2,4})',  # MM-DD-YY or MM.DD.YY
    ]

    for pattern in patterns:
        matches = re.findall(pattern, filename)
        if matches:
            # Take the last match (usually the most specific date)
            month, day, year = matches[-1]
            month = int(month)
            day = int(day)
            year = int(year)

            # Convert 2-digit year to 4-digit
            if year < 100:
                year = 2000 + year if year < 50 else 1900 + year

            # Validate date components
            if 1 <= month <= 12 and 1 <= day <= 31 and 2020 <= year <= 2030:
                return f"{year:04d}-{month:02d}-{day:02d}"

    return None


def discover_xer_files(xer_dir: Path, verbose: bool = True) -> pd.DataFrame:
    """
    Discover all XER files in directory and build metadata table.

    Scans the directory for .xer files, extracts dates from filenames,
    and classifies schedule types automatically.

    Args:
        xer_dir: Directory containing XER files
        verbose: Print discovery progress

    Returns:
        DataFrame with columns:
            - file_id: Unique identifier (1-indexed, assigned after sorting)
            - filename: Original filename
            - date: Schedule date extracted from filename
            - schedule_type: YATES, SECAI, or UNKNOWN
            - is_current: True for the latest file by date
    """
    if verbose:
        print(f"Scanning for XER files in: {xer_dir}")

    # Find all .xer files
    xer_files = list(xer_dir.glob("*.xer"))

    if not xer_files:
        raise ValueError(f"No XER files found in {xer_dir}")

    rows = []
    for xer_path in xer_files:
        filename = xer_path.name
        date = extract_date_from_filename(filename)
        schedule_type = classify_schedule_from_filename(filename)

        rows.append({
            "filename": filename,
            "date": date or "",
            "schedule_type": schedule_type,
        })

    df = pd.DataFrame(rows)

    # Sort by date (files without dates go to the end)
    df = df.sort_values("date", na_position="last").reset_index(drop=True)

    # Assign file_id after sorting
    df["file_id"] = range(1, len(df) + 1)

    # Mark the latest file as current (last one with a valid date)
    df["is_current"] = False
    valid_dates = df[df["date"] != ""]
    if len(valid_dates) > 0:
        latest_idx = valid_dates.index[-1]
        df.loc[latest_idx, "is_current"] = True

    if verbose:
        print(f"  Found {len(df)} XER files")
        type_counts = df['schedule_type'].value_counts()
        for stype, count in type_counts.items():
            print(f"    {stype}: {count} files")

    return df


# =============================================================================
# WBS Hierarchy Functions
# =============================================================================

def _build_hierarchy_tree(wbs_df: pd.DataFrame) -> dict:
    """Build lookup structures for WBS hierarchy traversal."""
    id_to_row = {row['wbs_id']: row.to_dict() for _, row in wbs_df.iterrows()}
    id_to_parent = dict(zip(wbs_df['wbs_id'], wbs_df['parent_wbs_id']))

    id_to_children = {wbs_id: [] for wbs_id in id_to_row}
    for wbs_id, parent_id in id_to_parent.items():
        if pd.notna(parent_id) and parent_id in id_to_children:
            id_to_children[parent_id].append(wbs_id)

    return {
        'id_to_row': id_to_row,
        'id_to_parent': id_to_parent,
        'id_to_children': id_to_children,
    }


def _get_ancestors(wbs_id: str, tree: dict, max_depth: int = 10) -> list[dict]:
    """Get list of ancestors from root to this node (inclusive)."""
    ancestors = []
    current_id = wbs_id
    depth = 0

    while depth < max_depth and current_id in tree['id_to_row']:
        row = tree['id_to_row'][current_id]
        ancestors.append({
            'wbs_id': current_id,
            'wbs_short_name': row['wbs_short_name'],
            'wbs_name': row['wbs_name'],
        })
        parent_id = tree['id_to_parent'].get(current_id)
        if pd.isna(parent_id) or parent_id not in tree['id_to_row']:
            break
        current_id = parent_id
        depth += 1

    return list(reversed(ancestors))


def _get_tier_label(node: dict) -> str:
    """Get human-friendly label for a WBS node."""
    name = str(node.get('wbs_name', ''))
    short = str(node.get('wbs_short_name', ''))

    if not name or name == short:
        return short
    if name.startswith('SAMSUNG') or name.startswith('Yates T FAB1'):
        return short
    if len(name) > 50:
        name = name[:47] + '...'
    return name


def _build_tier_columns_for_file(wbs_df: pd.DataFrame, num_tiers: int = 6) -> pd.DataFrame:
    """Build tier columns for WBS nodes in a single file."""
    tree = _build_hierarchy_tree(wbs_df)
    results = []

    for _, row in wbs_df.iterrows():
        wbs_id = row['wbs_id']
        ancestors = _get_ancestors(wbs_id, tree)
        depth = len(ancestors) - 1

        result = {'wbs_id': wbs_id, 'depth': depth}
        for i in range(num_tiers):
            tier_num = i + 1
            if i < len(ancestors):
                result[f'tier_{tier_num}'] = _get_tier_label(ancestors[i])
            else:
                result[f'tier_{tier_num}'] = None
        results.append(result)

    return pd.DataFrame(results)


def enhance_wbs_with_hierarchy(wbs_df: pd.DataFrame, num_tiers: int = DEFAULT_NUM_TIERS,
                                verbose: bool = True) -> pd.DataFrame:
    """
    Enhance WBS DataFrame with tier columns for hierarchy navigation.

    Adds depth and tier_1 through tier_N columns to enable easy filtering
    and grouping at any level of the WBS hierarchy.

    Args:
        wbs_df: WBS DataFrame with wbs_id, parent_wbs_id, wbs_short_name, wbs_name
        num_tiers: Number of tier columns to create (default 6)
        verbose: Print progress messages

    Returns:
        Enhanced DataFrame with original columns plus depth and tier columns
    """
    if verbose:
        print(f"  Enhancing WBS with {num_tiers} tier columns...")

    # Remove existing tier columns if present
    tier_cols = [f'tier_{i}' for i in range(1, num_tiers + 1)]
    cols_to_drop = [c for c in tier_cols + ['depth'] if c in wbs_df.columns]
    if cols_to_drop:
        wbs_df = wbs_df.drop(columns=cols_to_drop)

    # Process each file_id separately (hierarchy is file-specific)
    all_tier_dfs = []
    file_ids = wbs_df['file_id'].unique()

    for file_id in file_ids:
        file_wbs = wbs_df[wbs_df['file_id'] == file_id].copy()
        if len(file_wbs) > 0:
            tier_df = _build_tier_columns_for_file(file_wbs, num_tiers=num_tiers)
            all_tier_dfs.append(tier_df)

    if not all_tier_dfs:
        return wbs_df

    # Combine and merge
    tier_data = pd.concat(all_tier_dfs, ignore_index=True)
    result_df = wbs_df.merge(tier_data, on='wbs_id', how='left')

    # Reorder columns: original columns first, then depth, then tier columns
    original_cols = [c for c in wbs_df.columns if c not in tier_cols and c != 'depth']
    new_cols = ['depth'] + tier_cols
    result_df = result_df[original_cols + new_cols]

    if verbose:
        print(f"    Added columns: depth, {', '.join(tier_cols)}")

    return result_df


# =============================================================================
# Taxonomy Generation Functions
# =============================================================================

def generate_task_taxonomy(
    tasks_df: pd.DataFrame,
    wbs_df: pd.DataFrame,
    taskactv_df: pd.DataFrame = None,
    actvcode_df: pd.DataFrame = None,
    actvtype_df: pd.DataFrame = None,
    verbose: bool = True
) -> pd.DataFrame:
    """
    Generate taxonomy lookup table for tasks using full inference system.

    Uses priority-based inference from multiple sources:
    1. Activity codes (Z-TRADE, Z-BLDG, Z-LEVEL, Z-AREA) - highest priority
    2. Task code extraction (CN.SEA5.xxx → SUE)
    3. WBS hierarchy context
    4. Task name pattern matching - fallback

    Args:
        tasks_df: Tasks dataframe with task_id, task_name, task_code, wbs_id
        wbs_df: WBS dataframe with wbs_id, wbs_name, tier columns
        taskactv_df: Task-to-activity code assignments (optional for backward compat)
        actvcode_df: Activity code values (optional)
        actvtype_df: Activity code types (optional)
        verbose: Print progress messages

    Returns:
        Taxonomy lookup table with source tracking columns
    """
    if verbose:
        print(f"  Generating task taxonomy for {len(tasks_df):,} tasks...")

    # Check if we have activity code data for full inference
    has_activity_codes = (
        taskactv_df is not None and
        actvcode_df is not None and
        actvtype_df is not None and
        len(taskactv_df) > 0
    )

    if has_activity_codes:
        if verbose:
            print("    Using full taxonomy inference with activity codes...")

        # Build enriched context with activity codes
        context = build_task_context(
            tasks_df=tasks_df,
            wbs_df=wbs_df,
            taskactv_df=taskactv_df,
            actvcode_df=actvcode_df,
            actvtype_df=actvtype_df,
            verbose=verbose
        )

        # Load gridline mapping for coordinate lookup
        try:
            gridline_mapping = get_default_mapping()
            if verbose:
                print(f"    Loaded gridline mapping with {len(gridline_mapping.lookup)} FAB codes")
        except FileNotFoundError as e:
            if verbose:
                print(f"    Warning: Gridline mapping not found, proceeding without: {e}")
            gridline_mapping = None

        # Generate taxonomy using full inference
        results = []
        total = len(context)

        for idx, (_, row) in enumerate(context.iterrows()):
            if verbose and idx % 50000 == 0 and idx > 0:
                print(f"    Processed {idx:,}/{total:,} ({idx/total*100:.1f}%)")

            result = infer_all_fields(row, gridline_mapping=gridline_mapping)
            results.append(result)

        if verbose:
            print(f"    Inferred taxonomy for {total:,} tasks")

        return pd.DataFrame(results)

    else:
        # Fallback to basic classifier (backward compatibility)
        if verbose:
            print("    Using basic classifier (no activity codes available)...")

        classifier = TaskClassifier()
        wbs_names = wbs_df.set_index('wbs_id')['wbs_name'].to_dict()

        results = []
        total = len(tasks_df)

        for idx, (_, row) in enumerate(tasks_df.iterrows()):
            if verbose and idx % 50000 == 0 and idx > 0:
                print(f"    Processed {idx:,}/{total:,} ({idx/total*100:.1f}%)")

            task_name = str(row.get('task_name', ''))
            wbs_id = row.get('wbs_id')
            wbs_name = wbs_names.get(wbs_id, '')

            classification = classifier.classify_task(task_name, wbs_name)

            result = {
                'task_id': row.get('task_id'),
                'phase': classification['phase'],
                'phase_desc': classification['phase_desc'],
                'scope': classification['scope'],
                'scope_desc': classification['scope_desc'],
                'loc_type': classification['loc_type'],
                'loc_type_desc': classification['loc_type_desc'],
                'loc_id': classification['loc_id'],
                'building': classification['building'],
                'building_desc': classification['building_desc'],
                'level': classification['level'],
                'level_desc': classification['level_desc'],
                'label': classification['label'],
                'impact_code': classification.get('impact_code'),
                'impact_type': classification.get('impact_type'),
                'impact_type_desc': classification.get('impact_type_desc'),
                'attributed_to': classification.get('attributed_to'),
                'attributed_to_desc': classification.get('attributed_to_desc'),
                'root_cause': classification.get('root_cause'),
                'root_cause_desc': classification.get('root_cause_desc'),
            }
            results.append(result)

        if verbose:
            print(f"    Classified {total:,} tasks")

        return pd.DataFrame(results)


def prefix_id_columns(df: pd.DataFrame, file_id: int) -> pd.DataFrame:
    """
    Prefix all ID columns with file_id to ensure uniqueness across files.

    Transforms columns ending with '_id' (except 'file_id') from:
        715090 -> "48_715090"

    This ensures primary keys are unique and foreign key relationships
    are preserved within each file.

    For non-numeric values (data issues in some XER files), the value is
    preserved as-is with the file_id prefix.

    Args:
        df: DataFrame to transform
        file_id: The file_id to use as prefix

    Returns:
        DataFrame with transformed ID columns
    """
    df = df.copy()

    def transform_id(x, file_id):
        """Transform a single ID value with file_id prefix"""
        if pd.isna(x) or str(x).strip() == '':
            return ''

        # Try to convert to int for clean numeric IDs
        try:
            return f"{file_id}_{int(float(x))}"
        except (ValueError, TypeError):
            # For non-numeric values, still prefix but keep original value
            return f"{file_id}_{x}"

    for col in df.columns:
        # Skip file_id itself - it stays as the numeric file identifier
        if col == 'file_id':
            continue

        # Transform columns ending with _id
        if col.endswith('_id'):
            df[col] = df[col].apply(lambda x: transform_id(x, file_id))

    return df


def process_single_xer(xer_path: Path, file_id: int, verbose: bool = True) -> dict[str, pd.DataFrame]:
    """
    Process a single XER file and return all tables with file_id added
    and all ID columns prefixed with file_id for uniqueness.

    Returns dict mapping table name (lowercase) to DataFrame
    """
    if verbose:
        print(f"  Parsing {xer_path.name}...")

    try:
        parser = XERParser(str(xer_path))
        tables = parser.parse()
    except Exception as e:
        if verbose:
            print(f"    ⚠️  Error parsing: {e}")
        return None

    if not tables:
        if verbose:
            print(f"    ⚠️  No tables found")
        return None

    result = {}
    table_counts = []

    for table_name, df in tables.items():
        if df is None or len(df) == 0:
            continue

        # Add file_id as first column
        df_with_id = df.copy()
        df_with_id.insert(0, 'file_id', file_id)

        # Prefix all ID columns with file_id for uniqueness across files
        df_with_id = prefix_id_columns(df_with_id, file_id)

        # Use lowercase table names for output files
        result[table_name.lower()] = df_with_id
        table_counts.append(f"{table_name}:{len(df)}")

    if verbose:
        # Show summary
        task_count = len(tables.get('TASK', pd.DataFrame()))
        print(f"    ✓ {len(result)} tables, {task_count:,} tasks")

    return result


def batch_process(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    current_only: bool = False,
    schedule_type: str | None = DEFAULT_SCHEDULE_TYPE,
    verbose: bool = True
) -> dict[str, Path]:
    """
    Process all XER files from input directory and export all tables.

    Auto-discovers XER files in the input directory, extracts dates from
    filenames, and classifies schedule types (YATES/SECAI) automatically.

    Args:
        output_dir: Directory for output CSV files
        current_only: If True, only process the current (latest) file
        schedule_type: Filter by schedule type ('YATES', 'SECAI', or None for all)
        verbose: Print progress messages

    Returns:
        Dict mapping table name to output file path
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if verbose:
        print(f"XER Batch Processor")
        print(f"=" * 60)
        print(f"Input: {XER_DIR}")
        print(f"Output: {output_dir}")
        print()

    # Auto-discover XER files from directory
    files_df = discover_xer_files(XER_DIR, verbose=verbose)

    # Show current file
    current_files = files_df[files_df['is_current']]
    if verbose and len(current_files) > 0:
        print(f"  Current (latest): {current_files.iloc[0]['filename']}")
        print()

    # Apply filters
    files_to_process = files_df.copy()

    if current_only:
        files_to_process = files_to_process[files_to_process['is_current']]
        if verbose:
            print("Filter: Current file only")

    if schedule_type is not None:
        files_to_process = files_to_process[files_to_process['schedule_type'] == schedule_type]
        if verbose:
            print(f"Filter: {schedule_type} schedules only")
    else:
        if verbose:
            print("Filter: All schedule types")

    if verbose:
        print(f"Files to process: {len(files_to_process)}")

    # Reassign sequential file_ids after filtering (1, 2, 3, ... instead of gaps)
    files_to_process = files_to_process.reset_index(drop=True)
    files_to_process['file_id'] = range(1, len(files_to_process) + 1)

    if verbose:
        print(f"Reassigned file_ids: 1-{len(files_to_process)}")
        print()

    # Process each file and collect all tables
    all_tables: dict[str, list[pd.DataFrame]] = {}
    processed_count = 0
    error_count = 0

    if verbose:
        print(f"Processing {len(files_to_process)} XER file(s)...")
        print("-" * 60)

    for _, row in files_to_process.iterrows():
        filename = row['filename']
        file_id = row['file_id']
        xer_path = XER_DIR / filename

        if not xer_path.exists():
            if verbose:
                print(f"  ⚠️  File not found: {filename}")
            error_count += 1
            continue

        result = process_single_xer(xer_path, file_id, verbose)

        if result is not None:
            for table_name, df in result.items():
                if table_name not in all_tables:
                    all_tables[table_name] = []
                all_tables[table_name].append(df)
            processed_count += 1
        else:
            error_count += 1

    if verbose:
        print("-" * 60)
        print(f"Processed: {processed_count}, Errors: {error_count}")
        print()

    # Combine and save all tables
    output_files = {}

    # 1. Save xer_files.csv first
    files_output = output_dir / "xer_files.csv"
    files_to_process.to_csv(files_output, index=False)
    output_files['xer_files'] = files_output

    if verbose:
        print(f"Saving {len(all_tables) + 1} tables...")
        print("-" * 60)
        print(f"✓ xer_files.csv ({len(files_to_process)} rows)")

    # 2. Save all other tables (keep tables needed for taxonomy generation)
    tasks_combined = None
    wbs_combined = None
    taskactv_combined = None
    actvcode_combined = None
    actvtype_combined = None

    for table_name in sorted(all_tables.keys()):
        dfs = all_tables[table_name]
        if dfs:
            combined = pd.concat(dfs, ignore_index=True)

            # Enhance projwbs with hierarchy tier columns
            if table_name == 'projwbs':
                combined = enhance_wbs_with_hierarchy(combined, verbose=verbose)
                wbs_combined = combined

            # Keep tables for taxonomy generation
            if table_name == 'task':
                tasks_combined = combined
            elif table_name == 'taskactv':
                taskactv_combined = combined
            elif table_name == 'actvcode':
                actvcode_combined = combined
            elif table_name == 'actvtype':
                actvtype_combined = combined

            output_path = output_dir / f"{table_name}.csv"
            combined.to_csv(output_path, index=False)
            output_files[table_name] = output_path

            if verbose:
                print(f"✓ {table_name}.csv ({len(combined):,} rows)")

    # 3. Generate task taxonomy (derived data)
    if tasks_combined is not None and wbs_combined is not None:
        taxonomy_df = generate_task_taxonomy(
            tasks_combined,
            wbs_combined,
            taskactv_combined,
            actvcode_combined,
            actvtype_combined,
            verbose=verbose
        )

        # Save to derived directory
        derived_dir = Settings.PRIMAVERA_DERIVED_DIR
        derived_dir.mkdir(parents=True, exist_ok=True)
        taxonomy_path = derived_dir / "task_taxonomy.csv"
        taxonomy_df.to_csv(taxonomy_path, index=False)
        output_files['task_taxonomy'] = taxonomy_path

        if verbose:
            print(f"✓ task_taxonomy.csv ({len(taxonomy_df):,} rows) -> {derived_dir}")

    if verbose:
        print("-" * 60)
        print(f"\n✅ Batch processing complete!")
        print(f"\nOutput directory: {output_dir}")
        if tasks_combined is not None:
            print(f"Derived directory: {Settings.PRIMAVERA_DERIVED_DIR}")
        print(f"Total tables: {len(output_files)}")

    return output_files


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Batch process XER files (auto-discovered) - exports ALL tables',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Schedule Filtering:
  By default, only YATES (General Contractor) schedules are processed.
  Use --all to include all schedules, or --schedule-type to filter explicitly.

Examples:
  %(prog)s                              # YATES schedules only (default)
  %(prog)s --all                        # Process all schedules
  %(prog)s --schedule-type SECAI        # SECAI schedules only
  %(prog)s --current-only               # Only process current file
  %(prog)s --output-dir ./output        # Custom output directory
        """
    )

    parser.add_argument(
        '--output-dir', '-o',
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f'Output directory (default: {DEFAULT_OUTPUT_DIR})'
    )

    parser.add_argument(
        '--current-only', '-c',
        action='store_true',
        help='Only process the current file from manifest'
    )

    # Schedule type filtering - mutually exclusive group
    schedule_group = parser.add_mutually_exclusive_group()
    schedule_group.add_argument(
        '--all', '-a',
        action='store_true',
        dest='all_schedules',
        help='Process all schedules (YATES + SECAI + UNKNOWN)'
    )
    schedule_group.add_argument(
        '--schedule-type', '-s',
        type=str,
        choices=[SCHEDULE_YATES, SCHEDULE_SECAI, SCHEDULE_UNKNOWN],
        default=None,
        help=f'Filter by schedule type (default: {DEFAULT_SCHEDULE_TYPE})'
    )

    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress progress messages'
    )

    args = parser.parse_args()

    # Determine schedule_type filter
    if args.all_schedules:
        schedule_type = None  # No filter - process all
    elif args.schedule_type:
        schedule_type = args.schedule_type
    else:
        schedule_type = DEFAULT_SCHEDULE_TYPE  # Default to YATES

    try:
        batch_process(
            output_dir=args.output_dir,
            current_only=args.current_only,
            schedule_type=schedule_type,
            verbose=not args.quiet
        )
        return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
