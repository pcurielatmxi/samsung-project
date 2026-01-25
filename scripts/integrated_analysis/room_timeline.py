#!/usr/bin/env python3
"""
Room Timeline Analysis Tool

Retrieves all work entries and inspections for a specific room within a time window,
cross-referenced with P6 schedule snapshots to show planned vs actual timing.

Usage:
    python -m scripts.integrated_analysis.room_timeline FAB116406 --start 2024-01-01 --end 2024-03-31
    python -m scripts.integrated_analysis.room_timeline FAB116406 --start 2024-01-01 --end 2024-03-31 --output timeline.csv

The script compares actual work dates against P6 schedule snapshots to determine
if work was on-schedule, early, or late relative to the plan at that time.
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config.settings import Settings

settings = Settings()
PROCESSED_DIR = settings.PROCESSED_DATA_DIR


def load_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load RABA, PSI, TBM, and P6 task data."""

    # Quality inspections
    raba = pd.read_csv(PROCESSED_DIR / 'raba' / 'raba_consolidated.csv')
    raba['source'] = 'RABA'
    raba['event_date'] = pd.to_datetime(raba['report_date_normalized'], errors='coerce')

    psi = pd.read_csv(PROCESSED_DIR / 'psi' / 'psi_consolidated.csv')
    psi['source'] = 'PSI'
    psi['event_date'] = pd.to_datetime(psi['report_date_normalized'], errors='coerce')

    # TBM work entries
    tbm = pd.read_csv(PROCESSED_DIR / 'tbm' / 'work_entries_enriched.csv')
    tbm['source'] = 'TBM'
    tbm['event_date'] = pd.to_datetime(tbm['report_date'], errors='coerce')

    # P6 tasks - join from multiple source files
    p6 = load_p6_schedule_data()

    return raba, psi, tbm, p6


def load_p6_schedule_data() -> pd.DataFrame:
    """
    Load and join P6 schedule data from multiple source files.

    Joins:
    - task.csv: Schedule dates per task per snapshot
    - p6_task_taxonomy.csv: Location mapping (location_code)
    - xer_files.csv: Data date per file (extracted from filename)

    Returns DataFrame with: task_id, file_id, data_date, location_code,
    task_name, start_date, finish_date, actual_start, actual_finish, percent_complete
    """
    import re

    p6_dir = PROCESSED_DIR / 'primavera'

    # Check files exist
    task_path = p6_dir / 'task.csv'
    taxonomy_path = p6_dir / 'p6_task_taxonomy.csv'
    xer_path = p6_dir / 'xer_files.csv'

    if not all(p.exists() for p in [task_path, taxonomy_path, xer_path]):
        print("Warning: P6 source files not found")
        return pd.DataFrame()

    print("  Loading P6 schedule data (this may take a moment)...")

    # Load XER files and extract data_date from filename
    xer = pd.read_csv(xer_path)

    def extract_date_from_filename(fn):
        """Extract date from XER filename like 'SAMSUNG-TFAB1-03-08-2024- Live.xer'"""
        match = re.search(r'(\d{2}-\d{2}-\d{4})', str(fn))
        if match:
            try:
                return pd.to_datetime(match.group(1), format='%m-%d-%Y')
            except:
                return None
        return None

    xer['data_date'] = xer['filename'].apply(extract_date_from_filename)
    xer = xer[['file_id', 'data_date']].dropna()

    # Load taxonomy for location_code mapping
    taxonomy = pd.read_csv(taxonomy_path, usecols=['task_id', 'location_code'])
    taxonomy = taxonomy.dropna(subset=['location_code'])

    # Load task dates (only needed columns)
    task_cols = [
        'file_id', 'task_id', 'task_name',
        'target_start_date', 'target_end_date',
        'act_start_date', 'act_end_date',
        'phys_complete_pct', 'status_code'
    ]
    task = pd.read_csv(task_path, usecols=task_cols, low_memory=False)

    # Join task with xer_files for data_date
    task = task.merge(xer, on='file_id', how='inner')

    # Join with taxonomy for location_code
    # Note: task_id in taxonomy may be without file prefix
    # Extract base task_id from composite (e.g., "1_100225644" -> "100225644")
    task['base_task_id'] = task['task_id'].astype(str).str.split('_').str[-1]
    taxonomy['base_task_id'] = taxonomy['task_id'].astype(str).str.split('_').str[-1]

    p6 = task.merge(taxonomy[['base_task_id', 'location_code']], on='base_task_id', how='inner')

    # Rename columns for consistency
    p6 = p6.rename(columns={
        'target_start_date': 'start_date',
        'target_end_date': 'finish_date',
        'act_start_date': 'actual_start',
        'act_end_date': 'actual_finish',
        'phys_complete_pct': 'percent_complete',
        'status_code': 'status',
    })

    # Parse dates
    for col in ['data_date', 'start_date', 'finish_date', 'actual_start', 'actual_finish']:
        if col in p6.columns:
            p6[col] = pd.to_datetime(p6[col], errors='coerce')

    print(f"  Loaded {len(p6):,} task-snapshot records with location codes")

    return p6


def find_entries_for_room(
    df: pd.DataFrame,
    room_code: str,
    start_date: datetime,
    end_date: datetime,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Find all entries that include a room in their affected_rooms.

    Returns:
        Tuple of (single_room_matches, multi_room_matches)
    """
    if 'affected_rooms' not in df.columns:
        return pd.DataFrame(), pd.DataFrame()

    # Filter by date range first
    mask = (
        df['event_date'].notna() &
        (df['event_date'] >= start_date) &
        (df['event_date'] <= end_date)
    )
    df_filtered = df[mask].copy()

    if df_filtered.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Find entries that include this room
    def room_in_affected(affected_rooms_json: str, target_room: str) -> bool:
        if pd.isna(affected_rooms_json):
            return False
        try:
            rooms = json.loads(affected_rooms_json)
            return any(r.get('location_code') == target_room for r in rooms)
        except (json.JSONDecodeError, TypeError):
            return False

    room_mask = df_filtered['affected_rooms'].apply(lambda x: room_in_affected(x, room_code))
    matches = df_filtered[room_mask].copy()

    if matches.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Split by single vs multi-room
    single = matches[matches['affected_rooms_count'] == 1].copy()
    multi = matches[matches['affected_rooms_count'] > 1].copy()

    return single, multi


def get_schedule_at_date(
    p6: pd.DataFrame,
    room_code: str,
    reference_date: datetime,
) -> List[Dict[str, Any]]:
    """
    Get P6 tasks for a room using the schedule snapshot closest to (but before) reference_date.

    This shows what the "plan of record" was at the time of the work/inspection.
    Prioritizes tasks that were active or upcoming at the reference date.
    """
    if p6.empty:
        return []

    # Find tasks for this room
    room_tasks = p6[p6['location_code'] == room_code].copy()

    if room_tasks.empty:
        return []

    # Find the snapshot with data_date closest to but <= reference_date
    valid_snapshots = room_tasks[room_tasks['data_date'] <= reference_date]

    if valid_snapshots.empty:
        # No P6 snapshots exist for this room before the reference date
        # This is common for early construction phases (foundation) before
        # room-level tasks were added to the schedule
        return []

    # Get the latest data_date that's still <= reference_date
    latest_data_date = valid_snapshots['data_date'].max()
    snapshot = valid_snapshots[valid_snapshots['data_date'] == latest_data_date].copy()

    # Calculate relevance score for each task:
    # 1. In-progress at reference_date (started but not finished)
    # 2. Starting soon (within 30 days after reference_date)
    # 3. Recently completed (within 30 days before reference_date)
    # 4. Everything else
    def task_relevance(row):
        start = row['start_date']
        finish = row['finish_date']
        actual_start = row['actual_start']
        actual_finish = row['actual_finish']
        pct = row['percent_complete'] if pd.notna(row['percent_complete']) else 0

        # Use actual dates if available
        eff_start = actual_start if pd.notna(actual_start) else start
        eff_finish = actual_finish if pd.notna(actual_finish) else finish

        if pd.isna(eff_start) and pd.isna(eff_finish):
            return 99  # No dates, lowest priority

        # In-progress: started before reference, not yet finished
        if pd.notna(eff_start) and eff_start <= reference_date:
            if pd.isna(eff_finish) or eff_finish > reference_date:
                if pct > 0 and pct < 100:
                    return 1  # Definitely in progress
                return 2  # Started but unclear status

        # Starting soon (within 30 days)
        if pd.notna(eff_start) and eff_start > reference_date:
            days_until = (eff_start - reference_date).days
            if days_until <= 30:
                return 3

        # Recently completed (within 30 days)
        if pd.notna(eff_finish) and eff_finish <= reference_date:
            days_since = (reference_date - eff_finish).days
            if days_since <= 30:
                return 4

        # Future tasks
        if pd.notna(eff_start) and eff_start > reference_date:
            return 5

        return 10  # Default

    snapshot['relevance'] = snapshot.apply(task_relevance, axis=1)
    snapshot = snapshot.sort_values('relevance')

    results = []
    for _, task in snapshot.head(10).iterrows():  # Limit to 10 most relevant
        results.append({
            'task_id': task.get('task_id'),
            'task_name': task.get('task_name'),
            'data_date': task.get('data_date'),
            'planned_start': task.get('start_date'),
            'planned_finish': task.get('finish_date'),
            'actual_start': task.get('actual_start'),
            'actual_finish': task.get('actual_finish'),
            'percent_complete': task.get('percent_complete'),
            'status': task.get('status'),
            'relevance': int(task.get('relevance', 99)),
        })

    return results


def format_entry(entry: pd.Series, source: str) -> Dict[str, Any]:
    """Format a RABA/PSI/TBM entry for display."""
    result = {
        'source': source,
        'date': entry.get('event_date'),
    }

    if source in ['RABA', 'PSI']:
        result['id'] = entry.get('inspection_id', '')
        result.update({
            'type': entry.get('inspection_type_normalized', entry.get('inspection_type', '')),
            'outcome': entry.get('outcome', ''),
            'company': entry.get('contractor', entry.get('contractor_raw', '')),
            'trade': entry.get('trade', ''),
            'grid': entry.get('grid', ''),
            'summary': entry.get('summary', '')[:100] if pd.notna(entry.get('summary')) else '',
        })
    else:  # TBM
        # TBM uses different column names
        file_id = entry.get('file_id', '')
        row_num = entry.get('row_num', '')
        result['id'] = f"TBM-{file_id}-{row_num}" if file_id and row_num else ''
        result.update({
            'type': entry.get('work_activities', '')[:50] if pd.notna(entry.get('work_activities')) else '',
            'outcome': '',
            'company': entry.get('tier2_sc', entry.get('tier1_gc', '')),
            'trade': entry.get('trade_inferred', ''),
            'grid': entry.get('grid_raw', ''),
            'summary': entry.get('work_activities', '')[:100] if pd.notna(entry.get('work_activities')) else '',
            'foreman': entry.get('foreman', ''),
            'employees': entry.get('num_employees', ''),
        })

    # Parse affected rooms count
    result['room_count'] = entry.get('affected_rooms_count', 1)

    return result


def analyze_room_timeline(
    room_code: str,
    start_date: datetime,
    end_date: datetime,
    include_schedule: bool = True,
) -> Dict[str, Any]:
    """
    Analyze all work and inspections for a room within a time window.

    Returns a structured analysis with:
    - Single room matches (precise)
    - Multi-room matches (less precise)
    - Schedule context for each entry
    """
    print(f"\nLoading data...")
    raba, psi, tbm, p6 = load_data()

    print(f"Searching for room {room_code} from {start_date.date()} to {end_date.date()}...")

    # Find matches in each source
    raba_single, raba_multi = find_entries_for_room(raba, room_code, start_date, end_date)
    psi_single, psi_multi = find_entries_for_room(psi, room_code, start_date, end_date)
    tbm_single, tbm_multi = find_entries_for_room(tbm, room_code, start_date, end_date)

    # Combine results
    single_matches = []
    multi_matches = []

    for df, source in [(raba_single, 'RABA'), (psi_single, 'PSI'), (tbm_single, 'TBM')]:
        for _, entry in df.iterrows():
            formatted = format_entry(entry, source)
            if include_schedule and not p6.empty:
                formatted['schedule'] = get_schedule_at_date(p6, room_code, entry['event_date'])
            single_matches.append(formatted)

    for df, source in [(raba_multi, 'RABA'), (psi_multi, 'PSI'), (tbm_multi, 'TBM')]:
        for _, entry in df.iterrows():
            formatted = format_entry(entry, source)
            if include_schedule and not p6.empty:
                formatted['schedule'] = get_schedule_at_date(p6, room_code, entry['event_date'])
            multi_matches.append(formatted)

    # Sort by date
    single_matches.sort(key=lambda x: x['date'] if pd.notna(x['date']) else datetime.min)
    multi_matches.sort(key=lambda x: x['date'] if pd.notna(x['date']) else datetime.min)

    return {
        'room_code': room_code,
        'start_date': start_date,
        'end_date': end_date,
        'single_room_matches': single_matches,
        'multi_room_matches': multi_matches,
        'summary': {
            'single_count': len(single_matches),
            'multi_count': len(multi_matches),
            'total': len(single_matches) + len(multi_matches),
            'by_source': {
                'RABA': len(raba_single) + len(raba_multi),
                'PSI': len(psi_single) + len(psi_multi),
                'TBM': len(tbm_single) + len(tbm_multi),
            }
        }
    }


def print_results(results: Dict[str, Any]) -> None:
    """Print analysis results in a readable format."""

    print(f"\n{'='*80}")
    print(f"ROOM TIMELINE: {results['room_code']}")
    print(f"Period: {results['start_date'].date()} to {results['end_date'].date()}")
    print(f"{'='*80}")

    summary = results['summary']
    print(f"\nTotal Matches: {summary['total']}")
    print(f"  - Single room (precise): {summary['single_count']}")
    print(f"  - Multi-room (shared): {summary['multi_count']}")
    print(f"\nBy Source:")
    for source, count in summary['by_source'].items():
        print(f"  - {source}: {count}")

    # Single room matches
    if results['single_room_matches']:
        print(f"\n{'-'*80}")
        print("SINGLE ROOM MATCHES (Precise - only this room)")
        print(f"{'-'*80}")

        for entry in results['single_room_matches']:
            date_str = entry['date'].strftime('%Y-%m-%d') if pd.notna(entry['date']) else 'N/A'
            print(f"\n[{entry['source']}] {date_str} - {entry['type']}")
            print(f"  ID: {entry['id']}")
            if entry['outcome']:
                print(f"  Outcome: {entry['outcome']}")
            if entry['company']:
                print(f"  Company: {entry['company']}")
            if entry['trade']:
                print(f"  Trade: {entry['trade']}")
            if entry['grid']:
                print(f"  Grid: {entry['grid']}")
            if entry['summary']:
                print(f"  Summary: {entry['summary']}...")

            # Schedule context - only show if tasks are relevant to the inspection date
            if entry.get('schedule'):
                # Filter to show only tasks with relevance <= 5 (in-progress, starting soon, or recently completed)
                relevant_tasks = [t for t in entry['schedule'] if t.get('relevance', 99) <= 5]
                if relevant_tasks:
                    print(f"  P6 tasks at this date ({len(relevant_tasks)} relevant):")
                    for task in relevant_tasks[:3]:
                        planned_start = task['planned_start'].strftime('%Y-%m-%d') if pd.notna(task['planned_start']) else 'N/A'
                        planned_finish = task['planned_finish'].strftime('%Y-%m-%d') if pd.notna(task['planned_finish']) else 'N/A'
                        pct = f"{task['percent_complete']:.0f}%" if pd.notna(task['percent_complete']) else 'N/A'
                        print(f"    - {task['task_name'][:60]}")
                        print(f"      Planned: {planned_start} → {planned_finish} ({pct})")
                else:
                    print(f"  P6 schedule: No tasks in-progress/starting soon (foundation work not tracked by room)")

    # Multi-room matches
    if results['multi_room_matches']:
        print(f"\n{'-'*80}")
        print(f"MULTI-ROOM MATCHES (Shared with {results['multi_room_matches'][0]['room_count'] if results['multi_room_matches'] else 'N/A'} other rooms)")
        print(f"{'-'*80}")

        for entry in results['multi_room_matches'][:10]:  # Limit display
            date_str = entry['date'].strftime('%Y-%m-%d') if pd.notna(entry['date']) else 'N/A'
            print(f"\n[{entry['source']}] {date_str} - {entry['type']} (affects {entry['room_count']} rooms)")
            print(f"  ID: {entry['id']}")
            if entry['outcome']:
                print(f"  Outcome: {entry['outcome']}")
            if entry['company']:
                print(f"  Company: {entry['company']}")

        if len(results['multi_room_matches']) > 10:
            print(f"\n  ... and {len(results['multi_room_matches']) - 10} more multi-room entries")


def export_to_csv(results: Dict[str, Any], output_path: Path) -> None:
    """Export results to CSV for further analysis."""

    rows = []

    for entry in results['single_room_matches']:
        row = {
            'room_code': results['room_code'],
            'match_type': 'SINGLE',
            'source': entry['source'],
            'date': entry['date'],
            'id': entry['id'],
            'type': entry['type'],
            'outcome': entry['outcome'],
            'company': entry['company'],
            'trade': entry['trade'],
            'grid': entry['grid'],
            'summary': entry['summary'],
            'room_count': entry['room_count'],
        }

        # Add schedule info if available
        if entry.get('schedule'):
            task = entry['schedule'][0] if entry['schedule'] else {}
            row['sched_task_name'] = task.get('task_name', '')
            row['sched_data_date'] = task.get('data_date', '')
            row['sched_planned_start'] = task.get('planned_start', '')
            row['sched_planned_finish'] = task.get('planned_finish', '')
            row['sched_pct_complete'] = task.get('percent_complete', '')

        rows.append(row)

    for entry in results['multi_room_matches']:
        row = {
            'room_code': results['room_code'],
            'match_type': 'MULTI',
            'source': entry['source'],
            'date': entry['date'],
            'id': entry['id'],
            'type': entry['type'],
            'outcome': entry['outcome'],
            'company': entry['company'],
            'trade': entry['trade'],
            'grid': entry['grid'],
            'summary': entry['summary'],
            'room_count': entry['room_count'],
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False)
    print(f"\nExported {len(df)} entries to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze room timeline with schedule cross-reference',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show all work/inspections for a room in Q1 2024
  python -m scripts.integrated_analysis.room_timeline FAB116406 --start 2024-01-01 --end 2024-03-31

  # Export to CSV for further analysis
  python -m scripts.integrated_analysis.room_timeline FAB116406 --start 2024-01-01 --end 2024-03-31 --output timeline.csv

  # Skip schedule comparison (faster)
  python -m scripts.integrated_analysis.room_timeline FAB116406 --start 2024-01-01 --end 2024-03-31 --no-schedule
        """
    )

    parser.add_argument('room_code', help='Room code (e.g., FAB116406)')
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--output', '-o', help='Output CSV path')
    parser.add_argument('--no-schedule', action='store_true', help='Skip P6 schedule lookup')

    args = parser.parse_args()

    # Parse dates
    try:
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
        end_date = datetime.strptime(args.end, '%Y-%m-%d')
    except ValueError as e:
        print(f"Error: Invalid date format. Use YYYY-MM-DD. {e}")
        sys.exit(1)

    # Run analysis
    results = analyze_room_timeline(
        args.room_code,
        start_date,
        end_date,
        include_schedule=not args.no_schedule,
    )

    # Output
    print_results(results)

    if args.output:
        export_to_csv(results, Path(args.output))


if __name__ == '__main__':
    main()
