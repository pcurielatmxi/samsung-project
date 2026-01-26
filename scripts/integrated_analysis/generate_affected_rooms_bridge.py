"""
Generate affected_rooms_bridge table for Power BI.

Explodes the JSON `affected_rooms` column from RABA, PSI, and TBM into a
bridge/junction table where each room match is a separate row.

This enables Power BI to:
1. Filter by specific rooms across all data sources
2. Show which inspections/work entries affected each room
3. Distinguish single-room vs multi-room matches

Output Schema:
    source              - Data source (RABA, PSI, TBM)
    source_id           - Primary key in source table
    event_date          - Date of the event
    location_id         - FK to dim_location (integer)
    location_code       - Room code (e.g., FAB116101)
    building            - Building code
    room_name           - Human-readable room name
    match_type          - FULL or PARTIAL (grid overlap type)
    source_room_count   - Total rooms affected by this event (for context)
    grid_completeness   - What grid info was in source: FULL, ROW_ONLY, COL_ONLY, LEVEL_ONLY, NONE
    match_quality       - Summary of match types: PRECISE, MIXED, PARTIAL, NONE
    location_review_flag - Boolean suggesting human review needed

Usage:
    python -m scripts.integrated_analysis.generate_affected_rooms_bridge
"""

import json
import pandas as pd
from pathlib import Path
from typing import Optional

from src.config.settings import Settings


def parse_affected_rooms(json_str: str) -> list[dict]:
    """Parse affected_rooms JSON string into list of room dicts."""
    if pd.isna(json_str) or not json_str:
        return []

    try:
        rooms = json.loads(json_str)
        if isinstance(rooms, list):
            return rooms
        return []
    except (json.JSONDecodeError, TypeError):
        return []


def explode_source(
    df: pd.DataFrame,
    source: str,
    id_col: str,
    date_col: str,
) -> pd.DataFrame:
    """
    Explode affected_rooms JSON into bridge table rows.

    Args:
        df: Source DataFrame with affected_rooms column
        source: Source identifier (RABA, PSI, TBM)
        id_col: Column name for source primary key
        date_col: Column name for event date

    Returns:
        DataFrame with one row per room match
    """
    records = []

    for _, row in df.iterrows():
        rooms = parse_affected_rooms(row.get('affected_rooms', ''))
        room_count = len(rooms)

        if room_count == 0:
            continue

        source_id = row.get(id_col)
        event_date = row.get(date_col)

        # Get location quality columns from source record
        grid_completeness = row.get('grid_completeness')
        match_quality = row.get('match_quality')
        location_review_flag = row.get('location_review_flag')

        for room in rooms:
            records.append({
                'source': source,
                'source_id': str(source_id),
                'event_date': event_date,
                'location_id': room.get('location_id'),
                'location_code': room.get('location_code'),
                'building': room.get('building'),
                'room_name': room.get('room_name'),
                'match_type': room.get('match_type'),
                'source_room_count': room_count,
                'grid_completeness': grid_completeness,
                'match_quality': match_quality,
                'location_review_flag': location_review_flag,
            })

    return pd.DataFrame(records)


def generate_bridge_table() -> pd.DataFrame:
    """Generate the unified affected_rooms_bridge table."""
    settings = Settings()
    processed_dir = settings.PROCESSED_DATA_DIR

    bridge_parts = []

    # Load RABA
    raba_path = processed_dir / 'raba' / 'raba_consolidated.csv'
    if raba_path.exists():
        print(f"Loading RABA from {raba_path}...")
        raba = pd.read_csv(raba_path)
        raba_bridge = explode_source(
            raba,
            source='RABA',
            id_col='inspection_id',
            date_col='report_date_normalized',
        )
        print(f"  RABA: {len(raba)} records → {len(raba_bridge)} bridge rows")
        bridge_parts.append(raba_bridge)
    else:
        print(f"  RABA file not found: {raba_path}")

    # Load PSI
    psi_path = processed_dir / 'psi' / 'psi_consolidated.csv'
    if psi_path.exists():
        print(f"Loading PSI from {psi_path}...")
        psi = pd.read_csv(psi_path)
        psi_bridge = explode_source(
            psi,
            source='PSI',
            id_col='inspection_id',
            date_col='report_date_normalized',
        )
        print(f"  PSI: {len(psi)} records → {len(psi_bridge)} bridge rows")
        bridge_parts.append(psi_bridge)
    else:
        print(f"  PSI file not found: {psi_path}")

    # Load TBM
    tbm_path = processed_dir / 'tbm' / 'work_entries_enriched.csv'
    if tbm_path.exists():
        print(f"Loading TBM from {tbm_path}...")
        tbm = pd.read_csv(tbm_path)

        # TBM doesn't have a single ID column - create composite key
        # Using file_id + row index within file
        if 'file_id' in tbm.columns:
            tbm['_tbm_id'] = tbm.apply(
                lambda r: f"TBM-{r.get('file_id', 'UNK')}-{r.name}",
                axis=1
            )
        else:
            tbm['_tbm_id'] = tbm.index.map(lambda i: f"TBM-{i}")

        tbm_bridge = explode_source(
            tbm,
            source='TBM',
            id_col='_tbm_id',
            date_col='report_date',
        )
        print(f"  TBM: {len(tbm)} records → {len(tbm_bridge)} bridge rows")
        bridge_parts.append(tbm_bridge)
    else:
        print(f"  TBM file not found: {tbm_path}")

    # Combine all sources
    if not bridge_parts:
        print("No data found!")
        return pd.DataFrame()

    bridge = pd.concat(bridge_parts, ignore_index=True)

    # Ensure consistent types
    bridge['event_date'] = pd.to_datetime(bridge['event_date'], errors='coerce')
    bridge['location_id'] = pd.to_numeric(bridge['location_id'], errors='coerce').astype('Int64')
    bridge['source_room_count'] = pd.to_numeric(bridge['source_room_count'], errors='coerce').astype('Int64')

    # Sort by date and source
    bridge = bridge.sort_values(['event_date', 'source', 'source_id'])

    return bridge


def main():
    """Generate and save the affected_rooms_bridge table."""
    settings = Settings()

    print("Generating affected_rooms_bridge table...")
    print("=" * 60)

    bridge = generate_bridge_table()

    if bridge.empty:
        print("No bridge records generated.")
        return

    # Output path
    output_dir = settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'bridge_tables'
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / 'affected_rooms_bridge.csv'

    # Save
    bridge.to_csv(output_path, index=False)
    print("=" * 60)
    print(f"Saved {len(bridge)} rows to {output_path}")

    # Summary stats
    print("\nSummary:")
    print(f"  Total bridge rows: {len(bridge):,}")
    print(f"  By source:")
    for source, count in bridge['source'].value_counts().items():
        print(f"    {source}: {count:,}")
    print(f"  Unique rooms: {bridge['location_code'].nunique():,}")
    print(f"  Date range: {bridge['event_date'].min()} to {bridge['event_date'].max()}")

    # Match type distribution
    print(f"  Match types:")
    for mt, count in bridge['match_type'].value_counts().items():
        pct = count / len(bridge) * 100
        print(f"    {mt}: {count:,} ({pct:.1f}%)")

    # Grid completeness distribution
    if 'grid_completeness' in bridge.columns:
        print(f"  Grid completeness:")
        for gc, count in bridge['grid_completeness'].value_counts(dropna=False).items():
            pct = count / len(bridge) * 100
            label = gc if pd.notna(gc) else 'N/A'
            print(f"    {label}: {count:,} ({pct:.1f}%)")

    # Location review flag
    if 'location_review_flag' in bridge.columns:
        review_count = bridge['location_review_flag'].sum()
        review_pct = review_count / len(bridge) * 100 if len(bridge) > 0 else 0
        print(f"  Needs review: {review_count:,} ({review_pct:.1f}%)")


if __name__ == '__main__':
    main()
