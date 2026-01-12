"""Narrative statements data loader for snapshot reports.

Loads extracted narrative statements from P6 narratives, weekly reports, etc.
filtered to snapshot periods.
"""

import sys
from pathlib import Path
from typing import Dict, Any
import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings
from .base import SnapshotPeriod, DataAvailability, filter_by_period, get_date_range


# Delay categorization patterns
JUSTIFIED_PATTERNS = [
    'weather', 'rain', 'storm', 'flood', 'hurricane',
    'owner change', 'design change', 'scope change', 'rfi',
    'material delay', 'supply chain', 'shipping',
    'permit', 'inspection', 'authority',
    'covid', 'pandemic',
    'holiday', 'shutdown',
]

UNJUSTIFIED_PATTERNS = [
    'rework', 'quality', 'defect', 'failure', 'rejection',
    'coordination', 'conflict', 'interference',
    'manpower', 'staffing', 'labor shortage',
    'equipment breakdown', 'equipment failure',
    'productivity', 'performance',
    'schedule', 'sequence', 'predecessor',
]


def _categorize_delay(statement_text: str, category: str = None) -> str:
    """Categorize delay statement as JUSTIFIED or UNJUSTIFIED."""
    if pd.isna(statement_text):
        return 'UNKNOWN'

    text_lower = str(statement_text).lower()

    # Check justified patterns
    for pattern in JUSTIFIED_PATTERNS:
        if pattern in text_lower:
            return 'JUSTIFIED'

    # Check unjustified patterns
    for pattern in UNJUSTIFIED_PATTERNS:
        if pattern in text_lower:
            return 'UNJUSTIFIED'

    # Use category if available
    if category:
        cat_lower = str(category).lower()
        if 'weather' in cat_lower or 'owner' in cat_lower:
            return 'JUSTIFIED'
        if 'quality' in cat_lower or 'rework' in cat_lower or 'coordination' in cat_lower:
            return 'UNJUSTIFIED'

    return 'UNKNOWN'


def load_narrative_data(period: SnapshotPeriod) -> Dict[str, Any]:
    """Load narrative statements for a snapshot period.

    Statements inherit dates from their source document (data_date or document_date)
    when event_date is not available.

    Args:
        period: SnapshotPeriod to filter by

    Returns:
        Dict with:
        - 'statements': DataFrame of statements with dates in period
        - 'statements_undated': DataFrame of statements without dates (even after inheritance)
        - 'documents': DataFrame of source documents
        - 'availability': DataAvailability info
    """
    narratives_path = Settings.NARRATIVES_PROCESSED_DIR / 'narrative_statements.csv'
    files_path = Settings.NARRATIVES_PROCESSED_DIR / 'dim_narrative_file.csv'

    if not narratives_path.exists():
        return {
            'statements': pd.DataFrame(),
            'statements_undated': pd.DataFrame(),
            'documents': pd.DataFrame(),
            'availability': DataAvailability(
                source='Narratives',
                period=period,
                record_count=0,
                coverage_notes=['narrative_statements.csv not found'],
            ),
        }

    df = pd.read_csv(narratives_path, low_memory=False)

    # Load document metadata to inherit dates
    files_df = pd.DataFrame()
    if files_path.exists():
        files_df = pd.read_csv(files_path, low_memory=False)
        # Parse file dates
        for col in ['data_date', 'document_date']:
            if col in files_df.columns:
                files_df[col] = pd.to_datetime(files_df[col], errors='coerce')

    # Parse event date
    if 'event_date' in df.columns:
        df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')

    # Inherit dates from source documents for statements without event_date
    if not files_df.empty and 'narrative_file_id' in df.columns:
        # Merge to get file dates
        df = df.merge(
            files_df[['narrative_file_id', 'data_date', 'document_date', 'filename']].rename(
                columns={'data_date': 'file_data_date', 'document_date': 'file_document_date'}
            ),
            on='narrative_file_id',
            how='left'
        )

        # Inherit date: prefer data_date (schedule reference), fall back to document_date
        if 'event_date' not in df.columns:
            df['event_date'] = pd.NaT

        missing_date = df['event_date'].isna()
        # First try data_date (more precise - the schedule date the document refers to)
        df.loc[missing_date, 'event_date'] = df.loc[missing_date, 'file_data_date']
        # Then try document_date for remaining
        still_missing = df['event_date'].isna()
        df.loc[still_missing, 'event_date'] = df.loc[still_missing, 'file_document_date']

    # Split into dated vs undated
    has_date = df['event_date'].notna() if 'event_date' in df.columns else pd.Series([False] * len(df))

    dated_df = df[has_date].copy()
    undated_df = df[~has_date].copy()

    # Filter dated statements to period
    if not dated_df.empty and 'event_date' in dated_df.columns:
        filtered = filter_by_period(dated_df, 'event_date', period)
    else:
        filtered = pd.DataFrame()

    # Add delay categorization
    if not filtered.empty:
        text_col = 'statement_text' if 'statement_text' in filtered.columns else 'text'
        cat_col = 'category' if 'category' in filtered.columns else None

        if text_col in filtered.columns:
            filtered['delay_justified'] = filtered.apply(
                lambda row: _categorize_delay(
                    row.get(text_col, ''),
                    row.get(cat_col, '') if cat_col else None
                ),
                axis=1
            )

    # Use file metadata as documents (already loaded above)
    documents = files_df

    # Build availability
    date_range = get_date_range(filtered, 'event_date') if not filtered.empty else None

    coverage_notes = []
    if not filtered.empty:
        coverage_notes.append(f"{len(filtered):,} statements with dates in period")
        if 'category' in filtered.columns:
            categories = filtered['category'].value_counts().head(3)
            for cat, count in categories.items():
                coverage_notes.append(f"{cat}: {count}")
    else:
        coverage_notes.append('No dated statements in period')

    coverage_notes.append(f"{len(undated_df):,} undated statements (not period-filtered)")

    availability = DataAvailability(
        source='Narratives',
        period=period,
        record_count=len(filtered),
        date_range=date_range,
        coverage_notes=coverage_notes,
    )

    return {
        'statements': filtered,
        'statements_undated': undated_df,
        'documents': documents,
        'availability': availability,
    }
