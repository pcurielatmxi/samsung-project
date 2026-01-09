"""Narrative statements data loader.

Loads extracted statements from P6 narratives, weekly reports, and other documents.
"""

import sys
from pathlib import Path
from typing import Dict, Any
import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings
from .base import MonthlyPeriod, DataAvailability, filter_by_period, get_date_range


def load_narrative_data(period: MonthlyPeriod) -> Dict[str, Any]:
    """Load narrative statements for a monthly period.

    Filters statements by event_date field.
    Note: Many statements lack event dates (only ~41% have dates).

    Args:
        period: Monthly period to filter by

    Returns:
        Dict with:
        - 'statements': DataFrame of statements with event dates in period
        - 'statements_undated': DataFrame of statements without dates (for context)
        - 'documents': DataFrame of source document metadata
        - 'availability': DataAvailability info
    """
    # Load statements
    statements_path = Settings.NARRATIVES_PROCESSED_DIR / 'narrative_statements.csv'

    if not statements_path.exists():
        return {
            'statements': pd.DataFrame(),
            'statements_undated': pd.DataFrame(),
            'documents': pd.DataFrame(),
            'availability': DataAvailability(
                source='Narratives',
                period=period,
                record_count=0,
                coverage_notes=['File not found'],
            ),
        }

    statements = pd.read_csv(statements_path, low_memory=False)
    statements['event_date'] = pd.to_datetime(statements['event_date'], errors='coerce')

    # Split into dated and undated
    has_date = statements['event_date'].notna()
    dated_statements = statements[has_date].copy()
    undated_statements = statements[~has_date].copy()

    # Filter dated statements to period
    if not dated_statements.empty:
        filtered = filter_by_period(dated_statements, 'event_date', period)
    else:
        filtered = pd.DataFrame()

    # Load document metadata for context
    docs_path = Settings.NARRATIVES_PROCESSED_DIR / 'dim_narrative_file.csv'
    if docs_path.exists():
        documents = pd.read_csv(docs_path, low_memory=False)
        documents['document_date'] = pd.to_datetime(documents['document_date'], errors='coerce')
    else:
        documents = pd.DataFrame()

    # Enrich statements with document info
    if not filtered.empty and not documents.empty:
        if 'narrative_file_id' in filtered.columns and 'narrative_file_id' in documents.columns:
            filtered = filtered.merge(
                documents[['narrative_file_id', 'filename', 'document_type', 'document_date', 'author']],
                on='narrative_file_id',
                how='left',
                suffixes=('', '_doc')
            )

    # Parse parties and locations (stored as pipe-delimited strings)
    if not filtered.empty:
        if 'parties' in filtered.columns:
            filtered['parties_list'] = filtered['parties'].apply(
                lambda x: x.split('|') if pd.notna(x) and x else []
            )
            filtered['parties_count'] = filtered['parties_list'].apply(len)

        if 'locations' in filtered.columns:
            filtered['locations_list'] = filtered['locations'].apply(
                lambda x: x.split('|') if pd.notna(x) and x else []
            )

    # Categorize statements for delay attribution
    # Categories that indicate potentially unjustified delays (contractor responsibility)
    unjustified_categories = ['quality_issue', 'coordination', 'resource', 'safety']
    # Categories that indicate potentially justified delays (external factors)
    justified_categories = ['weather', 'owner_direction', 'design_issue', 'scope_change']
    # Neutral - need more context to determine
    neutral_categories = ['delay', 'progress', 'other', 'commitment', 'dispute']

    if not filtered.empty and 'category' in filtered.columns:
        def classify_justification(cat):
            if pd.isna(cat):
                return 'UNKNOWN'
            cat_lower = str(cat).lower()
            if cat_lower in justified_categories:
                return 'JUSTIFIED'
            elif cat_lower in unjustified_categories:
                return 'UNJUSTIFIED'
            else:
                return 'REVIEW'  # Needs human review

        filtered['delay_justified'] = filtered['category'].apply(classify_justification)

    # Coverage notes
    notes = []
    total_statements = len(statements)
    dated_count = len(dated_statements)
    notes.append(f"{dated_count}/{total_statements} statements have dates ({dated_count/total_statements*100:.0f}%)")

    if not filtered.empty:
        date_range = get_date_range(filtered, 'event_date')
        if date_range:
            notes.append(f"Event dates: {date_range[0]} to {date_range[1]}")

        if 'impact_days' in filtered.columns:
            with_impact = filtered['impact_days'].notna().sum()
            total_impact = filtered['impact_days'].sum()
            notes.append(f"{with_impact} statements with impact ({total_impact:.0f} total days)")

        if 'category' in filtered.columns:
            categories = filtered['category'].value_counts()
            top_cats = categories.head(3).to_dict()
            notes.append(f"Top categories: {top_cats}")

    availability = DataAvailability(
        source='Narratives',
        period=period,
        record_count=len(filtered),
        date_range=get_date_range(filtered, 'event_date') if not filtered.empty else None,
        coverage_notes=notes,
    )

    return {
        'statements': filtered,
        'statements_undated': undated_statements,
        'documents': documents,
        'availability': availability,
    }
