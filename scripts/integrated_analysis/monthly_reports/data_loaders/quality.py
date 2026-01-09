"""Quality inspection data loader (RABA + PSI).

Loads and combines quality inspection data from both third-party QC sources.
"""

import sys
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings
from .base import MonthlyPeriod, DataAvailability, filter_by_period, get_date_range


def _load_raba(period: MonthlyPeriod) -> tuple[pd.DataFrame, DataAvailability]:
    """Load RABA inspection data for period."""
    raba_path = Settings.RABA_PROCESSED_DIR / 'raba_consolidated.csv'

    if not raba_path.exists():
        return pd.DataFrame(), DataAvailability(
            source='RABA',
            period=period,
            record_count=0,
            coverage_notes=['File not found'],
        )

    df = pd.read_csv(raba_path, low_memory=False)
    df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')

    # Filter to period
    filtered = filter_by_period(df, 'report_date', period)

    # Standardize columns for combined output
    filtered['source'] = 'RABA'
    filtered['inspection_type'] = filtered.get('test_type_normalized', filtered.get('test_type', ''))
    filtered['inspection_category'] = filtered.get('test_category', '')

    # Coverage notes
    notes = []
    date_range = get_date_range(filtered, 'report_date')
    if date_range:
        notes.append(f"Dates: {date_range[0]} to {date_range[1]}")

    # Check for gaps (days without inspections)
    if not filtered.empty:
        dates = pd.to_datetime(filtered['report_date']).dt.date
        all_days = pd.date_range(period.start_date, period.end_date).date
        missing_days = set(all_days) - set(dates)
        if missing_days:
            notes.append(f"Missing {len(missing_days)} days")

    availability = DataAvailability(
        source='RABA',
        period=period,
        record_count=len(filtered),
        date_range=date_range,
        coverage_notes=notes,
    )

    return filtered, availability


def _load_psi(period: MonthlyPeriod) -> tuple[pd.DataFrame, DataAvailability]:
    """Load PSI inspection data for period."""
    psi_path = Settings.PSI_PROCESSED_DIR / 'psi_consolidated.csv'

    if not psi_path.exists():
        return pd.DataFrame(), DataAvailability(
            source='PSI',
            period=period,
            record_count=0,
            coverage_notes=['File not found'],
        )

    df = pd.read_csv(psi_path, low_memory=False)
    df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')

    # Filter to period
    filtered = filter_by_period(df, 'report_date', period)

    # Standardize columns for combined output
    filtered['source'] = 'PSI'
    filtered['inspection_type'] = filtered.get('inspection_type_normalized', filtered.get('inspection_type', ''))
    filtered['inspection_category'] = filtered.get('inspection_category', '')

    # Standardize outcome column (PSI may use different values)
    if 'outcome' not in filtered.columns and 'status' in filtered.columns:
        filtered['outcome'] = filtered['status']

    # Coverage notes
    notes = []
    date_range = get_date_range(filtered, 'report_date')
    if date_range:
        notes.append(f"Dates: {date_range[0]} to {date_range[1]}")

    availability = DataAvailability(
        source='PSI',
        period=period,
        record_count=len(filtered),
        date_range=date_range,
        coverage_notes=notes,
    )

    return filtered, availability


def load_quality_data(period: MonthlyPeriod) -> Dict[str, Any]:
    """Load combined quality inspection data for a monthly period.

    Combines RABA and PSI data with standardized columns.

    Args:
        period: Monthly period to filter by

    Returns:
        Dict with:
        - 'inspections': Combined DataFrame of all inspections
        - 'raba': RABA-only DataFrame
        - 'psi': PSI-only DataFrame
        - 'availability': List of DataAvailability for each source
    """
    raba_df, raba_avail = _load_raba(period)
    psi_df, psi_avail = _load_psi(period)

    # Common columns for combined view
    common_cols = [
        'inspection_id', 'report_date', 'source',
        'building', 'level', 'grid', 'area',
        'dim_location_id', 'dim_company_id', 'dim_trade_id', 'dim_trade_code',
        'outcome', 'inspection_type', 'inspection_category',
        'contractor', 'inspector',
        'failure_reason', 'failure_category', 'reinspection_required',
        'tests_total', 'tests_passed', 'tests_failed', 'issue_count',
    ]

    # Build combined DataFrame with available columns
    combined_dfs = []

    for df in [raba_df, psi_df]:
        if not df.empty:
            # Select columns that exist
            available_cols = [c for c in common_cols if c in df.columns]
            combined_dfs.append(df[available_cols])

    if combined_dfs:
        combined = pd.concat(combined_dfs, ignore_index=True)
    else:
        combined = pd.DataFrame(columns=common_cols)

    # Normalize outcome values
    outcome_map = {
        'PASS': 'PASS', 'Pass': 'PASS', 'pass': 'PASS', 'PASSED': 'PASS',
        'FAIL': 'FAIL', 'Fail': 'FAIL', 'fail': 'FAIL', 'FAILED': 'FAIL',
        'PARTIAL': 'PARTIAL', 'Partial': 'PARTIAL',
    }
    if 'outcome' in combined.columns:
        combined['outcome_normalized'] = combined['outcome'].map(
            lambda x: outcome_map.get(str(x).strip(), 'OTHER') if pd.notna(x) else 'UNKNOWN'
        )

    return {
        'inspections': combined,
        'raba': raba_df,
        'psi': psi_df,
        'availability': [raba_avail, psi_avail],
    }
