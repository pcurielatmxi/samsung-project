"""Quality inspection data loader for snapshot reports.

Loads RABA and PSI quality inspection data filtered to snapshot periods.
"""

import sys
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings
from .base import SnapshotPeriod, DataAvailability, filter_by_period, get_date_range


def _normalize_outcome(outcome: str) -> str:
    """Normalize outcome values to PASS/FAIL/PARTIAL/OTHER."""
    if pd.isna(outcome):
        return 'UNKNOWN'

    outcome_str = str(outcome).strip().upper()

    # Pass variants
    if outcome_str in ('PASS', 'PASSED', 'ACCEPTED', 'ACCEPT', 'APPROVED'):
        return 'PASS'

    # Fail variants
    if outcome_str in ('FAIL', 'FAILED', 'FAILURE', 'REJECTED', 'REJECT'):
        return 'FAIL'

    # Partial variants
    if outcome_str in ('PARTIAL', 'CONDITIONAL', 'CONDITIONALLY ACCEPTED'):
        return 'PARTIAL'

    return 'OTHER'


def _load_raba_data(period: SnapshotPeriod) -> tuple[pd.DataFrame, DataAvailability]:
    """Load RABA quality inspection data for period."""
    raba_path = Settings.RABA_PROCESSED_DIR / 'raba_consolidated.csv'

    if not raba_path.exists():
        return pd.DataFrame(), DataAvailability(
            source='RABA',
            period=period,
            record_count=0,
            coverage_notes=['raba_consolidated.csv not found'],
        )

    df = pd.read_csv(raba_path, low_memory=False)

    # Identify date column
    date_col = None
    for col in ['inspection_date', 'date', 'report_date']:
        if col in df.columns:
            date_col = col
            break

    if date_col is None:
        return pd.DataFrame(), DataAvailability(
            source='RABA',
            period=period,
            record_count=0,
            coverage_notes=['No date column found'],
        )

    # Filter to period
    filtered = filter_by_period(df, date_col, period)

    # Normalize outcome
    outcome_col = None
    for col in ['outcome', 'status', 'result', 'inspection_result']:
        if col in filtered.columns:
            outcome_col = col
            break

    if outcome_col and not filtered.empty:
        filtered['outcome_normalized'] = filtered[outcome_col].apply(_normalize_outcome)

    # Build availability
    date_range = get_date_range(filtered, date_col) if not filtered.empty else None

    coverage_notes = []
    if not filtered.empty:
        coverage_notes.append(f"{len(filtered):,} inspections")
        if 'outcome_normalized' in filtered.columns:
            pass_count = (filtered['outcome_normalized'] == 'PASS').sum()
            fail_count = (filtered['outcome_normalized'] == 'FAIL').sum()
            coverage_notes.append(f"Pass: {pass_count}, Fail: {fail_count}")
    else:
        coverage_notes.append('No data in period')

    availability = DataAvailability(
        source='RABA',
        period=period,
        record_count=len(filtered),
        date_range=date_range,
        coverage_notes=coverage_notes,
    )

    return filtered, availability


def _load_psi_data(period: SnapshotPeriod) -> tuple[pd.DataFrame, DataAvailability]:
    """Load PSI quality inspection data for period."""
    psi_path = Settings.PSI_PROCESSED_DIR / 'psi_consolidated.csv'

    if not psi_path.exists():
        return pd.DataFrame(), DataAvailability(
            source='PSI',
            period=period,
            record_count=0,
            coverage_notes=['psi_consolidated.csv not found'],
        )

    df = pd.read_csv(psi_path, low_memory=False)

    # Identify date column
    date_col = None
    for col in ['inspection_date', 'date', 'report_date']:
        if col in df.columns:
            date_col = col
            break

    if date_col is None:
        return pd.DataFrame(), DataAvailability(
            source='PSI',
            period=period,
            record_count=0,
            coverage_notes=['No date column found'],
        )

    # Filter to period
    filtered = filter_by_period(df, date_col, period)

    # Normalize outcome
    outcome_col = None
    for col in ['outcome', 'status', 'result', 'inspection_result']:
        if col in filtered.columns:
            outcome_col = col
            break

    if outcome_col and not filtered.empty:
        filtered['outcome_normalized'] = filtered[outcome_col].apply(_normalize_outcome)

    # Build availability
    date_range = get_date_range(filtered, date_col) if not filtered.empty else None

    coverage_notes = []
    if not filtered.empty:
        coverage_notes.append(f"{len(filtered):,} inspections")
        if 'outcome_normalized' in filtered.columns:
            pass_count = (filtered['outcome_normalized'] == 'PASS').sum()
            fail_count = (filtered['outcome_normalized'] == 'FAIL').sum()
            coverage_notes.append(f"Pass: {pass_count}, Fail: {fail_count}")
    else:
        coverage_notes.append('No data in period')

    availability = DataAvailability(
        source='PSI',
        period=period,
        record_count=len(filtered),
        date_range=date_range,
        coverage_notes=coverage_notes,
    )

    return filtered, availability


def load_quality_data(period: SnapshotPeriod) -> Dict[str, Any]:
    """Load quality inspection data from RABA and PSI for a snapshot period.

    Args:
        period: SnapshotPeriod to filter by

    Returns:
        Dict with:
        - 'raba': DataFrame of RABA inspections
        - 'psi': DataFrame of PSI inspections
        - 'inspections': Combined DataFrame with normalized outcomes
        - 'availability': List of DataAvailability for each source
    """
    raba_df, raba_avail = _load_raba_data(period)
    psi_df, psi_avail = _load_psi_data(period)

    # Combine with source indicator
    combined_frames = []

    if not raba_df.empty:
        raba_df = raba_df.copy()
        raba_df['source'] = 'RABA'
        combined_frames.append(raba_df)

    if not psi_df.empty:
        psi_df = psi_df.copy()
        psi_df['source'] = 'PSI'
        combined_frames.append(psi_df)

    if combined_frames:
        # Find common columns for combining
        common_cols = set(combined_frames[0].columns)
        for df in combined_frames[1:]:
            common_cols &= set(df.columns)

        # Combine on common columns
        combined = pd.concat([df[list(common_cols)] for df in combined_frames], ignore_index=True)
    else:
        combined = pd.DataFrame()

    return {
        'raba': raba_df,
        'psi': psi_df,
        'inspections': combined,
        'availability': [raba_avail, psi_avail],
    }
