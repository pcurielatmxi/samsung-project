"""Quality inspection data loader (RABA + PSI).

Loads quality inspection data from RABA and PSI files which share an
identical unified schema (same columns in same order) for easy append.
"""

import sys
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings
from .base import MonthlyPeriod, DataAvailability, filter_by_period, get_date_range


def _load_qc_file(path: Path, source: str, period: MonthlyPeriod) -> tuple[pd.DataFrame, DataAvailability]:
    """Load a QC inspection file for period."""
    if not path.exists():
        return pd.DataFrame(), DataAvailability(
            source=source,
            period=period,
            record_count=0,
            coverage_notes=['File not found'],
        )

    df = pd.read_csv(path, low_memory=False)
    df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')

    # Filter to period
    filtered = filter_by_period(df, 'report_date', period)

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
        source=source,
        period=period,
        record_count=len(filtered),
        date_range=date_range,
        coverage_notes=notes,
    )

    return filtered, availability


def load_quality_data(period: MonthlyPeriod) -> Dict[str, Any]:
    """Load combined quality inspection data for a monthly period.

    Both RABA and PSI files use the unified QC inspection schema with
    identical columns, so they can be directly concatenated.

    Args:
        period: Monthly period to filter by

    Returns:
        Dict with:
        - 'inspections': Combined DataFrame of all inspections
        - 'raba': RABA-only DataFrame
        - 'psi': PSI-only DataFrame
        - 'availability': List of DataAvailability for each source
    """
    # Load both sources (both have unified schema)
    # Try 4.consolidate folder first, fall back to old locations
    raba_path = Settings.RABA_PROCESSED_DIR / '4.consolidate' / 'raba_qc_inspections.csv'
    if not raba_path.exists():
        raba_path = Settings.RABA_PROCESSED_DIR / 'raba_consolidated.csv'

    psi_path = Settings.PSI_PROCESSED_DIR / '4.consolidate' / 'psi_qc_inspections.csv'
    if not psi_path.exists():
        psi_path = Settings.PSI_PROCESSED_DIR / 'psi_consolidated.csv'

    raba_df, raba_avail = _load_qc_file(raba_path, 'RABA', period)
    psi_df, psi_avail = _load_qc_file(psi_path, 'PSI', period)

    # Combine - both have identical schema so direct concat works
    dfs_to_combine = [df for df in [raba_df, psi_df] if not df.empty]
    if dfs_to_combine:
        combined = pd.concat(dfs_to_combine, ignore_index=True)
    else:
        combined = pd.DataFrame()

    # Normalize outcome values
    outcome_map = {
        'PASS': 'PASS', 'Pass': 'PASS', 'pass': 'PASS', 'PASSED': 'PASS',
        'FAIL': 'FAIL', 'Fail': 'FAIL', 'fail': 'FAIL', 'FAILED': 'FAIL',
        'PARTIAL': 'PARTIAL', 'Partial': 'PARTIAL',
        'CANCELLED': 'CANCELLED', 'Cancelled': 'CANCELLED',
    }
    if not combined.empty and 'outcome' in combined.columns:
        combined['outcome_normalized'] = combined['outcome'].map(
            lambda x: outcome_map.get(str(x).strip(), 'OTHER') if pd.notna(x) else 'UNKNOWN'
        )

    return {
        'inspections': combined,
        'raba': raba_df,
        'psi': psi_df,
        'availability': [raba_avail, psi_avail],
    }
