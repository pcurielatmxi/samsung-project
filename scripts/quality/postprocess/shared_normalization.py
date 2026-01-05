#!/usr/bin/env python3
"""
Shared normalization utilities for quality inspection data.

Provides standardized functions for normalizing dates, roles, and inspection types.
"""

import pandas as pd
import re
from typing import Optional


def normalize_date(date_str: Optional[str]) -> Optional[str]:
    """
    Normalize date to YYYY-MM-DD format.

    Handles:
    - YYYY-MM-DD (returns as-is)
    - MM/DD/YYYY
    - M/D/YYYY
    - Other formats (uses pandas flexible parser)

    Args:
        date_str: Date string in various formats

    Returns:
        Date in YYYY-MM-DD format, or None if unparseable
    """
    if pd.isna(date_str):
        return None

    date_str = str(date_str).strip()

    # Already in YYYY-MM-DD format
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str

    # Try MM/DD/YYYY or M/D/YYYY
    try:
        dt = pd.to_datetime(date_str, format='%m/%d/%Y', errors='coerce')
        if pd.notna(dt):
            return dt.strftime('%Y-%m-%d')
    except Exception:
        pass

    # Fallback: pandas flexible parser
    try:
        dt = pd.to_datetime(date_str, errors='coerce')
        if pd.notna(dt):
            return dt.strftime('%Y-%m-%d')
    except Exception:
        pass

    # Unable to parse
    return None


def normalize_role(role: Optional[str]) -> Optional[str]:
    """
    Normalize role field to lowercase and trim whitespace.

    Args:
        role: Role string (e.g., "Inspector", "CONTRACTOR")

    Returns:
        Normalized role (lowercase), or None if input is None
    """
    if pd.isna(role):
        return None

    return str(role).lower().strip()


def normalize_inspection_type(type_str: Optional[str]) -> Optional[str]:
    """
    Normalize inspection/test type to lowercase and trim whitespace.

    Args:
        type_str: Inspection type string

    Returns:
        Normalized inspection type (lowercase), or None if input is None
    """
    if pd.isna(type_str):
        return None

    return str(type_str).lower().strip()
