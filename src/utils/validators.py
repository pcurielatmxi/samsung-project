"""Data validation utilities."""
from typing import Any, List, Dict, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def validate_required_fields(
    data: List[Dict[str, Any]],
    required_fields: set[str],
) -> tuple[bool, List[str]]:
    """
    Validate that all records contain required fields.

    Args:
        data: List of dictionaries to validate
        required_fields: Set of required field names

    Returns:
        Tuple of (is_valid, list_of_invalid_records)
    """
    invalid_records = []

    for idx, record in enumerate(data):
        missing_fields = required_fields - set(record.keys())
        if missing_fields:
            invalid_records.append(
                f'Record {idx}: Missing fields {missing_fields}'
            )

    return len(invalid_records) == 0, invalid_records


def validate_field_types(
    data: List[Dict[str, Any]],
    field_types: Dict[str, type],
) -> tuple[bool, List[str]]:
    """
    Validate that fields have correct types.

    Args:
        data: List of dictionaries to validate
        field_types: Dictionary mapping field names to expected types

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    for idx, record in enumerate(data):
        for field, expected_type in field_types.items():
            if field in record:
                value = record[field]
                if value is not None and not isinstance(value, expected_type):
                    errors.append(
                        f'Record {idx}, field "{field}": '
                        f'Expected {expected_type.__name__}, '
                        f'got {type(value).__name__}'
                    )

    return len(errors) == 0, errors


def validate_record_count(
    actual: int,
    expected: int,
    tolerance: float = 0.0,
) -> bool:
    """
    Validate that record count matches expected.

    Args:
        actual: Actual record count
        expected: Expected record count
        tolerance: Tolerance percentage (0.0-1.0)

    Returns:
        True if count is within tolerance
    """
    if expected == 0:
        return actual == 0

    diff = abs(actual - expected) / expected
    return diff <= tolerance


def validate_date_format(
    date_str: str,
    date_formats: List[str] = None,
) -> bool:
    """
    Validate that a string is a valid date.

    Args:
        date_str: Date string to validate
        date_formats: List of acceptable formats

    Returns:
        True if date is valid
    """
    if date_formats is None:
        date_formats = ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']

    for fmt in date_formats:
        try:
            datetime.strptime(date_str, fmt)
            return True
        except ValueError:
            continue

    return False


def validate_no_duplicates(
    data: List[Dict[str, Any]],
    key_field: str,
) -> tuple[bool, List[Any]]:
    """
    Validate that there are no duplicate key values.

    Args:
        data: List of dictionaries to validate
        key_field: Field to check for duplicates

    Returns:
        Tuple of (is_valid, list_of_duplicate_values)
    """
    seen = set()
    duplicates = set()

    for record in data:
        key = record.get(key_field)
        if key in seen:
            duplicates.add(key)
        seen.add(key)

    return len(duplicates) == 0, list(duplicates)
