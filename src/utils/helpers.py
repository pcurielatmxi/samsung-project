"""General utility helper functions."""
from typing import Any, Dict, List, Optional
import logging
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)


def retry_on_exception(
    func: callable,
    max_attempts: int = 3,
    delay_seconds: int = 5,
    backoff_factor: float = 1.0,
    exceptions: tuple = (Exception,),
) -> Any:
    """
    Retry a function on exception.

    Args:
        func: Function to retry
        max_attempts: Maximum number of attempts
        delay_seconds: Delay between attempts in seconds
        backoff_factor: Multiplier for delay after each attempt
        exceptions: Tuple of exceptions to catch

    Returns:
        Function result or None if all attempts failed
    """
    attempt = 0
    current_delay = delay_seconds

    while attempt < max_attempts:
        try:
            return func()
        except exceptions as e:
            attempt += 1
            if attempt >= max_attempts:
                logger.error(f'Failed after {max_attempts} attempts: {str(e)}')
                raise
            logger.warning(
                f'Attempt {attempt} failed: {str(e)}. '
                f'Retrying in {current_delay}s...'
            )
            time.sleep(current_delay)
            current_delay *= backoff_factor

    return None


def flatten_nested_dict(
    d: Dict[str, Any],
    parent_key: str = '',
    sep: str = '_',
) -> Dict[str, Any]:
    """
    Flatten a nested dictionary.

    Args:
        d: Dictionary to flatten
        parent_key: Prefix for keys
        sep: Separator between key parts

    Returns:
        Flattened dictionary
    """
    items = []
    for k, v in d.items():
        new_key = f'{parent_key}{sep}{k}' if parent_key else k
        if isinstance(v, dict):
            items.extend(
                flatten_nested_dict(v, new_key, sep=sep).items()
            )
        elif isinstance(v, list) and v and isinstance(v[0], dict):
            # For lists of dicts, only flatten first item as example
            items.extend(
                flatten_nested_dict(v[0], new_key, sep=sep).items()
            )
        else:
            items.append((new_key, v))
    return dict(items)


def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split a list into chunks.

    Args:
        lst: List to chunk
        chunk_size: Size of each chunk

    Returns:
        List of chunks
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def merge_dicts(base: Dict[str, Any], *dicts: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge multiple dictionaries.

    Args:
        base: Base dictionary
        *dicts: Dictionaries to merge into base

    Returns:
        Merged dictionary
    """
    result = base.copy()
    for d in dicts:
        result.update(d)
    return result


def safe_get(d: Dict[str, Any], key: str, default: Any = None) -> Any:
    """
    Safely get value from dictionary with dot notation.

    Args:
        d: Dictionary
        key: Key to get (supports 'parent.child' notation)
        default: Default value if key not found

    Returns:
        Value or default
    """
    keys = key.split('.')
    value = d
    for k in keys:
        if isinstance(value, dict):
            value = value.get(k)
        else:
            return default
    return value if value is not None else default


def format_bytes(bytes_value: int) -> str:
    """
    Format bytes to human-readable format.

    Args:
        bytes_value: Number of bytes

    Returns:
        Formatted string (e.g., '1.5 MB')
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f'{bytes_value:.2f} {unit}'
        bytes_value /= 1024.0
    return f'{bytes_value:.2f} PB'


def format_duration(seconds: float) -> str:
    """
    Format seconds to human-readable duration.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string (e.g., '1h 30m 45s')
    """
    hours, remainder = divmod(int(seconds), 3600)
    minutes, secs = divmod(remainder, 60)

    parts = []
    if hours:
        parts.append(f'{hours}h')
    if minutes:
        parts.append(f'{minutes}m')
    if secs:
        parts.append(f'{secs}s')

    return ' '.join(parts) or '0s'
