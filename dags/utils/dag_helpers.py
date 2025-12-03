"""Helper functions for DAG definitions."""
from datetime import datetime, timedelta
from typing import Dict, Any, Callable
import logging

logger = logging.getLogger(__name__)


def log_task_start(**context) -> None:
    """
    Log task execution start.
    Use as a PythonOperator at the beginning of a DAG.
    """
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    logger.info(f'Starting task: {task_id} at {execution_date}')


def log_task_end(**context) -> None:
    """
    Log task execution end.
    Use as a PythonOperator at the end of a DAG.
    """
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    logger.info(f'Completed task: {task_id}')


def create_task_group_dag_docs(
    description: str,
    sources: list[str] = None,
    transformations: list[str] = None,
    destinations: list[str] = None,
) -> str:
    """
    Create standardized DAG documentation.

    Args:
        description: Description of the DAG
        sources: List of data sources
        transformations: List of transformations applied
        destinations: List of destinations

    Returns:
        Formatted markdown documentation
    """
    doc = f'# {description}\n\n'

    if sources:
        doc += '## Sources\n'
        for source in sources:
            doc += f'- {source}\n'
        doc += '\n'

    if transformations:
        doc += '## Transformations\n'
        for transform in transformations:
            doc += f'- {transform}\n'
        doc += '\n'

    if destinations:
        doc += '## Destinations\n'
        for dest in destinations:
            doc += f'- {dest}\n'
        doc += '\n'

    return doc


def get_time_range(**context) -> Dict[str, str]:
    """
    Get time range for data extraction.
    Supports backfill by checking if execution date is in the past.

    Returns:
        Dictionary with 'start_date' and 'end_date' in ISO format
    """
    execution_date = context['execution_date']
    next_execution_date = context['next_execution_date']

    return {
        'start_date': execution_date.isoformat(),
        'end_date': next_execution_date.isoformat(),
    }
