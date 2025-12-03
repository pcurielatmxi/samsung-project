"""Shared configuration for DAGs."""
from datetime import datetime, timedelta

# Default arguments for all DAGs
DEFAULT_ARGS = {
    'owner': 'etl-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1),
    'catchup': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
}

# Default DAG configuration
DEFAULT_DAG_CONFIG = {
    'schedule_interval': timedelta(days=1),
    'default_view': 'tree',
    'doc_md': 'ETL Pipeline DAG',
}

# Timeouts and delays
TASK_TIMEOUT_MINUTES = 60
SENSOR_TIMEOUT_MINUTES = 30

# Data configuration
BATCH_SIZE = 1000
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5
