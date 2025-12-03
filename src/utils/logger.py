"""Logging configuration for the ETL pipeline."""
import logging
import logging.handlers
from pathlib import Path
from src.config.settings import settings


def configure_logging(name: str) -> logging.Logger:
    """
    Configure logging for a module.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(settings.LOG_LEVEL)

    # Create formatters and handlers
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    log_dir = Path(settings.AIRFLOW_HOME) / 'logs' / 'etl'
    log_dir.mkdir(parents=True, exist_ok=True)
    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / f'{name}.log',
        maxBytes=10485760,  # 10MB
        backupCount=5,
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
