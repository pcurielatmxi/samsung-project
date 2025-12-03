"""
Configuration settings for the ETL pipeline.
Load configuration from environment variables or config files.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_file = Path(__file__).parent.parent.parent / '.env'
if env_file.exists():
    load_dotenv(env_file)


class Settings:
    """Application settings loaded from environment variables."""

    # Project paths
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    DATA_DIR = PROJECT_ROOT / 'data'
    RAW_DATA_DIR = DATA_DIR / 'raw'
    PROCESSED_DATA_DIR = DATA_DIR / 'processed'
    OUTPUT_DATA_DIR = DATA_DIR / 'output'

    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

    # ============================================================================
    # ProjectSight Configuration (Trimble - Web Scraping)
    # ============================================================================
    PROJECTSIGHT_BASE_URL = os.getenv('PROJECTSIGHT_BASE_URL', '')
    PROJECTSIGHT_USERNAME = os.getenv('PROJECTSIGHT_USERNAME', '')
    PROJECTSIGHT_PASSWORD = os.getenv('PROJECTSIGHT_PASSWORD', '')
    PROJECTSIGHT_TIMEOUT = int(os.getenv('PROJECTSIGHT_TIMEOUT', '30'))
    PROJECTSIGHT_HEADLESS = os.getenv('PROJECTSIGHT_HEADLESS', 'true').lower() == 'true'
    PROJECTSIGHT_DATA_FIELDS = os.getenv(
        'PROJECTSIGHT_DATA_FIELDS',
        'project_id,project_name,status,start_date,end_date'
    ).split(',')

    # ============================================================================
    # Fieldwire Configuration (API)
    # ============================================================================
    FIELDWIRE_BASE_URL = os.getenv('FIELDWIRE_BASE_URL', 'https://api.fieldwire.com')
    FIELDWIRE_API_KEY = os.getenv('FIELDWIRE_API_KEY', '')
    FIELDWIRE_TIMEOUT = int(os.getenv('FIELDWIRE_TIMEOUT', '30'))
    FIELDWIRE_RETRY_ATTEMPTS = int(os.getenv('FIELDWIRE_RETRY_ATTEMPTS', '3'))
    FIELDWIRE_RETRY_DELAY = int(os.getenv('FIELDWIRE_RETRY_DELAY', '5'))
    FIELDWIRE_BATCH_SIZE = int(os.getenv('FIELDWIRE_BATCH_SIZE', '100'))

    # ============================================================================
    # Database Configuration
    # ============================================================================
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', '5432'))
    DB_NAME = os.getenv('DB_NAME', 'etl_db')
    DB_USER = os.getenv('DB_USER', 'airflow')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')

    # ============================================================================
    # Airflow Configuration
    # ============================================================================
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')

    @classmethod
    def get_database_url(cls) -> str:
        """Generate database connection URL."""
        return (
            f'postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@'
            f'{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}'
        )

    @classmethod
    def validate_required_settings(cls) -> list[str]:
        """
        Validate that all required settings are configured.
        Returns list of missing required settings.
        """
        missing = []

        # Check ProjectSight settings
        if not cls.PROJECTSIGHT_BASE_URL:
            missing.append('PROJECTSIGHT_BASE_URL')
        if not cls.PROJECTSIGHT_USERNAME:
            missing.append('PROJECTSIGHT_USERNAME')
        if not cls.PROJECTSIGHT_PASSWORD:
            missing.append('PROJECTSIGHT_PASSWORD')

        # Check Fieldwire settings
        if not cls.FIELDWIRE_API_KEY:
            missing.append('FIELDWIRE_API_KEY')

        return missing


# Create settings instance
settings = Settings()
