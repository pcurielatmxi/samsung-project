"""
Configuration settings for the data analysis pipeline.
Load configuration from environment variables or config files.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_file = Path(__file__).parent.parent.parent / '.env'
if env_file.exists():
    load_dotenv(env_file)


def windows_to_wsl_path(windows_path: str) -> Path:
    """
    Convert Windows path to WSL2 path.

    Examples:
        C:\\Users\\foo -> /mnt/c/Users/foo
        D:\\Data -> /mnt/d/Data
        /mnt/c/Users/foo -> /mnt/c/Users/foo (unchanged)

    Args:
        windows_path: Windows-style path (e.g., C:\\Users\\...) or Unix path

    Returns:
        Path object with WSL2-compatible path
    """
    if not windows_path:
        return None

    # Normalize path separators
    normalized = windows_path.replace('\\', '/')

    # Already a Unix/WSL path
    if normalized.startswith('/'):
        return Path(normalized)

    # Convert Windows drive letter (C:/... -> /mnt/c/...)
    if len(normalized) >= 2 and normalized[1] == ':':
        drive_letter = normalized[0].lower()
        rest_of_path = normalized[2:].lstrip('/')
        return Path(f'/mnt/{drive_letter}/{rest_of_path}')

    # Return as-is if no conversion needed
    return Path(normalized)


class Settings:
    """Application settings loaded from environment variables."""

    # Project paths
    PROJECT_ROOT = Path(__file__).parent.parent.parent

    # ============================================================================
    # Data Directory Configuration
    # ============================================================================
    # External data directory (Windows folder via WSL2) for raw inputs and outputs.
    # Analysis files live in the repository under data/analysis/ (tracked by git).
    #
    # IMPORTANT: Data Traceability Classification
    # -------------------------------------------
    # For auditing purposes, data is separated by traceability:
    #
    #   raw/        Source files exactly as received (XER, PDF, Excel, CSV dumps)
    #               100% traceable to external source
    #
    #   processed/  Direct parsing/transformation of raw files (CSV tables)
    #               100% traceable to raw/ - no assumptions or inference
    #
    #   derived/    Enhanced data with assumptions, inference, or merged sources
    #               NOT fully traceable - includes analyst judgment/classification
    #               Examples: WBS taxonomy labels, delay attribution, correlations
    #
    # Structure:
    #   WINDOWS_DATA_DIR/
    #   ├── raw/{source}/           # Source files - fully traceable
    #   ├── processed/{source}/     # Parsed data - fully traceable to raw/
    #   └── derived/{source}/       # Enhanced data - includes assumptions
    #
    #   PROJECT_ROOT/data/analysis/{source}/  # Analysis outputs (tracked by git)

    _WINDOWS_DATA_DIR = os.getenv('WINDOWS_DATA_DIR', '')

    if _WINDOWS_DATA_DIR:
        # Use external Windows data directory (converted to WSL path)
        DATA_DIR = windows_to_wsl_path(_WINDOWS_DATA_DIR)
    else:
        # Default: local project data directory
        DATA_DIR = PROJECT_ROOT / 'data'

    # Top-level directories
    RAW_DATA_DIR = DATA_DIR / 'raw'
    PROCESSED_DATA_DIR = DATA_DIR / 'processed'
    DERIVED_DATA_DIR = DATA_DIR / 'derived'

    # Analysis directory (always in repository, tracked by git)
    ANALYSIS_DIR = PROJECT_ROOT / 'data' / 'analysis'

    # ============================================================================
    # Source-specific paths: Primavera (XER schedules)
    # ============================================================================
    PRIMAVERA_RAW_DIR = RAW_DATA_DIR / 'primavera'
    PRIMAVERA_PROCESSED_DIR = PROCESSED_DATA_DIR / 'primavera'
    PRIMAVERA_DERIVED_DIR = DERIVED_DATA_DIR / 'primavera'
    PRIMAVERA_ANALYSIS_DIR = ANALYSIS_DIR / 'primavera'

    # ============================================================================
    # Source-specific paths: Weekly Reports (PDF reports)
    # ============================================================================
    WEEKLY_REPORTS_RAW_DIR = RAW_DATA_DIR / 'weekly_reports'
    WEEKLY_REPORTS_PROCESSED_DIR = PROCESSED_DATA_DIR / 'weekly_reports'
    WEEKLY_REPORTS_DERIVED_DIR = DERIVED_DATA_DIR / 'weekly_reports'
    WEEKLY_REPORTS_ANALYSIS_DIR = ANALYSIS_DIR / 'weekly_reports'

    # ============================================================================
    # Source-specific paths: TBM (Excel workbooks)
    # ============================================================================
    TBM_RAW_DIR = RAW_DATA_DIR / 'tbm'
    TBM_PROCESSED_DIR = PROCESSED_DATA_DIR / 'tbm'
    TBM_DERIVED_DIR = DERIVED_DATA_DIR / 'tbm'
    TBM_ANALYSIS_DIR = ANALYSIS_DIR / 'tbm'

    # Field team's TBM folder (OneDrive - synced by field team)
    _FIELD_TBM_FILES = os.getenv('FIELD_TBM_FILES', '')
    FIELD_TBM_DIR = windows_to_wsl_path(_FIELD_TBM_FILES) if _FIELD_TBM_FILES else None

    # ============================================================================
    # Source-specific paths: Fieldwire (CSV dumps)
    # ============================================================================
    FIELDWIRE_RAW_DIR = RAW_DATA_DIR / 'fieldwire'
    FIELDWIRE_PROCESSED_DIR = PROCESSED_DATA_DIR / 'fieldwire'
    FIELDWIRE_DERIVED_DIR = DERIVED_DATA_DIR / 'fieldwire'
    FIELDWIRE_ANALYSIS_DIR = ANALYSIS_DIR / 'fieldwire'

    # ============================================================================
    # Source-specific paths: ProjectSight (exports)
    # ============================================================================
    PROJECTSIGHT_RAW_DIR = RAW_DATA_DIR / 'projectsight'
    PROJECTSIGHT_PROCESSED_DIR = PROCESSED_DATA_DIR / 'projectsight'
    PROJECTSIGHT_DERIVED_DIR = DERIVED_DATA_DIR / 'projectsight'
    PROJECTSIGHT_ANALYSIS_DIR = ANALYSIS_DIR / 'projectsight'

    # ============================================================================
    # Source-specific paths: RABA (quality inspection reports)
    # ============================================================================
    RABA_RAW_DIR = RAW_DATA_DIR / 'raba'
    RABA_PROCESSED_DIR = PROCESSED_DATA_DIR / 'raba'
    RABA_DERIVED_DIR = DERIVED_DATA_DIR / 'raba'
    RABA_ANALYSIS_DIR = ANALYSIS_DIR / 'raba'

    # ============================================================================
    # Source-specific paths: PSI (quality inspection reports)
    # ============================================================================
    PSI_RAW_DIR = RAW_DATA_DIR / 'psi'
    PSI_PROCESSED_DIR = PROCESSED_DATA_DIR / 'psi'
    PSI_DERIVED_DIR = DERIVED_DATA_DIR / 'psi'
    PSI_ANALYSIS_DIR = ANALYSIS_DIR / 'psi'

    # ============================================================================
    # Source-specific paths: Narratives (P6 narratives, weekly reports, etc.)
    # ============================================================================
    NARRATIVES_RAW_DIR = RAW_DATA_DIR / 'narratives'
    NARRATIVES_PROCESSED_DIR = PROCESSED_DATA_DIR / 'narratives'
    NARRATIVES_DERIVED_DIR = DERIVED_DATA_DIR / 'narratives'
    NARRATIVES_ANALYSIS_DIR = ANALYSIS_DIR / 'narratives'

    # ============================================================================
    # Integrated Analysis paths (combined data from multiple sources)
    # ============================================================================
    INTEGRATED_PROCESSED_DIR = PROCESSED_DATA_DIR / 'integrated'
    INTEGRATED_DERIVED_DIR = DERIVED_DATA_DIR / 'integrated'
    INTEGRATED_ANALYSIS_DIR = ANALYSIS_DIR / 'integrated'

    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

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
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')

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

        # Check Fieldwire settings (if using Fieldwire)
        # if not cls.FIELDWIRE_API_KEY:
        #     missing.append('FIELDWIRE_API_KEY')

        return missing

    @classmethod
    def print_path_config(cls) -> None:
        """Print current path configuration for verification."""
        print("=" * 70)
        print("Data Path Configuration")
        print("=" * 70)

        if cls._WINDOWS_DATA_DIR:
            print(f"Mode: External Windows directory")
            print(f"WINDOWS_DATA_DIR: {cls._WINDOWS_DATA_DIR}")
        else:
            print(f"Mode: Local project directory")

        print()
        print(f"DATA_DIR (external):   {cls.DATA_DIR}")
        print(f"  exists: {cls.DATA_DIR.exists()}")
        print()
        print(f"ANALYSIS_DIR (repo):   {cls.ANALYSIS_DIR}")
        print(f"  exists: {cls.ANALYSIS_DIR.exists()}")
        print()
        print("Traceability Legend:")
        print("  raw       = 100% traceable to source")
        print("  processed = 100% traceable to raw (no assumptions)")
        print("  derived   = includes assumptions/inference (NOT fully traceable)")
        print()

        sources = ['primavera', 'weekly_reports', 'tbm', 'fieldwire', 'projectsight', 'raba', 'psi', 'narratives', 'integrated']
        for source in sources:
            raw_attr = f"{source.upper()}_RAW_DIR"
            proc_attr = f"{source.upper()}_PROCESSED_DIR"
            derv_attr = f"{source.upper()}_DERIVED_DIR"
            anal_attr = f"{source.upper()}_ANALYSIS_DIR"

            print(f"{source}:")
            print(f"  raw:       {getattr(cls, raw_attr)}")
            print(f"  processed: {getattr(cls, proc_attr)}")
            print(f"  derived:   {getattr(cls, derv_attr)}")
            print(f"  analysis:  {getattr(cls, anal_attr)}")
            print()

        print("=" * 70)

    @classmethod
    def ensure_directories(cls) -> None:
        """Create data directories if they don't exist."""
        dirs_to_create = [
            # Top-level
            cls.RAW_DATA_DIR,
            cls.PROCESSED_DATA_DIR,
            cls.DERIVED_DATA_DIR,
            cls.ANALYSIS_DIR,
            # Primavera
            cls.PRIMAVERA_RAW_DIR,
            cls.PRIMAVERA_PROCESSED_DIR,
            cls.PRIMAVERA_DERIVED_DIR,
            cls.PRIMAVERA_ANALYSIS_DIR,
            # Weekly Reports
            cls.WEEKLY_REPORTS_RAW_DIR,
            cls.WEEKLY_REPORTS_PROCESSED_DIR,
            cls.WEEKLY_REPORTS_DERIVED_DIR,
            cls.WEEKLY_REPORTS_ANALYSIS_DIR,
            # TBM
            cls.TBM_RAW_DIR,
            cls.TBM_PROCESSED_DIR,
            cls.TBM_DERIVED_DIR,
            cls.TBM_ANALYSIS_DIR,
            # Fieldwire
            cls.FIELDWIRE_RAW_DIR,
            cls.FIELDWIRE_PROCESSED_DIR,
            cls.FIELDWIRE_DERIVED_DIR,
            cls.FIELDWIRE_ANALYSIS_DIR,
            # ProjectSight
            cls.PROJECTSIGHT_RAW_DIR,
            cls.PROJECTSIGHT_PROCESSED_DIR,
            cls.PROJECTSIGHT_DERIVED_DIR,
            cls.PROJECTSIGHT_ANALYSIS_DIR,
            # RABA
            cls.RABA_RAW_DIR,
            cls.RABA_PROCESSED_DIR,
            cls.RABA_DERIVED_DIR,
            cls.RABA_ANALYSIS_DIR,
            # PSI
            cls.PSI_RAW_DIR,
            cls.PSI_PROCESSED_DIR,
            cls.PSI_DERIVED_DIR,
            cls.PSI_ANALYSIS_DIR,
            # Narratives
            cls.NARRATIVES_RAW_DIR,
            cls.NARRATIVES_PROCESSED_DIR,
            cls.NARRATIVES_DERIVED_DIR,
            cls.NARRATIVES_ANALYSIS_DIR,
            # Integrated Analysis
            cls.INTEGRATED_PROCESSED_DIR,
            cls.INTEGRATED_DERIVED_DIR,
            cls.INTEGRATED_ANALYSIS_DIR,
        ]
        for dir_path in dirs_to_create:
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"Ensured: {dir_path}")


# Create settings instance
settings = Settings()


# Allow running this module directly to check configuration
if __name__ == "__main__":
    settings.print_path_config()
