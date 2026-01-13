"""Configuration for narratives embeddings."""

import os
import sys
from pathlib import Path

# Add project root to path
_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from dotenv import load_dotenv
load_dotenv(_project_root / ".env")


def _get_data_dir() -> Path:
    """Get the Windows data directory, converting to WSL path if needed."""
    data_dir = os.environ.get("WINDOWS_DATA_DIR", "")
    if not data_dir:
        raise ValueError("WINDOWS_DATA_DIR not set in environment")

    # Convert Windows path to WSL if needed
    if len(data_dir) >= 2 and data_dir[1] == ':':
        drive = data_dir[0].lower()
        rest = data_dir[2:].replace('\\', '/').lstrip('/')
        return Path(f'/mnt/{drive}/{rest}')

    return Path(data_dir.replace('\\', '/'))


# Embedding model configuration
EMBEDDING_MODEL = "gemini-embedding-001"
EMBEDDING_DIMENSIONS = 768
EMBEDDING_TASK_INDEX = "RETRIEVAL_DOCUMENT"
EMBEDDING_TASK_QUERY = "RETRIEVAL_QUERY"
EMBEDDING_BATCH_SIZE = 100  # Max texts per batch request

# ChromaDB configuration
DATA_DIR = _get_data_dir()

# WSL-local path for ChromaDB (fast queries)
# Located outside repo to avoid git tracking
CHROMA_PATH = Path.home() / ".local" / "share" / "samsung-embeddings" / "documents"

# OneDrive path for cross-computer sync (backup only)
CHROMA_ONEDRIVE_PATH = DATA_DIR / "derived" / "embeddings" / "documents"


def ensure_local_db():
    """Ensure local WSL database exists, copying from OneDrive if needed.

    Call this before any database operation. If WSL folder doesn't exist
    but OneDrive has a database, it will be copied to WSL for fast access.

    Returns:
        Path: The CHROMA_PATH (always use WSL path for operations)
    """
    import shutil
    import sqlite3

    # If WSL path exists and has data, use it
    if CHROMA_PATH.exists() and any(CHROMA_PATH.iterdir()):
        return CHROMA_PATH

    # Check if OneDrive has a VALID database to copy
    onedrive_db = CHROMA_ONEDRIVE_PATH / "chroma.sqlite3"
    onedrive_valid = False

    if onedrive_db.exists():
        try:
            # Verify database is readable
            conn = sqlite3.connect(str(onedrive_db))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM embeddings")
            count = cur.fetchone()[0]
            conn.close()
            onedrive_valid = count > 0
        except Exception:
            onedrive_valid = False

    if onedrive_valid:
        print(f"Local database not found. Copying from OneDrive...")
        print(f"  Source: {CHROMA_ONEDRIVE_PATH}")
        print(f"  Target: {CHROMA_PATH}")

        CHROMA_PATH.mkdir(parents=True, exist_ok=True)

        # Copy all files and folders
        for item in CHROMA_ONEDRIVE_PATH.iterdir():
            dest = CHROMA_PATH / item.name
            if item.is_dir():
                shutil.copytree(item, dest)
            else:
                shutil.copy2(item, dest)

        print(f"  Done. Database copied to local WSL path.")
    else:
        # Neither exists or OneDrive is invalid - create empty directory
        CHROMA_PATH.mkdir(parents=True, exist_ok=True)

    return CHROMA_PATH


def sync_to_onedrive():
    """Copy local WSL database to OneDrive for backup/sync.

    Call this after successful build operations to keep OneDrive in sync.
    """
    import shutil

    if not CHROMA_PATH.exists() or not any(CHROMA_PATH.iterdir()):
        print("No local database to sync.")
        return

    print(f"Syncing to OneDrive...")
    print(f"  Source: {CHROMA_PATH}")
    print(f"  Target: {CHROMA_ONEDRIVE_PATH}")

    # Remove old OneDrive data and copy fresh
    if CHROMA_ONEDRIVE_PATH.exists():
        shutil.rmtree(CHROMA_ONEDRIVE_PATH)

    CHROMA_ONEDRIVE_PATH.mkdir(parents=True, exist_ok=True)

    for item in CHROMA_PATH.iterdir():
        dest = CHROMA_ONEDRIVE_PATH / item.name
        if item.is_dir():
            shutil.copytree(item, dest)
        else:
            shutil.copy2(item, dest)

    print(f"  Done. Database synced to OneDrive.")

# Source directories - each key becomes a source_type in metadata
SOURCE_DIRS = {
    "narratives": DATA_DIR / "raw" / "narratives",
    "raba": DATA_DIR / "raw" / "raba" / "individual",
    "psi": DATA_DIR / "raw" / "psi" / "reports",
}

# Legacy path for backward compatibility
NARRATIVES_RAW_DIR = SOURCE_DIRS["narratives"]

# Processed output paths
NARRATIVES_OUTPUT = DATA_DIR / "processed/narratives"
DIM_FILE = NARRATIVES_OUTPUT / "dim_narrative_file.csv"
STMT_FILE = NARRATIVES_OUTPUT / "narrative_statements.csv"

# Search defaults
DEFAULT_LIMIT = 10
DEFAULT_CONTEXT = 0

# Collection names (unified document index)
CHUNKS_COLLECTION = "document_chunks"

# Legacy collection names (for migration from old index)
LEGACY_CHUNKS_COLLECTION = "narrative_chunks"
DOCUMENTS_COLLECTION = "narrative_documents"
STATEMENTS_COLLECTION = "narrative_statements"

# Gemini API key (already in .env from document processing)
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
