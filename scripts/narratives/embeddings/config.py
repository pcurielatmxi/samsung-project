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
CHROMA_PATH = DATA_DIR / "derived" / "embeddings" / "documents"

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
