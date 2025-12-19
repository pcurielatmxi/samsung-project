"""Plain text file parser."""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def parse_text(file_path: Path) -> str:
    """
    Read content from a plain text file.

    Args:
        file_path: Path to the text file

    Returns:
        File content

    Raises:
        Exception: If file reading fails
    """
    encodings = ["utf-8", "utf-8-sig", "latin-1", "cp1252"]

    for encoding in encodings:
        try:
            with open(file_path, "r", encoding=encoding) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.error(f"Failed to read text file {file_path}: {e}")
            raise

    # If all encodings fail, try with errors='replace'
    logger.warning(f"Could not detect encoding for {file_path}, using replacement characters")
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()
