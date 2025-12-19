"""PDF document parser using PyMuPDF."""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def parse_pdf(file_path: Path) -> str:
    """
    Extract text content from a PDF file using PyMuPDF.

    Args:
        file_path: Path to the PDF file

    Returns:
        Extracted text content

    Raises:
        ImportError: If PyMuPDF is not installed
        Exception: If PDF parsing fails
    """
    try:
        import fitz  # PyMuPDF
    except ImportError:
        raise ImportError(
            "PyMuPDF is required for PDF parsing. "
            "Install with: pip install pymupdf"
        )

    text_parts = []

    try:
        doc = fitz.open(file_path)
        for page_num, page in enumerate(doc, 1):
            page_text = page.get_text()
            if page_text.strip():
                text_parts.append(f"--- Page {page_num} ---\n{page_text}")
            else:
                logger.debug(f"No text extracted from page {page_num} of {file_path}")
        doc.close()
    except Exception as e:
        logger.error(f"Failed to parse PDF {file_path}: {e}")
        raise

    return "\n\n".join(text_parts)
