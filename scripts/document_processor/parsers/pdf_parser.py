"""PDF document parser using PyMuPDF."""

import logging
from pathlib import Path
from typing import List, Tuple, Optional

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
    pages = parse_pdf_pages(file_path)
    text_parts = []
    for page_num, page_text in pages:
        if page_text.strip():
            text_parts.append(f"--- Page {page_num} ---\n{page_text}")
    return "\n\n".join(text_parts)


def parse_pdf_pages(file_path: Path) -> List[Tuple[int, str]]:
    """
    Extract text content from a PDF file, returning each page separately.

    Args:
        file_path: Path to the PDF file

    Returns:
        List of (page_number, text) tuples. Page numbers are 1-indexed.

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

    pages = []

    try:
        doc = fitz.open(file_path)
        for page_num, page in enumerate(doc, 1):
            page_text = page.get_text()
            pages.append((page_num, page_text))
            if not page_text.strip():
                logger.debug(f"No text extracted from page {page_num} of {file_path}")
        doc.close()
    except Exception as e:
        logger.error(f"Failed to parse PDF {file_path}: {e}")
        raise

    return pages


def get_pdf_page_count(file_path: Path) -> int:
    """
    Get the number of pages in a PDF file.

    Args:
        file_path: Path to the PDF file

    Returns:
        Number of pages
    """
    try:
        import fitz
    except ImportError:
        raise ImportError("PyMuPDF is required. Install with: pip install pymupdf")

    try:
        doc = fitz.open(file_path)
        count = len(doc)
        doc.close()
        return count
    except Exception as e:
        logger.error(f"Failed to get page count for {file_path}: {e}")
        raise
