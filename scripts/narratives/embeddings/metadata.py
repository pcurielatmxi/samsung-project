"""
File metadata extraction for narrative documents.

Extracts dates, authors, and document types from filenames and file properties.
"""

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any


@dataclass
class FileMetadata:
    """Metadata extracted from a document file."""

    filename: str
    file_extension: str
    file_size_kb: int
    file_date: Optional[str]  # Extracted/inferred date (YYYY-MM-DD)
    author: Optional[str]
    document_type: Optional[str]
    subfolder: Optional[str]  # e.g., "weekly_reports", "BRG Expert Schedule Report"
    source_type: Optional[str] = None  # e.g., "narratives", "raba", "psi"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for ChromaDB metadata."""
        return {
            "filename": self.filename,
            "file_extension": self.file_extension,
            "file_size_kb": self.file_size_kb,
            "file_date": self.file_date or "",
            "author": self.author or "",
            "document_type": self.document_type or "",
            "subfolder": self.subfolder or "",
            "source_type": self.source_type or "",
        }


# Date patterns commonly found in filenames
DATE_PATTERNS = [
    # YYMMDD or YYYYMMDD
    (r"(\d{2})(\d{2})(\d{2})(?!\d)", lambda m: _parse_yymmdd(m.group(1), m.group(2), m.group(3))),
    (r"(\d{4})(\d{2})(\d{2})(?!\d)", lambda m: f"{m.group(1)}-{m.group(2)}-{m.group(3)}"),
    # YYYY-MM-DD or YYYY.MM.DD
    (r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})", lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"),
    # MM-DD-YY or MM.DD.YY or MM/DD/YY
    (r"(\d{1,2})[-./](\d{1,2})[-./](\d{2})(?!\d)", lambda m: _parse_mmddyy(m.group(1), m.group(2), m.group(3))),
    # M-D-YYYY or MM-DD-YYYY
    (r"(\d{1,2})[-./](\d{1,2})[-./](\d{4})", lambda m: f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"),
    # Week of YYYYMMDD
    (r"Week of (\d{8})", lambda m: f"{m.group(1)[:4]}-{m.group(1)[4:6]}-{m.group(1)[6:8]}"),
]

# Document type patterns
DOC_TYPE_PATTERNS = [
    (r"schedule.*narrative|narrative.*schedule", "schedule_narrative"),
    (r"milestone.*variance", "milestone_variance"),
    (r"weekly.*report|report.*weekly", "weekly_report"),
    (r"meeting.*notes|meeting.*materials", "meeting_notes"),
    (r"extension.*time|eot|co\s*#?\d+", "eot_claim"),
    (r"expert.*report|brg.*report", "expert_report"),
    (r"execution.*plan", "execution_plan"),
    (r"remaining.*work", "remaining_work"),
    (r"csa.*review|secai.*review", "csa_review"),
    (r"dpr|daily.*progress", "daily_progress"),
    (r"inspection|qc|qa", "quality"),
]

# Author patterns
AUTHOR_PATTERNS = [
    (r"\byates\b", "Yates"),
    (r"\bsecai\b", "SECAI"),
    (r"\bbrg\b", "BRG"),
    (r"\bsamsung\b", "Samsung"),
]


def _parse_yymmdd(yy: str, mm: str, dd: str) -> str:
    """Convert YYMMDD to YYYY-MM-DD, assuming 20XX for years < 50."""
    year = int(yy)
    if year < 50:
        year += 2000
    else:
        year += 1900
    return f"{year}-{int(mm):02d}-{int(dd):02d}"


def _parse_mmddyy(mm: str, dd: str, yy: str) -> str:
    """Convert MM-DD-YY to YYYY-MM-DD."""
    year = int(yy)
    if year < 50:
        year += 2000
    else:
        year += 1900
    return f"{year}-{int(mm):02d}-{int(dd):02d}"


def extract_date_from_filename(filename: str) -> Optional[str]:
    """Extract date from filename using various patterns."""
    for pattern, formatter in DATE_PATTERNS:
        match = re.search(pattern, filename, re.IGNORECASE)
        if match:
            try:
                date_str = formatter(match)
                # Validate the date
                datetime.strptime(date_str, "%Y-%m-%d")
                return date_str
            except (ValueError, IndexError):
                continue
    return None


def extract_document_type(filename: str, subfolder: str = "") -> Optional[str]:
    """Infer document type from filename and subfolder."""
    text = f"{subfolder} {filename}".lower()

    for pattern, doc_type in DOC_TYPE_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return doc_type

    return None


def extract_author(filename: str, subfolder: str = "") -> Optional[str]:
    """Infer author from filename and subfolder."""
    text = f"{subfolder} {filename}".lower()

    for pattern, author in AUTHOR_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return author

    return None


def extract_pdf_metadata(filepath: Path) -> Dict[str, Any]:
    """Extract metadata from PDF file properties."""
    try:
        import fitz
        doc = fitz.open(str(filepath))
        meta = doc.metadata
        doc.close()

        return {
            "pdf_author": meta.get("author", ""),
            "pdf_title": meta.get("title", ""),
            "pdf_created": meta.get("creationDate", ""),
        }
    except Exception:
        return {}


def extract_docx_metadata(filepath: Path) -> Dict[str, Any]:
    """Extract metadata from DOCX file properties."""
    try:
        from docx import Document
        doc = Document(str(filepath))
        props = doc.core_properties

        created = ""
        if props.created:
            created = props.created.strftime("%Y-%m-%d")

        return {
            "docx_author": props.author or "",
            "docx_title": props.title or "",
            "docx_created": created,
        }
    except Exception:
        return {}


def extract_file_metadata(
    filepath: Path,
    source_root: Path,
    source_type: Optional[str] = None
) -> FileMetadata:
    """Extract all metadata for a file.

    Args:
        filepath: Path to the file.
        source_root: Root directory for this source (for relative path calculation).
        source_type: Source type identifier (e.g., "narratives", "raba", "psi").
    """
    filename = filepath.name
    stat = filepath.stat()

    # Get subfolder relative to source root
    try:
        rel_path = filepath.relative_to(source_root)
        parts = rel_path.parts[:-1]  # Exclude filename
        subfolder = "/".join(parts) if parts else ""
    except ValueError:
        subfolder = ""

    # Extract date from filename
    file_date = extract_date_from_filename(filename)

    # If no date in filename, try file properties for PDFs/DOCX
    author = None
    if not file_date:
        ext = filepath.suffix.lower()
        if ext == ".pdf":
            props = extract_pdf_metadata(filepath)
            if props.get("pdf_created"):
                # PDF creation dates are often in format D:YYYYMMDDHHmmss
                created = props["pdf_created"]
                if created.startswith("D:") and len(created) >= 10:
                    try:
                        file_date = f"{created[2:6]}-{created[6:8]}-{created[8:10]}"
                    except (ValueError, IndexError):
                        pass
            author = props.get("pdf_author") or None
        elif ext == ".docx":
            props = extract_docx_metadata(filepath)
            file_date = props.get("docx_created") or None
            author = props.get("docx_author") or None

    # Infer author from filename if not found in properties
    if not author:
        author = extract_author(filename, subfolder)

    # Infer document type
    doc_type = extract_document_type(filename, subfolder)

    return FileMetadata(
        filename=filename,
        file_extension=filepath.suffix.lower(),
        file_size_kb=int(stat.st_size / 1024),
        file_date=file_date,
        author=author,
        document_type=doc_type,
        subfolder=subfolder,
        source_type=source_type,
    )
