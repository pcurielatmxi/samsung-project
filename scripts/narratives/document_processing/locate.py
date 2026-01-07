"""
Locate statements in source documents.

This postprocess script adds source_location to each statement by:
1. Extracting text from source document (PDF, DOCX, XLSX) with page positions
2. Using fuzzy matching to find each statement in the source
3. Adding page number, character offset, and match confidence
"""

import re
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

# Lazy imports for document processing
fitz = None  # PyMuPDF
Document = None  # python-docx
pd = None  # pandas

# Fuzzy matching
from rapidfuzz import fuzz
from rapidfuzz.process import extractOne


@dataclass
class PageText:
    """Text content from a single page."""
    page_num: int
    text: str
    start_offset: int  # Character offset in full document


@dataclass
class SourceLocation:
    """Location of a statement in source document."""
    page: int
    char_offset: int
    match_confidence: float
    match_type: str  # "exact", "fuzzy", "not_found"
    matched_text: Optional[str] = None


def _import_fitz():
    """Lazy import PyMuPDF."""
    global fitz
    if fitz is None:
        import fitz as _fitz
        fitz = _fitz
    return fitz


def _import_docx():
    """Lazy import python-docx."""
    global Document
    if Document is None:
        from docx import Document as _Document
        Document = _Document
    return Document


def _import_pandas():
    """Lazy import pandas."""
    global pd
    if pd is None:
        import pandas as _pd
        pd = _pd
    return pd


def extract_pdf_text(path: Path) -> tuple[str, list[PageText]]:
    """
    Extract text from PDF with page positions.

    Returns:
        Tuple of (full_text, list of PageText objects)
    """
    fitz = _import_fitz()
    doc = fitz.open(path)

    pages = []
    full_text = ""

    for page_num, page in enumerate(doc, start=1):
        text = page.get_text()
        pages.append(PageText(
            page_num=page_num,
            text=text,
            start_offset=len(full_text)
        ))
        full_text += text

    doc.close()
    return full_text, pages


def extract_docx_text(path: Path) -> tuple[str, list[PageText]]:
    """
    Extract text from DOCX.

    Note: DOCX doesn't have real page numbers, so we treat the whole doc as page 1.
    We could estimate pages by paragraph count, but that's unreliable.
    """
    Document = _import_docx()
    doc = Document(path)

    paragraphs = []
    for para in doc.paragraphs:
        if para.text.strip():
            paragraphs.append(para.text)

    full_text = "\n".join(paragraphs)

    # Treat as single "page" since DOCX doesn't have reliable page info
    pages = [PageText(page_num=1, text=full_text, start_offset=0)]

    return full_text, pages


def extract_xlsx_text(path: Path) -> tuple[str, list[PageText]]:
    """
    Extract text from XLSX.

    Each sheet is treated as a "page".
    """
    pd = _import_pandas()

    xlsx = pd.ExcelFile(path)
    pages = []
    full_text = ""

    for sheet_num, sheet_name in enumerate(xlsx.sheet_names, start=1):
        df = pd.read_excel(xlsx, sheet_name=sheet_name)
        # Convert to text representation
        text = df.to_string(index=False)
        pages.append(PageText(
            page_num=sheet_num,
            text=text,
            start_offset=len(full_text)
        ))
        full_text += text + "\n"

    return full_text, pages


def extract_document_text(path: Path) -> tuple[str, list[PageText]]:
    """Extract text from document based on extension."""
    ext = path.suffix.lower()

    if ext == '.pdf':
        return extract_pdf_text(path)
    elif ext in ('.docx', '.doc'):
        return extract_docx_text(path)
    elif ext in ('.xlsx', '.xls'):
        return extract_xlsx_text(path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")


def normalize_text(text: str) -> str:
    """Normalize text for matching (lowercase, collapse whitespace)."""
    text = text.lower()
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def find_statement_location(
    statement: str,
    full_text: str,
    pages: list[PageText],
    min_confidence: float = 60.0
) -> SourceLocation:
    """
    Find the location of a statement in the document.

    Uses multiple strategies:
    1. Exact match (after normalization)
    2. Fuzzy match on full statement
    3. Fuzzy match on statement prefix (first 100 chars)
    """
    norm_statement = normalize_text(statement)
    norm_full = normalize_text(full_text)

    # Strategy 1: Exact match
    pos = norm_full.find(norm_statement)
    if pos >= 0:
        page = get_page_for_offset(pos, full_text, pages)
        return SourceLocation(
            page=page,
            char_offset=pos,
            match_confidence=100.0,
            match_type="exact"
        )

    # Strategy 2: Fuzzy match using sliding window
    best_match = find_best_fuzzy_match(norm_statement, norm_full, pages)
    if best_match and best_match.match_confidence >= min_confidence:
        return best_match

    # Strategy 3: Try matching just the first part (for partial matches)
    if len(norm_statement) > 50:
        prefix = norm_statement[:100]
        pos = norm_full.find(prefix)
        if pos >= 0:
            page = get_page_for_offset(pos, full_text, pages)
            return SourceLocation(
                page=page,
                char_offset=pos,
                match_confidence=80.0,
                match_type="prefix"
            )

        # Fuzzy match on prefix
        prefix_match = find_best_fuzzy_match(prefix, norm_full, pages, window_size=150)
        if prefix_match and prefix_match.match_confidence >= min_confidence:
            prefix_match.match_type = "fuzzy_prefix"
            return prefix_match

    # Not found
    return SourceLocation(
        page=-1,
        char_offset=-1,
        match_confidence=0.0,
        match_type="not_found"
    )


def find_best_fuzzy_match(
    needle: str,
    haystack: str,
    pages: list[PageText],
    window_size: int = None
) -> Optional[SourceLocation]:
    """
    Find best fuzzy match using sliding window.
    """
    if window_size is None:
        window_size = len(needle) + 50  # Allow some slack

    if len(haystack) < window_size:
        # Document is shorter than window
        score = fuzz.ratio(needle, haystack)
        if score >= 60:
            return SourceLocation(
                page=1,
                char_offset=0,
                match_confidence=score,
                match_type="fuzzy"
            )
        return None

    best_score = 0
    best_pos = -1

    # Slide window across document
    step = max(1, window_size // 4)  # Step size for efficiency
    for i in range(0, len(haystack) - window_size + 1, step):
        window = haystack[i:i + window_size]
        score = fuzz.ratio(needle, window)
        if score > best_score:
            best_score = score
            best_pos = i

    if best_score >= 60:
        # Refine position with smaller steps around best match
        refined_pos = best_pos
        for offset in range(-step, step + 1, max(1, step // 10)):
            pos = best_pos + offset
            if 0 <= pos <= len(haystack) - window_size:
                window = haystack[pos:pos + window_size]
                score = fuzz.ratio(needle, window)
                if score > best_score:
                    best_score = score
                    refined_pos = pos

        page = get_page_for_offset(refined_pos, haystack, pages)
        return SourceLocation(
            page=page,
            char_offset=refined_pos,
            match_confidence=best_score,
            match_type="fuzzy"
        )

    return None


def get_page_for_offset(offset: int, full_text: str, pages: list[PageText]) -> int:
    """Get page number for a character offset."""
    for i, page in enumerate(pages):
        next_offset = pages[i + 1].start_offset if i + 1 < len(pages) else len(full_text)
        if page.start_offset <= offset < next_offset:
            return page.page_num
    return pages[-1].page_num if pages else -1


def process_record(input_data: dict, source_path: Path) -> dict:
    """
    Process a formatted record to add source locations.

    This is the entry point called by the script stage.

    Args:
        input_data: The formatted JSON content (with metadata and content)
        source_path: Path to the original source document

    Returns:
        Updated content dict with source_location added to each statement
    """
    # The input is the full JSON with metadata and content
    # We need to return just the content part (pipeline will wrap it in metadata)
    data = input_data.get('content', input_data)
    statements = data.get('statements', [])
    if not statements:
        return data

    # Extract text from source document
    try:
        full_text, pages = extract_document_text(source_path)
    except Exception as e:
        # If we can't read the source, mark all as not found
        for stmt in statements:
            stmt['source_location'] = {
                'page': -1,
                'char_offset': -1,
                'match_confidence': 0.0,
                'match_type': 'error',
                'error': str(e)
            }
        # Add error stats
        data['_locate_stats'] = {
            'total_statements': len(statements),
            'located': 0,
            'not_found': len(statements),
            'locate_rate': 0.0,
            'error': str(e)
        }
        return data

    # Find location for each statement
    for stmt in statements:
        text = stmt.get('text', '')
        if not text:
            stmt['source_location'] = {
                'page': -1,
                'char_offset': -1,
                'match_confidence': 0.0,
                'match_type': 'empty'
            }
            continue

        location = find_statement_location(text, full_text, pages)
        stmt['source_location'] = {
            'page': location.page,
            'char_offset': location.char_offset,
            'match_confidence': round(location.match_confidence, 1),
            'match_type': location.match_type
        }

    # Add summary stats
    found = sum(1 for s in statements if s['source_location']['match_type'] != 'not_found')
    data['_locate_stats'] = {
        'total_statements': len(statements),
        'located': found,
        'not_found': len(statements) - found,
        'locate_rate': round(found / len(statements) * 100, 1) if statements else 0
    }

    return data
