#!/usr/bin/env python3
"""
Extract Narrative Documents to Markdown (Simple/Fast version)

Converts PDF, DOCX, and XLSX files from primavera_narratives to markdown format.
Uses pymupdf for fast PDF extraction. No table parsing - just text extraction.

Output: processed/primavera_narratives/{original_filename}.md
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings

# Paths
RAW_NARRATIVES_DIR = Settings.RAW_DATA_DIR / "primavera_narratives"
PROCESSED_NARRATIVES_DIR = Settings.PROCESSED_DATA_DIR / "primavera_narratives"


def extract_pdf_to_markdown(pdf_path: Path) -> str:
    """Extract text from PDF using pymupdf (fast)."""
    import fitz  # pymupdf

    parts = []
    parts.append(f"# {pdf_path.stem}\n")
    parts.append(f"*Source: {pdf_path.name}*\n\n---\n")

    try:
        doc = fitz.open(pdf_path)
        for page_num, page in enumerate(doc, 1):
            text = page.get_text()
            if text.strip():
                parts.append(f"\n## Page {page_num}\n")
                parts.append(text)
        doc.close()
    except Exception as e:
        parts.append(f"\n**Error:** {str(e)}\n")

    return "\n".join(parts)


def extract_docx_to_markdown(docx_path: Path) -> str:
    """Extract text from DOCX."""
    from docx import Document

    parts = []
    parts.append(f"# {docx_path.stem}\n")
    parts.append(f"*Source: {docx_path.name}*\n\n---\n")

    try:
        doc = Document(docx_path)
        for para in doc.paragraphs:
            text = para.text.strip()
            if text:
                parts.append(f"\n{text}\n")
    except Exception as e:
        parts.append(f"\n**Error:** {str(e)}\n")

    return "\n".join(parts)


def extract_xlsx_to_markdown(xlsx_path: Path) -> str:
    """Extract text from XLSX."""
    import pandas as pd

    parts = []
    parts.append(f"# {xlsx_path.stem}\n")
    parts.append(f"*Source: {xlsx_path.name}*\n\n---\n")

    try:
        xlsx = pd.ExcelFile(xlsx_path)
        for sheet in xlsx.sheet_names:
            df = pd.read_excel(xlsx, sheet_name=sheet)
            if not df.empty:
                parts.append(f"\n## {sheet}\n")
                parts.append(df.to_markdown(index=False))
    except Exception as e:
        parts.append(f"\n**Error:** {str(e)}\n")

    return "\n".join(parts)


def process_all_narratives(verbose: bool = True) -> dict:
    """Process all narrative files."""
    PROCESSED_NARRATIVES_DIR.mkdir(parents=True, exist_ok=True)

    results = {'success': [], 'failed': [], 'skipped': []}
    all_files = sorted(RAW_NARRATIVES_DIR.iterdir())

    if verbose:
        print(f"Extracting narratives to markdown")
        print(f"Input: {RAW_NARRATIVES_DIR}")
        print(f"Output: {PROCESSED_NARRATIVES_DIR}")
        print(f"Files: {len(all_files)}")
        print()

    for f in all_files:
        if not f.is_file():
            continue

        suffix = f.suffix.lower()
        out = PROCESSED_NARRATIVES_DIR / f"{f.stem}.md"

        try:
            if suffix == '.pdf':
                print(f"  [PDF] {f.name[:60]}")
                content = extract_pdf_to_markdown(f)
            elif suffix == '.docx':
                print(f"  [DOCX] {f.name[:60]}")
                content = extract_docx_to_markdown(f)
            elif suffix == '.xlsx':
                print(f"  [XLSX] {f.name[:60]}")
                content = extract_xlsx_to_markdown(f)
            else:
                results['skipped'].append(f.name)
                continue

            out.write_text(content, encoding='utf-8')
            results['success'].append(f.name)

        except Exception as e:
            print(f"    ERROR: {e}")
            results['failed'].append((f.name, str(e)))

    print(f"\nDone: {len(results['success'])} success, {len(results['failed'])} failed, {len(results['skipped'])} skipped")
    return results


if __name__ == '__main__':
    process_all_narratives()
