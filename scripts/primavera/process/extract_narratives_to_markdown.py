#!/usr/bin/env python3
"""
Extract Narrative Documents to Markdown (Simple/Fast version)

Converts PDF, DOCX, and XLSX files from primavera_narratives to markdown format.
Uses pymupdf for fast PDF extraction. No table parsing - just text extraction.

Processes files recursively in subfolders, maintaining folder structure in output.

Output: processed/primavera_narratives/{subfolder}/{original_filename}.md
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


def get_all_files_recursive(directory: Path) -> list[Path]:
    """Get all files recursively from directory and subdirectories."""
    all_files = []
    for item in sorted(directory.iterdir()):
        if item.is_file():
            all_files.append(item)
        elif item.is_dir():
            all_files.extend(get_all_files_recursive(item))
    return all_files


def process_all_narratives(verbose: bool = True) -> dict:
    """Process all narrative files, including subfolders."""
    PROCESSED_NARRATIVES_DIR.mkdir(parents=True, exist_ok=True)

    results = {'success': [], 'failed': [], 'skipped': []}

    # Get all files recursively
    all_files = get_all_files_recursive(RAW_NARRATIVES_DIR)

    if verbose:
        print(f"Extracting narratives to markdown")
        print(f"Input: {RAW_NARRATIVES_DIR}")
        print(f"Output: {PROCESSED_NARRATIVES_DIR}")
        print(f"Total files (including subfolders): {len(all_files)}")
        print()

    current_folder = None

    for f in all_files:
        suffix = f.suffix.lower()

        # Calculate relative path to maintain subfolder structure
        rel_path = f.relative_to(RAW_NARRATIVES_DIR)
        out_dir = PROCESSED_NARRATIVES_DIR / rel_path.parent
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"{f.stem}.md"

        # Print folder header when entering new subfolder
        folder = str(rel_path.parent) if rel_path.parent != Path('.') else '(root)'
        if folder != current_folder:
            current_folder = folder
            print(f"\n=== {folder} ===")

        try:
            if suffix == '.pdf':
                print(f"  [PDF] {f.name[:55]}")
                content = extract_pdf_to_markdown(f)
            elif suffix == '.docx':
                print(f"  [DOCX] {f.name[:55]}")
                content = extract_docx_to_markdown(f)
            elif suffix == '.xlsx':
                print(f"  [XLSX] {f.name[:55]}")
                content = extract_xlsx_to_markdown(f)
            else:
                results['skipped'].append(str(rel_path))
                continue

            out.write_text(content, encoding='utf-8')
            results['success'].append(str(rel_path))

        except Exception as e:
            print(f"    ERROR: {e}")
            results['failed'].append((str(rel_path), str(e)))

    print(f"\nDone: {len(results['success'])} success, {len(results['failed'])} failed, {len(results['skipped'])} skipped")
    return results


if __name__ == '__main__':
    process_all_narratives()
