#!/usr/bin/env python3
"""
Convert construction drawing PDFs to PNG images for upload to Fieldwire.

Usage:
    python scripts/convert_drawings_to_png.py
    python scripts/convert_drawings_to_png.py --dpi 200  # Higher resolution
"""

import argparse
import sys
from pathlib import Path

import fitz  # PyMuPDF


def convert_pdf_to_png(pdf_path: Path, output_dir: Path, dpi: int = 150) -> Path:
    """Convert first page of PDF to PNG."""
    doc = fitz.open(pdf_path)
    page = doc[0]

    # Calculate zoom for desired DPI (PDF default is 72 DPI)
    zoom = dpi / 72
    mat = fitz.Matrix(zoom, zoom)

    # Render to pixmap
    pix = page.get_pixmap(matrix=mat, alpha=False)

    # Output filename: keep drawing number, remove title for cleaner names
    stem = pdf_path.stem
    if " - " in stem:
        drawing_number = stem.split(" - ")[0].strip()
    else:
        drawing_number = stem

    output_path = output_dir / f"{drawing_number}.png"
    pix.save(output_path)

    doc.close()
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Convert drawing PDFs to PNG")
    parser.add_argument("--input", "-i", type=Path, default=Path("data/drawings"),
                        help="Input directory with PDF files")
    parser.add_argument("--output", "-o", type=Path, default=Path("data/drawings/png"),
                        help="Output directory for PNG files")
    parser.add_argument("--dpi", type=int, default=150,
                        help="DPI for PNG conversion (default: 150)")

    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: Input directory not found: {args.input}")
        sys.exit(1)

    args.output.mkdir(parents=True, exist_ok=True)

    pdf_files = sorted(args.input.glob("*.pdf"))
    print(f"Found {len(pdf_files)} PDF files\n")

    for i, pdf_path in enumerate(pdf_files, 1):
        print(f"[{i}/{len(pdf_files)}] {pdf_path.name}")
        try:
            output_path = convert_pdf_to_png(pdf_path, args.output, args.dpi)
            print(f"    → {output_path.name}")
        except Exception as e:
            print(f"    ERROR: {e}")

    print(f"\nDone! PNG files saved to: {args.output}")


if __name__ == "__main__":
    main()
