"""
Gemini Python SDK client for document processing.

Uses google-genai library to process PDFs with optional structured output.
"""

import json
import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Optional, Any, Union
from dataclasses import dataclass
from contextlib import contextmanager

import fitz  # PyMuPDF
from dotenv import load_dotenv
from google import genai

# Load environment variables from project root .env
_project_root = Path(__file__).parent.parent.parent.parent
load_dotenv(_project_root / ".env")

# Gemini limits
MAX_FILE_SIZE_MB = 50
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
MAX_PAGES = 1000


def _is_ascii_safe(filepath: Path) -> bool:
    """Check if filepath can be safely encoded as ASCII."""
    try:
        str(filepath).encode('ascii')
        return True
    except UnicodeEncodeError:
        return False


@contextmanager
def _safe_upload_path(filepath: Path):
    """
    Context manager that yields an ASCII-safe path for file upload.

    The google-genai library has a bug where it cannot handle non-ASCII
    characters in file paths. This workaround copies the file to a temp
    location with an ASCII-safe name if needed.
    """
    if _is_ascii_safe(filepath):
        yield filepath
    else:
        # Copy to temp file with ASCII-safe name
        temp_dir = Path(tempfile.gettempdir())
        safe_name = f"{uuid.uuid4().hex}{filepath.suffix}"
        temp_file = temp_dir / safe_name
        try:
            shutil.copy2(filepath, temp_file)
            yield temp_file
        finally:
            if temp_file.exists():
                temp_file.unlink()


@dataclass
class DocumentInfo:
    """Information about a document for validation."""
    filepath: Path
    file_size_bytes: int
    file_size_mb: float
    page_count: int
    is_valid: bool
    error: Optional[str] = None


@dataclass
class GeminiResponse:
    """Response from Gemini API."""
    success: bool
    result: Optional[Any]
    error: Optional[str]
    model: str
    usage: Optional[dict] = None
    doc_info: Optional[DocumentInfo] = None


def get_document_info(filepath: Union[str, Path]) -> DocumentInfo:
    """
    Get document information and validate against Gemini limits.

    Args:
        filepath: Path to PDF file

    Returns:
        DocumentInfo with size, page count, and validation status
    """
    filepath = Path(filepath)

    if not filepath.exists():
        return DocumentInfo(
            filepath=filepath,
            file_size_bytes=0,
            file_size_mb=0.0,
            page_count=0,
            is_valid=False,
            error=f"File not found: {filepath}",
        )

    # Get file size
    file_size_bytes = filepath.stat().st_size
    file_size_mb = file_size_bytes / (1024 * 1024)

    # Get page count for PDFs
    page_count = 0
    if filepath.suffix.lower() == ".pdf":
        try:
            with fitz.open(filepath) as doc:
                page_count = len(doc)
        except Exception as e:
            return DocumentInfo(
                filepath=filepath,
                file_size_bytes=file_size_bytes,
                file_size_mb=file_size_mb,
                page_count=0,
                is_valid=False,
                error=f"Failed to read PDF: {e}",
            )

    # Validate against limits
    errors = []
    if file_size_bytes > MAX_FILE_SIZE_BYTES:
        errors.append(f"File size {file_size_mb:.1f}MB exceeds {MAX_FILE_SIZE_MB}MB limit")
    if page_count > MAX_PAGES:
        errors.append(f"Page count {page_count} exceeds {MAX_PAGES} page limit")

    return DocumentInfo(
        filepath=filepath,
        file_size_bytes=file_size_bytes,
        file_size_mb=file_size_mb,
        page_count=page_count,
        is_valid=len(errors) == 0,
        error="; ".join(errors) if errors else None,
    )


def _get_client() -> genai.Client:
    """Get authenticated Gemini client."""
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise ValueError("No API key found. Set GEMINI_API_KEY or GOOGLE_API_KEY in .env")
    return genai.Client(api_key=api_key)


def _convert_schema_to_gemini(schema: dict) -> dict:
    """
    Convert standard JSON Schema to Gemini's schema format.

    Gemini schema differences:
    - No $schema key allowed
    - Nullable uses: type: "STRING", nullable: true
    - Type names are uppercase: STRING, INTEGER, BOOLEAN, ARRAY, OBJECT
    """
    if not schema:
        return schema

    def convert_type(type_value):
        """Convert JSON Schema type to Gemini type."""
        if isinstance(type_value, list):
            # ["string", "null"] -> ("STRING", True)
            non_null = [t for t in type_value if t != "null"]
            is_nullable = "null" in type_value
            if non_null:
                return non_null[0].upper(), is_nullable
            return "STRING", is_nullable
        return type_value.upper() if type_value else "STRING", False

    def convert_node(node: dict) -> dict:
        """Recursively convert a schema node."""
        if not isinstance(node, dict):
            return node

        result = {}

        for key, value in node.items():
            # Skip $schema
            if key == "$schema":
                continue

            if key == "type":
                gemini_type, is_nullable = convert_type(value)
                result["type"] = gemini_type
                if is_nullable:
                    result["nullable"] = True

            elif key == "properties":
                result["properties"] = {
                    k: convert_node(v) for k, v in value.items()
                }

            elif key == "items":
                result["items"] = convert_node(value)

            elif key == "enum":
                result["enum"] = value

            elif key in ("required", "description", "title"):
                result[key] = value

            # Skip other JSON Schema specific keys
            elif key in ("$id", "$ref", "definitions", "additionalProperties",
                         "minItems", "maxItems", "minLength", "maxLength",
                         "minimum", "maximum", "pattern", "format"):
                continue

            else:
                # Copy other keys as-is
                result[key] = value

        return result

    return convert_node(schema)


def process_document(
    filepath: Union[str, Path],
    prompt: str,
    schema: Optional[dict] = None,
    model: str = "gemini-3-flash-preview",
) -> GeminiResponse:
    """
    Process a PDF document with Gemini.

    Args:
        filepath: Path to PDF file
        prompt: Extraction/analysis prompt
        schema: Optional JSON schema for structured output
        model: Gemini model to use

    Returns:
        GeminiResponse with extracted content
    """
    filepath = Path(filepath)

    # Validate document
    doc_info = get_document_info(filepath)
    if not doc_info.is_valid:
        return GeminiResponse(
            success=False,
            result=None,
            error=doc_info.error,
            model=model,
            doc_info=doc_info,
        )

    try:
        client = _get_client()
    except ValueError as e:
        return GeminiResponse(
            success=False,
            result=None,
            error=str(e),
            model=model,
        )
    except Exception as e:
        return GeminiResponse(
            success=False,
            result=None,
            error=f"Failed to initialize Gemini client: {e}",
            model=model,
        )

    try:
        # Upload the PDF file (handle non-ASCII filenames)
        with _safe_upload_path(filepath) as upload_path:
            uploaded_file = client.files.upload(file=upload_path)

            # Build config for structured output if schema provided
            config = {}
            if schema:
                gemini_schema = _convert_schema_to_gemini(schema)
                config = {
                    "response_mime_type": "application/json",
                    "response_schema": gemini_schema,
                }

            # Generate content
            response = client.models.generate_content(
                model=model,
                contents=[
                    uploaded_file,
                    prompt,
                ],
                config=config if config else None,
            )

        # Parse result
        result_text = response.text

        # If schema was provided, parse JSON
        if schema:
            try:
                result = json.loads(result_text)
            except json.JSONDecodeError as e:
                return GeminiResponse(
                    success=False,
                    result=result_text,
                    error=f"Failed to parse JSON response: {e}",
                    model=model,
                )
        else:
            result = result_text

        # Extract usage metadata if available
        usage = None
        if hasattr(response, 'usage_metadata'):
            usage = {
                "prompt_tokens": getattr(response.usage_metadata, 'prompt_token_count', None),
                "output_tokens": getattr(response.usage_metadata, 'candidates_token_count', None),
                "total_tokens": getattr(response.usage_metadata, 'total_token_count', None),
            }

        return GeminiResponse(
            success=True,
            result=result,
            error=None,
            model=model,
            usage=usage,
            doc_info=doc_info,
        )

    except Exception as e:
        return GeminiResponse(
            success=False,
            result=None,
            error=str(e),
            model=model,
            doc_info=doc_info,
        )


def process_document_text(
    text: str,
    prompt: str,
    schema: Optional[dict] = None,
    model: str = "gemini-3-flash-preview",
) -> GeminiResponse:
    """
    Process text content with Gemini.

    Args:
        text: Text content to process
        prompt: Processing prompt
        schema: Optional Gemini-format schema for structured output
        model: Gemini model to use

    Returns:
        GeminiResponse with processed content
    """
    try:
        client = _get_client()
    except ValueError as e:
        return GeminiResponse(
            success=False,
            result=None,
            error=str(e),
            model=model,
        )
    except Exception as e:
        return GeminiResponse(
            success=False,
            result=None,
            error=f"Failed to initialize Gemini client: {e}",
            model=model,
        )

    try:
        # Build full prompt with content
        full_prompt = f"{prompt}\n\nContent:\n---\n{text}\n---"

        # Build config for structured output if schema provided
        config = {}
        if schema:
            gemini_schema = _convert_schema_to_gemini(schema)
            config = {
                "response_mime_type": "application/json",
                "response_schema": gemini_schema,
            }

        # Generate content
        response = client.models.generate_content(
            model=model,
            contents=full_prompt,
            config=config if config else None,
        )

        # Parse result
        result_text = response.text

        # If schema was provided, parse JSON
        if schema:
            try:
                result = json.loads(result_text)
            except json.JSONDecodeError as e:
                return GeminiResponse(
                    success=False,
                    result=result_text,
                    error=f"Failed to parse JSON response: {e}",
                    model=model,
                )
        else:
            result = result_text

        # Extract usage metadata if available
        usage = None
        if hasattr(response, 'usage_metadata'):
            usage = {
                "prompt_tokens": getattr(response.usage_metadata, 'prompt_token_count', None),
                "output_tokens": getattr(response.usage_metadata, 'candidates_token_count', None),
                "total_tokens": getattr(response.usage_metadata, 'total_token_count', None),
            }

        return GeminiResponse(
            success=True,
            result=result,
            error=None,
            model=model,
            usage=usage,
        )

    except Exception as e:
        return GeminiResponse(
            success=False,
            result=None,
            error=str(e),
            model=model,
        )


# Test function
if __name__ == "__main__":
    import sys

    # Simple test with a RABA PDF
    if len(sys.argv) < 2:
        print("Usage: python gemini_client.py <pdf_path> [prompt]")
        print("\nExample:")
        print('  python gemini_client.py /path/to/file.pdf "Summarize this document"')
        sys.exit(1)

    pdf_path = sys.argv[1]
    prompt = sys.argv[2] if len(sys.argv) > 2 else "What is this document about? Provide a brief summary."

    print(f"Processing: {pdf_path}")
    print(f"Prompt: {prompt}")
    print("-" * 60)

    response = process_document(pdf_path, prompt)

    if response.success:
        print("SUCCESS")
        print(f"Model: {response.model}")
        if response.doc_info:
            print(f"Document: {response.doc_info.file_size_mb:.2f}MB, {response.doc_info.page_count} pages")
        if response.usage:
            print(f"Usage: {response.usage}")
        print("-" * 60)
        print(response.result)
    else:
        print("FAILED")
        print(f"Error: {response.error}")
        if response.doc_info:
            print(f"Document: {response.doc_info.file_size_mb:.2f}MB, {response.doc_info.page_count} pages")
