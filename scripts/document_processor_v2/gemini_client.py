"""
Gemini Python SDK client for document processing.

Uses google-genai library to process PDFs with optional structured output.
"""

import json
import os
from pathlib import Path
from typing import Optional, Any, Union
from dataclasses import dataclass

from dotenv import load_dotenv
from google import genai
from google.genai import types

# Load environment variables from .env
load_dotenv(Path(__file__).parent.parent.parent / ".env")


@dataclass
class GeminiResponse:
    """Response from Gemini API."""
    success: bool
    result: Optional[Any]
    error: Optional[str]
    model: str
    usage: Optional[dict] = None


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
        model: Gemini model to use (default: gemini-2.5-flash)

    Returns:
        GeminiResponse with extracted content
    """
    filepath = Path(filepath)

    if not filepath.exists():
        return GeminiResponse(
            success=False,
            result=None,
            error=f"File not found: {filepath}",
            model=model,
        )

    # Initialize client with API key
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        return GeminiResponse(
            success=False,
            result=None,
            error="No API key found. Set GEMINI_API_KEY or GOOGLE_API_KEY in .env",
            model=model,
        )

    try:
        client = genai.Client(api_key=api_key)
    except Exception as e:
        return GeminiResponse(
            success=False,
            result=None,
            error=f"Failed to initialize Gemini client: {e}",
            model=model,
        )

    try:
        # Upload the PDF file
        uploaded_file = client.files.upload(file=filepath)

        # Build config for structured output if schema provided
        config = {}
        if schema:
            config = {
                "response_mime_type": "application/json",
                "response_schema": schema,
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
        if response.usage:
            print(f"Usage: {response.usage}")
        print("-" * 60)
        print(response.result)
    else:
        print("FAILED")
        print(f"Error: {response.error}")
