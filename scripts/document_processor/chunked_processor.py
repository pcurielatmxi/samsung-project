"""Chunked document processor for handling large files."""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any

from chunking import (
    ChunkBoundary,
    ChunkingResult,
    PageInfo,
    analyze_pages,
    compute_chunks,
    get_chunk_text,
    DEFAULT_TARGET_CHUNK_TOKENS,
    DEFAULT_OVERLAP_PAGES,
)
from claude_client import ClaudeClient, ClaudeResponse

logger = logging.getLogger(__name__)


# Schema for chunk output - includes both extracted data and context output
CHUNK_OUTPUT_SCHEMA_TEMPLATE = {
    "type": "object",
    "properties": {
        "extracted_data": {
            "type": "object",
            "description": "The extracted data according to the user's schema"
        },
        "context_output": {
            "type": "object",
            "properties": {
                "document_summary": {
                    "type": "string",
                    "description": "Brief summary of what this document is about and key findings so far (max 200 words)"
                },
                "trailing_context": {
                    "type": "string",
                    "description": "Context from the end of this chunk that may continue into the next chunk. Include the last paragraph and any incomplete items (max 150 words)"
                },
                "open_items": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of items that appear incomplete and may continue in the next chunk (e.g., 'test_set_5_incomplete', 'table_continues')"
                }
            },
            "required": ["document_summary", "trailing_context", "open_items"]
        }
    },
    "required": ["extracted_data", "context_output"]
}


def build_chunk_schema(user_schema: dict) -> dict:
    """
    Build the full chunk output schema by wrapping user's schema.

    Args:
        user_schema: The user's extraction schema

    Returns:
        Complete schema for chunk output including context fields
    """
    schema = CHUNK_OUTPUT_SCHEMA_TEMPLATE.copy()
    schema["properties"] = schema["properties"].copy()
    schema["properties"]["extracted_data"] = user_schema
    return schema


def build_single_chunk_schema(user_schema: dict) -> dict:
    """
    Build schema for single-chunk documents (no context output needed).

    Args:
        user_schema: The user's extraction schema

    Returns:
        Schema wrapping extracted_data only
    """
    return {
        "type": "object",
        "properties": {
            "extracted_data": user_schema
        },
        "required": ["extracted_data"]
    }


CHUNKED_PROMPT_TEMPLATE = """You are processing a CHUNK of a larger document. This is chunk {chunk_index} of {total_chunks}.

IMPORTANT: This is only a portion of the full document. Extract data from THIS CHUNK ONLY, and provide context for the next chunk.

{context_section}

---
EXTRACTION INSTRUCTIONS:
{user_prompt}

---
DOCUMENT CHUNK (Pages {start_page}-{end_page}):
{chunk_content}

---
OUTPUT REQUIREMENTS:
1. "extracted_data": Extract information according to the instructions above for THIS CHUNK ONLY
2. "context_output": Provide context for processing subsequent chunks:
   - "document_summary": Brief summary of the document type and key information found so far
   - "trailing_context": The last paragraph or incomplete section that may continue on the next page
   - "open_items": List any items that appear incomplete (e.g., tables that continue, test sets with pending results)
"""


CONTEXT_SECTION_TEMPLATE = """CONTEXT FROM PREVIOUS CHUNKS:
Document Summary: {document_summary}

Previous Chunk Trailing Context: {trailing_context}

Open Items to Complete: {open_items}

NOTE: If you see items in "Open Items to Complete", look for their completion in this chunk and include the complete data in extracted_data.
"""


SINGLE_CHUNK_PROMPT_TEMPLATE = """Extract information from this document according to the instructions below.

---
EXTRACTION INSTRUCTIONS:
{user_prompt}

---
DOCUMENT CONTENT:
{content}

---
Provide your response in the "extracted_data" field.
"""


@dataclass
class ChunkResult:
    """Result from processing a single chunk."""
    chunk_index: int
    pages: Dict[str, int]  # {"start": N, "end": M}
    tokens_estimated: int
    context_input: Optional[Dict[str, Any]]
    context_output: Optional[Dict[str, Any]]
    extracted_data: Any
    chunk_metadata: Dict[str, Any]
    success: bool
    error: Optional[str] = None


@dataclass
class ChunkedProcessingResult:
    """Complete result from processing a document with chunking."""
    success: bool
    metadata: Dict[str, Any]
    chunks: List[ChunkResult]
    error: Optional[str] = None


class ChunkedDocumentProcessor:
    """Processes documents with automatic chunking for large files."""

    def __init__(
        self,
        client: ClaudeClient,
        user_prompt: str,
        user_schema: dict,
        max_tokens: int = 100_000,
        target_chunk_tokens: int = DEFAULT_TARGET_CHUNK_TOKENS,
        overlap_pages: int = DEFAULT_OVERLAP_PAGES,
    ):
        """
        Initialize the chunked processor.

        Args:
            client: ClaudeClient instance for API calls
            user_prompt: The extraction prompt from the user
            user_schema: The JSON schema for extraction
            max_tokens: Token threshold for triggering chunking
            target_chunk_tokens: Target tokens per chunk
            overlap_pages: Pages to overlap between chunks
        """
        self.client = client
        self.user_prompt = user_prompt
        self.user_schema = user_schema
        self.max_tokens = max_tokens
        self.target_chunk_tokens = target_chunk_tokens
        self.overlap_pages = overlap_pages

        # Pre-build schemas
        self.chunk_schema = build_chunk_schema(user_schema)
        self.single_chunk_schema = build_single_chunk_schema(user_schema)

    async def process_document(
        self,
        pages: List[tuple],  # List of (page_num, text) tuples
        file_path: Optional[Path] = None,
    ) -> ChunkedProcessingResult:
        """
        Process a document, automatically chunking if needed.

        Args:
            pages: List of (page_number, text) tuples
            file_path: Optional source file path for metadata

        Returns:
            ChunkedProcessingResult with all chunk outputs
        """
        # Analyze pages and compute chunks
        page_infos = analyze_pages(pages)
        chunking_result = compute_chunks(
            page_infos,
            max_tokens=self.max_tokens,
            target_chunk_tokens=self.target_chunk_tokens,
            overlap_pages=self.overlap_pages,
        )

        # Build metadata
        metadata = {
            "source_file": str(file_path) if file_path else None,
            "filename": file_path.name if file_path else None,
            "processed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "model": self.client.model,
            "total_pages": chunking_result.total_pages,
            "total_tokens_estimated": chunking_result.total_tokens,
            "chunking": {
                "required": chunking_result.chunking_required,
                "strategy": chunking_result.strategy,
                "target_chunk_tokens": chunking_result.target_chunk_tokens,
                "overlap_pages": chunking_result.overlap_pages,
                "total_chunks": len(chunking_result.chunks),
            },
        }

        # Process based on chunking requirement
        if not chunking_result.chunking_required:
            # Single chunk - simpler processing
            result = await self._process_single_chunk(
                page_infos, chunking_result.chunks[0], file_path
            )
            return ChunkedProcessingResult(
                success=result.success,
                metadata=metadata,
                chunks=[result],
                error=result.error,
            )
        else:
            # Multiple chunks - process with context passing
            results = await self._process_multiple_chunks(
                page_infos, chunking_result.chunks, file_path
            )
            success = all(r.success for r in results)
            errors = [r.error for r in results if r.error]
            return ChunkedProcessingResult(
                success=success,
                metadata=metadata,
                chunks=results,
                error="; ".join(errors) if errors else None,
            )

    async def _process_single_chunk(
        self,
        page_infos: List[PageInfo],
        chunk: ChunkBoundary,
        file_path: Optional[Path],
    ) -> ChunkResult:
        """Process a single-chunk document (no context passing needed)."""
        # Get full content
        content = get_chunk_text(page_infos, chunk)

        # Build prompt
        prompt = SINGLE_CHUNK_PROMPT_TEMPLATE.format(
            user_prompt=self.user_prompt,
            content=content,
        )

        # Call Claude
        response = await self.client.analyze_document(
            content=prompt,
            prompt="",  # Prompt is embedded in content
            schema=self.single_chunk_schema,
            file_path=file_path,
        )

        if not response.success:
            return ChunkResult(
                chunk_index=0,
                pages={"start": chunk.start_page, "end": chunk.end_page},
                tokens_estimated=chunk.estimated_tokens,
                context_input=None,
                context_output=None,
                extracted_data=None,
                chunk_metadata={
                    "cost_usd": response.cost_usd,
                    "duration_ms": response.duration_ms,
                    "session_id": response.session_id,
                },
                success=False,
                error=response.error,
            )

        # Extract result
        result_data = response.result
        extracted_data = result_data.get("extracted_data") if isinstance(result_data, dict) else result_data

        return ChunkResult(
            chunk_index=0,
            pages={"start": chunk.start_page, "end": chunk.end_page},
            tokens_estimated=chunk.estimated_tokens,
            context_input=None,
            context_output=None,
            extracted_data=extracted_data,
            chunk_metadata={
                "cost_usd": response.cost_usd,
                "duration_ms": response.duration_ms,
                "session_id": response.session_id,
            },
            success=True,
        )

    async def _process_multiple_chunks(
        self,
        page_infos: List[PageInfo],
        chunks: List[ChunkBoundary],
        file_path: Optional[Path],
    ) -> List[ChunkResult]:
        """Process multiple chunks with context passing."""
        results = []
        previous_context_output = None

        for chunk in chunks:
            # Build context section
            if previous_context_output:
                context_section = CONTEXT_SECTION_TEMPLATE.format(
                    document_summary=previous_context_output.get("document_summary", ""),
                    trailing_context=previous_context_output.get("trailing_context", ""),
                    open_items=", ".join(previous_context_output.get("open_items", [])) or "None",
                )
                context_input = previous_context_output.copy()
            else:
                context_section = "This is the FIRST chunk of the document. No previous context available."
                context_input = None

            # Get chunk content
            chunk_content = get_chunk_text(page_infos, chunk)

            # Build prompt
            prompt = CHUNKED_PROMPT_TEMPLATE.format(
                chunk_index=chunk.chunk_index + 1,
                total_chunks=len(chunks),
                context_section=context_section,
                user_prompt=self.user_prompt,
                start_page=chunk.start_page,
                end_page=chunk.end_page,
                chunk_content=chunk_content,
            )

            # Call Claude
            response = await self.client.analyze_document(
                content=prompt,
                prompt="",  # Prompt is embedded in content
                schema=self.chunk_schema,
                file_path=file_path,
            )

            if not response.success:
                results.append(ChunkResult(
                    chunk_index=chunk.chunk_index,
                    pages={"start": chunk.start_page, "end": chunk.end_page},
                    tokens_estimated=chunk.estimated_tokens,
                    context_input=context_input,
                    context_output=None,
                    extracted_data=None,
                    chunk_metadata={
                        "cost_usd": response.cost_usd,
                        "duration_ms": response.duration_ms,
                        "session_id": response.session_id,
                    },
                    success=False,
                    error=response.error,
                ))
                # Continue with next chunk even if this one failed
                continue

            # Parse response
            result_data = response.result
            if isinstance(result_data, dict):
                extracted_data = result_data.get("extracted_data")
                context_output = result_data.get("context_output")
            else:
                extracted_data = result_data
                context_output = None

            # Store for next iteration
            previous_context_output = context_output

            results.append(ChunkResult(
                chunk_index=chunk.chunk_index,
                pages={"start": chunk.start_page, "end": chunk.end_page},
                tokens_estimated=chunk.estimated_tokens,
                context_input=context_input,
                context_output=context_output,
                extracted_data=extracted_data,
                chunk_metadata={
                    "cost_usd": response.cost_usd,
                    "duration_ms": response.duration_ms,
                    "session_id": response.session_id,
                },
                success=True,
            ))

            logger.info(
                f"Chunk {chunk.chunk_index + 1}/{len(chunks)} processed "
                f"(${response.cost_usd:.4f}, {response.duration_ms}ms)"
            )

        return results


def result_to_dict(result: ChunkedProcessingResult) -> dict:
    """Convert ChunkedProcessingResult to a JSON-serializable dict."""
    return {
        "metadata": result.metadata,
        "chunks": [
            {
                "chunk_index": c.chunk_index,
                "pages": c.pages,
                "tokens_estimated": c.tokens_estimated,
                "context_input": c.context_input,
                "context_output": c.context_output,
                "extracted_data": c.extracted_data,
                "chunk_metadata": c.chunk_metadata,
                "success": c.success,
                "error": c.error,
            }
            for c in result.chunks
        ],
    }
