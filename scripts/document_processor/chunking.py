"""Document chunking utilities for large file processing."""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

# Default chunking parameters
DEFAULT_TARGET_CHUNK_TOKENS = 30_000  # Target tokens per chunk
DEFAULT_MAX_CHUNK_TOKENS = 40_000     # Hard limit per chunk
DEFAULT_OVERLAP_PAGES = 2              # Pages to overlap between chunks
DEFAULT_MIN_CHUNK_PAGES = 3            # Minimum pages per chunk

# Token estimation: ~4 chars per token
CHARS_PER_TOKEN = 4


@dataclass
class PageInfo:
    """Information about a single page."""
    page_num: int  # 1-indexed
    text: str
    char_count: int
    estimated_tokens: int


@dataclass
class ChunkBoundary:
    """Defines the boundaries of a chunk."""
    chunk_index: int
    start_page: int  # 1-indexed, inclusive
    end_page: int    # 1-indexed, inclusive
    estimated_tokens: int
    is_first: bool = False
    is_last: bool = False


@dataclass
class ChunkingResult:
    """Result of chunking analysis."""
    total_pages: int
    total_tokens: int
    chunking_required: bool
    chunks: List[ChunkBoundary]
    strategy: str  # "none", "token_based"
    target_chunk_tokens: int
    overlap_pages: int
    page_infos: List[PageInfo] = field(default_factory=list)


def estimate_tokens(text: str) -> int:
    """Estimate token count from text."""
    if not text:
        return 0
    return len(text) // CHARS_PER_TOKEN


def analyze_pages(pages: List[Tuple[int, str]]) -> List[PageInfo]:
    """
    Analyze pages and compute token estimates.

    Args:
        pages: List of (page_num, text) tuples (1-indexed page numbers)

    Returns:
        List of PageInfo objects
    """
    page_infos = []
    for page_num, text in pages:
        char_count = len(text)
        tokens = estimate_tokens(text)
        page_infos.append(PageInfo(
            page_num=page_num,
            text=text,
            char_count=char_count,
            estimated_tokens=tokens,
        ))
    return page_infos


def compute_chunks(
    page_infos: List[PageInfo],
    max_tokens: int,
    target_chunk_tokens: int = DEFAULT_TARGET_CHUNK_TOKENS,
    overlap_pages: int = DEFAULT_OVERLAP_PAGES,
    min_chunk_pages: int = DEFAULT_MIN_CHUNK_PAGES,
) -> ChunkingResult:
    """
    Compute optimal chunk boundaries for a document.

    Args:
        page_infos: List of PageInfo objects for each page
        max_tokens: Maximum tokens before chunking is required
        target_chunk_tokens: Target tokens per chunk
        overlap_pages: Number of pages to overlap between chunks
        min_chunk_pages: Minimum pages per chunk

    Returns:
        ChunkingResult with chunk boundaries
    """
    if not page_infos:
        return ChunkingResult(
            total_pages=0,
            total_tokens=0,
            chunking_required=False,
            chunks=[],
            strategy="none",
            target_chunk_tokens=target_chunk_tokens,
            overlap_pages=overlap_pages,
            page_infos=[],
        )

    total_tokens = sum(p.estimated_tokens for p in page_infos)
    total_pages = len(page_infos)

    # Check if chunking is needed
    if total_tokens <= max_tokens:
        # Single chunk - no chunking needed
        return ChunkingResult(
            total_pages=total_pages,
            total_tokens=total_tokens,
            chunking_required=False,
            chunks=[ChunkBoundary(
                chunk_index=0,
                start_page=1,
                end_page=total_pages,
                estimated_tokens=total_tokens,
                is_first=True,
                is_last=True,
            )],
            strategy="none",
            target_chunk_tokens=target_chunk_tokens,
            overlap_pages=overlap_pages,
            page_infos=page_infos,
        )

    # Chunking required - compute boundaries
    chunks = []
    current_start = 1  # 1-indexed
    chunk_index = 0

    while current_start <= total_pages:
        # Find end page for this chunk
        current_tokens = 0
        current_end = current_start

        for i in range(current_start - 1, total_pages):  # Convert to 0-indexed
            page_tokens = page_infos[i].estimated_tokens

            # Check if adding this page exceeds target
            if current_tokens + page_tokens > target_chunk_tokens:
                # Only stop if we have minimum pages
                if current_end - current_start + 1 >= min_chunk_pages:
                    break

            current_tokens += page_tokens
            current_end = i + 1  # Convert back to 1-indexed

        # Ensure we make progress (at least one page per chunk)
        if current_end < current_start:
            current_end = current_start
            current_tokens = page_infos[current_start - 1].estimated_tokens

        chunks.append(ChunkBoundary(
            chunk_index=chunk_index,
            start_page=current_start,
            end_page=current_end,
            estimated_tokens=current_tokens,
            is_first=(chunk_index == 0),
            is_last=False,  # Will update after loop
        ))

        chunk_index += 1

        # Next chunk starts with overlap
        next_start = current_end - overlap_pages + 1
        if next_start <= current_start:
            next_start = current_end + 1  # Ensure forward progress

        # If remaining pages are few, include them in last chunk
        remaining_pages = total_pages - current_end
        if 0 < remaining_pages <= overlap_pages + min_chunk_pages:
            # Extend current chunk to end
            chunks[-1].end_page = total_pages
            chunks[-1].estimated_tokens = sum(
                page_infos[i].estimated_tokens
                for i in range(chunks[-1].start_page - 1, total_pages)
            )
            break

        current_start = next_start

        # Safety: prevent infinite loop
        if current_start > total_pages:
            break

    # Mark last chunk
    if chunks:
        chunks[-1].is_last = True

    logger.info(
        f"Document chunked: {total_pages} pages, {total_tokens:,} tokens -> "
        f"{len(chunks)} chunks"
    )

    return ChunkingResult(
        total_pages=total_pages,
        total_tokens=total_tokens,
        chunking_required=True,
        chunks=chunks,
        strategy="token_based",
        target_chunk_tokens=target_chunk_tokens,
        overlap_pages=overlap_pages,
        page_infos=page_infos,
    )


def get_chunk_text(
    page_infos: List[PageInfo],
    chunk: ChunkBoundary,
) -> str:
    """
    Get the text content for a specific chunk.

    Args:
        page_infos: List of all PageInfo objects
        chunk: ChunkBoundary defining the chunk

    Returns:
        Combined text for the chunk with page markers
    """
    parts = []
    for i in range(chunk.start_page - 1, chunk.end_page):  # Convert to 0-indexed
        page_info = page_infos[i]
        parts.append(f"--- Page {page_info.page_num} ---\n{page_info.text}")

    return "\n\n".join(parts)
