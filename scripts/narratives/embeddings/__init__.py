"""
Narratives embeddings module for semantic search.

Provides CLI and Python API for searching narrative document chunks
using Gemini embeddings stored in ChromaDB.

Usage:
    # CLI
    python -m scripts.narratives.embeddings build
    python -m scripts.narratives.embeddings build --limit 10
    python -m scripts.narratives.embeddings search "HVAC delays"
    python -m scripts.narratives.embeddings status

    # Python API
    from scripts.narratives.embeddings import search_chunks
    results = search_chunks("HVAC delays", document_type="schedule_narrative", limit=10)
"""

from .store import search_chunks, get_store, ChunkResult
from .builder import build_index

__all__ = ["search_chunks", "build_index", "get_store", "ChunkResult"]
