"""
Narratives embeddings module for semantic search.

Provides CLI and Python API for searching narrative documents and statements
using Gemini embeddings stored in ChromaDB.

Usage:
    # CLI
    python -m scripts.narratives.embeddings build
    python -m scripts.narratives.embeddings search "HVAC delays"
    python -m scripts.narratives.embeddings status

    # Python API
    from scripts.narratives.embeddings import search_statements, search_documents
    results = search_statements("HVAC delays", category="delay", limit=10)
"""

from .store import search_statements, search_documents
from .builder import build_index

__all__ = ["search_statements", "search_documents", "build_index"]
