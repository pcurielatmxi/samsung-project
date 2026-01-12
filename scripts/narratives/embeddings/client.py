"""Gemini embedding client wrapper."""

from typing import List, Optional
from google import genai

from . import config


class EmbeddingClient:
    """Client for generating embeddings via Gemini API."""

    def __init__(self, api_key: Optional[str] = None):
        """Initialize the Gemini client.

        Args:
            api_key: Gemini API key. Defaults to GEMINI_API_KEY from config.
        """
        self.api_key = api_key or config.GEMINI_API_KEY
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not set")

        self.client = genai.Client(api_key=self.api_key)
        self.model = config.EMBEDDING_MODEL
        self.dimensions = config.EMBEDDING_DIMENSIONS

    def embed_for_index(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for indexing documents/statements.

        Args:
            texts: List of texts to embed.

        Returns:
            List of embedding vectors.
        """
        if not texts:
            return []

        # Process in batches
        all_embeddings = []
        for i in range(0, len(texts), config.EMBEDDING_BATCH_SIZE):
            batch = texts[i:i + config.EMBEDDING_BATCH_SIZE]
            result = self.client.models.embed_content(
                model=self.model,
                contents=batch,
                config={
                    "task_type": config.EMBEDDING_TASK_INDEX,
                    "output_dimensionality": self.dimensions
                }
            )
            all_embeddings.extend([e.values for e in result.embeddings])

        return all_embeddings

    def embed_for_query(self, query: str) -> List[float]:
        """Generate embedding for a search query.

        Args:
            query: Search query text.

        Returns:
            Embedding vector.
        """
        result = self.client.models.embed_content(
            model=self.model,
            contents=query,
            config={
                "task_type": config.EMBEDDING_TASK_QUERY,
                "output_dimensionality": self.dimensions
            }
        )
        return result.embeddings[0].values


# Singleton instance for convenience
_client: Optional[EmbeddingClient] = None


def get_client() -> EmbeddingClient:
    """Get or create the singleton embedding client."""
    global _client
    if _client is None:
        _client = EmbeddingClient()
    return _client


def embed_for_index(texts: List[str]) -> List[List[float]]:
    """Generate embeddings for indexing. Convenience wrapper."""
    return get_client().embed_for_index(texts)


def embed_for_query(query: str) -> List[float]:
    """Generate embedding for query. Convenience wrapper."""
    return get_client().embed_for_query(query)
