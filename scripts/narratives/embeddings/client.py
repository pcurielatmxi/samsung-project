"""Gemini embedding client wrapper."""

import time
from typing import List, Optional
from google import genai
from google.genai.errors import ClientError

from . import config

# Rate limiting - Gemini has 1500 RPM limit for embedding
# With 100 texts per batch, 1500 RPM = 15 batches/minute = 4 seconds per batch
RATE_LIMIT_DELAY = 5.0  # 5 seconds between batches to stay well under limit

# Gemini embedding has ~10K token input limit per text
# Rough estimate: 4 chars = 1 token, so limit to 30K chars for safety
MAX_TEXT_LENGTH = 30000


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

    def embed_for_index(self, texts: List[str], verbose: bool = True) -> List[List[float]]:
        """Generate embeddings for indexing documents/statements.

        Args:
            texts: List of texts to embed.
            verbose: If True, print progress.

        Returns:
            List of embedding vectors.
        """
        if not texts:
            return []

        # Truncate long texts
        truncated = 0
        processed_texts = []
        for t in texts:
            if len(t) > MAX_TEXT_LENGTH:
                processed_texts.append(t[:MAX_TEXT_LENGTH] + "...")
                truncated += 1
            else:
                processed_texts.append(t)

        if truncated > 0 and verbose:
            print(f"  Truncated {truncated} texts exceeding {MAX_TEXT_LENGTH} chars")

        # Process in batches with rate limiting
        all_embeddings = []
        total_batches = (len(processed_texts) + config.EMBEDDING_BATCH_SIZE - 1) // config.EMBEDDING_BATCH_SIZE

        for batch_num, i in enumerate(range(0, len(processed_texts), config.EMBEDDING_BATCH_SIZE), 1):
            batch = processed_texts[i:i + config.EMBEDDING_BATCH_SIZE]

            if verbose and batch_num % 10 == 0:
                print(f"  Embedding batch {batch_num}/{total_batches}...")

            # Retry with exponential backoff for rate limits
            max_retries = 5
            for retry in range(max_retries):
                try:
                    result = self.client.models.embed_content(
                        model=self.model,
                        contents=batch,
                        config={
                            "task_type": config.EMBEDDING_TASK_INDEX,
                            "output_dimensionality": self.dimensions
                        }
                    )
                    all_embeddings.extend([e.values for e in result.embeddings])
                    break
                except ClientError as e:
                    if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                        wait_time = (2 ** retry) * 5  # 5, 10, 20, 40, 80 seconds
                        if retry < max_retries - 1:
                            if verbose:
                                print(f"  Rate limited, waiting {wait_time}s (retry {retry+1}/{max_retries})...")
                            time.sleep(wait_time)
                        else:
                            raise
                    else:
                        raise

            # Rate limit between batches
            if batch_num < total_batches:
                time.sleep(RATE_LIMIT_DELAY)

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
