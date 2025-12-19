"""Token estimation utilities."""

import logging

logger = logging.getLogger(__name__)

# Approximate tokens per character for English text
# Claude uses ~4 characters per token on average
CHARS_PER_TOKEN = 4


def estimate_tokens(text: str) -> int:
    """
    Estimate the number of tokens in a text string.

    This uses a simple character-based estimation. For more accurate
    results, you could use tiktoken or the Anthropic tokenizer.

    Args:
        text: Text to estimate tokens for

    Returns:
        Estimated token count
    """
    if not text:
        return 0

    # Simple estimation: ~4 characters per token for English
    char_count = len(text)
    estimated_tokens = char_count // CHARS_PER_TOKEN

    return estimated_tokens


def is_document_too_large(text: str, max_tokens: int = 100_000) -> bool:
    """
    Check if a document exceeds the token limit.

    Args:
        text: Document text
        max_tokens: Maximum allowed tokens (default: 100K)

    Returns:
        True if document exceeds limit
    """
    estimated = estimate_tokens(text)
    if estimated > max_tokens:
        logger.warning(
            f"Document has ~{estimated:,} tokens, exceeds limit of {max_tokens:,}"
        )
        return True
    return False


def get_token_stats(text: str) -> dict:
    """
    Get token statistics for a document.

    Args:
        text: Document text

    Returns:
        Dictionary with token stats
    """
    char_count = len(text)
    word_count = len(text.split())
    estimated_tokens = estimate_tokens(text)

    return {
        "characters": char_count,
        "words": word_count,
        "estimated_tokens": estimated_tokens,
    }
