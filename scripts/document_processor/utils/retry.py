"""Retry logic with exponential backoff."""

import asyncio
import logging
import random
from functools import wraps
from typing import Callable, TypeVar, Any

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RetryableError(Exception):
    """Error that should trigger a retry."""
    pass


class RateLimitError(RetryableError):
    """Rate limit exceeded error."""
    pass


async def retry_with_backoff(
    func: Callable[..., T],
    *args,
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    **kwargs
) -> T:
    """
    Execute a function with exponential backoff retry logic.

    Args:
        func: Async function to execute
        *args: Positional arguments for the function
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        jitter: Whether to add random jitter to delays
        **kwargs: Keyword arguments for the function

    Returns:
        Result of the function

    Raises:
        Exception: The last exception if all retries fail
    """
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            if asyncio.iscoroutinefunction(func):
                return await func(*args, **kwargs)
            else:
                return func(*args, **kwargs)

        except RetryableError as e:
            last_exception = e
            if attempt == max_retries:
                logger.error(f"All {max_retries + 1} attempts failed: {e}")
                raise

            # Calculate delay with exponential backoff
            delay = min(base_delay * (2 ** attempt), max_delay)
            if jitter:
                delay = delay * (0.5 + random.random())

            logger.warning(
                f"Attempt {attempt + 1}/{max_retries + 1} failed: {e}. "
                f"Retrying in {delay:.1f}s..."
            )
            await asyncio.sleep(delay)

        except Exception as e:
            # Non-retryable error, raise immediately
            raise

    raise last_exception


def is_rate_limit_error(error_message: str) -> bool:
    """Check if an error message indicates a rate limit."""
    rate_limit_indicators = [
        "rate limit",
        "rate_limit",
        "429",
        "too many requests",
        "overloaded",
        "capacity",
    ]
    error_lower = error_message.lower()
    return any(indicator in error_lower for indicator in rate_limit_indicators)
