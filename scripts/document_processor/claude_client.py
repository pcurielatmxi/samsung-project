"""Claude Code subprocess wrapper for document analysis."""

import asyncio
import json
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Any

from utils.retry import retry_with_backoff, RetryableError, RateLimitError, is_rate_limit_error

logger = logging.getLogger(__name__)


@dataclass
class ClaudeResponse:
    """Response from Claude Code."""
    success: bool
    result: Optional[Any]
    error: Optional[str]
    duration_ms: int
    cost_usd: float
    session_id: Optional[str]
    raw_response: dict


class ClaudeClient:
    """Client for calling Claude Code in non-interactive mode."""

    def __init__(
        self,
        model: str = "sonnet",
        timeout: int = 300,
        max_retries: int = 5,
    ):
        """
        Initialize Claude client.

        Args:
            model: Model to use (sonnet, opus, haiku)
            timeout: Timeout in seconds for each call
            max_retries: Maximum retry attempts for rate limits
        """
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries

    async def analyze_document(
        self,
        content: str,
        prompt: str,
        schema: dict,
        file_path: Optional[Path] = None,
    ) -> ClaudeResponse:
        """
        Analyze document content using Claude Code.

        Args:
            content: Document text content
            prompt: Analysis prompt
            schema: JSON schema for output (required, enforced via --json-schema)
            file_path: Optional source file path for context

        Returns:
            ClaudeResponse with analysis results

        Raises:
            ValueError: If schema is not provided
        """
        if not schema:
            raise ValueError("JSON schema is required for document analysis")

        # Build the full prompt
        full_prompt = self._build_prompt(content, prompt, schema, file_path)

        # Store schema for use in _execute_claude
        self._current_schema = schema

        # Execute with retry logic
        try:
            response = await retry_with_backoff(
                self._execute_claude,
                full_prompt,
                max_retries=self.max_retries,
            )
            return response
        except Exception as e:
            logger.error(f"Claude analysis failed: {e}")
            return ClaudeResponse(
                success=False,
                result=None,
                error=str(e),
                duration_ms=0,
                cost_usd=0.0,
                session_id=None,
                raw_response={},
            )

    def _build_prompt(
        self,
        content: str,
        prompt: str,
        schema: dict,
        file_path: Optional[Path],
    ) -> str:
        """Build the full prompt for Claude.

        Note: Schema is enforced via --json-schema CLI flag, not in prompt text.
        """
        parts = []

        # Add file context if provided
        if file_path:
            parts.append(f"Analyzing document: {file_path.name}")
            parts.append("")

        # Add the user's prompt
        parts.append(prompt)
        parts.append("")

        # Add the document content
        parts.append("Document content:")
        parts.append("---")
        parts.append(content)
        parts.append("---")

        return "\n".join(parts)

    async def _execute_claude(self, prompt: str) -> ClaudeResponse:
        """Execute Claude Code subprocess."""
        cmd = [
            "claude",
            "-p", "-",  # Read prompt from stdin
            "--output-format", "json",
            "--model", self.model,
        ]

        # Add --json-schema if schema is provided for structured output
        if self._current_schema:
            schema_str = json.dumps(self._current_schema)
            cmd.extend(["--json-schema", schema_str])

        try:
            # Run subprocess asynchronously with stdin for prompt
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(input=prompt.encode("utf-8")),
                    timeout=self.timeout,
                )
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                raise RetryableError(f"Claude Code timed out after {self.timeout}s")

            stdout_str = stdout.decode("utf-8").strip()
            stderr_str = stderr.decode("utf-8").strip()

            if process.returncode != 0:
                error_msg = stderr_str or stdout_str or f"Exit code {process.returncode}"
                if is_rate_limit_error(error_msg):
                    raise RateLimitError(f"Rate limit: {error_msg}")
                raise RetryableError(f"Claude Code failed: {error_msg}")

            # Parse JSON response
            try:
                response_data = json.loads(stdout_str)
            except json.JSONDecodeError as e:
                raise RetryableError(f"Invalid JSON response: {e}")

            # Check for error in response
            if response_data.get("is_error"):
                error_msg = response_data.get("result", "Unknown error")
                if is_rate_limit_error(str(error_msg)):
                    raise RateLimitError(f"Rate limit: {error_msg}")
                raise RetryableError(f"Claude error: {error_msg}")

            # Extract structured_output (from --json-schema enforcement)
            structured_output = response_data.get("structured_output")
            if structured_output is None:
                # Fallback to parsing result text if structured_output not present
                result_text = response_data.get("result", "")
                parsed_result = self._try_parse_json(result_text)
                logger.warning("No structured_output in response, falling back to text parsing")
            else:
                parsed_result = structured_output

            return ClaudeResponse(
                success=True,
                result=parsed_result,
                error=None,
                duration_ms=response_data.get("duration_ms", 0),
                cost_usd=response_data.get("total_cost_usd", 0.0),
                session_id=response_data.get("session_id"),
                raw_response=response_data,
            )

        except (RateLimitError, RetryableError):
            raise
        except Exception as e:
            raise RetryableError(f"Subprocess error: {e}")

    def _try_parse_json(self, text: str) -> Any:
        """Try to parse text as JSON, return original if not valid JSON."""
        if not text:
            return text

        # Try to find JSON in the response (might be wrapped in markdown)
        text = text.strip()

        # Remove markdown code blocks if present
        if text.startswith("```json"):
            text = text[7:]
        elif text.startswith("```"):
            text = text[3:]

        if text.endswith("```"):
            text = text[:-3]

        text = text.strip()

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # Return original text if not valid JSON
            return text
