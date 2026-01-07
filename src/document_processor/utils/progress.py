"""
Progress display with live status line and logging.

Provides real-time visibility into pipeline progress with:
- Live updating status line (current file, rate, ETA)
- Structured logging to file
- Clean terminal output
"""

import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional


# Gemini pricing (per 1M tokens) - gemini-2.0-flash
GEMINI_INPUT_COST_PER_M = 0.10   # $0.10 per 1M input tokens
GEMINI_OUTPUT_COST_PER_M = 0.40  # $0.40 per 1M output tokens


@dataclass
class StageProgress:
    """Track progress for a single stage."""
    stage_name: str
    total_files: int
    processed: int = 0
    errors: int = 0
    skipped: int = 0
    total_tokens: int = 0
    start_time: float = field(default_factory=time.time)
    current_file: Optional[str] = None

    @property
    def done(self) -> int:
        """Files actually processed (not skipped)."""
        return self.processed + self.errors

    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time

    @property
    def rate(self) -> float:
        """Files per second."""
        if self.elapsed > 0:
            return self.done / self.elapsed
        return 0.0

    @property
    def eta_seconds(self) -> Optional[float]:
        """Estimated seconds remaining."""
        remaining = self.total_files - self.done
        if remaining <= 0:
            return 0
        if self.rate > 0:
            return remaining / self.rate
        return None

    @property
    def percent(self) -> float:
        if self.total_files > 0:
            return (self.done / self.total_files) * 100
        return 0.0

    @property
    def avg_tokens(self) -> float:
        """Average tokens per successful file."""
        if self.processed > 0:
            return self.total_tokens / self.processed
        return 0.0

    @property
    def estimated_total_tokens(self) -> int:
        """Estimated total tokens for all files."""
        if self.processed > 0:
            return int(self.avg_tokens * self.total_files)
        return 0

    @property
    def estimated_cost(self) -> float:
        """Estimated total cost in USD (using output token pricing as approximation)."""
        # Use output pricing as conservative estimate (output tokens cost more)
        return (self.estimated_total_tokens / 1_000_000) * GEMINI_OUTPUT_COST_PER_M

    @property
    def current_cost(self) -> float:
        """Current cost so far."""
        return (self.total_tokens / 1_000_000) * GEMINI_OUTPUT_COST_PER_M


def format_duration(seconds: float) -> str:
    """Format seconds as human readable duration."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"


class ProgressDisplay:
    """
    Live progress display with status line and logging.

    Features:
    - Single updating status line (no scroll)
    - Per-file logging to file
    - Clean stage summaries
    """

    def __init__(
        self,
        log_file: Optional[Path] = None,
        verbose: bool = False,
    ):
        self.verbose = verbose
        self.log_file = log_file
        self.current_stage: Optional[StageProgress] = None
        self._last_line_len = 0
        self._is_tty = sys.stdout.isatty()

        # Always suppress noisy library loggers (unless verbose)
        if not verbose:
            self._suppress_library_loggers()

        # Suppress console logging in TTY mode for clean status line
        if self._is_tty:
            self._suppress_console_logging()

        # Setup file logger
        self.file_logger = None
        if log_file:
            self._setup_file_logger(log_file)

    def _suppress_console_logging(self):
        """Suppress console logging to keep status line clean."""
        # Get root logger and remove StreamHandlers
        root = logging.getLogger()
        for handler in root.handlers[:]:
            if isinstance(handler, logging.StreamHandler):
                if handler.stream in (sys.stdout, sys.stderr):
                    root.removeHandler(handler)

        # Suppress noisy library loggers
        self._suppress_library_loggers()

    def _suppress_library_loggers(self):
        """Suppress noisy third-party library loggers."""
        # These libraries produce verbose output we don't need
        noisy_loggers = [
            'google',               # Google SDK
            'google.genai',         # Gemini client
            'google.auth',          # Auth library
            'google.api_core',      # API core
            'google_genai',         # Gemini SDK (uses underscore!)
            'google_genai.models',  # AFC messages come from here
            'urllib3',              # HTTP library
            'httpx',                # HTTP library
            'httpcore',             # HTTP core
            'grpc',                 # gRPC
            'absl',                 # Abseil logging (used by Google libs)
        ]
        for name in noisy_loggers:
            logger = logging.getLogger(name)
            logger.setLevel(logging.ERROR)  # Only show errors
            logger.handlers = []
            logger.propagate = False

    def _setup_file_logger(self, log_file: Path):
        """Setup file-based logging."""
        log_file.parent.mkdir(parents=True, exist_ok=True)

        self.file_logger = logging.getLogger("document_processor.progress")
        self.file_logger.setLevel(logging.DEBUG)
        self.file_logger.propagate = False  # Don't propagate to root logger

        # Remove existing handlers
        self.file_logger.handlers = []

        # File handler with detailed format (includes DEBUG)
        fh = logging.FileHandler(log_file, mode='a')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter(
            '%(asctime)s | %(levelname)-7s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        self.file_logger.addHandler(fh)

        # Console handler (INFO only) when not in TTY
        if not self._is_tty:
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(logging.INFO)
            ch.setFormatter(logging.Formatter(
                '%(asctime)s | %(message)s',
                datefmt='%H:%M:%S'
            ))
            self.file_logger.addHandler(ch)

        self.file_logger.info("=" * 60)
        self.file_logger.info(f"Pipeline started at {datetime.now().isoformat()}")
        self.file_logger.info("=" * 60)

    def _log(self, level: str, message: str):
        """Log to file if available."""
        if self.file_logger:
            getattr(self.file_logger, level)(message)

    def _clear_line(self):
        """Clear the current line."""
        if self._is_tty:
            sys.stdout.write('\r' + ' ' * self._last_line_len + '\r')
            sys.stdout.flush()

    def _write_status(self, text: str):
        """Write status line (overwrites previous)."""
        if self._is_tty:
            self._clear_line()
            # Truncate to terminal width (assume 120)
            if len(text) > 120:
                text = text[:117] + "..."
            sys.stdout.write('\r' + text)
            sys.stdout.flush()
            self._last_line_len = len(text)
        # When not a TTY, skip status line updates (use logging instead)

    def _print_line(self, text: str):
        """Print a line (clears status first)."""
        if self._is_tty:
            self._clear_line()
            print(text)
            self._last_line_len = 0
        # When not a TTY, don't print (use logging instead)

    def pipeline_start(self, total_files: int, stages: list):
        """Called when pipeline starts."""
        stage_names = [s.name for s in stages]
        self._print_line(f"Pipeline: {total_files} files → {' → '.join(stage_names)}")
        self._log("info", f"Total files: {total_files}, Stages: {stage_names}")

    def stage_start(
        self,
        stage_name: str,
        total_files: int,
        to_process: int,
        completed: int = 0,
        failed: int = 0,
        blocked: int = 0,
        pending: int = 0,
    ):
        """Called when a stage starts."""
        self.current_stage = StageProgress(
            stage_name=stage_name,
            total_files=to_process,
            skipped=completed,
        )

        # Build status breakdown
        status_parts = [f"{total_files} total"]
        if completed > 0:
            status_parts.append(f"{completed} done")
        if failed > 0:
            status_parts.append(f"{failed} errors")
        if blocked > 0:
            status_parts.append(f"{blocked} blocked")
        if pending > 0:
            status_parts.append(f"{pending} pending")

        status_str = ", ".join(status_parts)

        if to_process == 0:
            self._print_line(f"\n[{stage_name}] Nothing to process ({status_str})")
        else:
            self._print_line(f"\n[{stage_name}] Processing {to_process} files ({status_str})")

        self._log("info", f"Stage '{stage_name}': {status_str}, processing {to_process}")

    def file_start(self, filename: str):
        """Called when processing a file starts."""
        if self.current_stage:
            self.current_stage.current_file = filename
            self._update_status()

        self._log("debug", f"Processing: {filename}")

    def file_complete(self, filename: str, tokens: int = 0, duration_ms: int = 0):
        """Called when a file completes successfully."""
        if self.current_stage:
            self.current_stage.processed += 1
            self.current_stage.total_tokens += tokens
            self.current_stage.current_file = None
            self._update_status()

        duration_s = duration_ms / 1000
        self._log("info", f"OK: {filename} ({tokens} tokens, {duration_s:.1f}s)")

    def file_error(self, filename: str, error: str, retryable: bool = True):
        """Called when a file fails."""
        if self.current_stage:
            self.current_stage.errors += 1
            self.current_stage.current_file = None
            self._update_status()

        retry_tag = "[retryable]" if retryable else "[permanent]"
        self._log("error", f"FAIL {retry_tag}: {filename} - {error}")

        if self.verbose:
            self._print_line(f"  ✗ {filename}: {error[:60]}")

    def file_skip(self, filename: str, reason: str):
        """Called when a file is skipped."""
        self._log("debug", f"SKIP: {filename} - {reason}")

    def _update_status(self):
        """Update the status line with current progress."""
        if not self.current_stage:
            return

        s = self.current_stage

        # Build status line components
        parts = []

        # Stage and progress
        parts.append(f"[{s.stage_name}]")
        parts.append(f"{s.done}/{s.total_files}")
        parts.append(f"({s.percent:.0f}%)")

        # Rate
        if s.rate > 0:
            parts.append(f"{s.rate:.1f}/s")

        # ETA
        if s.eta_seconds and s.eta_seconds > 0:
            parts.append(f"ETA: {format_duration(s.eta_seconds)}")

        # Cost estimate (show after a few files processed)
        if s.processed >= 3 and s.estimated_cost > 0:
            parts.append(f"~${s.estimated_cost:.2f}")

        # Errors
        if s.errors > 0:
            parts.append(f"err:{s.errors}")

        # Current file (truncated)
        if s.current_file:
            max_len = 30
            fname = s.current_file
            if len(fname) > max_len:
                fname = "..." + fname[-(max_len-3):]
            parts.append(f"→ {fname}")

        status = " | ".join(parts)
        self._write_status(status)

    def qc_result(self, passed: bool, filename: str, reason: str = ""):
        """Called after QC check."""
        result = "PASS" if passed else "FAIL"
        self._log("info", f"QC {result}: {filename} - {reason}")

        if not passed:
            self._print_line(f"  QC FAIL: {filename} - {reason[:60]}")

    def qc_halt(self, failure_rate: float, threshold: float):
        """Called when QC triggers halt."""
        self._print_line("")
        self._print_line("=" * 60)
        self._print_line(f"QC HALT: Failure rate {failure_rate*100:.1f}% > threshold {threshold*100:.0f}%")
        self._print_line("=" * 60)
        self._log("error", f"QC HALT: {failure_rate*100:.1f}% failure rate")

    def stage_complete(self, stats: dict):
        """Called when a stage completes."""
        self._clear_line()  # Clear status line

        s = self.current_stage
        if not s:
            return

        elapsed = s.elapsed
        rate = s.rate

        # Build completion message
        msg_parts = [f"{s.processed} OK", f"{s.errors} errors", format_duration(elapsed)]
        if rate > 0:
            msg_parts.append(f"{rate:.1f}/s")
        if s.total_tokens > 0:
            msg_parts.append(f"{s.total_tokens:,} tokens")
            msg_parts.append(f"${s.current_cost:.2f}")

        self._print_line(f"[{s.stage_name}] Complete: {' | '.join(msg_parts)}")

        self._log("info", f"Stage '{s.stage_name}' complete: "
                  f"processed={s.processed}, errors={s.errors}, "
                  f"tokens={s.total_tokens}, cost=${s.current_cost:.2f}, "
                  f"elapsed={format_duration(elapsed)}, rate={rate:.1f}/s")

        self.current_stage = None

    def pipeline_complete(self, results: dict):
        """Called when pipeline completes."""
        self._print_line("")
        self._print_line("Pipeline complete.")
        self._log("info", "Pipeline complete")

        if self.file_logger:
            self.file_logger.info("=" * 60)
