"""
Quality checking system for document processing pipeline.

Samples processed files and uses LLM to verify output quality.
Tracks failure rates and halts pipeline if threshold exceeded.
"""

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from .config import StageConfig, PipelineConfig
from .clients.gemini_client import process_document_text, GeminiResponse


@dataclass
class QCResult:
    """Result of a quality check."""
    passed: bool
    verdict: str  # "PASS" or "FAIL"
    reason: str
    input_path: Path
    output_path: Path


@dataclass
class QCTracker:
    """Tracks quality check results for a stage."""
    stage_name: str
    total_samples: int = 0
    failures: int = 0
    failed_files: List[QCResult] = field(default_factory=list)

    @property
    def failure_rate(self) -> float:
        """Calculate failure rate."""
        if self.total_samples == 0:
            return 0.0
        return self.failures / self.total_samples

    def should_halt(self, threshold: float, min_samples: int) -> bool:
        """Check if failure rate exceeds threshold after minimum samples."""
        return self.total_samples >= min_samples and self.failure_rate > threshold

    def add_result(self, result: QCResult) -> None:
        """Add a QC result to the tracker."""
        self.total_samples += 1
        if not result.passed:
            self.failures += 1
            self.failed_files.append(result)


QC_HALT_FILENAME = ".qc_halt.json"


def get_qc_halt_path(output_dir: Path) -> Path:
    """Get path to QC halt file."""
    return output_dir / QC_HALT_FILENAME


def check_qc_halt(output_dir: Path) -> Optional[dict]:
    """
    Check if QC halt file exists.

    Returns:
        QC halt data if file exists, None otherwise
    """
    halt_path = get_qc_halt_path(output_dir)
    if halt_path.exists():
        try:
            with open(halt_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {"error": "Failed to read halt file"}
    return None


def write_qc_halt(
    output_dir: Path,
    tracker: QCTracker,
    threshold: float,
) -> Path:
    """
    Write QC halt file.

    Args:
        output_dir: Output directory
        tracker: QC tracker with failure data
        threshold: Failure threshold that was exceeded

    Returns:
        Path to halt file
    """
    halt_path = get_qc_halt_path(output_dir)

    halt_data = {
        "stage": tracker.stage_name,
        "failure_rate": tracker.failure_rate,
        "threshold": threshold,
        "total_samples": tracker.total_samples,
        "failures": tracker.failures,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message": (
            f"QC failure rate ({tracker.failure_rate*100:.1f}%) exceeded threshold "
            f"({threshold*100:.0f}%) for stage '{tracker.stage_name}'. "
            f"Review the prompt and fix issues before re-running. "
            f"Use --bypass-qc-halt to continue despite failures."
        ),
        "failed_files": [
            {
                "input": str(r.input_path),
                "output": str(r.output_path),
                "verdict": r.verdict,
                "reason": r.reason,
            }
            for r in tracker.failed_files
        ],
    }

    halt_path.parent.mkdir(parents=True, exist_ok=True)
    with open(halt_path, "w", encoding="utf-8") as f:
        json.dump(halt_data, f, indent=2)

    return halt_path


def clear_qc_halt(output_dir: Path) -> bool:
    """
    Remove QC halt file.

    Returns:
        True if file was removed, False if it didn't exist
    """
    halt_path = get_qc_halt_path(output_dir)
    if halt_path.exists():
        halt_path.unlink()
        return True
    return False


async def run_quality_check(
    stage: StageConfig,
    input_path: Path,
    output_path: Path,
    model: str = "gemini-2.5-flash-preview-05-20",
) -> QCResult:
    """
    Run quality check on a processed file.

    Args:
        stage: Stage configuration (must have qc_prompt)
        input_path: Path to input file
        output_path: Path to output file

    Returns:
        QCResult with pass/fail verdict and reason
    """
    if not stage.qc_prompt:
        raise ValueError(f"Stage '{stage.name}' has no QC prompt configured")

    # Read input content
    try:
        if input_path.suffix.lower() == ".pdf":
            # For PDFs, we can't easily read the content for comparison
            # Just note that it's a PDF
            input_content = f"[PDF document: {input_path.name}]"
        else:
            with open(input_path, "r", encoding="utf-8") as f:
                input_data = json.load(f)
            input_content = json.dumps(input_data.get("content", input_data), indent=2)
    except Exception as e:
        input_content = f"[Error reading input: {e}]"

    # Read output content
    try:
        with open(output_path, "r", encoding="utf-8") as f:
            output_data = json.load(f)
        output_content = json.dumps(output_data.get("content", output_data), indent=2)
    except Exception as e:
        return QCResult(
            passed=False,
            verdict="FAIL",
            reason=f"Error reading output file: {e}",
            input_path=input_path,
            output_path=output_path,
        )

    # Build QC prompt with input and output
    qc_prompt = stage.qc_prompt.format(
        input_content=input_content,
        output_content=output_content,
    )

    # Run QC with LLM
    response = process_document_text(
        text="",  # Content is in the prompt
        prompt=qc_prompt,
        model=model,
    )

    if not response.success:
        return QCResult(
            passed=False,
            verdict="FAIL",
            reason=f"QC LLM call failed: {response.error}",
            input_path=input_path,
            output_path=output_path,
        )

    # Parse verdict from response
    verdict, reason = _parse_qc_response(response.result)

    return QCResult(
        passed=(verdict == "PASS"),
        verdict=verdict,
        reason=reason,
        input_path=input_path,
        output_path=output_path,
    )


def _parse_qc_response(response_text: str) -> tuple[str, str]:
    """
    Parse QC response to extract verdict and reason.

    Expected format:
        VERDICT: PASS|FAIL
        REASON: <explanation>
    """
    if not response_text:
        return "FAIL", "Empty response from QC"

    # Look for VERDICT line
    verdict_match = re.search(r'VERDICT:\s*(PASS|FAIL)', response_text, re.IGNORECASE)
    if verdict_match:
        verdict = verdict_match.group(1).upper()
    else:
        # Try to infer from content
        text_lower = response_text.lower()
        if "pass" in text_lower and "fail" not in text_lower:
            verdict = "PASS"
        elif "fail" in text_lower:
            verdict = "FAIL"
        else:
            verdict = "FAIL"  # Default to fail if unclear

    # Look for REASON line
    reason_match = re.search(r'REASON:\s*(.+?)(?:\n|$)', response_text, re.IGNORECASE | re.DOTALL)
    if reason_match:
        reason = reason_match.group(1).strip()
    else:
        # Use entire response as reason
        reason = response_text.strip()[:500]

    return verdict, reason
