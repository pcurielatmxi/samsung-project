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
from .clients.gemini_client import process_document_text, GeminiResponse, _get_client
from .stages.llm_stage import extract_docx_text, extract_xlsx_text

# Document extensions that need special handling for QC
PDF_EXTENSIONS = {'.pdf'}
DOCX_EXTENSIONS = {'.docx', '.doc'}
XLSX_EXTENSIONS = {'.xlsx', '.xls'}
DOCUMENT_EXTENSIONS = PDF_EXTENSIONS | DOCX_EXTENSIONS | XLSX_EXTENSIONS


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
    Write QC halt file with detailed failure information.

    Args:
        output_dir: Output directory
        tracker: QC tracker with failure data
        threshold: Failure threshold that was exceeded

    Returns:
        Path to halt file
    """
    halt_path = get_qc_halt_path(output_dir)

    # Collect all failure reasons for summary
    failure_reasons = [r.reason for r in tracker.failed_files]

    # Build issue summary - consolidate similar issues
    issue_summary = _build_issue_summary(failure_reasons)

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
            f"Review the issues below and fix the prompt before re-running."
        ),
        "issue_summary": issue_summary,
        "recommendations": _generate_recommendations(issue_summary),
        "failed_files": [
            {
                "input": str(r.input_path),
                "output": str(r.output_path),
                "verdict": r.verdict,
                "reason": r.reason,
            }
            for r in tracker.failed_files
        ],
        "next_steps": [
            "1. Review the issue_summary and recommendations above",
            "2. Examine the failed_files to see specific examples",
            "3. Update your extraction/format prompt to address the issues",
            "4. Delete this .qc_halt.json file",
            "5. Re-run the pipeline (or use --bypass-qc-halt to continue anyway)",
        ],
    }

    halt_path.parent.mkdir(parents=True, exist_ok=True)
    with open(halt_path, "w", encoding="utf-8") as f:
        json.dump(halt_data, f, indent=2)

    return halt_path


def _build_issue_summary(failure_reasons: List[str]) -> List[str]:
    """
    Build a summary of issues from failure reasons.

    Groups similar issues and returns a deduplicated list.
    """
    if not failure_reasons:
        return []

    # For now, return unique reasons (truncated for readability)
    # In the future, could use LLM to cluster/summarize
    unique_issues = []
    seen_prefixes = set()

    for reason in failure_reasons:
        # Use first 100 chars as a rough dedup key
        prefix = reason[:100].lower().strip()
        if prefix not in seen_prefixes:
            seen_prefixes.add(prefix)
            # Truncate long reasons
            if len(reason) > 500:
                reason = reason[:500] + "..."
            unique_issues.append(reason)

    return unique_issues


def _generate_recommendations(issues: List[str]) -> List[str]:
    """
    Generate recommendations based on common issue patterns.
    """
    recommendations = []
    issues_text = " ".join(issues).lower()

    # Check for common patterns and suggest fixes
    if "missing" in issues_text or "not found" in issues_text or "empty" in issues_text:
        recommendations.append(
            "MISSING DATA: Some fields are empty. Check if the prompt clearly instructs "
            "the model to extract these fields. Consider adding examples."
        )

    if "incorrect" in issues_text or "wrong" in issues_text or "mismatch" in issues_text:
        recommendations.append(
            "INCORRECT VALUES: Data doesn't match source. Check if field definitions "
            "are clear and unambiguous. The model may be confusing similar fields."
        )

    if "hallucin" in issues_text or "invented" in issues_text or "not in source" in issues_text:
        recommendations.append(
            "HALLUCINATIONS: Model is generating data not in the source. Add explicit "
            "instruction: 'Only extract information explicitly stated in the document. "
            "Use null for missing fields.'"
        )

    if "format" in issues_text or "type" in issues_text or "invalid" in issues_text:
        recommendations.append(
            "FORMAT ERRORS: Output structure is wrong. Check schema definition and ensure "
            "the prompt shows expected output format with examples."
        )

    if "truncat" in issues_text or "incomplete" in issues_text or "cut off" in issues_text:
        recommendations.append(
            "TRUNCATION: Output is being cut off. The document may be too large or "
            "the expected output too verbose. Consider simplifying the schema."
        )

    if not recommendations:
        recommendations.append(
            "Review the specific failure reasons in failed_files to identify patterns. "
            "Consider adding more explicit instructions or examples to the prompt."
        )

    return recommendations


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
    model: str = "gemini-2.0-flash",
) -> QCResult:
    """
    Run quality check on a processed file.

    For PDF inputs (stage 1): Uploads PDF to Gemini for full comparison.
    For JSON inputs (later stages): Uses text-based comparison.

    Args:
        stage: Stage configuration (must have qc_prompt)
        input_path: Path to input file (PDF or JSON from prior stage)
        output_path: Path to output file (JSON)

    Returns:
        QCResult with pass/fail verdict and reason
    """
    if not stage.qc_prompt:
        raise ValueError(f"Stage '{stage.name}' has no QC prompt configured")

    # Read output content (always JSON)
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

    # Check input file type
    ext = input_path.suffix.lower()

    if ext in PDF_EXTENSIONS:
        # PDF: Upload to Gemini for visual comparison
        return await _run_qc_with_pdf(
            stage=stage,
            pdf_path=input_path,
            output_content=output_content,
            output_path=output_path,
            model=model,
        )
    elif ext in DOCX_EXTENSIONS or ext in XLSX_EXTENSIONS:
        # DOCX/XLSX: Extract text and compare
        return await _run_qc_with_document(
            stage=stage,
            doc_path=input_path,
            output_content=output_content,
            output_path=output_path,
            model=model,
        )
    else:
        # JSON from prior stage: Text-based comparison
        return await _run_qc_with_text(
            stage=stage,
            input_path=input_path,
            output_content=output_content,
            output_path=output_path,
            model=model,
        )


async def _run_qc_with_pdf(
    stage: StageConfig,
    pdf_path: Path,
    output_content: str,
    output_path: Path,
    model: str,
) -> QCResult:
    """
    Run QC by uploading PDF to Gemini for comparison with output.

    This allows Gemini to see the actual source document content.
    """
    try:
        client = _get_client()
    except Exception as e:
        return QCResult(
            passed=False,
            verdict="FAIL",
            reason=f"Failed to initialize Gemini client: {e}",
            input_path=pdf_path,
            output_path=output_path,
        )

    try:
        # Upload the PDF file
        uploaded_file = client.files.upload(file=pdf_path)

        # Build QC prompt with output content
        # The PDF is provided as a file, so we only embed the output in the prompt
        qc_prompt = stage.qc_prompt.format(
            input_content="[See attached PDF document]",
            output_content=output_content,
        )

        # Generate content with PDF + prompt
        response = client.models.generate_content(
            model=model,
            contents=[
                uploaded_file,
                qc_prompt,
            ],
        )

        # Parse verdict from response
        verdict, reason = _parse_qc_response(response.text)

        return QCResult(
            passed=(verdict == "PASS"),
            verdict=verdict,
            reason=reason,
            input_path=pdf_path,
            output_path=output_path,
        )

    except Exception as e:
        return QCResult(
            passed=False,
            verdict="FAIL",
            reason=f"QC with PDF failed: {e}",
            input_path=pdf_path,
            output_path=output_path,
        )


async def _run_qc_with_document(
    stage: StageConfig,
    doc_path: Path,
    output_content: str,
    output_path: Path,
    model: str,
) -> QCResult:
    """
    Run QC by extracting text from DOCX/XLSX and comparing with output.
    """
    ext = doc_path.suffix.lower()

    # Extract text from document
    try:
        if ext in DOCX_EXTENSIONS:
            input_content = extract_docx_text(doc_path)
        elif ext in XLSX_EXTENSIONS:
            input_content = extract_xlsx_text(doc_path)
        else:
            return QCResult(
                passed=False,
                verdict="FAIL",
                reason=f"Unsupported document type: {ext}",
                input_path=doc_path,
                output_path=output_path,
            )

        if not input_content.strip():
            return QCResult(
                passed=False,
                verdict="FAIL",
                reason=f"No text content extracted from {doc_path.name}",
                input_path=doc_path,
                output_path=output_path,
            )

    except Exception as e:
        return QCResult(
            passed=False,
            verdict="FAIL",
            reason=f"Failed to extract text from document: {e}",
            input_path=doc_path,
            output_path=output_path,
        )

    # Truncate input if too long (to avoid token limits)
    max_input_chars = 50000
    if len(input_content) > max_input_chars:
        input_content = input_content[:max_input_chars] + "\n\n[... content truncated for QC ...]"

    # Build QC prompt with extracted text and output
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
            input_path=doc_path,
            output_path=output_path,
        )

    # Parse verdict from response
    verdict, reason = _parse_qc_response(response.result)

    return QCResult(
        passed=(verdict == "PASS"),
        verdict=verdict,
        reason=reason,
        input_path=doc_path,
        output_path=output_path,
    )


async def _run_qc_with_text(
    stage: StageConfig,
    input_path: Path,
    output_content: str,
    output_path: Path,
    model: str,
) -> QCResult:
    """
    Run QC using text-based comparison (for JSON inputs from prior stages).
    """
    # Read input content from prior stage JSON
    try:
        with open(input_path, "r", encoding="utf-8") as f:
            input_data = json.load(f)
        input_content = json.dumps(input_data.get("content", input_data), indent=2)
    except Exception as e:
        return QCResult(
            passed=False,
            verdict="FAIL",
            reason=f"Error reading input file: {e}",
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
