# Document Processor

N-stage document processing pipeline with LLM extraction, schema formatting, and quality checking.

## Purpose

Batch process unstructured documents (PDFs) into structured JSON through configurable pipeline stages. Designed for high-volume processing with:
- Automatic retry of failures
- Quality checking to catch bad prompts early
- Cost tracking and progress reporting
- Idempotent operation (safe to re-run)

## Architecture

```
src/document_processor/
├── cli.py                 # CLI entry point
├── pipeline.py            # Pipeline orchestration
├── config.py              # Configuration loading
├── quality_check.py       # QC sampling and halt logic
├── stages/
│   ├── base.py            # Abstract stage interface
│   ├── llm_stage.py       # LLM processing (Gemini)
│   ├── script_stage.py    # Python script processing
│   └── registry.py        # Stage type factory
├── clients/
│   └── gemini_client.py   # Gemini API client (PDF upload, text, structured output)
├── utils/
│   ├── file_utils.py      # JSON I/O, atomic writes
│   ├── progress.py        # Live progress display, logging, cost tracking
│   └── status.py          # Pipeline status analysis
└── templates/
    ├── qc_prompts_guide.md    # QC prompt writing guide
    ├── extract_qc_prompt.txt  # Template for extraction QC
    └── format_qc_prompt.txt   # Template for formatting QC
```

## Stage Types

### `llm` - LLM Processing
Sends documents to Gemini for extraction or formatting.

```json
{
  "name": "extract",
  "type": "llm",
  "model": "gemini-3-flash-preview",
  "prompt_file": "extract_prompt.txt",
  "schema_file": "schema.json",        // Optional: structured output
  "qc_prompt_file": "extract_qc.txt"   // Optional: enables QC
}
```

- Stage 1: Uploads PDF to Gemini
- Later stages: Sends JSON text from prior stage
- If `schema_file` provided: Returns validated JSON matching schema

### `script` - Python Script
Runs a Python function for deterministic transformations.

```json
{
  "name": "clean",
  "type": "script",
  "script": "postprocess.py",
  "function": "process_record"
}
```

- Script loaded from config directory
- Function signature: `process_record(data: dict, source_path: Path) -> dict`
- No QC (deterministic)

## Pipeline Flow

```
Input PDFs → Stage 1 (extract) → Stage 2 (format) → Stage 3 (clean) → Output JSON
     ↓              ↓                   ↓                  ↓
  raw/*.pdf    1.extract/*.json    2.format/*.json    3.clean/*.json
```

### File Status per Stage
- **completed**: `{stem}.{stage}.json` exists
- **failed**: `{stem}.{stage}.error.json` exists (will retry by default)
- **blocked**: Prior stage not completed
- **pending**: Ready to process

## Configuration

Pipeline configs live in `scripts/{source}/document_processing/`:

```
scripts/psi/document_processing/
├── config.json           # Pipeline configuration
├── extract_prompt.txt    # Stage 1 prompt
├── format_prompt.txt     # Stage 2 prompt
├── schema.json           # Output schema for format stage
├── postprocess.py        # Stage 3 script
└── extract_qc_prompt.txt # Optional QC prompt
```

### config.json Structure

```json
{
  "input_dir": "${WINDOWS_DATA_DIR}/raw/psi/reports",
  "output_dir": "${WINDOWS_DATA_DIR}/processed/psi",
  "file_extensions": [".pdf"],
  "concurrency": 5,

  "qc_batch_size": 50,
  "qc_failure_threshold": 0.10,
  "qc_min_samples": 10,

  "stages": [
    {
      "name": "extract",
      "type": "llm",
      "model": "gemini-3-flash-preview",
      "prompt_file": "extract_prompt.txt"
    },
    {
      "name": "format",
      "type": "llm",
      "model": "gemini-3-flash-preview",
      "prompt_file": "format_prompt.txt",
      "schema_file": "schema.json"
    },
    {
      "name": "clean",
      "type": "script",
      "script": "postprocess.py",
      "function": "process_record"
    }
  ]
}
```

## CLI Usage

```bash
# Run all stages
python -m src.document_processor scripts/psi/document_processing/

# Run specific stage
python -m src.document_processor scripts/psi/document_processing/ --stage extract

# Common flags
--force              # Reprocess completed files
--retry-errors       # Retry only failed files
--limit N            # Process N files max per stage
--dry-run            # Preview without processing
--verbose            # Show detailed output

# QC flags
--bypass-qc-halt     # Continue despite QC halt
--disable-qc         # Skip quality checks

# Enhancement flags
--enhance            # Enable enhancement pass for LLM stages

# Status
--status             # Show pipeline status
--status --errors    # Show error details
--clear-halt         # Remove QC halt file
```

## Enhancement Mode

Enhancement runs a second LLM pass to review and correct extraction output.

### Configuration

Add `enhance_prompt_file` to LLM stage config:

```json
{
  "name": "extract",
  "type": "llm",
  "model": "gemini-3-flash-preview",
  "prompt_file": "extract_prompt.txt",
  "enhance_prompt_file": "enhance_prompt.txt"
}
```

### Usage

```bash
# Run with enhancement enabled
python -m src.document_processor scripts/psi/document_processing/ --enhance
```

### How It Works

1. Initial extraction runs as normal
2. If `--enhance` flag is set and stage has `enhance_prompt_file`:
   - Initial output is passed to enhancement prompt
   - Enhancement prompt reviews and corrects the output
   - Corrected output replaces initial output
3. Token usage includes both passes (~2x cost)

### When to Use

- **Development**: Use QC to identify issues, iterate on extraction prompt
- **Production**: Add `--enhance` when accuracy is critical
- **Cost control**: Only use `--enhance` when quality justifies ~2x cost

## Quality Checking

QC samples processed files to catch bad prompts before wasting API credits.

### How It Works
1. After every `qc_batch_size` files (default: 50), sample 1 file for QC
2. QC prompt compares input (PDF or prior JSON) with output
3. LLM returns PASS/FAIL verdict with reason
4. If failure rate > `qc_failure_threshold` after `qc_min_samples`, halt pipeline

### QC for PDF Inputs (Stage 1)
- Uploads source PDF to Gemini
- QC can actually see and verify against source document

### QC for JSON Inputs (Later Stages)
- Compares prior stage JSON with current stage output
- Text-based comparison

### QC Halt File
When QC halts, `.qc_halt.json` is created with:
- `issue_summary`: Deduplicated failure reasons
- `recommendations`: Auto-generated fix suggestions
- `failed_files`: Specific files that failed with reasons
- `next_steps`: Instructions for fixing

## Output Format

Each stage writes JSON with metadata:

```json
{
  "metadata": {
    "source_file": "/path/to/input.pdf",
    "processed_at": "2026-01-07T12:00:00Z",
    "stage": "extract",
    "model": "gemini-3-flash-preview",
    "usage": {
      "prompt_tokens": 1500,
      "output_tokens": 400,
      "total_tokens": 1900
    }
  },
  "content": {
    // Extracted/formatted data
  }
}
```

Error files (`*.error.json`):
```json
{
  "source_file": "/path/to/input.pdf",
  "stage": "extract",
  "error": "Error message",
  "retryable": true,
  "timestamp": "2026-01-07T12:00:00Z"
}
```

## Progress Display

Real-time progress with detailed per-file logging:

```
Starting pipeline: document_processing
  Input:  /path/to/raw/psi/reports
  Output: /path/to/processed/psi
  Stages: ['extract', 'format', 'clean']
Scanning files...

[extract] Processing 100 files (150 total, 50 done)
OK | file: DOC001 | stage: extract | time: 2.5s | tokens: 1500 in, 400 out | cost: $0.0003 | progress: 1/100 (1%) | elapsed: 2s | ETA: 4m 10s | est_total: $0.0300
OK | file: DOC002 | stage: extract | time: 3.1s | tokens: 1800 in, 450 out | cost: $0.0004 | progress: 2/100 (2%) | elapsed: 5s | ETA: 4m 8s | est_total: $0.0350
...
STAGE COMPLETE | stage: extract | processed: 100 OK, 2 errors | time: 5m 30s (0.3/s) | tokens: 180,000 in, 45,000 out | total_cost: $0.0360
```

## Environment Variables

Required in `.env`:
```
GEMINI_API_KEY=your_api_key
WINDOWS_DATA_DIR=/path/to/data
```

## Key Design Decisions

1. **Implicit stage chaining**: Stage N input = Stage N-1 output (no explicit `input_stage`)
2. **Retry by default**: Failed files are retried on next run
3. **Error file cleanup**: Successful retry removes `.error.json`
4. **QC samples, not validates all**: 1 file per batch keeps costs low
5. **PDF upload for QC**: Stage 1 QC actually sees source document
6. **Halt file blocks reruns**: Must acknowledge QC failures before continuing

## Adding a New Pipeline

1. Create config directory: `scripts/{source}/document_processing/`
2. Create `config.json` with stages
3. Write prompts for each LLM stage
4. Optionally add schema for structured output
5. Optionally add QC prompts (recommended for stage 1)
6. Run with `--dry-run` to verify setup
7. Run with `--limit 10` to test on sample

## Troubleshooting

### High Error Rate
- Check error files in output directory
- Run with `--verbose` for detailed output
- Review prompt for ambiguity

### QC Halt
- Read `.qc_halt.json` for issue summary and recommendations
- Fix prompt based on failure patterns
- Delete halt file and retry

### Slow Processing
- Increase `concurrency` (default: 5)
- Large PDFs take longer to upload
- Check rate limits in Gemini console

### Schema Validation Errors
- Gemini schema format differs from JSON Schema
- Use uppercase types: `STRING`, `INTEGER`, `BOOLEAN`, `ARRAY`, `OBJECT`
- Use `nullable: true` instead of `["string", "null"]`

## Schema Design Guidelines

### Enum Fields: Always Include an Escape Hatch

**Lesson Learned (2026-01-22):** Strict enum fields can hide edge cases by forcing the LLM to pick the "closest" option even when none truly fit. This masks data quality issues that only surface during analysis.

**Problem Example:**
```json
// Original schema - no escape hatch
"outcome": {
  "enum": ["PASS", "FAIL", "PARTIAL"]
}
```
When processing ~15K quality reports, the LLM was forced to classify trip charges and observation reports as FAIL or PARTIAL, hiding ~40% of records that didn't fit these categories.

**Solution: Add OTHER + raw text field:**
```json
// Better schema - captures edge cases
"outcome": {
  "enum": ["PASS", "FAIL", "PARTIAL", "CANCELLED", "MEASUREMENT", "OTHER"]
},
"outcome_raw": {
  "type": ["string", "null"],
  "description": "Original text when OTHER selected or outcome uncertain"
}
```

**Guidelines:**
1. **Always include OTHER** in enum fields for classification/categorization
2. **Add a companion `_raw` field** to capture original text when OTHER is used
3. **Document OTHER in the prompt** with clear guidance on when to use it
4. **Review OTHER records periodically** to discover new categories that should be added
5. **Prefer free-form strings** over enums when categories aren't well-defined upfront

**Prompt Pattern for OTHER:**
```
OTHER:
  - Use when document doesn't clearly fit any of the above categories
  - ALWAYS populate {field}_raw with the actual text from the document
  - This captures edge cases for later review rather than forcing wrong categories
```

**Trade-offs:**
| Approach | Pros | Cons |
|----------|------|------|
| Strict enum | Clean data, easy analytics | Hides edge cases, forces misclassification |
| Enum + OTHER | Captures unknowns, discoverable | Requires review of OTHER records |
| Free-form string | Maximum flexibility | Harder to analyze, inconsistent values |

For initial extraction where you're still learning the data, prefer **Enum + OTHER**. Once categories stabilize, you can tighten the schema.
