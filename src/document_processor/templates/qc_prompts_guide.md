# Quality Check and Enhancement Prompt Guidelines

## Purpose

This guide covers two related prompt types:
1. **QC prompts** - Verify pipeline output quality by sampling (low cost, catches issues early)
2. **Enhancement prompts** - Improve output quality by running a second pass (higher cost, better results)

## QC Prompts

QC prompts verify that pipeline stages produce accurate, complete output. The QC system samples processed files and uses an LLM to compare input vs output, catching problems early before wasting API credits on bad prompts.

## When QC Runs

- After each batch of `qc_batch_size` files (default: 50), 1 file is sampled for QC
- If failure rate exceeds `qc_failure_threshold` (default: 10%) after `qc_min_samples` (default: 10), pipeline halts
- QC adds ~1 API call per batch, so cost is minimal compared to catching bad prompts early

## QC Prompt Structure

QC prompts must return a verdict in this format:
```
VERDICT: PASS
REASON: <brief explanation>
```
or
```
VERDICT: FAIL
REASON: <what went wrong>
```

## Stage 1: Extract (PDF Input)

For extraction stages, the source PDF is uploaded to Gemini. The QC prompt should:
- Reference "the attached PDF" for the source document
- Focus on **accuracy** (is the extracted data correct?) and **completeness** (was key data missed?)
- NOT focus on formatting (that's stage 2's job)

### Template Variables
- `{output_content}` - The JSON extraction output
- `{input_content}` - Set to "[See attached PDF document]" (PDF is attached separately)

### Sample Extract QC Prompt

```
You are a quality checker for document extraction. Your job is to verify that the extraction output accurately captures the key information from the source document.

## SOURCE DOCUMENT
The attached PDF is the source document that was processed.

## EXTRACTION OUTPUT
{output_content}

## VERIFICATION TASK

Compare the extraction output against the attached PDF. Check for:

1. **Accuracy**: Is the extracted information correct?
   - Dates, numbers, and identifiers match the source
   - Names and descriptions are spelled correctly
   - No information was misread or misinterpreted

2. **Completeness**: Was key information captured?
   - Main data points are present
   - Critical fields are not empty when the source has data
   - (Minor omissions of non-essential details are acceptable)

3. **No Hallucinations**: Is all extracted data actually in the source?
   - No invented or assumed information
   - No data from other documents mixed in

## VERDICT

If the extraction is accurate and reasonably complete:
VERDICT: PASS
REASON: <brief confirmation of what was verified>

If there are significant errors, missing critical data, or hallucinations:
VERDICT: FAIL
REASON: <specific issues found>
```

## Stage 2: Format (JSON Input)

For formatting stages, both input and output are JSON. The QC prompt should:
- Compare input data to output schema
- Focus on **correct mapping** and **data preservation**
- Verify schema compliance

### Template Variables
- `{input_content}` - JSON from prior stage (extraction output)
- `{output_content}` - JSON formatted output

### Sample Format QC Prompt

```
You are a quality checker for data formatting. Your job is to verify that the formatted output correctly represents the input data.

## INPUT (from extraction stage)
{input_content}

## FORMATTED OUTPUT
{output_content}

## VERIFICATION TASK

Compare the formatted output against the input. Check for:

1. **Data Preservation**: Is all important data retained?
   - Key fields from input appear in output
   - Values were not accidentally dropped
   - Arrays/lists maintain their items

2. **Correct Mapping**: Are values in the right fields?
   - Dates are in date fields
   - Numbers are in numeric fields
   - Text is properly categorized

3. **Valid Structure**: Is the output well-formed?
   - Required fields are present
   - Data types are appropriate
   - No malformed or truncated values

4. **No Corruption**: Was data modified incorrectly?
   - Values match between input and output
   - No unintended transformations
   - No mixing of data between records

## VERDICT

If the formatting correctly transforms the input:
VERDICT: PASS
REASON: <brief confirmation>

If there are mapping errors, data loss, or corruption:
VERDICT: FAIL
REASON: <specific issues found>
```

## Configuration

To enable QC for a stage, add `qc_prompt_file` to the stage config:

```json
{
  "stages": [
    {
      "name": "extract",
      "type": "llm",
      "model": "gemini-3-flash-preview",
      "prompt_file": "extract_prompt.txt",
      "qc_prompt_file": "extract_qc_prompt.txt"
    },
    {
      "name": "format",
      "type": "llm",
      "model": "gemini-3-flash-preview",
      "prompt_file": "format_prompt.txt",
      "schema_file": "schema.json",
      "qc_prompt_file": "format_qc_prompt.txt"
    }
  ]
}
```

## Tips for Writing QC Prompts

1. **Be specific to your domain** - Reference the actual fields and data types in your schema
2. **Prioritize critical fields** - Focus QC on must-have data, not nice-to-have
3. **Set realistic expectations** - Minor formatting differences shouldn't fail QC
4. **Provide examples** - If certain patterns are acceptable/unacceptable, show them
5. **Keep it concise** - QC runs frequently; shorter prompts = lower cost

## Debugging QC Failures

When QC halts the pipeline, check `.qc_halt.json` in the output directory:
- `failed_files` lists which files failed with reasons
- Review the reasons to identify if it's a prompt problem or data problem
- Fix the extraction/format prompt and delete `.qc_halt.json` to retry
- Use `--bypass-qc-halt` only if you've reviewed failures and they're acceptable

---

## Enhancement Prompts

Enhancement prompts run a second LLM pass to review and correct the initial output. This is useful when:
- High accuracy is critical
- The extraction prompt produces occasional errors
- You want to catch and fix issues automatically

### When Enhancement Runs

- Only when `--enhance` flag is passed to the CLI
- Only for LLM stages with `enhance_prompt_file` configured
- Runs after the initial extraction, using the same schema
- Roughly doubles token cost per file

### Configuration

Add `enhance_prompt_file` to LLM stage config:

```json
{
  "stages": [
    {
      "name": "extract",
      "type": "llm",
      "model": "gemini-3-flash-preview",
      "prompt_file": "extract_prompt.txt",
      "enhance_prompt_file": "enhance_prompt.txt"
    }
  ]
}
```

### Template Variables

- `{output_content}` - The initial extraction output (JSON string)

### Sample Enhancement Prompt

```
You are a quality reviewer for document extraction. Your task is to review and correct the extraction output below.

## EXTRACTION OUTPUT TO REVIEW
{output_content}

## REVIEW TASK

Carefully review the extraction output and fix any issues you find:

1. **Accuracy**: Correct any misread values, dates, numbers, or identifiers
2. **Completeness**: Fill in any obviously missing fields that can be inferred from context
3. **Consistency**: Ensure data types are correct (dates are dates, numbers are numbers)
4. **Formatting**: Fix any formatting issues (extra whitespace, inconsistent casing, etc.)
5. **No Hallucinations**: Do NOT add information that isn't clearly implied by the existing data

## OUTPUT

Return the corrected extraction in the same format as the input. If no corrections are needed, return the input unchanged.
```

### Tips for Enhancement Prompts

1. **Be conservative** - Enhancement should fix errors, not add information
2. **Preserve structure** - Output format must match input format
3. **Focus on common errors** - Target the types of mistakes your extraction prompt makes
4. **Don't over-correct** - When in doubt, preserve the original value
5. **Cost consideration** - Enhancement roughly doubles cost; use when quality justifies it

### QC vs Enhancement

| Aspect | QC | Enhancement |
|--------|-----|-------------|
| Purpose | Detect problems | Fix problems |
| Runs on | Sampled files (~1/batch) | Every file (when enabled) |
| Cost | Low (~2% overhead) | High (~100% overhead) |
| Output | Pass/Fail verdict | Corrected data |
| Use when | Developing prompts | Production with high accuracy needs |

### Recommended Workflow

1. Start with extraction prompt only
2. Add QC prompt to catch issues during development
3. Iterate on extraction prompt until QC pass rate is acceptable
4. Add enhancement prompt for production runs where accuracy is critical
5. Use `--enhance` flag only when needed to control costs
