# Quality Check Prompt Guidelines

## Purpose

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
      "model": "gemini-2.0-flash",
      "prompt_file": "extract_prompt.txt",
      "qc_prompt_file": "extract_qc_prompt.txt"
    },
    {
      "name": "format",
      "type": "llm",
      "model": "gemini-2.0-flash",
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
