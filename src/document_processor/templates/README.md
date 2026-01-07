# Document Processor Templates

Sample prompts and guidelines for creating document processing pipelines.

## Files

| File | Purpose |
|------|---------|
| `qc_prompts_guide.md` | Comprehensive guide to writing QC prompts |
| `extract_qc_prompt.txt` | Template QC prompt for extraction stages (PDF input) |
| `format_qc_prompt.txt` | Template QC prompt for formatting stages (JSON input) |

## Usage

Copy the relevant template to your pipeline's config directory and customize for your specific document type and schema.

```bash
# Example: Setting up QC for PSI pipeline
cp templates/extract_qc_prompt.txt scripts/psi/document_processing/extract_qc_prompt.txt
# Edit to reference your specific fields and requirements
```

Then add `qc_prompt_file` to your stage config:

```json
{
  "name": "extract",
  "type": "llm",
  "prompt_file": "extract_prompt.txt",
  "qc_prompt_file": "extract_qc_prompt.txt"
}
```
