# AI Enrich

Batch AI column generation utility for CSV enrichment with per-row caching.

## Purpose

Add AI-generated columns (classifications, tags, scores) to DataFrames using batched LLM calls. Designed for:
- Processing thousands of rows efficiently (batched API calls)
- Caching results per primary key (avoid reprocessing)
- Resumable operations (skip cached, retry errors)

## Architecture

```
src/ai_enrich/
├── __init__.py      # Package exports
├── enrich.py        # Core enrichment logic
├── cli.py           # CLI interface
└── __main__.py      # Entry point
```

## Processing Flow

```
Input DataFrame (1000 rows)
         │
         ▼
┌─────────────────────────────┐
│ 1. Check cache for each PK  │ → Skip rows with existing cache files
└─────────────────────────────┘
         │
         ▼ (uncached rows)
┌─────────────────────────────┐
│ 2. Batch into groups of N   │ (default: 20)
└─────────────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│ 3. For each batch:          │
│    - Call prompt_fn(rows)   │
│    - Send to Gemini         │
│    - Parse keyed response   │
│    - Cache each row result  │
└─────────────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│ 4. Merge cached results     │
│    into 'ai_output' column  │
└─────────────────────────────┘
```

## Prompt Contract

The `prompt_fn` receives rows with primary keys and must instruct the LLM to return a keyed response:

```python
def prompt_fn(rows: list[dict], pk_cols: list[str]) -> str:
    # rows: list of row dicts with all columns
    # pk_cols: primary key column names
    # Must return prompt that produces: {"pk1": {...}, "pk2": {...}}
```

## Cache Structure

```
cache_dir/
├── ISS-001.json      # {"category": "schedule", "tags": [...]}
├── ISS-002.json
├── FAB|1F.json       # Composite key with | delimiter
└── _errors/
    └── ISS-004.json  # {"error": "...", "input_row": {...}}
```

## CLI Usage

```bash
# Basic usage
python -m src.ai_enrich input.csv \
    --prompt prompt.txt \
    --schema schema.json \
    --primary-key issue_id \
    --cache-dir cache/issues

# Composite primary key
python -m src.ai_enrich input.csv \
    --prompt prompt.txt \
    --schema schema.json \
    --primary-key building level \
    --cache-dir cache/locations

# Specify columns to include in prompt
python -m src.ai_enrich input.csv \
    --prompt prompt.txt \
    --schema schema.json \
    --primary-key id \
    --columns title,description \
    --cache-dir cache/issues

# Dry run (preview prompt)
python -m src.ai_enrich input.csv \
    --prompt prompt.txt \
    --schema schema.json \
    --primary-key id \
    --cache-dir cache/issues \
    --dry-run

# Check cache status
python -m src.ai_enrich --status --cache-dir cache/issues

# Retry errors
python -m src.ai_enrich input.csv ... --retry-errors

# Force reprocess all
python -m src.ai_enrich input.csv ... --force
```

## Python API

```python
from src.ai_enrich import enrich_dataframe, EnrichConfig

def classify_prompt(rows: list[dict], pk_cols: list[str]) -> str:
    pk_col = pk_cols[0]
    items = "\n".join(
        f"[{row[pk_col]}] {row['title']}: {row['description'][:200]}"
        for row in rows
    )
    return f"""Classify each issue.
Categories: schedule, quality, safety
Severity: low, medium, high

Issues:
{items}

Return JSON mapping each ID to classification."""

row_schema = {
    "type": "object",
    "properties": {
        "category": {"type": "string"},
        "severity": {"type": "string"},
        "tags": {"type": "array", "items": {"type": "string"}}
    },
    "required": ["category", "severity", "tags"]
}

df_enriched, result = enrich_dataframe(
    df=issues_df,
    prompt_fn=classify_prompt,
    row_schema=row_schema,
    primary_key="issue_id",
    cache_dir=Path("cache/issues"),
    config=EnrichConfig(batch_size=25, model="gemini-2.0-flash"),
)

# Result: df with 'ai_output' column containing dicts
```

## Prompt Template File Format

```text
Classify each issue into category and severity. Add relevant tags.

Categories: schedule, quality, safety, coordination, resource
Severity: low, medium, high, critical

Issues:
{rows}

Return JSON mapping each issue ID to its classification.
```

The `{rows}` placeholder is replaced with formatted row data showing primary keys.

## Schema File Format

Standard JSON Schema for each row's output:

```json
{
  "type": "object",
  "properties": {
    "category": {
      "type": "string",
      "enum": ["schedule", "quality", "safety", "coordination"]
    },
    "severity": {
      "type": "string",
      "enum": ["low", "medium", "high", "critical"]
    },
    "tags": {
      "type": "array",
      "items": {"type": "string"}
    }
  },
  "required": ["category", "severity", "tags"]
}
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `batch_size` | 20 | Rows per LLM call |
| `model` | gemini-2.0-flash | Gemini model |
| `concurrency` | 5 | Max parallel batches (not yet implemented) |
| `force` | false | Reprocess cached rows |
| `retry_errors` | false | Retry previously failed rows |
