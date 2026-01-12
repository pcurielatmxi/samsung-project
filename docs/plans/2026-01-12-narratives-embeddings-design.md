# Narratives Embeddings Layer Design

**Date:** 2026-01-12
**Status:** Approved
**Purpose:** Enable semantic search across narrative documents for LLM-assisted investigation

---

## Overview

Add an embeddings layer to the narratives document processing pipeline that allows semantic search across both document summaries and individual forensic statements. Primary use case is CLI-based search for LLMs investigating available documentary evidence.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Granularity | Both document + statement level | Flexibility to search at either level |
| Embedding provider | Google Gemini | Consistency with existing document processing pipeline |
| Embedding model | `gemini-embedding-001` | Current stable model, `text-embedding-004` deprecates Jan 2026 |
| Vector dimensions | 768 | Sufficient quality for ~500 vectors, smaller storage |
| Vector storage | ChromaDB | File-based, excellent metadata filtering, minimal setup |
| Interface | Python API + CLI | API for integration, CLI for LLM/analyst ad-hoc queries |
| Output format | Rich context | Full metadata returned to minimize LLM round-trips |
| Build approach | Separate CLI command | Decoupled from document processing, optional dependency |
| Idempotency | Content hash tracking | Detect changes, skip unchanged, remove stale |

---

## Architecture

```
scripts/narratives/embeddings/
├── __init__.py
├── config.py          # Gemini + ChromaDB settings
├── client.py          # Gemini embedding client wrapper
├── store.py           # ChromaDB operations (add, search, delete)
├── builder.py         # Build logic - reads CSVs, embeds, stores
└── cli.py             # CLI entry point (build, search, status)
```

### Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│  processed/narratives/                                      │
│  ├── dim_narrative_file.csv  ──┐                           │
│  └── narrative_statements.csv ─┼─► builder.py ─► ChromaDB  │
└────────────────────────────────┴────────────────────────────┘
```

### Storage Location

```
{WINDOWS_DATA_DIR}/derived/narratives/embeddings/
└── chroma.sqlite3   # Persistent vector store
```

---

## Data Model

### Document Collection (`narrative_documents`)

| Field | Type | Source |
|-------|------|--------|
| `id` | string | `narrative_file_id` (e.g., "NAR-001") |
| `text` | string | Document summary (embedded) |
| `metadata.title` | string | `document_title` |
| `metadata.type` | string | `document_type` (schedule_narrative, milestone_variance, etc.) |
| `metadata.date` | string | `document_date` |
| `metadata.data_date` | string | `data_date` (P6 data date if applicable) |
| `metadata.author` | string | `author` |
| `metadata.path` | string | `relative_path` |
| `metadata.statement_count` | int | Number of statements in doc |
| `metadata.content_hash` | string | MD5 of summary for change detection |

### Statement Collection (`narrative_statements`)

| Field | Type | Source |
|-------|------|--------|
| `id` | string | `statement_id` (e.g., "STMT-00001") |
| `text` | string | Statement text (embedded) |
| `metadata.narrative_file_id` | string | Parent document ID |
| `metadata.statement_index` | int | Position in document (0-indexed) |
| `metadata.category` | string | delay, scope_change, quality_issue, etc. |
| `metadata.event_date` | string | Date of event if known |
| `metadata.parties` | string | Pipe-delimited parties |
| `metadata.locations` | string | Pipe-delimited locations |
| `metadata.impact_days` | int | Schedule impact if quantified |
| `metadata.source_page` | int | Page in source document |
| `metadata.content_hash` | string | MD5 of text for change detection |

### Statement Categories

From schema.json - filterable via `--category`:
- `delay`
- `scope_change`
- `quality_issue`
- `safety`
- `owner_direction`
- `progress`
- `resource`
- `weather`
- `design_issue`
- `coordination`
- `dispute`
- `commitment`
- `other`

---

## Gemini Embedding Configuration

### Model Selection

**Model:** `gemini-embedding-001`
- Current stable production model
- Matryoshka embeddings (scalable dimensions)
- Top rank on MTEB multilingual benchmark
- 2048 token input limit
- `text-embedding-004` deprecates January 14, 2026

### Embedding Strategy

```python
from google import genai

client = genai.Client()

# Indexing documents/statements (RETRIEVAL_DOCUMENT)
def embed_for_index(text: str) -> list[float]:
    result = client.models.embed_content(
        model="gemini-embedding-001",
        contents=text,
        config={
            "task_type": "RETRIEVAL_DOCUMENT",
            "output_dimensionality": 768
        }
    )
    return result.embeddings[0].values

# Search queries (RETRIEVAL_QUERY)
def embed_for_query(query: str) -> list[float]:
    result = client.models.embed_content(
        model="gemini-embedding-001",
        contents=query,
        config={
            "task_type": "RETRIEVAL_QUERY",
            "output_dimensionality": 768
        }
    )
    return result.embeddings[0].values
```

### Text Preparation

```python
# Documents: embed summary directly
embed_for_index(document.summary)

# Statements: embed with category prefix for context
embed_for_index(f"[{statement.category}] {statement.text}")
```

### Batching

- Gemini supports batch embedding (up to 100 texts per request)
- For ~80 docs + ~500 statements, 6-7 batch requests total
- Completes in seconds

---

## CLI Interface

### Commands

```bash
# Build/rebuild index (idempotent)
python -m scripts.narratives.embeddings build
python -m scripts.narratives.embeddings build --force  # Rebuild all

# Search statements (default)
python -m scripts.narratives.embeddings search "HVAC delays"

# Search with filters
python -m scripts.narratives.embeddings search "scope changes" --category scope_change
python -m scripts.narratives.embeddings search "Berg Electric" --party "Berg"
python -m scripts.narratives.embeddings search "FAB delays" --location "FAB"
python -m scripts.narratives.embeddings search "Q1 issues" --after 2024-01-01 --before 2024-03-31

# Search documents only
python -m scripts.narratives.embeddings search "milestone variance" --documents

# Show context around matches
python -m scripts.narratives.embeddings search "delay" --context 2

# Check index status
python -m scripts.narratives.embeddings status
```

### Filter Options

| Flag | Description |
|------|-------------|
| `--category <cat>` | Filter by statement category |
| `--party <name>` | Filter by party mentioned (substring match) |
| `--location <loc>` | Filter by location (substring match) |
| `--after <date>` | Event date after YYYY-MM-DD |
| `--before <date>` | Event date before YYYY-MM-DD |
| `--type <type>` | Document type (for --documents mode) |
| `--limit <n>` | Max results (default: 10) |
| `--context <n>` | Show N statements before/after each match |
| `--documents` | Search document summaries instead of statements |
| `--force` | Rebuild all embeddings (ignore cache) |

---

## Search Result Format

Optimized for LLM consumption with rich context:

```
$ python -m scripts.narratives.embeddings search "HVAC delay" --context 1

Found 3 matches in statements:

[1] Score: 0.89
    Statement: "HVAC work delayed 14 days due to design revision RFI-2847"
    Category: delay | Event: 2024-03-15 | Impact: 14 days
    Parties: Samsung, Yates | Location: FAB L2
    Source: raw/narratives/2024-03 Schedule Narrative.pdf (page 4)
    Document: March 2024 Schedule Narrative (NAR-042)

    Context:
      [prev] "Mechanical rough-in completed ahead of schedule in FAB L1"
      [next] "Samsung issued revised HVAC drawings on 2024-03-18"

[2] Score: 0.84
    Statement: "HVAC ductwork installation suspended pending coordination"
    Category: coordination | Event: 2024-02-28
    Parties: Yates, McKinstry | Location: SUE L3
    Source: raw/narratives/Weekly Meeting Minutes 2024-02-28.docx (page 2)
    Document: Weekly Meeting Minutes Feb 28 (NAR-038)

[3] Score: 0.79
    ...
```

### Document Search Output

```
$ python -m scripts.narratives.embeddings search "milestone variance" --documents

Found 2 matching documents:

[1] Score: 0.91
    Document: Milestone Variance Report - March 2024 (NAR-045)
    Type: milestone_variance | Date: 2024-03-31 | Data Date: 2024-03-28
    Author: Yates Construction
    Path: raw/narratives/Milestone Variance Report 2024-03.pdf
    Statements: 23

    Summary: Analysis of milestone variances for March 2024 reporting period.
    Key variances include FAB mechanical completion (14 days late), SUE
    electrical rough-in (7 days late), and SUW drywall (on track)...

[2] Score: 0.85
    ...
```

---

## Idempotency Strategy

### Change Detection

```python
def compute_content_hash(text: str) -> str:
    """MD5 hash for change detection."""
    return hashlib.md5(text.encode()).hexdigest()
```

### Build Algorithm

```python
def build_index(force: bool = False):
    # 1. Load current CSV data
    documents = load_dim_narrative_file()
    statements = load_narrative_statements()

    # 2. Get existing IDs from ChromaDB
    existing_doc_ids = get_collection_ids("narrative_documents")
    existing_stmt_ids = get_collection_ids("narrative_statements")

    # 3. Compute what needs updating
    csv_doc_ids = {d.narrative_file_id for d in documents}
    csv_stmt_ids = {s.statement_id for s in statements}

    # 4. Delete stale (in ChromaDB but not in CSV)
    stale_docs = existing_doc_ids - csv_doc_ids
    stale_stmts = existing_stmt_ids - csv_stmt_ids
    delete_from_collection("narrative_documents", stale_docs)
    delete_from_collection("narrative_statements", stale_stmts)

    # 5. Add/update (check content_hash for changes)
    for doc in documents:
        if force or needs_update(doc, existing_doc_ids):
            embedding = embed_for_index(doc.summary)
            upsert_document(doc, embedding)

    for stmt in statements:
        if force or needs_update(stmt, existing_stmt_ids):
            text = f"[{stmt.category}] {stmt.text}"
            embedding = embed_for_index(text)
            upsert_statement(stmt, embedding)
```

### Status Command Output

```
$ python -m scripts.narratives.embeddings status

Narratives Embedding Index Status
=================================
ChromaDB location: /mnt/c/.../derived/narratives/embeddings/

Documents collection:
  Total: 78
  Last updated: 2026-01-12 14:30:22

Statements collection:
  Total: 523
  Last updated: 2026-01-12 14:30:45

Source CSV status:
  dim_narrative_file.csv: 78 records
  narrative_statements.csv: 523 records

Sync status: IN SYNC (no changes detected)
```

---

## Statement Navigation

Sequential navigation via metadata query (no graph DB needed):

```python
def get_context(statement_id: str, n: int = 1) -> dict:
    """Get N statements before and after the given statement."""
    stmt = get_statement(statement_id)
    file_id = stmt.metadata.narrative_file_id
    index = stmt.metadata.statement_index

    # Query for surrounding statements
    prev_stmts = query_statements(
        where={
            "narrative_file_id": file_id,
            "statement_index": {"$gte": index - n, "$lt": index}
        },
        order_by="statement_index"
    )

    next_stmts = query_statements(
        where={
            "narrative_file_id": file_id,
            "statement_index": {"$gt": index, "$lte": index + n}
        },
        order_by="statement_index"
    )

    return {"prev": prev_stmts, "next": next_stmts}
```

---

## Dependencies

Add to `requirements.txt`:

```
chromadb>=0.4.0
google-genai>=0.5.0  # New unified SDK
```

---

## Environment Configuration

Existing `.env` file already contains `GEMINI_API_KEY` from document processing.

New config in `scripts/narratives/embeddings/config.py`:

```python
import os
from pathlib import Path

# Embedding model
EMBEDDING_MODEL = "gemini-embedding-001"
EMBEDDING_DIMENSIONS = 768
EMBEDDING_TASK_INDEX = "RETRIEVAL_DOCUMENT"
EMBEDDING_TASK_QUERY = "RETRIEVAL_QUERY"

# ChromaDB location
CHROMA_PATH = Path(os.environ["WINDOWS_DATA_DIR"]) / "derived/narratives/embeddings"

# Source data
NARRATIVES_OUTPUT = Path(os.environ["WINDOWS_DATA_DIR"]) / "processed/narratives"
DIM_FILE = NARRATIVES_OUTPUT / "dim_narrative_file.csv"
STMT_FILE = NARRATIVES_OUTPUT / "narrative_statements.csv"

# Search defaults
DEFAULT_LIMIT = 10
DEFAULT_CONTEXT = 0
```

---

## Implementation Plan

1. **Setup** - Create module structure, add dependencies
2. **Client** - Gemini embedding wrapper with batch support
3. **Store** - ChromaDB operations (collections, upsert, query, delete)
4. **Builder** - CSV loading, change detection, embedding orchestration
5. **CLI** - Argument parsing, command dispatch, output formatting
6. **Testing** - Manual verification with sample queries

---

## Future Enhancements (Out of Scope)

- Hybrid search (keyword + semantic)
- Cross-collection search (statements + documents in one query)
- Integration with monthly reports pipeline
- Web UI for non-CLI users
