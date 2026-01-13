# Unified Document Embeddings Design

**Date**: 2026-01-13
**Status**: Approved
**Goal**: Expand semantic search to cover narratives, RABA, and PSI documents in a single unified index with source filtering.

---

## Overview

Currently the embeddings index only covers narrative documents (360 files, 195 indexed). This design expands it to include:

- **Narratives**: 360 documents (schedule narratives, meeting notes, reports)
- **RABA**: 9,398 quality inspection PDFs (RKCI Celvis)
- **PSI**: 6,309 quality inspection PDFs (Construction Hive)

Total: ~16,000 documents in a unified searchable index.

## Architecture

### Single Unified Index

All documents indexed in one ChromaDB collection with `source_type` metadata for filtering:

```
document_chunks collection
├── narratives (360 files)
├── raba (9,398 files)
└── psi (6,309 files)
```

**Rationale**: Enables cross-source semantic search while allowing source-specific filtering.

## Configuration Changes

**File**: `scripts/narratives/embeddings/config.py`

```python
# Source directories - each key becomes a source_type
SOURCE_DIRS = {
    "narratives": DATA_DIR / "raw" / "narratives",
    "raba": DATA_DIR / "raw" / "raba" / "individual",
    "psi": DATA_DIR / "raw" / "psi" / "reports",
}

# Unified index location
CHROMA_PATH = DATA_DIR / "derived" / "embeddings" / "documents"
CHUNKS_COLLECTION = "document_chunks"
```

## Metadata Schema

Every chunk includes:

| Field | Type | Description |
|-------|------|-------------|
| `source_type` | string | `narratives`, `raba`, or `psi` (NEW) |
| `source_file` | string | Filename |
| `relative_path` | string | Path relative to source directory |
| `chunk_index` | int | Position in document (0-indexed) |
| `total_chunks` | int | Total chunks in file |
| `page_number` | int | Page/section number |
| `chunk_type` | string | File extension (pdf/docx/txt/xlsx/pptx) |
| `file_hash` | string | MD5 for change detection |
| `file_date` | string | Extracted date (YYYY-MM-DD) |
| `author` | string | Inferred author |
| `document_type` | string | Classification |
| `subfolder` | string | Relative folder path |
| `file_size_kb` | int | File size |

Full path reconstructable via: `SOURCE_DIRS[source_type] / relative_path`

## Builder Changes

**File**: `scripts/narratives/embeddings/builder.py`

### New CLI flags

```bash
# Build single source (required for initial builds)
python -m scripts.narratives.embeddings build --source narratives
python -m scripts.narratives.embeddings build --source raba
python -m scripts.narratives.embeddings build --source psi

# Build all sources
python -m scripts.narratives.embeddings build --all

# Batch control
python -m scripts.narratives.embeddings build --source narratives --limit 50
```

### Key changes

1. **`scan_documents(source_type)`** - Takes source name, looks up directory from `SOURCE_DIRS`

2. **`build_chunk_metadata()`** - Adds `source_type` field

3. **`build_index(source=None)`** - New `source` parameter:
   - Requires explicit `--source` or `--all` flag
   - Processes only specified source

4. **Stale chunk cleanup** - Only deletes stale chunks for the source being built

## Store & Search Changes

**File**: `scripts/narratives/embeddings/store.py`

### New filter parameter

```python
def search_chunks(
    query: str,
    source_type: Optional[str] = None,  # NEW
    document_type: Optional[str] = None,
    author: Optional[str] = None,
    subfolder: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
    limit: int = 10
) -> List[ChunkResult]:
```

### CLI search examples

```bash
# Search with source filter
python -m scripts.narratives.embeddings search "HVAC delay" --source narratives
python -m scripts.narratives.embeddings search "drywall" --source raba

# Combine filters
python -m scripts.narratives.embeddings search "inspection failed" \
    --source psi \
    --after 2024-01-01 \
    --before 2024-06-30

# Search all sources (default)
python -m scripts.narratives.embeddings search "quality issue"
```

### Status output

```
============================================================
Document Embeddings Index Status
============================================================
ChromaDB location: .../derived/embeddings/documents

Index contents:
  Total chunks: 45,000
  Total files: 16,067

  By source:
    narratives: 5,000 chunks (360 files)
    raba: 28,000 chunks (9,398 files)
    psi: 12,000 chunks (6,309 files)
============================================================
```

## Migration Plan

### Step 1: Code changes
1. Update `config.py` with `SOURCE_DIRS` and new paths
2. Update `builder.py` with `--source` flag and source-aware logic
3. Update `store.py` with `source_type` filter
4. Update `cli.py` with new flags
5. Migrate existing chunks: add `source_type: narratives` to all existing entries

### Step 2: Build narratives (first)
```bash
python -m scripts.narratives.embeddings build --source narratives
python -m scripts.narratives.embeddings status
python -m scripts.narratives.embeddings search "HVAC delay" --source narratives
```

### Step 3: Build RABA
```bash
python -m scripts.narratives.embeddings build --source raba --limit 100  # test
python -m scripts.narratives.embeddings build --source raba              # full
```

### Step 4: Build PSI
```bash
python -m scripts.narratives.embeddings build --source psi --limit 100   # test
python -m scripts.narratives.embeddings build --source psi               # full
```

## Estimated Scale

| Source | Files | Est. Chunks | Est. Vectors |
|--------|-------|-------------|--------------|
| narratives | 360 | ~5,000 | ~5,000 |
| raba | 9,398 | ~28,000 | ~28,000 |
| psi | 6,309 | ~19,000 | ~19,000 |
| **Total** | **16,067** | **~52,000** | **~52,000** |

Storage: ~52K vectors × 768 dims × 4 bytes = ~160 MB for embeddings

## Files to Modify

1. `scripts/narratives/embeddings/config.py` - Add SOURCE_DIRS, update paths
2. `scripts/narratives/embeddings/builder.py` - Add --source flag, source-aware scanning
3. `scripts/narratives/embeddings/store.py` - Add source_type filter
4. `scripts/narratives/embeddings/cli.py` - Add --source to build/search/status
5. `scripts/narratives/embeddings/metadata.py` - Pass source_type through
