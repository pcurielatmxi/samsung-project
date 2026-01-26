# Narratives Processing

**Last Updated:** 2026-01-16

## Purpose

Extract and index project narrative documents (schedules, meeting notes, correspondence) for forensic analysis and semantic search.

## Two Systems

### 1. Document Processing Pipeline

Extracts statements from raw documents with source page verification.

**Stages:**
1. **Extract** (LLM): PDF/DOCX → structured statements
2. **Format** (LLM): Validate schema + outcome determination
3. **Locate** (Python): Fuzzy match statements to source pages
4. **Refine** (Python+LLM): Find exact quotes from source
5. **Consolidate** (Python): Generate CSV fact/dimension tables

**Usage:**
```bash
cd scripts/narratives/document_processing
./run.sh extract --limit 10
./run.sh consolidate
```

**Output:** `processed/narratives/{dim_narrative_file,narrative_statements}.csv`

### 2. Embeddings Search

Semantic search across narratives using Gemini embeddings + ChromaDB.

**Usage:**
```bash
# Build index (auto-syncs to OneDrive on success)
python -m scripts.narratives.embeddings build --source narratives
python -m scripts.narratives.embeddings build --source narratives --no-sync  # Skip auto-sync

# Enrich with structured metadata (locations, CSI, companies)
python -m scripts.narratives.embeddings enrich --source narratives
python -m scripts.narratives.embeddings enrich --source narratives --limit 10  # Test first

# Search
python -m scripts.narratives.embeddings search "HVAC delay" --author Yates
python -m scripts.narratives.embeddings status

# Manual sync
python -m scripts.narratives.embeddings sync
```

**Index:** `~/.local/share/samsung-embeddings/documents/`
**Backup:** Auto-synced to OneDrive at `{WINDOWS_DATA_DIR}/backup/embeddings/documents/`

**Metadata Enrichment:**
Chunks can be enriched with structured metadata extracted from text:
- **Document-level** (propagated to all chunks): Companies, locations, CSI codes mentioned anywhere in document
- **Chunk-level** (specific mentions): Data extracted from each chunk's text
- **No re-embedding**: Updates metadata only, preserves existing embeddings
- **Versioned**: Tracks enrichment version in manifest (`metadata_version`)
- **Idempotent**: Safe to run multiple times, only processes unenriched files

### Robustness Features

The embeddings system uses manifest-based tracking for reliability:

- **Content hashing**: SHA-256 of file contents (not mtime) for change detection
- **Manifest tracking**: `manifest.json` tracks all indexed files
- **Automatic backups**: Created before destructive operations
- **Safe partial runs**: `--limit` flag doesn't delete existing chunks
- **Explicit cleanup**: `--cleanup-deleted` flag required to remove stale files

**Backup/Restore:**
```bash
python -m scripts.narratives.embeddings backup           # Create backup
python -m scripts.narratives.embeddings list-backups     # List backups
python -m scripts.narratives.embeddings restore          # Restore latest
python -m scripts.narratives.embeddings restore -b FILE  # Restore specific
python -m scripts.narratives.embeddings verify           # Check consistency
```

## Structure

```
narratives/
├── document_processing/
│   ├── config.json, run.sh
│   ├── locate.py, refine.py      # Source verification
│   └── consolidate.py            # CSV export
└── embeddings/
    ├── cli.py                    # Command interface
    ├── builder.py                # Index construction
    ├── chunker.py                # Document chunking
    └── store.py                  # ChromaDB storage
```

## Key Data

- **304 narrative documents** indexed
- **13,687 searchable chunks**
- Sources: schedule narratives, weekly reports, expert reports, meeting notes
