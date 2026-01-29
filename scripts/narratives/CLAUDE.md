# Narratives Processing

**Last Updated:** 2026-01-29

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
# Build index (auto-syncs to OneDrive)
python -m scripts.narratives.embeddings build --source narratives

# Enrich with structured metadata (locations, CSI, companies)
python -m scripts.narratives.embeddings enrich --source narratives

# Search
python -m scripts.narratives.embeddings search "HVAC delay" --author Yates
python -m scripts.narratives.embeddings status
```

**Index:** `~/.local/share/samsung-embeddings/documents/`
**Backup:** Auto-synced to OneDrive at `{WINDOWS_DATA_DIR}/backup/embeddings/documents/`

**Metadata Enrichment:**
Extracts locations (FAB codes, buildings, levels), CSI codes, and companies from chunk text. Uses two-level extraction:
- **Document-level**: Propagated to ALL chunks in file
- **Chunk-level**: Specific to each chunk's text

Properties: No re-embedding (fast), versioned, idempotent, auto-sync to OneDrive.

**Metadata Schema** (pipe-separated strings):
```
doc_locations, doc_buildings, doc_levels, doc_csi_codes, doc_csi_sections
doc_company_ids, doc_company_names
chunk_* (same fields, chunk-specific)
```

**See:** `scripts/narratives/embeddings/ENRICHMENT.md` for full details.

### Robustness Features

- **Content hashing**: SHA-256 for change detection
- **Manifest tracking**: `manifest.json` tracks indexed files
- **Automatic backups**: Before destructive operations
- **Safe partial runs**: `--limit` doesn't delete existing chunks

**Backup/Restore:**
```bash
python -m scripts.narratives.embeddings backup
python -m scripts.narratives.embeddings list-backups
python -m scripts.narratives.embeddings restore
python -m scripts.narratives.embeddings verify
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
