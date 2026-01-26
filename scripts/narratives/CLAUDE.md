# Narratives Processing

**Last Updated:** 2026-01-26

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
Chunks can be enriched with structured metadata extracted from text to enable unified cross-dataset queries.

**What's Extracted:**
- **Locations**: FAB codes (FAB116101), buildings (SUE), levels (1F)
- **CSI Codes**: 6-digit codes (033053) and sections (03), plus keyword-based inference
- **Companies**: Resolved to company IDs from `dim_company.csv`

**Two-Level Extraction** (solves "context spread" problem):
- **Document-level** (propagated to ALL chunks): If "Yates" mentioned in chunk 2, all chunks get `doc_company_ids=[1]`
- **Chunk-level** (specific mentions): Only data extracted from THIS chunk's text

**Properties:**
- **No re-embedding**: Updates metadata only, preserves existing embeddings (fast: ~10 files/sec)
- **Versioned**: Tracks enrichment version in manifest (`metadata_version`)
- **Idempotent**: Safe to run multiple times, only processes unenriched files
- **Auto-sync**: Syncs to OneDrive after successful enrichment

**Complete Workflow:**
```bash
# Step 1: Build dimension table (if not already done)
python -m scripts.integrated_analysis.dimensions.build_dim_company

# Step 2: Test enrichment on small batch
python -m scripts.narratives.embeddings enrich --source narratives --limit 10

# Step 3: Verify metadata
python -m scripts.narratives.embeddings search "FAB116101" --limit 1
# (inspect metadata in results)

# Step 4: Enrich all sources
python -m scripts.narratives.embeddings enrich --source narratives
python -m scripts.narratives.embeddings enrich --source raba
python -m scripts.narratives.embeddings enrich --source psi

# Step 5: Check status
python -m scripts.narratives.embeddings status
```

**Metadata Schema** (stored as pipe-separated strings):
```
doc_locations: "FAB116101|FAB116102"
doc_buildings: "SUE"
doc_levels: "1F|2F"
doc_csi_codes: "033053"
doc_csi_sections: "03|09|23"
doc_company_ids: "1|5"
doc_company_names: "Yates|Berg"

chunk_locations: ""
chunk_buildings: ""
chunk_levels: ""
chunk_csi_codes: ""
chunk_csi_sections: ""
chunk_company_ids: ""
chunk_company_names: ""
```

**See:** `scripts/narratives/embeddings/ENRICHMENT.md` for full architecture and implementation details

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
