# Embeddings Metadata Enrichment

**Purpose:** Add structured metadata (locations, CSI codes, companies) to existing embedding chunks WITHOUT re-embedding, enabling unified cross-dataset queries.

## Architecture

### Two-Level Metadata Extraction

**Problem:** Context spreads across chunks. Company mentioned in chunk #2, issue in chunk #5.

**Solution:** Extract metadata at BOTH document and chunk levels:

```python
# Document-level (propagated to ALL chunks)
doc_metadata = {
    "doc_locations": ["FAB116101", "FAB116102"],  # All locations in document
    "doc_buildings": ["SUE"],                     # All buildings
    "doc_levels": ["1F", "2F"],                   # All levels
    "doc_csi_codes": ["033053"],                  # All CSI codes
    "doc_csi_sections": ["03"],                   # Unique sections
    "doc_company_ids": [1, 5],                    # All company IDs
    "doc_company_names": ["Yates", "Berg"],       # For display
}

# Chunk-level (specific to THIS chunk)
chunk_metadata = {
    "chunk_locations": ["FAB116101"],             # Mentioned in THIS chunk
    "chunk_buildings": ["SUE"],
    "chunk_levels": ["1F"],
    "chunk_csi_codes": [],                        # None in this chunk
    "chunk_csi_sections": [],
    "chunk_company_ids": [],
    "chunk_company_names": [],
}

# Final chunk metadata = doc_metadata + chunk_metadata
```

### Extraction Methods

| Data Type | Pattern-Based | Keyword-Based | Source |
|-----------|---------------|---------------|--------|
| **Location Codes** | `FAB1\d{5}` | N/A | Text |
| **Buildings** | `SUE\|SUW\|FAB\|FIZ` | N/A | Text |
| **Levels** | `1F\|2F\|Level 3` | N/A | Text |
| **CSI Codes** | `\d{6}` or `\d{2} \d{4}` | "concrete" → 03 | Text |
| **Companies** | N/A | Alias lookup | `dim_company.csv` |

**Document-level:** Aggressive extraction (includes keyword-based CSI)
**Chunk-level:** Conservative extraction (explicit mentions only)

### Metadata Schema

All metadata fields are **lists** (arrays) stored in ChromaDB metadata:

```json
{
    "source_file": "report.pdf",
    "source_type": "narratives",
    "author": "Yates",
    "file_date": "2024-01-15",

    // NEW - Document-level (inherited by all chunks)
    "doc_locations": ["FAB116101", "FAB116102"],
    "doc_buildings": ["SUE"],
    "doc_levels": ["1F"],
    "doc_csi_codes": ["033053"],
    "doc_csi_sections": ["03"],
    "doc_company_ids": [1, 5],
    "doc_company_names": ["Yates", "Berg"],

    // NEW - Chunk-level (specific to this chunk)
    "chunk_locations": [],
    "chunk_buildings": [],
    "chunk_levels": [],
    "chunk_csi_codes": [],
    "chunk_csi_sections": [],
    "chunk_company_ids": [],
    "chunk_company_names": []
}
```

## Usage

### Basic Enrichment

```bash
# Enrich all narratives (auto-syncs to OneDrive on success)
python -m scripts.narratives.embeddings enrich --source narratives

# Test on subset first
python -m scripts.narratives.embeddings enrich --source narratives --limit 10

# Check status (shows metadata_version)
python -m scripts.narratives.embeddings status
```

### Advanced Options

```bash
# Force re-enrichment (re-process all files)
python -m scripts.narratives.embeddings enrich --source narratives --force

# Skip auto-sync
python -m scripts.narratives.embeddings enrich --source narratives --no-sync

# Enrich all sources
python -m scripts.narratives.embeddings enrich --source narratives
python -m scripts.narratives.embeddings enrich --source raba
python -m scripts.narratives.embeddings enrich --source psi
```

### Query Examples (Future)

Once enrichment is done, you can query by structured metadata:

```python
# Find narratives about Yates work at FAB116101
results = search_chunks_by_metadata(
    location_codes=["FAB116101"],
    company_ids=[1],  # Yates
    after="2024-01-01"
)

# Find concrete work (CSI 03) narratives
results = search_chunks_by_metadata(
    csi_sections=["03"],
    after="2024-01-01"
)

# Unified query: Get all data for a room
room_data = {
    'p6_tasks': query_p6("FAB116101", "2024-01-01", "2024-06-30"),
    'raba': query_raba("FAB116101", "2024-01-01", "2024-06-30"),
    'narratives': search_chunks_by_metadata(
        location_codes=["FAB116101"],
        after="2024-01-01",
        before="2024-06-30"
    )
}
```

## Implementation Details

### No Re-Embedding

Enrichment **only updates metadata**. Existing embeddings are preserved:

```python
# Get existing chunk
chunk = store.get_chunk(chunk_id)

# Extract metadata
metadata = extract_metadata(chunk.text)

# Update metadata only (NO re-embedding)
store.update_chunk_metadata(chunk_id, metadata)
```

### Versioning

Tracks enrichment version in manifest:

```json
{
  "files": {
    "report.pdf": {
      "content_hash": "sha256:abc123...",
      "chunks": [...],
      "metadata_version": "v1",
      "enriched_at": "2026-01-26T10:30:00Z"
    }
  }
}
```

**When to increment version:**
- Changing extraction patterns
- Adding new metadata fields
- Fixing extraction bugs

**Idempotency:**
- Enrichment skips files already at current `metadata_version`
- Use `--force` to re-enrich all files

### Performance

Enrichment is **fast** (no API calls, no re-embedding):

| Source | Files | Chunks | Time | Speed |
|--------|-------|--------|------|-------|
| narratives | 309 | 13,687 | ~30s | ~10 files/sec |
| raba | 9,398 | 20,903 | ~15min | ~10 files/sec |
| psi | 6,309 | 10,566 | ~10min | ~10 files/sec |

**Estimated:** 10-30 seconds per 1000 chunks (file I/O + regex only).

## Company Data Dependency

Enrichment requires `dim_company.csv` for company name resolution:

```bash
# Build dim_company first (if not already done)
python -m scripts.integrated_analysis.dimensions.build_dim_company

# Then enrich
python -m scripts.narratives.embeddings enrich --source narratives
```

**Location:** `{WINDOWS_DATA_DIR}/processed/integrated_analysis/dimensions/dim_company.csv`

If `dim_company.csv` is missing:
- Warning is shown
- Company extraction is skipped
- Other metadata (locations, CSI) still extracted

## Extraction Patterns

### Location Codes

```python
# FAB1XXXXX room codes
ROOM_CODE_PATTERN = r'\bFAB1[12345][0-9]{4}\b'

# Examples:
"Work at FAB116101" → ["FAB116101"]
"FAB116101 and FAB116102" → ["FAB116101", "FAB116102"]
```

### Buildings

```python
# Building acronyms
BUILDING_PATTERNS = [
    r'\b(SUE|SUW|FAB|FIZ|FIS)\b',
    r'\b(Sub[- ]?Fab[- ]?East|Sub[- ]?Fab[- ]?West)\b'
]

# Normalization:
"Sub-Fab East" → "SUE"
"SUE building" → "SUE"
```

### Levels

```python
# Level/floor patterns
LEVEL_PATTERNS = [
    r'\b([12345]F|[12345]th Floor|Level [12345])\b',
    r'\bL-?[12345]\b'
]

# Normalization (all to XF format):
"1F" → "1F"
"2nd floor" → "2F"
"Level 3" → "3F"
"L-1" → "1F"
```

### CSI Codes

```python
# 6-digit codes
CSI_6DIGIT_PATTERN = r'\b(\d{6})\b'

# Spaced format
CSI_SPACED_PATTERN = r'\b(\d{2})\s+(\d{4})\b'

# Section-only
CSI_SECTION_PATTERN = r'\bCSI\s+(\d{2})\b'

# Examples:
"033053 work" → codes=["033053"], sections=["03"]
"03 3053" → codes=["033053"], sections=["03"]
"CSI 03 concrete" → codes=[], sections=["03"]
```

**Keyword-based inference:**
```python
CSI_KEYWORDS = {
    '03': ['concrete', 'formwork', 'rebar'],
    '09': ['drywall', 'gypsum', 'ceiling'],
    '23': ['hvac', 'duct', 'mechanical'],
    '26': ['electrical', 'conduit', 'panel'],
    # ...
}
```

**When enabled:**
- Document-level: YES (aggressive)
- Chunk-level: NO (conservative)

### Companies

```python
# Load aliases from dim_company.csv
company_aliases = {
    "Yates": 1,
    "Yates Construction": 1,
    "Berg": 5,
    "Berg Steel": 5,
    # ...
}

# Case-insensitive word boundary search
"Yates crews worked..." → company_ids=[1], company_names=["Yates"]
```

## Migration Plan

### Phase 1: Implementation (Complete)

- ✅ Metadata extraction module
- ✅ Store update methods
- ✅ Manifest versioning
- ✅ CLI command
- ✅ Documentation

### Phase 2: Testing & Deployment

```bash
# 1. Test on small batch
python -m scripts.narratives.embeddings enrich --source narratives --limit 10

# 2. Verify metadata
python -m scripts.narratives.embeddings search "FAB116101" --limit 1
# (inspect metadata in results)

# 3. Enrich all sources
python -m scripts.narratives.embeddings enrich --source narratives
python -m scripts.narratives.embeddings enrich --source raba
python -m scripts.narratives.embeddings enrich --source psi
```

### Phase 3: Query Integration

1. **Extend search with structured filters** (`store.py`)
2. **Add to room_timeline.py** - include narrative chunks
3. **Monthly reports** - pull relevant narratives by location/date
4. **Unified query API** - single function for all data sources

## Future Enhancements

### Advanced Search

```python
def search_chunks_by_metadata(
    location_codes=None,
    buildings=None,
    levels=None,
    csi_sections=None,
    company_ids=None,
    after=None,
    before=None,
    semantic_query=None,  # Optional semantic search
    limit=50
):
    """
    Structured + semantic search.
    """
```

### Multi-Pass Search

```python
# Pass 1: Exact matches (chunk-level)
exact = search_chunks_by_metadata(
    chunk_locations=["FAB116101"],
    chunk_company_ids=[1]
)

# Pass 2: Document-level context
contextual = search_chunks_by_metadata(
    doc_company_ids=[1],  # Company mentioned in doc
    chunk_locations=["FAB116101"]  # Location in this chunk
)

# Merge results
return merge_and_deduplicate(exact, contextual)
```

### Automatic Context Retrieval

```python
# Automatically include adjacent chunks
for match in matches:
    match.context_before = get_adjacent_chunks(match, before=2)
    match.context_after = get_adjacent_chunks(match, after=2)
```

## Troubleshooting

### "dim_company.csv not found"

```bash
# Build dimension table first
python -m scripts.integrated_analysis.dimensions.build_dim_company
```

### "No chunks in DB for file"

File is in manifest but chunks missing from ChromaDB. Re-build:

```bash
python -m scripts.narratives.embeddings build --source narratives --force
```

### Re-enrich after fixing extraction logic

Increment `METADATA_VERSION` in `cli.py`, then:

```bash
python -m scripts.narratives.embeddings enrich --source narratives
# (only processes files with old version)
```

Or force re-enrich all:

```bash
python -m scripts.narratives.embeddings enrich --source narratives --force
```
