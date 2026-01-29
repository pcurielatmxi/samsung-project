# Fieldwire Scripts

**Last Updated:** 2026-01-29

## Purpose

Process Fieldwire data dumps to extract TBM (Toolbox Meeting) audit data for labor productivity analysis. Calculates Labor Planning Index (LPI) - measuring planned vs actual labor deployment.

**Key Metrics:**
- **TBM Manpower**: Planned headcount | **Actual MP**: Field count (Direct + Indirect)
- **Verified**: Workers at planned location | **LPI** = Verified / Planned (target: 80%)

## Data Source

**Input:** Fieldwire CSV dumps (UTF-16LE, tab-separated)
**Location:** `{WINDOWS_DATA_DIR}/raw/fieldwire/*.csv`
**Export:** www.fieldwire.com → Samsung - Progress Tracking project

**Note:** Power BI processes raw files directly for LPI. These scripts generate **idle time tags** only.

## Data Model

| Status | Count | Purpose |
|--------|-------|---------|
| **TBM** | 3,028 | Core TBM audit records with manpower |
| Activity Sub-Task | 1,622 | Field observations linked to TBMs |
| Activity Task | 975 | P6-linked activities |
| P6 Unverified | 299 | P6 items not verified |
| Inspection Request | 268 | QC records |
| Scaffold | 179 | Scaffold tracking |

## Key Fields

### Core Fields
- **ID**: Unique task ID
- **Status**: Record type (TBM, Activity Sub-Task, etc.)
- **Category**: Work type (Firestop, Drywall, etc.)
- **Tier 1**: Grid location (L-18, J-14)
- **Building/Level**: FAB, SUE, SUW + Floor 1-4
- **Company**: Axios, Berg, MK Marlow

### Manpower (TBM Records)
- **TBM Manpower** (col 28): Planned headcount
- **Direct/Indirect Manpower** (cols 29-30): Observed workers

### Idle Checklists (cols 103-108)
Active, Passive, Obstructed, Meeting, No Manpower, Work Has Not Started

## SECAI Historical Data

Historical data (2025-10-31 to 2025-12-12) with different structure:
- `Direct Workers` → `Direct Manpower`, `Indirect Workers` → `Indirect Manpower`
- `Category` contains company name (vs separate `Company` column)
- Missing: Building, Level columns
- IDs prefixed with "SECAI-" to avoid collision

## Idle Time Tagging Pipeline

Extracts and classifies idle time indicators using AI enrichment.

### Idle Time Tags

| Tag | Description |
|-----|-------------|
| `passive` | Workers present but not engaged |
| `standing` | Workers standing idle |
| `not_started` | Work not started, no manpower |
| `obstruction` | Blocked by obstructions |
| `waiting` | Waiting for materials/instructions |
| `permit` | Permit delays |
| `meeting` | In meetings |
| `phone`/`talking` | Non-work activities |

### Pipeline Flow

```
raw/fieldwire/*.csv
    ↓ [extract_tbm_content.py]
    │  Parse files, extract checklists & narratives
    ↓
processed/fieldwire/tbm_content.csv
    ↓ [ai_enrich]
    │  Classify narratives into tags (gemini-3-flash-preview)
    ↓
processed/fieldwire/tbm_content_enriched.csv
```

### Output Files

| File | Description |
|------|-------------|
| `tbm_content.csv` | Extracted content with checklist tags |
| `tbm_content_enriched.csv` | With AI-generated tags |
| `ai_cache/` | Per-row JSON cache (do not delete) |

### Usage

```bash
# Recommended: orchestration script
./run_idle_tags.sh           # Extract + enrich
./run_idle_tags.sh --status  # Show progress
./run_idle_tags.sh --force   # Reprocess all (clear cache)

# Manual steps
python -m scripts.fieldwire.extract_tbm_content

python -m src.ai_enrich \
    "{WINDOWS_DATA_DIR}/processed/fieldwire/tbm_content.csv" \
    --prompt scripts/fieldwire/ai_enrichment/idle_tags_prompt.txt \
    --schema scripts/fieldwire/ai_enrichment/idle_tags_schema.json \
    --primary-key id \
    --columns narratives \
    --cache-dir "{WINDOWS_DATA_DIR}/processed/fieldwire/ai_cache"
```

### Files

```
fieldwire/
├── CLAUDE.md                    # This file
├── run_idle_tags.sh             # Orchestration script
├── extract_tbm_content.py       # Extract checklists & narratives
├── ai_enrichment/
│   ├── idle_tags_prompt.txt     # AI prompt
│   └── idle_tags_schema.json    # Output schema
└── process/                     # Exploratory scripts (not production)
```

## Integration

| Source | Join Key | Purpose |
|--------|----------|---------|
| TBM (Excel) | Date + Location + Company | Cross-validate planned vs actual |
| P6 Schedule | Activity ID, WBS Code | Link to schedule tasks |
| dim_location | Building + Level + Tier 1 | Standardize locations |
| dim_company | Company name | Standardize contractors |
