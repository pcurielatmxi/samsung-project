# Fieldwire Scripts

**Last Updated:** 2026-01-20

## Purpose

Process Fieldwire data dumps to extract TBM (Toolbox Meeting) audit data for labor productivity analysis. This enables calculation of the Labor Planning Index (LPI) - measuring how well planned labor deployment matches actual field execution.

## Business Context

### What is TBM Analysis?

TBM (Toolbox Meeting) plans specify where workers will be deployed each day:
- **TBM Manpower**: Planned headcount at each location
- **Actual MP**: Workers counted in the field (Direct + Indirect)
- **Verified**: Workers confirmed at their planned TBM location
- **LPI** = Verified / Planned (target: 80%)

### Key Questions Answered

1. **Labor Planning Accuracy**: Are workers where they're supposed to be?
2. **Idle Time Impact**: Why aren't workers productive? (obstructions, waiting, no work started)
3. **Contractor Performance**: How do Axios, Berg, MK Marlow compare on LPI?
4. **Cost Impact**: What's the dollar cost of unverified/idle labor?

## Data Source

**Input:** Fieldwire CSV data dumps (UTF-16LE encoded, tab-separated)
**Location:** `{WINDOWS_DATA_DIR}/raw/fieldwire/*.csv`
**Export Source:** www.fieldwire.com → Samsung - Progress Tracking project

**Note:** Power BI processes raw Fieldwire files directly for LPI metrics. These scripts generate **idle time tags** only, which Power BI joins back to the raw data by ID.

## Data Model

```
┌─────────────────────┐
│       TBM           │  Status = "TBM"
│   (Daily Plans)     │  3,028 records
├─────────────────────┤
│ • TBM Manpower      │  Planned workers
│ • Direct Manpower   │  Workers actively working
│ • Indirect Manpower │  Support workers
│ • Checklist 1-6     │  Idle time indicators
│ • Tier 1 (grid)     │  Location (L-18, J-14)
│ • Company           │  Axios, Berg, MK Marlow
└─────────────────────┘
         │
         │ Related Task 1-N
         ▼
┌─────────────────────┐
│  Activity Sub-Task  │  Status = "Activity Sub-Task"
│ (Field Observations)│  1,622 records
└─────────────────────┘

┌─────────────────────┐
│   Manpower Count    │  Category = "Manpower Count"
│ (Daily START/END)   │  112 records
│  by contractor      │  Daily headcount totals
└─────────────────────┘
```

## SECAI Historical Data

The SECAI Fieldwire project contains historical data (2025-10-31 to 2025-12-12) with a different structure:

| SECAI Column | Main Column | Notes |
|--------------|-------------|-------|
| Direct Workers | Direct Manpower | Name mapping |
| Indirect Workers | Indirect Manpower | Name mapping |
| Category | Company | Category contains company name |
| N/A | Building, Level | Missing in SECAI |

**Status Mapping:**
| SECAI Status | Main Status | Notes |
|--------------|-------------|-------|
| Manpower (During) | TBM | Work location observations |
| Manpower (Start) | Manpower Count | START headcount |
| Manpower (End) | Manpower Count | END headcount |
| Obstruction | TBM | With is_obstructed flag |

**ID Prefixing:** SECAI records are prefixed with "SECAI-" to avoid ID collision.

## Record Types (Status Field)

| Status | Count | Purpose |
|--------|-------|---------|
| **TBM** | 3,028 | Core TBM audit records - daily plans with manpower |
| Activity Sub-Task | 1,622 | Field work observations linked to TBMs |
| Activity Task | 975 | Higher-level P6-linked activities |
| P6 Unverified | 299 | P6 items not verified in field |
| Inspection Request | 268 | QC inspection records (Pass/Fail/Cancelled) |
| Scaffold | 179 | Scaffold-specific tracking |

## Key Fields

### Core Fields (All Records)

| Column | Field | Description |
|--------|-------|-------------|
| 1 | ID | Unique task ID |
| 2 | Title | Task description |
| 3 | Status | Record type (TBM, Activity Sub-Task, etc.) |
| 4 | Category | Work type (Firestop, Drywall, etc.) |
| 6 | Start date | YYYY-MM-DD format |
| 11 | Tier 1 | Grid location (L-18, J-14, N-4) |
| 21 | Building | FAB, FAB1, Main FAB, SUP, SUE, SUW |
| 22 | Level | Floor (1, 2, 3, 4) |
| 23 | Company | Axios, Berg, MK Marlow, Multiple, Brand Safway |

### Manpower Fields (TBM Records)

| Column | Field | Description |
|--------|-------|-------------|
| 28 | TBM Manpower | Planned headcount (decimal values like 0.8, 2.5) |
| 29 | Direct Manpower | Workers observed actively working |
| 30 | Indirect Manpower | Support/overhead workers |
| 31 | Total Idle Hours | Rarely populated |

### Idle Time Checklists (Columns 103-108)

Format: `{Yes|Not set}: {Category} ({Inspector}) - {Date}`

| Column | Field | Indicates |
|--------|-------|-----------|
| 103 | Active | Workers actively working |
| 104 | Passive | Idle - slow/minimal activity |
| 105 | Obstructed | Blocked by other trades |
| 106 | Meeting | In meetings |
| 107 | No Manpower | No workers at location |
| 108 | Work Has Not Started | Waiting to begin |

**Inspector Codes:** HBA, JCA, ACA, MAH, NCE, JNI, CRO, RAL

### Tags (Columns 41-42)

- `manpower_status` - General manpower tracking
- `manpower_tbm` - TBM-specific records
- `non_tbm` - Non-TBM observations

## Categories

| Category | Count | Include in TBM Analysis |
|----------|-------|-------------------------|
| Firestop | 1,494 | Yes |
| Drywall | 1,202 | Yes |
| Doors | 1,097 | No (separate workflow) |
| Tape & Float | 830 | Yes |
| Scaffold | 493 | Yes |
| Expansion Joint | 297 | Yes |
| Miscellaneous | 248 | Yes |
| Manpower Count | 112 | Separate (daily totals) |
| Framing | 80 | Yes |
| Control Joints | 70 | Yes |
| Cleaning | 52 | Yes |

## Contractors

| Company | Records | Trade Focus |
|---------|---------|-------------|
| Axios | 2,900 | Drywall, Firestop, Cleaning |
| Berg | 912 | Scaffold, Drywall |
| MK Marlow | 684 | Control Joints, Doors |
| Multiple | 324 | Multi-contractor areas |
| Brand Safway | 19 | Scaffold |

## Idle Time Tagging Pipeline

Extracts and classifies idle time indicators from Fieldwire messages and checklists using AI enrichment.

### Use Cases

1. **Idle Time Cost Analysis**: Quantify labor hours lost to each idle category (passive, waiting, obstruction) for cost impact reporting
2. **Contractor Accountability**: Identify which contractors have highest idle time rates by category
3. **Root Cause Analysis**: Correlate idle time tags with locations/dates to find systemic issues (e.g., recurring obstructions at specific grids)
4. **LPI Deep Dive**: Explain WHY workers at planned locations aren't productive (complement to LPI metrics)

### Data Sources

| File | ID Prefix | Filter | Description |
|------|-----------|--------|-------------|
| `Manpower_-_SECAI_Power_BI_Data_Dump_*.csv` | `MP-` | `Status = "Manpower (During)"` | Mid-day field observations |
| `Samsung_-_Progress_Tracking_QC_Inspections_Data_Dump_*.csv` | `TBM-` | `Status = "TBM" AND Category != "Manpower Count"` | TBM location observations |

**Note:** Only the latest file for each source is processed. Older files are ignored.

### Idle Time Tags

| Tag | Description |
|-----|-------------|
| `passive` | Workers present but not actively engaged |
| `standing` | Workers observed standing idle |
| `not_started` | Work has not started, no manpower present |
| `obstruction` | Work blocked by physical obstructions |
| `phone` | Workers using phones instead of working |
| `waiting` | Waiting for materials, instructions, equipment |
| `permit` | Delayed due to permit issues |
| `acting` | Simulating work activity |
| `meeting` | Workers in meetings |
| `talking` | Workers engaged in conversation |
| `delay` | General delays, schedule impacts |

### Pipeline Flow

```
{WINDOWS_DATA_DIR}/raw/fieldwire/*.csv (UTF-16LE, tab-separated)
    ↓ [extract_tbm_content.py]
    │  • Parse both Manpower and Progress Tracking files
    │  • Extract checklist tags automatically
    │  • Extract narratives (filter out change logs)
    ↓
{WINDOWS_DATA_DIR}/processed/fieldwire/tbm_content.csv
    │  Columns: id, source, title, status, category, start_date,
    │           checklist_tags, narratives, narrative_count
    ↓ [ai_enrich]
    │  • Classify narratives into idle time tags
    │  • Uses gemini-3-flash-preview model
    │  • Caches results per row (expensive to regenerate)
    ↓
{WINDOWS_DATA_DIR}/processed/fieldwire/tbm_content_enriched.csv
    │  Columns: + ai_output (containing AI tags)
```

### Output Files

| File | Location | Description |
|------|----------|-------------|
| `tbm_content.csv` | `{WINDOWS_DATA_DIR}/processed/fieldwire/` | Extracted content with checklist tags |
| `tbm_content_enriched.csv` | `{WINDOWS_DATA_DIR}/processed/fieldwire/` | With AI-generated tags |
| `ai_cache/` | `{WINDOWS_DATA_DIR}/processed/fieldwire/ai_cache/` | Per-row JSON cache (do not delete) |

### Usage

**Recommended: Use the orchestration script (schedulable)**
```bash
cd scripts/fieldwire

./run_idle_tags.sh           # Normal run (extract + enrich)
./run_idle_tags.sh --status  # Show pipeline status
./run_idle_tags.sh --force   # Reprocess all rows (clear cache)
```

**Schedule with cron:**
```bash
# Run daily at 6am
0 6 * * * /path/to/scripts/fieldwire/run_idle_tags.sh >> /path/to/idle_tags.log 2>&1
```

**Manual steps (if needed):**
```bash
# Step 1: Extract content from Fieldwire dumps
python -m scripts.fieldwire.extract_tbm_content

# Step 2: Run AI enrichment on narratives
python -m src.ai_enrich \
    "{WINDOWS_DATA_DIR}/processed/fieldwire/tbm_content.csv" \
    --prompt scripts/fieldwire/ai_enrichment/idle_tags_prompt.txt \
    --schema scripts/fieldwire/ai_enrichment/idle_tags_schema.json \
    --primary-key id \
    --columns narratives \
    --cache-dir "{WINDOWS_DATA_DIR}/processed/fieldwire/ai_cache" \
    --batch-size 20

# Check enrichment progress
python -m src.ai_enrich \
    "{WINDOWS_DATA_DIR}/processed/fieldwire/tbm_content.csv" \
    --cache-dir "{WINDOWS_DATA_DIR}/processed/fieldwire/ai_cache" \
    --status
```

### Files

```
fieldwire/
├── CLAUDE.md                    # This file
├── run_idle_tags.sh             # Orchestration script (schedulable)
├── extract_tbm_content.py       # Extract checklists & narratives
├── ai_enrichment/
│   ├── idle_tags_prompt.txt     # AI prompt for narrative classification
│   └── idle_tags_schema.json    # Output schema (tags array)
└── process/                     # Exploratory scripts (not used in production)
    └── ...
```

### Tag Sources

Tags come from two sources that are combined in the output:

1. **Checklist Tags** (automatic): Extracted from Fieldwire checklist columns, mapped to standard tags
2. **AI Tags** (from narratives): LLM classifies inspector narratives into tags

The `checklist_tags` column contains auto-extracted tags. The `ai_output` column contains `{'tags': [...]}` from AI classification.

## Integration with Other Sources

| Source | Join Key | Purpose |
|--------|----------|---------|
| TBM (Excel) | Date + Location + Company | Cross-validate planned vs actual |
| P6 Schedule | Activity ID, WBS Code | Link to schedule tasks |
| dim_location | Building + Level + Tier 1 | Standardize location codes |
| dim_company | Company name | Standardize contractor names |
