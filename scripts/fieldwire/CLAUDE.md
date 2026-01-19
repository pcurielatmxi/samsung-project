# Fieldwire Scripts

**Last Updated:** 2026-01-19

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
**Location:** `{WINDOWS_DATA_DIR}/processed/fieldwire/*.csv`
**Export Source:** www.fieldwire.com → Samsung - Progress Tracking project

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

## Data Flow

```
{WINDOWS_DATA_DIR}/processed/fieldwire/*.csv (UTF-16LE)
    ↓ [Stage 1: Parse & Normalize]
processed/fieldwire/tbm_audits.csv
    ↓ [Stage 2: Enrich]
processed/fieldwire/tbm_audits_enriched.csv (with dimension IDs)
    ↓ [Stage 3: Aggregate]
processed/fieldwire/lpi_summary.csv (daily LPI by contractor)
```

## Structure

```
fieldwire/
├── CLAUDE.md                # This file
└── process/
    ├── run.sh               # Pipeline orchestrator
    ├── parse_fieldwire.py   # CSV parser (UTF-16 → normalized)
    ├── enrich_tbm.py        # Add dimension IDs
    └── calculate_lpi.py     # LPI and idle metrics
```

## Usage

```bash
cd scripts/fieldwire/process
./run.sh parse      # Stage 1: Parse Fieldwire dump
./run.sh enrich     # Stage 2: Add dimension IDs
./run.sh lpi        # Stage 3: Calculate LPI metrics
./run.sh all        # Run all stages
./run.sh status     # Show processing status
```

## Output Files

| File | Description |
|------|-------------|
| `tbm_audits.csv` | Normalized TBM records |
| `tbm_audits_enriched.csv` | With dim_location_id, dim_company_id |
| `lpi_summary.csv` | Daily LPI by contractor |
| `idle_analysis.csv` | Idle time breakdown by category |
| `manpower_counts.csv` | Daily START/END totals |

## Metrics Calculated

### LPI (Labor Planning Index)
```
LPI = Verified / TBM Manpower
Target: 80%
```

### Idle Categories (from Checklists)
- Passive (slow activity)
- Obstructed (blocked)
- Meeting
- No Manpower
- Work Has Not Started

### Cost Impact
```
Idle Cost = Idle Workers × Idle Hours × $50/hr
(Default: 2.09 hrs/worker)
```

## Data Quality Notes

1. **Encoding:** Files are UTF-16LE, must convert to UTF-8
2. **TBM Manpower decimals:** Values like 0.8, 2.5 represent partial day allocations
3. **Empty fields:** Many records have blank Direct/Indirect Manpower
4. **Date format:** YYYY-MM-DD in Start date field
5. **Checklist parsing:** Need regex to extract Yes/Not set prefix

## Pipeline Results (2026-01-19)

**Data Coverage:**
- TBM Location Records: 2,761
- Manpower Count Records: 112
- Date Range: 2025-12-15 to 2026-01-16

**Dimension Coverage:**
- Location: 66.2% (FAB building fully mapped, SUP partial)
- Company: 98.5% (Axios, Berg, MK Marlow)
- Trade: 99.8%

**LPI Summary (Overall):**
- Total Planned: 10,747
- Total Verified: 1,233 (based on Active checklist observations)
- LPI: 11.5%

**Idle Time Breakdown:**
| Category | Count | % |
|----------|-------|---|
| Active | 1,252 | 41.2% |
| No Manpower | 1,195 | 39.3% |
| Passive | 340 | 11.2% |
| Not Started | 216 | 7.1% |
| Obstructed | 19 | 0.6% |
| Meeting | 15 | 0.5% |

## Integration with Other Sources

| Source | Join Key | Purpose |
|--------|----------|---------|
| TBM (Excel) | Date + Location + Company | Cross-validate planned vs actual |
| P6 Schedule | Activity ID, WBS Code | Link to schedule tasks |
| dim_location | Building + Level + Tier 1 | Standardize location codes |
| dim_company | Company name | Standardize contractor names |
