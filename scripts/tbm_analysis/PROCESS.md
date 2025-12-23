# TBM Analysis Report Generation Process

## Overview

This document describes the process for generating TBM (Tool Box Meeting) Analysis Reports for the Samsung Taylor FAB1 project. The reports analyze contractor workforce planning accuracy by comparing planned work (TBM submissions) against field-verified observations.

## Complete Workflow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DAILY TBM PROCESS                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. RAW INPUT (Out of Scope)                                        │
│     └── Client provides: Yates - SECAI Daily Work Plan MM.DD.YY.xlsx│
│         • Contains all contractors' planned work for the day        │
│         • Columns: Subcontractor, Floor, Personnel, Activity, Loc   │
│                                                                     │
│  2. UPLOAD TO FIELDWIRE (Out of Scope)                              │
│     └── TBM data uploaded to Fieldwire for field tracking           │
│         • Tasks created per location/gridpoint                      │
│         • Status set to "TBM"                                       │
│                                                                     │
│  3. FIELD VERIFICATION (Out of Scope - Manual)                      │
│     └── MXI field team uses Fieldwire mobile app                    │
│         • Visits each location                                      │
│         • Records Direct/Indirect manpower counts                   │
│         • Documents observations (idle time, issues)                │
│         • Takes photos                                              │
│         • Adds messages with field notes                            │
│                                                                     │
│  4. FIELDWIRE DATA DUMP ─────────────────────────────► IN SCOPE     │
│     └── Export from Fieldwire → CSV                                 │
│         • Location: Fieldwire Data Dump/                            │
│         • Format: Samsung_-_Progress_Tracking_TBM_Analysis_         │
│                   Data_Dump_YYYY-MM-DD_*.csv                        │
│         • Encoding: UTF-16 with tab separator                       │
│                                                                     │
│  5. NARRATIVE GENERATION ─────────────────────────────► IN SCOPE    │
│     └── Claude Code (non-interactive)                               │
│         • Analyze Fieldwire observations                            │
│         • Generate category-specific narratives                     │
│         • Calculate KPIs and metrics                                │
│                                                                     │
│  6. REPORT STYLING & OUTPUT ──────────────────────────► IN SCOPE    │
│     └── Generate branded DOCX                                       │
│         • MXI brand colors and styling                              │
│         • Structured categories                                     │
│         • Tables, alerts, observations                              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Scope of This Automation

**IN SCOPE:**
- Processing Fieldwire data dump (CSV parsing, filtering by date/contractor)
- Narrative generation using Claude Code non-interactive mode
- Report styling and DOCX generation with MXI branding

**OUT OF SCOPE:**
- Receiving raw TBM Excel from client
- Uploading to Fieldwire
- Field verification process

---

## Data Source: Fieldwire Data Dump

### Location
```
{WINDOWS_DATA_DIR}/../../Field Tracking/TBM Analysis/Fieldwire Data Dump/
```

### File Format
- **Naming**: `Samsung_-_Progress_Tracking_TBM_Analysis_Data_Dump_YYYY-MM-DD_*.csv`
- **Encoding**: UTF-16 with tab separator
- **Header rows**: 3 rows of metadata before column headers

### Key Columns

| Column | Description | Example |
|--------|-------------|---------|
| `ID` | Fieldwire task ID | 1886 |
| `Title` | Task title with location | "K-11 (12.19.25)" |
| `Status` | Always "TBM" for these reports | TBM |
| `Category` | Work category | Control Joints, Cleaning |
| `Company` | Contractor name | Berg, MK Marlow, Axios |
| `Level` | Floor number | 1, 2, 3 |
| `Plan` | Floor plan name | 1st-Floor, 3rd Floor |
| `Tier1` | Gridpoint location | G-2, K-11, FAB1-EL02 |
| `DirectManpower` | Workers directly performing work | 2.0 |
| `IndirectManpower` | Supervisors, support staff | 1.0 |
| `TBMManpower` | Planned manpower from TBM | 4.0 |
| `Startdate` | Task date | 2025-12-19 |
| `Message1-62` | Field observations and notes | Inspector comments |
| `Checklist1-18` | Activity status checkboxes | Active, Passive, Idle |

### Filtering Logic
- Filter by `Status == 'TBM'`
- Filter by `Startdate` for target report date
- Filter by `Company` for contractor-specific reports

---

## Report Structure

### Contractor Groupings
- **Berg & MK Marlow**: Combined report (Yates subcontractors)
- **Axios**: Separate report

### Report Categories

| Category | Content | Data Source |
|----------|---------|-------------|
| **Cover Page** | MXI branding, date, project info | Static + date |
| **Executive Dashboard** | KPIs: LPI, DILR, Idle Time, Compliance | Calculated from data |
| **TBM Summary** | Table: Contractor, Planned, Actual, Accuracy | Aggregated manpower |
| **Cat 1: TBM Performance** | Submission timing, verification window | Narrative analysis |
| **Cat 2: Zero Verification** | Locations with TBM but 0 verified workers | Where Actual=0, Planned>0 |
| **Cat 3: Idle Workers** | Idle time observations with cost calc | Checklist + Messages |
| **Cat 4: High Verification** | Locations with >100% accuracy | Where Actual>Planned |
| **Cat 5: Key Findings** | Summary statistics | Aggregated metrics |
| **Cat 6: Contractor Comparison** | Performance by contractor | Per-company metrics |
| **Cat 7: Process Limitations** | Time constraints, methodology notes | Narrative |
| **Cat 8: Conclusions** | Recommendations | Narrative analysis |

---

## Document Content Specification

### Cover Page

```
MXI
Document Control • Project Management • Field Services

TBM ANALYSIS REPORT
{Contractor Group} Subcontractor Submittals

SAMSUNG TAYLOR FAB - SECAI
1530 FM973, Taylor, TX 76574

[Summary Stats Table]
| Total Tasks | Contractors | Floors | Report Pages |
|-------------|-------------|--------|--------------|
| {count}     | {count}     | {count}| {count}      |

{Month Day, Year}

Boots to Bytes

CONFIDENTIAL - FOR INTERNAL USE ONLY
```

### Executive Dashboard

**KPI Cards (6 metrics in 2 rows of 3):**

Row 1:
- **Labor Productivity Index (LPI)**: `{verified/planned × 100}%` | Target: 80%
- **Direct-Indirect Labor Ratio (DILR)**: `{indirect/(direct+indirect) × 100}%` | Target: <25%
- **Idle Time Hours**: `{total_idle_hours}` | @ $65/hr = ${cost}

Row 2:
- **TBM Compliance Rate**: `{locations_with_verification/total × 100}%`
- **Zero Verification Rate**: `{zero_locations/total × 100}%` | X of Y locations
- **Monthly Projection**: `${daily_cost × 26 working days}K`

**TBM Summary Table:**
```
| Contractor  | Planned MP | TBM Presence | Actual | Accuracy |
|-------------|------------|--------------|--------|----------|
| Berg        | 125        | 116          | 55     | 44%      |
| MK Marlow   | 84         | 87           | 43     | 51%      |
| TOTAL       | 209        | 203          | 98     | 46%      |
```
*Accuracy cells color-coded: <40% red, 40-70% yellow, >70% green*

### Category 1: TBM Planning & Documentation Performance

**Content Pattern:**
1. Opening narrative assessing overall performance
2. Subheader "1.1 TBM Submission Timing Impact"
3. Paragraph explaining when TBM was received and impact on verification window
4. ALERT BOX: "OPERATIONAL IMPACT: The {X}-minute verification window makes comprehensive verification mathematically impossible..."
5. Optional timing table showing Event | Time | Impact

**Example Opening:**
> "TBM verification shows improved performance with 46% overall accuracy. MK Marlow continues to outperform Berg in workforce documentation."

**Example Alert:**
> "OPERATIONAL IMPACT: The 68-minute verification window makes comprehensive verification mathematically impossible. With 56 locations to verify, MXI has approximately 1.2 minutes per location."

### Category 2: Zero Verification Locations

**Content Pattern:**
1. Intro paragraph explaining zero verification significance
2. ALERT BOX with count: "{X} LOCATIONS WITH 0% VERIFICATION"
3. Subheader "2.1 {Contractor} - Zero Verification ({X} locations, {Y} workers)"
4. Table for each contractor:
```
| Floor | Location                          | TBM | Verified |
|-------|-----------------------------------|-----|----------|
| 4F    | C33                               | 3   | 0        |
| 2F    | D5.8-D6 SUE                       | 6   | 0        |
```
5. Repeat for each contractor

### Category 3: Idle Worker Observations - ACTUAL COST

**Content Pattern:**
1. METHODOLOGY ALERT: "CALCULATION METHODOLOGY: Idle time cost based on 10-hour workdays with 1-hour idle duration per observation..."
2. Intro paragraph: "Field verification identified workers physically observed idle. This represents ACTUAL WASTE - quantifiable cost."
3. ALERT BOX: "IDLE TIME: {X} OBSERVATIONS - {Y} WORKER-HOURS - ${cost} DAILY WASTE"
4. For each contractor:
   - Subheader "3.X {Contractor} - Idle Time Observations ({X} locations)"
   - For each idle observation:
     ```
     {TaskID}: {Location} ({Level}) - {STATUS}

     [SHADED BOX] Field Observation ({Inspector}): {observation text from Fieldwire}

     Analysis: {STATUS} - {workers} workers × 1 hour × $65/hr = ${cost} waste. {explanation}
     ```
5. Cost summary table:
```
| Contractor | Idle Observations | Worker-Hours | Cost @ $65/hr |
|------------|-------------------|--------------|---------------|
| Berg       | 2                 | 4            | $260          |
| MK Marlow  | 3                 | 9            | $585          |
| TOTAL      | 5                 | 13           | $845          |
```

**Status Classifications:**
- `IDLE TIME` - Workers standing idle, not working
- `PARTIAL IDLE` - Some workers idle, some working
- `WAITING` - Workers waiting for materials/approval/equipment
- `PREPARATION TIME` - Setup, paperwork, safety briefings

### Category 4: High Verification Locations - Best Practices

**Content Pattern:**
1. Intro paragraph: "Several locations showed excellent verification rates, demonstrating proper TBM implementation."
2. Table:
```
| Contractor | Location            | TBM | Verified | Accuracy |
|------------|---------------------|-----|----------|----------|
| Berg       | C7.7-C8 SUE (3F)    | 5   | 9        | 180%     |
| Berg       | D30.7-D31 SUE (3F)  | 5   | 12       | 240%     |
```
3. Note explaining >100%: "Accuracy >100% indicates more workers verified than committed in TBM, suggesting either additional workforce deployment or workers from adjacent areas."

### Category 5: Key Findings

**Content Pattern:**
Bullet list of key metrics with context:
- `IMPROVED ACCURACY: 46% overall (up from historical 30-40% range)`
- `Berg: 44% accuracy with 125 planned, 55 verified`
- `MK Marlow: 51% accuracy with 84 planned, 43 verified`
- `IDLE TIME COST: $1,300 in documented waste (20 idle worker-hours)`
- `Berg idle: $520 (2 observations - workers waiting for materials)`
- `MK Marlow idle: $780 (3 observations - waiting for approval/equipment)`
- `31 locations with 0% verification (55% of all locations)`
- `10 locations showed >100% verification (over-deployment or worker mobility)`
- `Monthly idle cost projection: $34K requires attention`

### Category 6: Contractor Performance Comparison

**Content Pattern:**
1. Subheader "6.1 {Contractor} Performance"
2. Metrics table:
```
| Metric         | Value |
|----------------|-------|
| Total Tasks    | 32    |
| Total Planned  | 125   |
| Total Verified | 55    |
| Accuracy       | 44%   |
| Idle Locations | 2     |
| Idle Cost      | $260  |
```
3. Repeat for each contractor

### Category 7: TBM Process Limitations

**Content Pattern:**
1. ALERT BOX: "SYSTEMIC ISSUE: {X}-minute verification window insufficient for {Y} locations"
2. Subheader "7.1 Root Cause Analysis" - Explains why verification rates are low
3. Subheader "7.2 Time Required for Proper Verification" - Table:
```
| Activity               | Time Required | Time Available |
|------------------------|---------------|----------------|
| TBM Review & Analysis  | 30 minutes    | Limited        |
| Route Planning         | 15 minutes    | Limited        |
| Field Verification     | 6+ hours      | 68 minutes     |
```
4. Subheader "7.3 Impact on Report Accuracy" - Bullet list of caveats
5. Subheader "7.4 Required Process Improvements" - Bullet list of requirements

### Category 8: Conclusions & Recommendations

**Content Pattern:**
1. Subheader "8.1 Primary Issues" - Bullet list:
   - Material/Equipment Delays
   - Approval Bottlenecks
   - TBM Documentation gaps
   - Idle time waste quantification
2. Subheader "8.2 Recommendations" - Bullet list with CRITICAL items first:
   - `CRITICAL: Require TBM submission by 8:00 AM...`
   - Establish minimum notice period
   - Address early worker departure
   - Improve material logistics
   - Streamline approval process
   - Continue monitoring idle time
   - Recognize improvement

### End of Report
```
— End of Report —
```

---

## KPI Calculations

### Labor Productivity Index (LPI)
```
LPI = (Verified Workers / Planned Workers) × 100
```

### Direct-Indirect Labor Ratio (DILR)
```
DILR = (Indirect Workers / (Direct + Indirect Workers)) × 100
Target: < 25%
```

### Idle Time Cost
```
Idle Cost = Idle Workers × Hours × Labor Rate ($65/hr default)
Assumption: 1 hour idle duration per observation
```

### TBM Compliance Rate
```
Compliance = (Locations with verification / Total locations) × 100
```

### Zero Verification Rate
```
Zero Rate = (Locations with 0 verified / Total locations) × 100
```

---

## Observation Classification

Field observations are classified based on Message content and Checklist values:

| Classification | Indicators | Cost Impact |
|----------------|------------|-------------|
| **IDLE** | Checklist: "Yes: Idle", Message contains "idle", "standing", "waiting" | Full idle cost |
| **WAITING** | Message contains "waiting for", "approval", "materials" | Partial cost |
| **PARTIAL IDLE** | Mixed activity - some workers idle | Proportional cost |
| **ACTIVE** | Checklist: "Yes: Active", workers observed working | No waste |
| **NO MANPOWER** | Checklist: "Yes: No Manpower", no workers present | Verification gap |

---

## Narrative Generation Requirements

The narrative generator (Claude Code) should produce:

1. **Category-specific prose** analyzing the data patterns
2. **Individual task observations** with:
   - Location identifier (gridpoint + level)
   - Inspector name (from Message attribution)
   - Field observation quote
   - Analysis text explaining the situation
3. **Cost calculations** for idle time
4. **Recommendations** based on patterns observed

### Input to Claude
- Filtered Fieldwire data for target date/contractors
- Aggregated metrics (planned vs actual by location)
- Message content for observations

### Output from Claude
- JSON structure matching report generator schema
- Narrative text for each category
- Task-level observations with analysis

---

## File Outputs

### Per Report Date
```
{TBM Analysis}/{Contractor}/{MM-DD-YY}/
├── MXI - {Contractor} TBM Analysis - MM.DD.YY.docx    # Narrative report
├── MXI - {Contractor} TBM Analysis Combined - MM.DD.YY.pdf  # With photos
├── report_data.json                                    # Intermediate data
└── fieldwire_filtered.csv                              # Filtered source data
```

---

## MXI Brand Colors

| Color | RGB | Hex | Usage |
|-------|-----|-----|-------|
| Primary Blue | (30, 58, 95) | #1E3A5F | Headers, titles |
| Secondary Blue | (46, 89, 132) | #2E5984 | Subheaders |
| Accent Blue | (74, 144, 217) | #4A90D9 | Category headers |
| Critical Red | (196, 30, 58) | #C41E3A | Alerts, idle indicators |
| Warning Orange | (230, 126, 34) | #E67E22 | Warnings |
| Success Green | (39, 174, 96) | #27AE60 | Good metrics |
| Dark Gray | (44, 62, 80) | #2C3E50 | Body text |
| Light Gray | (236, 240, 241) | #ECF0F1 | Backgrounds |

---

## Implementation

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     PIPELINE ARCHITECTURE                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   Fieldwire CSV Dump                                                │
│          │                                                          │
│          ▼                                                          │
│   ┌──────────────────┐                                              │
│   │ tbm_data_        │  Deterministic calculations:                 │
│   │ processor.py     │  - Parse CSV, filter by date/contractor      │
│   │                  │  - Extract observations from Messages        │
│   │                  │  - Calculate KPIs (LPI, DILR, idle cost)     │
│   │                  │  - Build zero/high verification lists        │
│   └────────┬─────────┘                                              │
│            │                                                        │
│            ▼                                                        │
│      report_content.json (all data, no narratives)                  │
│            │                                                        │
│            ▼                                                        │
│   ┌──────────────────┐                                              │
│   │ narrative_       │  Targeted AI generation:                     │
│   │ generator.py     │  - Category-specific narrative functions     │
│   │                  │  - Calls Claude Code for each section        │
│   │                  │  - Falls back to templates if Claude fails   │
│   └────────┬─────────┘                                              │
│            │                                                        │
│            ▼                                                        │
│      report_content_with_narratives.json                            │
│            │                                                        │
│            ▼                                                        │
│   ┌──────────────────┐                                              │
│   │ docx_            │  Document styling:                           │
│   │ generator.py     │  - MXI brand colors and formatting           │
│   │                  │  - Tables, alert boxes, observation boxes    │
│   │                  │  - Professional DOCX output                  │
│   └────────┬─────────┘                                              │
│            │                                                        │
│            ▼                                                        │
│      MXI - {Contractor} TBM Analysis - MM.DD.YY.docx                │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Script Structure

```
scripts/tbm_analysis/
├── __init__.py                  # Module docstring
├── PROCESS.md                   # This documentation
├── tbm_data_processor.py        # Step 1: Parse Fieldwire → JSON
├── narrative_generator.py       # Step 2: Add AI narratives → JSON
├── docx_generator.py            # Step 3: JSON → styled DOCX
└── prompts/
    └── narrative_prompt.md      # Reference for narrative style
```

### Usage

**Step-by-step (recommended for debugging):**
```bash
cd /home/pdcur/samsung-project
source .venv/bin/activate

# Step 1: Process Fieldwire data
python scripts/tbm_analysis/tbm_data_processor.py \
    --date 12-19-25 \
    --contractors "Berg,MK Marlow"

# Step 2: Add narratives (optional - has fallbacks)
python scripts/tbm_analysis/narrative_generator.py \
    --input "/path/to/report_content.json"

# Step 2b: Add narratives WITH quality checks (recommended for production)
python scripts/tbm_analysis/narrative_generator.py \
    --input "/path/to/report_content.json" \
    --quality-check

# Step 3: Generate DOCX
python scripts/tbm_analysis/docx_generator.py \
    --input "/path/to/report_content_with_narratives.json"
```

**Quick generation (skip narratives):**
```bash
# Process data and generate DOCX directly
python scripts/tbm_analysis/tbm_data_processor.py --date 12-19-25 --contractors "Berg,MK Marlow"
python scripts/tbm_analysis/docx_generator.py --input "/path/to/report_content.json"
```

**Production workflow (with quality checks):**
```bash
python scripts/tbm_analysis/tbm_data_processor.py --date 12-19-25 --contractors "Berg,MK Marlow"
python scripts/tbm_analysis/narrative_generator.py --input "/path/to/report_content.json" --quality-check
python scripts/tbm_analysis/docx_generator.py --input "/path/to/report_content_with_narratives.json"
```

### Claude Code Integration

The narrative generator calls Claude Code for targeted sections:

```python
# Example: Generate Category 1 opening narrative
result = subprocess.run(
    ['claude', '-p', prompt],
    capture_output=True,
    text=True,
    timeout=60
)
```

Each category has:
- A specific prompt for what to generate
- Context data (JSON) from the report content
- A fallback template if Claude fails

### Quality Check System

When `--quality-check` is enabled, each AI-generated narrative goes through a dual-reviewer voting system:

**Reviewers:**
1. **Technical Reviewer** - Validates data accuracy, completeness, no fabrication
2. **Communication Reviewer** - Validates professional tone, no blame, constructive framing

**Flow:**
1. Generate narrative with Claude
2. Run both reviewers on the narrative
3. If both pass → approved
4. If either fails → use feedback to refine (up to 2 retries)
5. After max retries, mark as "best_effort" and continue

**Quality Status:**
The `quality_status` dict in the JSON tracks:
- `overall`: "approved", "best_effort", or "unchecked"
- `categories`: per-category status mapping
- `failed_categories`: list of categories that failed after max retries

### Output Files

Each report run produces:
- `report_content.json` - Deterministic data from Fieldwire
- `report_content_with_narratives.json` - With AI-generated text and quality_status
- `{prefix} MXI - {Contractor} TBM Analysis - MM.DD.YY.docx` - Final report

**Filename Prefixes:**
- `[QC-APPROVED]` - All narratives passed quality checks
- `[QC-BEST-EFFORT-N]` - N categories failed after max retries
- `[GENERATED]` - Quality checks were not run

## Next Steps (Future Enhancements)

1. **Photo integration** - Embed Fieldwire photos in report
2. **Combined PDF generation** - Merge DOCX + photos into single PDF
3. **Automated scheduling** - Run daily after Fieldwire dump
4. **Historical comparison** - Compare metrics across dates
