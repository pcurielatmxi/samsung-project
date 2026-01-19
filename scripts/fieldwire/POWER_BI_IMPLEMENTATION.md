# Fieldwire TBM Metrics - Power BI Implementation Guide

**Created:** 2026-01-19
**Updated:** 2026-01-19 (corrected metric definitions per field team)

Implement TBM (Toolbox Meeting) audit metrics directly from the raw Fieldwire CSV export.

---

## Metric Definitions (from Field Team)

| Metric | Definition |
|--------|------------|
| **TBM Actual** | SUM of `TBM Manpower` WHERE `Category = "Manpower Count"` — headcount at morning TBM |
| **TBM Planned** | SUM of `TBM Manpower` WHERE `Status = "TBM"` AND `Category ≠ "Manpower Count"` — planned deployment |
| **Verified** | SUM of (`Direct MP` + `Indirect MP`) WHERE `Status = "TBM"` AND `Category ≠ "Manpower Count"` AND `TBM Manpower > 0` |
| **Unverified** | SUM of (`Direct MP` + `Indirect MP`) WHERE `Status = "TBM"` AND `Category ≠ "Manpower Count"` AND (`TBM Manpower = 0` OR `TBM Manpower IS NULL`) |
| **Not Found** | TBM Actual - (Verified + Unverified) — workers counted at TBM but not found at any location |
| **LPI %** | Verified / TBM Planned × 100 |

**Key Insight:**
- **TBM Actual** = total headcount at the morning Toolbox Meeting (everyone present at start of day)
- **TBM Planned** = where workers were supposed to be deployed (planned manpower by location)
- **Verified** = workers found at locations that HAD planned manpower
- **Unverified** = workers found at locations with NO planned manpower (showed up somewhere unexpected)
- **Not Found** = workers counted at morning TBM but not observed at any work location during audits

**Important:** `Verified + Unverified ≠ TBM Actual` because not all workers counted at the morning TBM are necessarily found at work locations during the day.

---

## Source File

**File:** `Samsung_-_Progress_Tracking_QC_Inspections_Data_Dump_*.csv`
**Location:** `processed/fieldwire/`
**Encoding:** UTF-16 LE (Unicode)
**Delimiter:** Tab-separated
**Header Rows:** 4 (skip first 3, row 4 = column headers)

---

## Power Query: Import Raw Data

### Step 1: Load Raw CSV

```m
let
    // Load file with UTF-16 encoding
    Source = Csv.Document(
        File.Contents("C:\Users\pdcur\OneDrive - MXI\Desktop\Samsung Dashboard\Data\processed\fieldwire\Samsung_-_Progress_Tracking_QC_Inspections_Data_Dump_2026-01-18_1768753098.csv"),
        [Delimiter="#(tab)", Encoding=1200, QuoteStyle=QuoteStyle.None]
    ),

    // Skip first 3 rows (metadata), promote row 4 as headers
    SkippedRows = Table.Skip(Source, 3),
    PromotedHeaders = Table.PromoteHeaders(SkippedRows, [PromoteAllScalars=true])
in
    PromotedHeaders
```

**Note:** Encoding `1200` = UTF-16 LE

---

## Power Query: Create TBM Locations Table

TBM location records - where workers were planned to be.

```m
let
    Source = #"Fieldwire Raw",

    // Filter: Status = "TBM" AND Category != "Manpower Count"
    FilteredTBM = Table.SelectRows(Source, each
        [Status] = "TBM" and [Category] <> "Manpower Count"
    ),

    // Select relevant columns
    SelectedColumns = Table.SelectColumns(FilteredTBM, {
        "ID", "Title", "Status", "Category", "Start date",
        "Tier 1", "Tier 2", "Tier 3", "Building", "Level", "Company",
        "TBM Manpower", "Direct Manpower", "Indirect Manpower",
        "Active", "Passive", "Obstructed", "Meeting", "No Manpower", "Work Has Not Started"
    }),

    // Rename columns
    RenamedColumns = Table.RenameColumns(SelectedColumns, {
        {"Start date", "Date"},
        {"Tier 1", "Grid"},
        {"TBM Manpower", "PlannedMP"},
        {"Direct Manpower", "DirectMP"},
        {"Indirect Manpower", "IndirectMP"},
        {"Work Has Not Started", "NotStarted"}
    }),

    // Type conversions
    TypedColumns = Table.TransformColumnTypes(RenamedColumns, {
        {"Date", type date},
        {"PlannedMP", type number},
        {"DirectMP", type number},
        {"IndirectMP", type number}
    }),

    // Add calculated columns
    AddActualMP = Table.AddColumn(TypedColumns, "ActualMP", each
        ([DirectMP] ?? 0) + ([IndirectMP] ?? 0), type number),

    // HasPlannedMP - needed for Verified calculation
    AddHasPlanned = Table.AddColumn(AddActualMP, "HasPlannedMP", each
        ([PlannedMP] ?? 0) > 0, type logical),

    // Parse checklist columns for idle analysis
    AddIsActive = Table.AddColumn(AddHasPlanned, "IsActive", each
        Text.StartsWith(Text.Trim([Active] ?? ""), "Yes"), type logical),
    AddIsPassive = Table.AddColumn(AddIsActive, "IsPassive", each
        Text.StartsWith(Text.Trim([Passive] ?? ""), "Yes"), type logical),
    AddIsObstructed = Table.AddColumn(AddIsPassive, "IsObstructed", each
        Text.StartsWith(Text.Trim([Obstructed] ?? ""), "Yes"), type logical),
    AddIsMeeting = Table.AddColumn(AddIsObstructed, "IsMeeting", each
        Text.StartsWith(Text.Trim([Meeting] ?? ""), "Yes"), type logical),
    AddIsNoManpower = Table.AddColumn(AddIsMeeting, "IsNoManpower", each
        Text.StartsWith(Text.Trim([#"No Manpower"] ?? ""), "Yes"), type logical),
    AddIsNotStarted = Table.AddColumn(AddIsNoManpower, "IsNotStarted", each
        Text.StartsWith(Text.Trim([NotStarted] ?? ""), "Yes"), type logical),

    // Remove raw checklist text columns
    RemovedChecklistText = Table.RemoveColumns(AddIsNotStarted, {
        "Active", "Passive", "Obstructed", "Meeting", "No Manpower", "NotStarted"
    }),

    // Normalize Building names
    NormalizedBuilding = Table.TransformColumns(RemovedChecklistText, {
        {"Building", each
            if Text.Upper(_ ?? "") = "FAB" or Text.Upper(_ ?? "") = "FAB1" or
               Text.Upper(_ ?? "") = "MAIN FAB" or Text.Upper(_ ?? "") = "MAIN FAB1" or
               Text.Upper(_ ?? "") = "T1 FAB" or Text.Upper(_ ?? "") = "T1 FAB1" then "FAB"
            else if Text.Upper(_ ?? "") = "SUP" then "SUP"
            else if Text.Upper(_ ?? "") = "SUE" then "SUE"
            else if Text.Upper(_ ?? "") = "SUW" then "SUW"
            else Text.Upper(_ ?? ""), type text}
    }),

    // Normalize Level (handle "3.0" -> "3F")
    AddLevelNorm = Table.AddColumn(NormalizedBuilding, "LevelNorm", each
        let
            lvl = Text.Trim([Level] ?? ""),
            lvlNum = try Number.From(lvl) otherwise null,
            result = if lvlNum <> null then Text.From(Number.RoundDown(lvlNum)) & "F" else lvl
        in result, type text),

    // Add Building_Level combined field
    AddBuildingLevel = Table.AddColumn(AddLevelNorm, "Building_Level", each
        if [Building] <> null and [LevelNorm] <> null then [Building] & "-" & [LevelNorm] else null, type text)
in
    AddBuildingLevel
```

---

## Power Query: Create Manpower Counts Table

Daily headcount totals (TBM Actual).

```m
let
    Source = #"Fieldwire Raw",

    // Filter: Category = "Manpower Count"
    FilteredMP = Table.SelectRows(Source, each [Category] = "Manpower Count"),

    // Select columns
    SelectedColumns = Table.SelectColumns(FilteredMP, {
        "ID", "Title", "Status", "Category", "Start date", "Company",
        "TBM Manpower", "Direct Manpower", "Indirect Manpower"
    }),

    // Parse count type from Title (e.g., "Berg 12.17.25 START")
    AddCountType = Table.AddColumn(SelectedColumns, "CountType", each
        if Text.Contains(Text.Upper([Title] ?? ""), "START") then "START"
        else if Text.Contains(Text.Upper([Title] ?? ""), "END") then "END"
        else "OTHER", type text),

    // Parse date from Title
    AddParsedDate = Table.AddColumn(AddCountType, "ParsedDate", each
        let
            title = [Title] ?? "",
            parts = Text.Split(title, " "),
            datePart = List.Select(parts, each Text.Contains(_, ".")){0}?,
            dateComponents = if datePart <> null then Text.Split(datePart, ".") else null,
            month = if dateComponents <> null and List.Count(dateComponents) >= 3
                    then try Number.From(dateComponents{0}) otherwise null else null,
            day = if dateComponents <> null and List.Count(dateComponents) >= 3
                  then try Number.From(dateComponents{1}) otherwise null else null,
            year = if dateComponents <> null and List.Count(dateComponents) >= 3
                   then try Number.From(dateComponents{2}) otherwise null else null,
            fullYear = if year <> null then (if year >= 25 then 2025 else 2026) else null,
            result = if month <> null and day <> null and fullYear <> null
                     then #date(fullYear, month, day) else null
        in result, type date),

    // Use Start date if available, otherwise parsed date
    AddDate = Table.AddColumn(AddParsedDate, "Date", each
        let startDate = try Date.From([#"Start date"]) otherwise null
        in if startDate <> null then startDate else [ParsedDate], type date),

    // Rename columns
    RenamedColumns = Table.RenameColumns(AddDate, {
        {"TBM Manpower", "Headcount"},
        {"Direct Manpower", "DirectMP"},
        {"Indirect Manpower", "IndirectMP"}
    }),

    // Type conversions
    TypedColumns = Table.TransformColumnTypes(RenamedColumns, {
        {"Headcount", type number},
        {"DirectMP", type number},
        {"IndirectMP", type number}
    }),

    // Select final columns
    FinalColumns = Table.SelectColumns(TypedColumns, {
        "ID", "Title", "Company", "Date", "CountType", "Headcount", "DirectMP", "IndirectMP"
    }),

    // Filter to valid records
    FilteredValid = Table.SelectRows(FinalColumns, each [Date] <> null)
in
    FilteredValid
```

---

## Data Model

```
┌─────────────────┐
│   Date Table    │
└────────┬────────┘
         │ 1:*
    ┌────┴────────────────┐
    │                     │
    ▼                     ▼
┌──────────────┐    ┌──────────────┐
│ TBM Locations│    │ Manpower     │
│ (Planned +   │    │ Counts       │
│  Verified)   │    │ (Actual)     │
└──────────────┘    └──────────────┘
```

**Relationships:**
| From | To | Cardinality |
|------|-----|-------------|
| TBM Locations[Date] | Date[Date] | *:1 |
| Manpower Counts[Date] | Date[Date] | *:1 |

---

## DAX Measures

### Core LPI Measures

```dax
// TBM Planned = SUM of TBM Manpower from location records
TBM Planned =
SUM('TBM Locations'[PlannedMP])

// TBM Actual = SUM of Headcount from Manpower Count records
TBM Actual =
SUM('Manpower Counts'[Headcount])

// Verified = SUM of (Direct + Indirect) WHERE PlannedMP > 0
Verified =
CALCULATE(
    SUM('TBM Locations'[ActualMP]),
    'TBM Locations'[HasPlannedMP] = TRUE
)

// Unverified = SUM of (Direct + Indirect) WHERE PlannedMP = 0 or NULL
Unverified =
CALCULATE(
    SUM('TBM Locations'[ActualMP]),
    'TBM Locations'[HasPlannedMP] = FALSE
)

// Total Found = Verified + Unverified (people found at any work location)
Total Found = [Verified] + [Unverified]

// Not Found = Workers at morning TBM but not found at any location
Not Found = [TBM Actual] - [Total Found]

// Not Found % (of TBM Actual)
Not Found % = DIVIDE([Not Found], [TBM Actual], 0) * 100

// LPI % = Verified / TBM Planned
LPI % =
DIVIDE([Verified], [TBM Planned], 0) * 100

// LPI Display (formatted)
LPI Display = FORMAT([LPI %] / 100, "0.0%")
```

### LPI Status

```dax
// LPI Status for conditional formatting
LPI Status =
SWITCH(
    TRUE(),
    [LPI %] >= 80, "✓ On Target",
    [LPI %] >= 60, "⚠ Below Target",
    [LPI %] >= 40, "⚠ At Risk",
    "✗ Critical"
)

// LPI Status Color
LPI Color =
SWITCH(
    TRUE(),
    [LPI %] >= 80, "#28a745",  // Green
    [LPI %] >= 60, "#ffc107",  // Yellow
    [LPI %] >= 40, "#fd7e14",  // Orange
    "#dc3545"                   // Red
)
```

### Breakdown by Count Type

```dax
// TBM Actual - START only (morning headcount)
TBM Actual START =
CALCULATE(
    SUM('Manpower Counts'[Headcount]),
    'Manpower Counts'[CountType] = "START"
)

// TBM Actual - END only (end of day headcount)
TBM Actual END =
CALCULATE(
    SUM('Manpower Counts'[Headcount]),
    'Manpower Counts'[CountType] = "END"
)
```

### Idle Analysis Measures (from Checklists)

```dax
// Total Locations with Observations
Total Locations = COUNTROWS('TBM Locations')

// Locations with Active workers
Active Locations =
COUNTROWS(FILTER('TBM Locations', 'TBM Locations'[IsActive] = TRUE))

// Locations with Passive workers
Passive Locations =
COUNTROWS(FILTER('TBM Locations', 'TBM Locations'[IsPassive] = TRUE))

// Locations Obstructed
Obstructed Locations =
COUNTROWS(FILTER('TBM Locations', 'TBM Locations'[IsObstructed] = TRUE))

// Locations in Meeting
Meeting Locations =
COUNTROWS(FILTER('TBM Locations', 'TBM Locations'[IsMeeting] = TRUE))

// Locations with No Manpower
No Manpower Locations =
COUNTROWS(FILTER('TBM Locations', 'TBM Locations'[IsNoManpower] = TRUE))

// Locations Not Started
Not Started Locations =
COUNTROWS(FILTER('TBM Locations', 'TBM Locations'[IsNotStarted] = TRUE))

// Total Idle Locations
Idle Locations =
[Passive Locations] + [Obstructed Locations] + [Meeting Locations] +
[No Manpower Locations] + [Not Started Locations]
```

### Percentages

```dax
// Active Rate (of locations)
Active Rate % = DIVIDE([Active Locations], [Total Locations], 0) * 100

// Idle Rate (of locations)
Idle Rate % = DIVIDE([Idle Locations], [Total Locations], 0) * 100

// Verification Rate (different from LPI - based on locations, not manpower)
Verification Rate % =
DIVIDE(
    COUNTROWS(FILTER('TBM Locations', 'TBM Locations'[ActualMP] > 0 AND 'TBM Locations'[HasPlannedMP] = TRUE)),
    COUNTROWS(FILTER('TBM Locations', 'TBM Locations'[HasPlannedMP] = TRUE)),
    0
) * 100
```

### Cost Estimates

```dax
// Hourly Rate (parameter - could make this a What-If)
Hourly Rate = 50

// Hours per Day
Hours Per Day = 8

// Unverified Cost (workers planned but not observed)
Unverified Cost $ = [Unverified] * [Hours Per Day] * [Hourly Rate]

// Daily Unverified Cost
Daily Unverified Cost $ =
DIVIDE([Unverified Cost $], DISTINCTCOUNT('TBM Locations'[Date]), 0)
```

### Trend Measures

```dax
// LPI Previous Week
LPI % PW =
CALCULATE(
    [LPI %],
    DATEADD('Date'[Date], -7, DAY)
)

// Week over Week Change
LPI WoW Change = [LPI %] - [LPI % PW]

// 7-Day Moving Average
LPI 7D Avg =
AVERAGEX(
    DATESINPERIOD('Date'[Date], MAX('Date'[Date]), -7, DAY),
    [LPI %]
)
```

---

## Summary Table for Validation

Create this measure table to validate calculations match the weekly report:

```dax
// Summary Check Table (create as a calculated table or use SUMMARIZE)

Metric Summary =
UNION(
    ROW("Metric", "TBM Planned", "Value", [TBM Planned]),
    ROW("Metric", "TBM Actual (Total)", "Value", [TBM Actual]),
    ROW("Metric", "TBM Actual (START)", "Value", [TBM Actual START]),
    ROW("Metric", "TBM Actual (END)", "Value", [TBM Actual END]),
    ROW("Metric", "Verified", "Value", [Verified]),
    ROW("Metric", "Unverified", "Value", [Unverified]),
    ROW("Metric", "LPI %", "Value", [LPI %])
)
```

---

## Column Reference

| Column Index | Column Name | Used In | Purpose |
|--------------|-------------|---------|---------|
| 1 | ID | Both | Unique identifier |
| 2 | Title | Manpower Counts | Parse date and START/END |
| 3 | Status | TBM Locations | Filter: "TBM" |
| 4 | Category | Both | Filter: ≠"Manpower Count" or ="Manpower Count" |
| 6 | Start date | Both | Date of record |
| 11 | Tier 1 | TBM Locations | Grid location |
| 21 | Building | TBM Locations | FAB, SUP, etc. |
| 22 | Level | TBM Locations | Floor (1, 2, 3, 4) |
| 23 | Company | Both | Axios, Berg, MK Marlow |
| 28 | TBM Manpower | Both | **PlannedMP** (locations) or **Headcount** (counts) |
| 29 | Direct Manpower | TBM Locations | Workers actively working |
| 30 | Indirect Manpower | TBM Locations | Support workers |
| 103 | Active | TBM Locations | Checklist observation |
| 104 | Passive | TBM Locations | Checklist observation |
| 105 | Obstructed | TBM Locations | Checklist observation |
| 106 | Meeting | TBM Locations | Checklist observation |
| 107 | No Manpower | TBM Locations | Checklist observation |
| 108 | Work Has Not Started | TBM Locations | Checklist observation |

---

## Recommended Visuals

### 1. KPI Cards Row
| Card | Measure | Format | Conditional Color |
|------|---------|--------|-------------------|
| LPI % | [LPI %] | 0.0% | Yes (target 80%) |
| TBM Actual | [TBM Actual] | #,##0 | No |
| TBM Planned | [TBM Planned] | #,##0 | No |
| Verified | [Verified] | #,##0 | Green |
| Unverified | [Unverified] | #,##0 | Yellow (unexpected locations) |
| Not Found | [Not Found] | #,##0 | Red (workers missing) |

### 2. Planned vs Verified vs Actual (Clustered Column)
- X-Axis: Date or Company
- Y-Axis: [TBM Planned], [Verified], [TBM Actual]

### 3. LPI Trend Line
- X-Axis: Date
- Y-Axis: [LPI %]
- Reference Line: 80% target
- Secondary Line: [LPI 7D Avg]

### 4. LPI by Contractor (Bar Chart)
- Axis: Company
- Values: [LPI %]
- Sort: Descending
- Reference Line: 80%

### 5. Waterfall: Planned → Verified → Unverified
Shows the breakdown of where planned manpower ends up.

### 6. Contractor Performance Table
| Company | Planned | Actual | Verified | Unverified | LPI % |

---

## Slicers

1. **Date Range** - Date slicer
2. **Company** - Dropdown (Axios, Berg, MK Marlow)
3. **Building** - Dropdown (FAB, SUP, SUE, SUW)
4. **Level** - Dropdown (1F, 2F, 3F, 4F)
5. **Category** - Dropdown (Firestop, Drywall, etc.)

---

## Glossary

| Term | Definition |
|------|------------|
| **TBM** | Toolbox Meeting - daily morning planning session where workers are counted |
| **TBM Actual** | Headcount at morning TBM (total workers present at start of day) |
| **TBM Planned** | SUM of TBM Manpower from location records (where workers should be deployed) |
| **Verified** | Workers found at locations WITH planned manpower (PlannedMP > 0) |
| **Unverified** | Workers found at locations WITHOUT planned manpower (PlannedMP = 0 or NULL) |
| **Total Found** | Verified + Unverified (all workers found at any work location during audits) |
| **Not Found** | TBM Actual - Total Found (workers at morning TBM but not found at any location) |
| **LPI** | Labor Planning Index = Verified / TBM Planned (target: 80%) |
| **Direct Manpower** | Workers actively working at a location |
| **Indirect Manpower** | Support/overhead workers at a location |
| **PlannedMP** | TBM Manpower value (how many were supposed to be there) |
| **START** | Morning headcount record |
| **END** | End-of-day headcount record |

---

## Validation Checklist

Before publishing, verify:

1. **LPI Sanity:** Should be 0-100%, typically 20-50% based on current data
2. **TBM Planned > 0:** Should have records with planned manpower
3. **Not Found Check:** TBM Actual ≥ Total Found (Not Found should be ≥ 0)
4. **Date Range:** Confirm data covers expected period (Dec 2025 - Jan 2026)
5. **Company Coverage:** All 3 contractors should appear (Axios, Berg, MK Marlow)
6. **Cross-Reference:** Compare totals to weekly TBM report values

**Relationship Check:**
```
TBM Actual = Total Found + Not Found (must equal)
Total Found = Verified + Unverified (must equal)
TBM Actual ≥ Total Found (Not Found ≥ 0)
LPI % = Verified / TBM Planned (primary KPI)
```

**Worker Flow Diagram:**
```
Morning TBM (TBM Actual)
    │
    ├──► Found at work locations (Total Found)
    │       ├──► At PLANNED locations (Verified)
    │       └──► At UNPLANNED locations (Unverified)
    │
    └──► NOT found at any location (Not Found)
```
