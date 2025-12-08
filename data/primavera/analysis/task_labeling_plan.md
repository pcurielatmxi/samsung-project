# Task Labeling Plan - YATES Schedule

**Created:** 2025-12-08
**Status:** Proposed - Awaiting Approval

## Executive Summary

Analysis of the YATES schedule (66 files, 12,433 tasks in latest version) reveals rich labeling data across multiple sources. This plan proposes extracting and standardizing labels into a new `task_labels` table for consistent analysis.

## Available Data Sources

### 1. Activity Codes (Primary Source)

**Coverage:** 100% of tasks have at least one activity code assigned.

| Activity Type | Coverage | Description |
|--------------|----------|-------------|
| **Z-TRADE** | 47.8% | Trade/work type (74 values) |
| **Z-AREA** | 61.4% | Location area (166 values) |
| **Z-BLDG** | 37.2% | Building (10 values) |
| **Z-LEVEL** | 58.9% | Floor level (77 values) |
| **Z-ROOM** | 20.6% | Room name (99 values) |
| **Z-SUB CONTRACTOR** | 27.5% | Subcontractor (23 values) |
| **Z-BID PACKAGE** | 21.9% | Bid package (17 values) |
| **Z-RESPONSIBLE** | 35.8% | Responsible person (11 values) |

**Consistency:** Z-TRADE, Z-AREA, Z-BLDG, Z-LEVEL present in all 66 YATES files.

### 2. WBS Hierarchy (Secondary Source)

**Coverage:** 100% of tasks linked to WBS nodes.

WBS provides hierarchical location context with 505 unique nodes containing tasks. Key patterns:
- Area-level nodes: SEA-1 through SEA-5, SEB-1 through SEB-5, SWA-1 through SWA-5, SWB-1 through SWB-5
- Building nodes: FAB, SUP, FIZ (Data Center)

### 3. Task Code Structure (Tertiary Source)

**Format:** `TYPE.AREA.SEQUENCE`

| First Segment | Tasks | Meaning |
|--------------|-------|---------|
| CN | 6,321 (51%) | Construction |
| INT | 336 (3%) | Interior |
| STAIR | 172 (1%) | Stairs |
| FIZ | 109 (1%) | Data Center |
| PC | 81 (1%) | Precast |
| MS | 59 (0.5%) | Milestone |

Second segment encodes area (SWA5, SEB1, SEA5, etc.) with 546 unique values.

### 4. Task Name Patterns (Supplementary)

Embedded information extractable via regex:
- **Level references:** L1 (844), L2 (846), L3 (1,102), L4 (1,012), L5 (33), L6 (214)
- **Gridline references:** 4,661 tasks (37%)
- **Area codes:** SWB, SWA, SEA, SEB patterns

## Building/Area Taxonomy

Based on analysis, the project has this physical structure:

```
Samsung Taylor FAB1
├── FAB Building (Gridlines 3-33)
│   ├── Team A - South (Gridlines 3-17)
│   └── Team B - North (Gridlines 17-33)
├── Support Building - West (SUW/SWA/SWB)
│   ├── SWA-1 through SWA-5 (South quadrants)
│   └── SWB-1 through SWB-5 (North quadrants)
├── Support Building - East (SUE/SEA/SEB)
│   ├── SEA-1 through SEA-5 (South quadrants)
│   └── SEB-1 through SEB-5 (North quadrants)
├── Data Center (FIZ, Gridlines 1-3)
│   ├── FIZ1 (West Inner)
│   └── FIZ2-4 (Other zones)
└── Penthouses
    ├── NE Penthouse
    ├── NW Penthouse
    ├── SE Penthouse
    └── SW Penthouse
```

## Proposed Label Schema

### Output Table: `generated/task_labels.csv`

| Column | Type | Description | Source Priority |
|--------|------|-------------|-----------------|
| file_id | int | XER file reference | - |
| task_id | str | Task reference | - |
| building | str | FAB, SUP_EAST, SUP_WEST, DATA_CENTER | Z-BLDG → WBS → task_code |
| area | str | Standardized area (SEA1-5, SWA1-5, etc.) | Z-AREA → WBS → task_code |
| level | str | L1-L6, ROOF, PENTHOUSE | Z-LEVEL → task_name |
| gridlines | str | Gridline range (e.g., "19-22") | task_name pattern |
| trade | str | Standardized trade category | Z-TRADE |
| subcontractor | str | Company name | Z-SUB CONTRACTOR |
| task_type | str | CONSTRUCTION, MILESTONE, PRECONSTRUCTION | Z-PHASE → task_code |
| room | str | Room name if applicable | Z-ROOM |
| responsible | str | Responsible person | Z-RESPONSIBLE |

### Standardization Mappings (Examples)

**Building Standardization:**
```
FAB, Fab Building → FAB
SUP, SUP-E, SUE, Support East → SUP_EAST
SUP-W, SUW, Support West → SUP_WEST
FIZ, Data Center → DATA_CENTER
```

**Area Standardization:**
```
SEA - 1, SEA1, SUEN1 → SEA1
SWB - 5, SWB5 → SWB5
NE, NORTH EAST PENTHOUSE → NE_PENTHOUSE
```

**Trade Standardization:**
```
03 CONC, Cast-in Place Concrete → CONCRETE
05 Steel Erect, Steel Erector → STEEL_ERECTION
ARCH, TBD Architectural Finishes → ARCHITECTURAL
```

## Implementation Plan

### Phase 1: Build Extraction Script
1. Create `scripts/extract_task_labels.py`
2. Implement activity code extraction with type priority
3. Implement WBS hierarchy traversal for fallback
4. Implement task code/name pattern extraction

### Phase 2: Create Standardization Mappings
1. Extract all unique values from each source
2. Create mapping CSV files in `data/primavera/mappings/`
3. Review and refine mappings with domain knowledge

### Phase 3: Generate Labels
1. Process YATES files (start with latest, then all)
2. Output to `data/primavera/generated/task_labels.csv`
3. Generate coverage report

### Phase 4: Iterate and Refine
1. Analyze gaps in label coverage
2. Add extraction rules for edge cases
3. Expand to SECAI schedule

## Questions for Review

1. **Building granularity:** Should SUP_EAST/SUP_WEST be separate or combined as SUPPORT?
2. **Area granularity:** Keep SEA1-5 detail or aggregate to SEA (south) vs SEB (north)?
3. **Trade priority:** Use Z-TRADE or Z-SUB CONTRACTOR when both available?
4. **Multi-value handling:** Some tasks span multiple areas - keep as comma-separated list?

## Next Steps

1. Review and approve this plan
2. Create initial standardization mappings
3. Implement extraction script for single file
4. Validate output against manual inspection
5. Scale to all YATES files
