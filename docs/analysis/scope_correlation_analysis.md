# Scope Correlation Analysis: Cross-Source Location Labeling Strategy

**Date:** 2025-12-10
**Purpose:** Define a standardized labeling system to correlate data across P6 schedules, weekly reports, quality documents, and other sources.

---

## Executive Summary

This analysis investigates how location references appear across different data sources to develop a **standardized location labeling system** that enables cross-source data correlation. The key finding is that **FAB room codes** (e.g., `FAB1-40063`) combined with a **coordinate system** (Building + Level + Gridline) provides the most robust correlation key.

---

## 1. Coordinate System Overview (From Drawings)

### 1.1 Grid System

Based on architectural drawings (FAB1A0-7001, FAB1A0-7002, FAB1A0-7302SP):

| Axis | Range | Description |
|------|-------|-------------|
| **Horizontal (Columns)** | 1-34 | East-West gridlines, numbered |
| **Vertical (Rows)** | A-N | North-South gridlines, lettered |
| **Sub-gridlines** | A.5, E.3, F.4, G.5, H.3, etc. | Intermediate gridlines |

### 1.2 Floor Levels

| Level Code | Description | Elevation |
|------------|-------------|-----------|
| B1F | Basement Level 1 | Below grade |
| 1F | Level 01 | Ground floor |
| 2F | Level 02 | +13'-8" |
| 3F | Level 03 | Main cleanroom |
| 3K | Level 03K | Interstitial/ceiling |
| 4F-6F | Upper levels | Mechanical/support |

### 1.3 Buildings

| Code | Full Name | Type |
|------|-----------|------|
| FAB | Main Fabrication Building | Primary |
| GCS-A | Gas/Chemical Supply Building A | Support |
| GCS-B | Gas/Chemical Supply Building B | Support |
| CUB | Central Utilities Building | Support |
| FIZ | Fab Integration Zone | Support |
| WSUP / W-SUP | West Support Building | Support |
| ESUP / E-SUP | East Support Building | Support |

### 1.4 Phase Designation

| Code | Description |
|------|-------------|
| PH1 / Phase 1 | Initial production phase |
| PH2 / Phase 2 | Expansion phase |

---

## 2. Data Source Analysis

### 2.1 P6 Schedule (Primavera)

**Location Data Availability:**

| Data Type | Records | Coverage |
|-----------|---------|----------|
| Tasks with Building in name | 169,906 | High |
| Tasks with Floor in name | 74,529 | High |
| Tasks with Gridline pattern | 9,575 | Medium |
| Activity codes (AREA, ZONE) | Available | Medium |

**Task Name Pattern:**
```
{Activity}, {Sub-location/Room}(PH{n}), {Grid}, {Floor}F, {Building}
```

**Examples:**
- `Process Exhaust hook-up, Toxic/Flammable Ph.1(A~C/4~5), 1F, GCS-A`
- `Drywall installation, WB-1, Elec. RM(PH1), 3F, WSUP, FAB`
- `Fire fighting pipe installation, WB-3, Elec. RM(PH1), 3F, WSUP, FAB`

**Activity Code Types for Location:**
- `AREA` - 36 codes (BFW, BGE, BCE, etc.)
- `ZONE` - 5 codes (ADMIN, GREEN, BLUE, RED, ORANGE)
- `N/S ZONE` - 2 codes (ZONE A, ZONE B)
- `STRUCTURE` - 8 codes (Junction boxes)
- `(HPMS) WBS 3 - Area Lv1/Lv2` - Area hierarchy
- `(HPMS) WBS 3 - Floor` - Floor designation
- `(HPMS) WBS 3 - Room` - Room codes

### 2.2 Weekly Reports

**Location References Found:**

| Pattern | Work Items | Issues |
|---------|------------|--------|
| FAB | 90 | 90 |
| FIZ | 28 | 35 |
| CUB | 2 | 0 |
| GCS | 1 | 1 |
| Level | 9 | - |

**Typical References:**
- Building names in prose (e.g., "FAB Building", "GCS area")
- Milestone areas (A1, B1)
- Pour designations (C2, D4, E3)
- SUW/SUE references (pit areas)

### 2.3 RFI Log

| Pattern | Occurrences |
|---------|-------------|
| FAB | 238 |
| Level | 175 |
| FIZ | 94 |
| Floor | 94 |
| Grid | 86 |

RFIs often reference specific locations: `"D6 Lenton terminator at 33 Line"`, `"SUE and SUW area"`

### 2.4 Submittal Log

| Pattern | Occurrences |
|---------|-------------|
| FAB | 357 |
| SUP | 20 |

Submittals are typically organized by CSI specification section, not location.

### 2.5 ProjectSight Labor Entries

| Field | Data Available |
|-------|----------------|
| Company | Yes (604K entries) |
| Trade | Yes (01 - General Requirements, 31 - Earthwork, 03 - Concrete) |
| Location | **No** - not captured in current data |

**Gap Identified:** Labor entries lack location tagging. Hours are tracked by person/company/trade but not by building or area.

---

## 3. FAB Room Coding System

From drawings, rooms have unique identifiers:

**Pattern:** `FAB1-XXXXX` where XXXXX is a 5-digit number

**Examples from drawings:**
- `FAB1-40063` - Room on Level 02
- `FAB1-40051` - Adjacent room
- `FAB1-40092` - Electrical room

These room codes appear in:
- Architectural drawings
- Door schedules (FAB1A0-0124, FAB1A0-0125)
- Interior finish schedules (FAB1A0-0112)

---

## 4. Proposed Standardized Location Label

### 4.1 Label Format

```
{BUILDING}-{LEVEL}-{GRID_AREA}[-{ROOM}]
```

**Components:**

| Component | Format | Values | Required |
|-----------|--------|--------|----------|
| BUILDING | 3-4 chars | FAB, GCS-A, GCS-B, CUB, FIZ, WSUP, ESUP | Yes |
| LEVEL | 2-3 chars | B1F, 1F, 2F, 3F, 3K, 4F, 5F, 6F | Yes |
| GRID_AREA | Variable | A-C/1-5, D-E/13-15, etc. | When available |
| ROOM | 5-digit | 40063, 40051, etc. | When available |

**Examples:**
- `FAB-3F-A~C/4~5` - FAB, Level 3, Grid A-C columns 4-5
- `GCS-A-1F-A~C/2~4` - GCS-A, Level 1, Grid A-C columns 2-4
- `FAB-2F-40063` - FAB, Level 2, Room 40063
- `CUB-1F` - CUB, Level 1 (no grid specified)

### 4.2 Extraction Rules

**From P6 Task Names:**
```python
import re

def extract_location(task_name):
    result = {}

    # Building
    building_match = re.search(r'\b(FAB|GCS-[AB]|GCS|CUB|FIZ|WSUP|W-SUP|ESUP|E-SUP)\b', task_name, re.I)
    if building_match:
        result['building'] = building_match.group(1).upper().replace('W-SUP','WSUP').replace('E-SUP','ESUP')

    # Floor
    floor_match = re.search(r'\b([B]?\d+[FK]?)\b', task_name)
    if floor_match:
        result['level'] = floor_match.group(1).upper()

    # Gridline
    grid_match = re.search(r'\(([A-Z]~[A-Z]/\d+~\d+)\)', task_name)
    if grid_match:
        result['grid'] = grid_match.group(1)

    # Phase
    phase_match = re.search(r'Ph\.?(\d)|PH(\d)', task_name)
    if phase_match:
        result['phase'] = f"PH{phase_match.group(1) or phase_match.group(2)}"

    return result
```

**From Free Text (Weekly Reports, RFIs):**
- Use NLP/regex patterns
- Build lookup table for common references:
  - "main FAB" → FAB
  - "Milestone Area A1" → map to grid
  - "pit area" → specific location TBD

---

## 5. Correlation Strategy

### 5.1 Primary Correlation Keys

| Level | Key | Coverage | Use Case |
|-------|-----|----------|----------|
| **Coarse** | Building only | 100% | High-level rollups |
| **Medium** | Building + Level | ~75% | Floor-level analysis |
| **Fine** | Building + Level + Grid | ~10% | Detailed area analysis |
| **Precise** | Room code | Limited | Specific room tracking |

### 5.2 Fallback Hierarchy

When exact location unavailable:
1. Extract building → aggregate to building level
2. Extract level → aggregate to building-level
3. Extract grid pattern → use as-is
4. No location → flag for manual review or exclude

### 5.3 Grid-to-Area Mapping

Create lookup table mapping gridlines to named areas:

| Grid Range | Area Name | Building |
|------------|-----------|----------|
| A~C/1~10 | West Zone | FAB |
| D~E/13~17 | Central Zone | FAB |
| A~C/28~34 | East Zone | FAB |
| A~C/1~5 | Toxic/Flammable | GCS-A |

This mapping enables inferring location from area names and vice versa.

---

## 6. Implementation Plan

### Phase 1: P6 Location Extraction (Week 1-2)

**Tasks:**
1. Create `location_parser.py` module with extraction functions
2. Process all tasks to extract location components
3. Generate `task_locations.csv` with:
   - task_id, building, level, grid, room, phase, raw_location_text
4. Validate extraction accuracy on sample
5. Document unmatched patterns for manual review

**Deliverable:** Enriched task data with standardized location fields

### Phase 2: Location Lookup Tables (Week 2-3)

**Tasks:**
1. Build master building/level/grid reference table
2. Map activity codes to standardized locations
3. Create area-to-grid mapping from drawings
4. Build room code lookup (from door schedules)

**Deliverable:** Reference tables for location standardization

### Phase 3: Weekly Report Location Tagging (Week 3-4)

**Tasks:**
1. Develop NLP patterns for weekly report text
2. Extract location mentions from work_progressing, key_issues
3. Tag RFIs and submittals with locations
4. Create manual review queue for ambiguous references

**Deliverable:** Weekly report data with location tags

### Phase 4: Cross-Source Correlation (Week 4-5)

**Tasks:**
1. Join P6 tasks with weekly report items by date + location
2. Correlate RFIs to schedule tasks by location + date
3. Build correlation quality metrics
4. Generate correlation report

**Deliverable:** Correlated dataset for analysis

---

## 7. Data Quality Considerations

### 7.1 Known Gaps

| Source | Gap | Impact | Mitigation |
|--------|-----|--------|------------|
| ProjectSight Labor | No location field | Cannot correlate hours to location | Aggregate by company only |
| Weekly Report Text | Informal references | Low extraction accuracy | Manual tagging + ML training |
| Submittals | CSI-coded, not location | Weak location signal | Link via RFI cross-references |

### 7.2 Accuracy Expectations

| Extraction Type | Expected Accuracy |
|-----------------|-------------------|
| P6 Task Name → Building | 95%+ |
| P6 Task Name → Level | 85% |
| P6 Task Name → Grid | 60% (when present) |
| Weekly Report → Building | 70% |
| RFI → Location | 75% |

### 7.3 Validation Approach

1. Sample 100 tasks per building, manually verify extraction
2. Cross-check P6 activity codes vs task name extraction
3. Review weekly report items where no location found
4. Build exception report for unmatched patterns

---

## 8. Expected Outcomes

### 8.1 Enabled Analyses

Once location correlation is complete:

1. **Delay by Location**: Which buildings/levels have most delays?
2. **Productivity by Area**: Labor hours per task by location
3. **Issue Clustering**: Are problems concentrated in specific areas?
4. **Progress Tracking**: Work completion by building/floor over time
5. **Root Cause**: Correlate delays with RFIs/issues by location

### 8.2 Power BI Integration

Location fields enable:
- Building/Level slicers
- Heatmaps by grid area
- Drill-down from building → level → grid → room
- Cross-filter between schedule, issues, and labor

---

## 9. Appendix: Sample Extractions

### P6 Task Examples

| Task Name | Building | Level | Grid | Phase |
|-----------|----------|-------|------|-------|
| Process Exhaust hook-up, Toxic/Flammable Ph.1(A~C/4~5), 1F, GCS-A | GCS-A | 1F | A~C/4~5 | PH1 |
| Drywall installation, WB-1, Elec. RM(PH1), 3F, WSUP, FAB | FAB | 3F | - | PH1 |
| Architecture Work, Toxic/Flammable Ph.1(A~C/4~5), 1F, GCS-A | GCS-A | 1F | A~C/4~5 | PH1 |
| VLF Test, WA-1, Elec. RM(PH1), 2F, WSUP, FAB | FAB | 2F | - | PH1 |

### Building Reference Counts (P6)

| Building | Task Count |
|----------|------------|
| FAB | 62,241 |
| GCS (total) | 34,435 |
| GCS-A | 15,174 |
| GCS-B | 8,134 |
| CUB | 18,363 |
| WSUP | 14,154 |
| ESUP | 12,333 |
| FIZ | 2,927 |

---

## 10. Next Steps

1. **Review this document** with stakeholders for alignment
2. **Confirm grid-to-area mapping** with project team
3. **Begin Phase 1** - P6 location extraction script
4. **Identify additional drawings** if room-level correlation needed
5. **Determine priority** - building-level vs room-level correlation

---

*Generated by Claude Code analysis of Samsung Taylor FAB1 project data*
