# Grid-Based BI Model for Location Filtering

**Date:** 2026-01-27
**Status:** Proposed
**Context:** Simplifying location filtering in Power BI for quality/TBM data

---

## Problem

The current location model has two parallel systems:

1. **`dim_location_id`** (FK on facts) - Single "best match" location
2. **`affected_rooms_bridge`** (many-to-many) - All rooms overlapping event's grid bounds

This creates confusion:
- `dim_location_id` semantics vary (sometimes a room, sometimes a level/gridline fallback)
- Multi-room events don't filter properly via room slicers
- Bridge table answers questions users aren't asking

## Key Insight

**Source data is fundamentally grid-based.** Inspectors enter locations as grid coordinates (e.g., "B-D / 8-12, Level 2F"). Rooms are a derived mapping we impose afterward.

Users think in both grids and rooms, but grids match the source data granularity.

## Proposed Model

### New Dimension: `dim_grid_cell`

| Column | Type | Example |
|--------|------|---------|
| grid_cell_id | INT (PK) | 1 |
| level | VARCHAR | 2F |
| grid_row | CHAR | G |
| grid_col | INT | 10 |
| display_name | VARCHAR | 2F-G/10 |

Size: ~400 cells × ~7 levels = ~2,800 rows

### New Bridge: `grid_coverage_bridge`

Explodes each event to all grid cells it covers.

| Column | Type | Description |
|--------|------|-------------|
| source | VARCHAR | RABA, PSI, TBM |
| source_id | VARCHAR | FK to fact table |
| level | VARCHAR | 1F, 2F, etc. |
| grid_row | CHAR | A-N |
| grid_col | INT | 1-33 |

**Example:** Event at "2F, B-D / 8-10" becomes 9 rows (3 rows × 3 cols).

### BI Relationships

```
┌─────────────┐      ┌─────────────────────┐      ┌───────────────┐
│ fact_raba   │      │ grid_coverage_bridge│      │ dim_grid_cell │
│ fact_psi    ├──────┤                     ├──────┤               │
│ fact_tbm    │      │ source_id ──────────│      │ level         │
└─────────────┘      │ level               │      │ grid_row      │
                     │ grid_row            │      │ grid_col      │
                     │ grid_col            │      └───────────────┘
                     └─────────────────────┘
```

Keep `dim_location_id` on facts for single-room cases and labeling.

## What Changes

| Artifact | Action |
|----------|--------|
| `affected_rooms_bridge.csv` | Retire (delete or archive) |
| `affected_rooms` JSON column | Keep for reference, not used in BI |
| `dim_location_id` on facts | Keep for single-room matches |
| `dim_location` table | Keep for room names/context |
| `grid_coverage_bridge.csv` | **New** - generate this |
| `dim_grid_cell.csv` | **New** - generate this |

## Heatmap Visualization

Power BI Matrix visual with conditional formatting:

- **Rows:** Grid Row (A-N)
- **Columns:** Grid Column (1-33)
- **Values:** Inspection count or failure rate
- **Slicer:** Level (1F, 2F, etc.)
- **Color scale:** By measure value (density/rate)

### Sample DAX Measures

```dax
Inspection Count =
DISTINCTCOUNT(grid_coverage_bridge[source_id])

Failure Rate =
DIVIDE(
    CALCULATE([Inspection Count], fact_raba[outcome] = "FAIL"),
    [Inspection Count]
)
```

## Room Context (Optional)

A lookup panel showing which rooms correspond to the selected grid area:

> "Selected area (2F, G-J / 8-12) contains: FAB116201, FAB116202, FAB116205"

This uses `dim_location` grid bounds for reverse lookup. It's informational, not a filter.

## Benefits

| Benefit | Explanation |
|---------|-------------|
| Matches source data | No false precision from room mapping |
| Clean heatmaps | Regular grid cells, not irregular room shapes |
| Works with incomplete mapping | 86% room coverage doesn't limit analysis |
| Intuitive for field users | They enter data in grid coordinates |
| Simpler model | One bridge table instead of two systems |

## Implementation Steps

1. Check grid coverage stats (% of events with usable grid data)
2. Generate `dim_grid_cell.csv`
3. Generate `grid_coverage_bridge.csv`
4. Update Power BI model
5. Build heatmap visualization
6. Retire `affected_rooms_bridge`

## Open Questions

- Should grid columns be integers or allow decimals (e.g., 10.5)?
- How to handle events with only partial grid info (row but no column)?
- Include building in grid cell, or unified grid across buildings per CLAUDE.md?
