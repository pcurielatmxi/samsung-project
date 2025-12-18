# Schedule Slippage Analysis Plan

**Created:** 2025-12-18
**Status:** Planning
**Data Source:** YATES P6 Schedule Exports (72 versions, Oct 2022 - Nov 2025)

---

## Executive Summary

This document outlines the approach for analyzing schedule slippage in the Samsung Taylor FAB1 project using Primavera P6 schedule version comparisons. Since the YATES schedule lacks a true baseline, we will track changes across 72 schedule versions to assess slippage attribution.

**Key Challenge:** The schedule grew from 843 tasks to 12,433 tasks (14.7x) over the project timeline, making direct task-level comparison across the full timeline limited to only 759 persistent tasks (6% of current schedule).

**Recommended Approach:** Start with milestone-level analysis (stable, meaningful) then drill down to task-level for critical path analysis.

---

## Data Characteristics

### Schedule Versions

| Metric | Value |
|--------|-------|
| Total XER files | 72 |
| Date range | Oct 2022 - Nov 2025 |
| First version tasks | 843 |
| Latest version tasks | 12,433 |
| Growth factor | 14.7x |

### Task-Level Stability

| Metric | Value | Implication |
|--------|-------|-------------|
| Consecutive version persistence | 99.0% mean | Very stable version-to-version |
| Full timeline persistence | 90% (759 of 843) | Good for longitudinal tracking |
| Task code consistency | 60% have minor name variations | Cosmetic only (spacing, punctuation) |

### WBS-Level Stability

| Metric | Value | Implication |
|--------|-------|-------------|
| WBS persistence | 98.3% mean | Slightly less stable than tasks |
| Average tasks per WBS | 63 | High aggregation |
| Within-WBS variance | 4.7 days | Significant heterogeneity |
| Between-WBS variance | 1.0 days | WBS hides task-level variation |

**Finding:** Task-level analysis adds value - within-WBS variance is 5x larger than between-WBS variance.

### Critical Path Characteristics

| Metric | Value |
|--------|-------|
| Critical tasks (latest) | 1,289 (float <= 0) |
| Near-critical (0-10d float) | 1,026 |
| CP persistence (consecutive) | 73% mean |
| Unique tasks ever critical | 8,642 |
| Persistently critical (30+ versions) | ~50 tasks |

---

## Milestone Analysis Findings

### Milestone Stability

| Metric | First (Oct 2022) | Latest (Nov 2025) |
|--------|------------------|-------------------|
| Total milestones | 47 | 137 |
| Persistent | - | 39 (83% of original) |
| Added | - | 98 |
| Removed | - | 8 |

### Why 98 Milestones Were Added

| Category | Count | % | Description |
|----------|-------|---|-------------|
| **Decomposition** | 47 | 48% | TCO phases, elevator turnovers, priority areas |
| **Coordination** | 28 | 29% | Drawing issuance (IFC/IFR), RFI tracking |
| **New Scope** | 11 | 11% | IMP panels, repairs, scope completion |
| **Impact Tracking** | 3 | 3% | Delay notices, scope changes |
| **Other** | 9 | 9% | Miscellaneous |

**Conclusion:** ~88% of added milestones are NOT new scope - they are decomposition or coordination tracking.

### Completed Milestone Slippage (Original Plan vs Actual)

| Metric | Value |
|--------|-------|
| Count | 34 completed |
| Mean slippage | +49 days (1.6 months) |
| Median slippage | 0 days |
| Range | -142 to +467 days |

**Biggest Delays (Completed):**
- SUP WEST Structural Steel DSAD Topout: +467 days
- FAB Building Dry In: +457 days
- SUP EAST Structural Steel DSAD Topout: +445 days

### Project Completion Milestones (Not Started)

| Milestone | Original Plan | Current Forecast | Slip |
|-----------|--------------|------------------|------|
| Substantial Completion | Sep 2023 | May 2026 | +32.6 months |
| Shell Completion | Jul 2023 | Feb 2026 | +32.3 months |
| Structural Steel Complete | Apr 2023 | Nov 2025 | +31.7 months |
| SUP East Dry In | Aug 2023 | Feb 2026 | +30.6 months |
| SUP West Dry In | Aug 2023 | Jan 2026 | +29.3 months |

### When Did Slippage Occur?

Major slippage accumulation periods identified:

| Period | Slip Added | Notes |
|--------|-----------|-------|
| Nov 2022 | +70-85 days | Initial re-planning |
| Oct-Nov 2023 | +100-200 days | Scope clarification |
| Jan-Feb 2024 | +150-165 days | Major re-baseline |
| Jun 2024 | +80-190 days | Schedule update |
| Sep 2025 | +250-320 days | Recent slippage |

---

## Recommended Analysis Approach

### Phase 1: Milestone-Level Analysis

**Scope:** Track 39 persistent milestones across all 72 versions.

**Outputs:**
1. Milestone date progression table (version-by-version)
2. Cumulative slippage chart per milestone
3. Slippage velocity analysis (days slipped per month)
4. Correlation with weekly report events

**Key Fields:**
- `early_end_date` - Calculated forecast (CPM)
- `act_end_date` - Actual completion (for completed milestones)
- `target_end_date` - Planned date (baseline changes)

**Script:** `analyze/milestone_slippage.py`

### Phase 2: Critical Path Task Analysis

**Scope:** Track tasks with `total_float_hr_cnt <= 0` version-by-version.

**Outputs:**
1. Critical path composition changes over time
2. Tasks that drove the most slippage (above-median date changes)
3. Persistent critical tasks (on CP in 30+ versions)
4. Critical path entry/exit analysis

**Key Metrics:**
- `early_end_date` change between consecutive versions
- Float degradation (tasks becoming critical)
- Slippage contribution (individual task date change vs median)

**Script:** `analyze/critical_path_slippage.py`

### Phase 3: Full Task-Level Analysis

**Scope:** Track all 759 persistent tasks from Oct 2022 baseline.

**Outputs:**
1. Per-task slippage from original plan to actual/current
2. Slippage by WBS category
3. Slippage by trade/building/location
4. Identify task groups with similar slippage patterns

**Limitations:**
- Only 6% of current tasks existed in first schedule
- New tasks inherit context slippage
- Progressive elaboration confounds analysis

**Script:** `analyze/task_slippage.py`

### Phase 4: Slippage Attribution

**Goal:** Attribute schedule slippage to root causes.

**Approach:**
1. Correlate milestone/task slippage with weekly report issues
2. Map slippage periods to documented events (RFIs, scope changes, weather)
3. Categorize by attribution:
   - Owner-caused (design changes, late decisions)
   - Contractor-caused (performance, quality)
   - External (weather, supply chain)
   - Coordination (interface issues)

**Data Sources:**
- P6 schedule versions (this analysis)
- Weekly reports (parsed issues with dates)
- Quality records (rework events)

---

## Data Requirements

### Input Tables (from `processed/primavera/`)

| Table | Key Fields |
|-------|------------|
| `xer_files.csv` | file_id, date, schedule_type |
| `task.csv` | task_code, early_end_date, act_end_date, total_float_hr_cnt, status_code |
| `projwbs.csv` | wbs_short_name, tier columns |

### Derived Data (from `derived/primavera/`)

| Table | Purpose |
|-------|---------|
| `task_taxonomy.csv` | Trade, building, location classification |

### Output Location

```
data/analysis/primavera/
├── milestone_slippage/
│   ├── milestone_timeline.csv       # Date progression per milestone
│   ├── milestone_summary.csv        # Slippage summary per milestone
│   └── slippage_periods.csv         # Major slippage events
├── critical_path_slippage/
│   ├── cp_composition.csv           # CP tasks per version
│   ├── cp_drivers.csv               # Tasks driving slippage
│   └── cp_persistence.csv           # Persistently critical tasks
└── task_slippage/
    ├── persistent_task_slippage.csv # 759 persistent tasks
    └── slippage_by_category.csv     # Aggregated by WBS/trade/location
```

---

## Script Structure

```
scripts/primavera/
├── analyze/                          # NEW - Slippage analysis scripts
│   ├── milestone_slippage.py         # Phase 1: Milestone tracking
│   ├── critical_path_slippage.py     # Phase 2: CP analysis
│   ├── task_slippage.py              # Phase 3: Task-level analysis
│   └── slippage_attribution.py       # Phase 4: Root cause correlation
├── docs/
│   └── schedule_slippage_analysis.md # This document
├── process/                          # Existing - XER parsing
└── derive/                           # Existing - Task taxonomy
```

---

## Key Considerations

### What Constitutes "Slippage"?

1. **For incomplete tasks:** Compare `early_end_date` across versions
2. **For completed tasks:** Compare `act_end_date` vs original `early_end_date`
3. **Target date changes:** Track `target_end_date` changes (plan revisions)

### Handling Schedule Recalculation

P6 recalculates all dates with each update. Between consecutive versions:
- 98-99% of tasks have `early_end_date` changes
- This is NOT noise - it reflects CPM recalculation
- Focus on tasks with ABOVE-MEDIAN slippage as drivers

### Progressive Elaboration vs Scope Creep

- 94% of current tasks didn't exist in first schedule
- Most growth is detail elaboration (1 summary task -> many detailed tasks)
- New tasks inherit the delay context when created
- Track WHEN tasks were added to assess inherited delay

### Critical Path Volatility

- Only 73% of CP tasks persist between consecutive versions
- 8,642 unique tasks have been critical at some point
- Focus on persistently critical tasks (30+ versions) for attribution

---

## Next Steps

1. [ ] Implement `analyze/milestone_slippage.py` - Phase 1
2. [ ] Generate milestone timeline and slippage summary
3. [ ] Correlate with weekly report events
4. [ ] Implement `analyze/critical_path_slippage.py` - Phase 2
5. [ ] Implement `analyze/task_slippage.py` - Phase 3
6. [ ] Integrate with weekly reports for attribution - Phase 4

---

## References

- [XER File Overlap Analysis](../../../docs/analysis/primavera/xer_file_overlap_analysis.md)
- [Scope Growth Analysis](../../../docs/analysis/primavera/scope_growth_analysis.md)
- [Schedule Weekly Correlation](../../../docs/analysis/primavera/schedule_weekly_correlation.md)
- [Data Sources](../../../docs/SOURCES.md)
