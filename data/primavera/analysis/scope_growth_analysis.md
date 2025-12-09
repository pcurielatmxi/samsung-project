# YATES Schedule Scope Growth Analysis

**Analysis Date:** December 2024
**Analyst:** MXI
**Data Sources:** YATES P6 Schedules (65 versions), Weekly Reports (37 reports)

---

## Executive Summary

This analysis examines the scope growth in the YATES construction schedule from October 2022 to June 2023, correlating schedule changes with contemporaneous weekly project reports to identify root causes.

**Key Finding:** The YATES schedule grew from **840 tasks (Oct 2022) to ~2,700 tasks (Jun 2023)** — a **220% increase**. This growth was primarily driven by:

1. **Support Buildings (SUP) scope elaboration** — design not finalized until early 2023
2. **FIZ Retrofit decision** — major design change in March 2023
3. **Production acceleration** — fabrication rates increased requiring detailed tracking
4. **Quality/rework requirements** — precast defects created additional task sequences
5. **Subcontractor mobilizations** — new crews requiring task-level tracking

---

## Data Sources

### YATES Schedule Versions (Overlap Period)

| Data Date | Task Count | Growth | File |
|-----------|------------|--------|------|
| 2022-10-10 | 840 | — | YATES-T FAB1 schedule - 10-10-22 - To SECAI.xer |
| 2022-11-07 | 1,053 | +25% ★ | YATES-T FAB1 schedule - 11-07-22 - To SECAI.xer |
| 2023-01-30 | 1,805 | +35% ★ | YATES-T FAB1 schedule update - 01-30-23 - To SECAI.xer |
| 2023-03-26 | 2,299 | +27% ★ | YATES-T FAB1 schedule update - 3-26-23 - To SECAI.xer |
| 2023-06-18 | 2,712 | — | YATES-T FAB1 schedule - 06-18-23.xer |

### Weekly Reports Coverage

- **37 reports** from August 21, 2022 to June 12, 2023
- Located in: `data/raw/weekly_reports/`
- Parsed data in: `data/weekly_reports/tables/`

---

## Timeline of Scope Growth Events

### Period 1: October-November 2022 — Initial Scope Definition

**Schedule Growth:** 840 → 1,053 tasks (+25%)

During this period, the initial structural schedule was being established with focus on precast fabrication and erection sequences.

#### Evidence from Weekly Reports:

> "Precast Fabrication – Production holding up erection. a. Goal Dates i. G-Columns 17/18 10/14 ii. Girders 17-18 11/1 iii. E-Line 17/18 11/1 iv. C&L Columns 11/1 b. Waffle Slab Clearance issues came to light c. Column issues from Jacobs Review with Corbel heights d. Upper Columns at Fab pending design..."
>
> — *[Taylor Fab1] Weekly Reports for Week of 20221016.pdf*

> "Laydown Areas & Crane Access – Still working with SECAI to finalize details on laydown areas and in particular which areas will be paved or not."
>
> — *[Taylor Fab1] Weekly Reports for Week of 20221016.pdf*

**Interpretation:** The early schedule focused on FAB building structural work. Support Buildings scope was not yet detailed.

---

### Period 2: January 2023 — Acceleration & HEI Engagement

**Schedule Growth:** 1,336 → 1,805 tasks (+35%)

A significant growth event coinciding with acceleration efforts and engagement of additional precast fabricators.

#### Evidence from Weekly Reports:

> "Support Buildings Design Change Due to Equipment Loads — Design changing and resequencing of work as such. Schedule sent to Tindall disregarding fabrication production rates..."
>
> — *[Taylor Fab1] Weekly Reports for Week of 0123.pdf*

> "Raker Waffle Fabrication Production — Fabrication production = 6/wk. Raker production boosted to 15-18/wk. 6 Rakers onsite - 1 installed. Erection production = 12/wk (working both North and South)..."
>
> — *[Taylor Fab1] Weekly Reports for Week of 0123.pdf*

> "Heldenfels FAB Columns — HEI currently repairing and recasting columns. Numerous FAB columns not yet released for fabrication..."
>
> — *[Taylor Fab1] Weekly Reports for Week of 0123.pdf*

> "SUP's C&L Columns — Erection waiting for fabrication production since start..."
>
> — *[Taylor Fab1] Weekly Reports for Week of 0123.pdf*

> "Tindal, Coreslab, Gate, HEI – SECAI believed to no longer be desiring to novate these due to production issues..."
>
> — *[Taylor Fab1] Weekly Reports for Week of 0123.pdf*

**Interpretation:**
- Support Buildings design was changing due to equipment load requirements
- Precast fabrication rates were being accelerated (6/week → 15-18/week)
- Multiple precast fabricators (HEI, Tindall, Coreslab, Gates) engaged
- These changes required more detailed task tracking in the schedule

---

### Period 3: March 2023 — FIZ Retrofit Decision (Key Inflection Point)

**Schedule Growth:** 1,805 → 2,299 tasks (+27%)

The most significant scope change occurred in March 2023 with the FIZ retrofit decision.

#### Evidence from Weekly Reports:

> "FIZ Steel – Secai/SAS made decision to retrofit, which allows us to proceed, however still awaiting on final design of retrofit..."
>
> — *[Taylor Fab1] Weekly Reports for Week of 20230313.pdf*

> "SUP Roof Delay – IFC Drawings released for both SUE and SUW. Tindall now working on generating shop tickets..."
>
> — *Taylor Fab1 Weekly Reports for Week of 20230306.pdf*

> "SUP Roof Delay – Still awaiting Level 4 Roof Beams, but exploring if we can hurry and get L3 decks in while waiting..."
>
> — *[Taylor Fab1] Weekly Reports for Week of 20230313.pdf*

> "HEI requested prioritization of FAB and FIZ columns. We told them adamantly Gridline 3, EUV, FIZ..."
>
> — *Taylor Fab1 Weekly Reports for Week of 20230306.pdf*

> "DT Slabs – Meeting held and getting ready to start working on DT slabs and raker slabs as soon as areas available..."
>
> — *Taylor Fab1 Weekly Reports for Week of 20230306.pdf*

**Interpretation:**
- The FIZ retrofit decision fundamentally changed the structural steel scope
- Support Buildings roof design was still being finalized (SUE/SUW IFC drawings just released)
- New work packages (DT Slabs, raker slabs) being initiated
- These design decisions required new task sequences in the schedule

---

### Period 4: April-June 2023 — Interior Work Emerging

**Schedule:** ~2,700 tasks (stabilized)

Interior finish work began appearing in weekly reports, signaling the next phase of scope elaboration.

#### Evidence from Weekly Reports:

> "Subcontracting a. Patriot b. W&W Erection c. BERG Drywall d. FD Thomas (Upper Truss Intumescent & Touch Up) e. Novated Package WW AFCO f. Doors & Hardware g. Buck hoist – Lewis & Crane..."
>
> — *[Taylor Fab1] Weekly Reports for Week of 20230403.pdf*

> "Drywall & Insulation..."
>
> — *[Taylor Fab1] Weekly Reports for Week of 20230403.pdf*

> "Interiors Budget..."
>
> — *[Taylor Fab1] Weekly Reports for Week of 20230403.pdf*

**Interpretation:**
- Interior trades (BERG Drywall, FD Thomas fireproofing) being subcontracted
- Interior budgets being established
- This foreshadows the massive interior scope growth seen in later schedule versions

---

## Quality Issues Contributing to Scope Growth

Quality and rework issues created additional task sequences for engineering review, repairs, and re-sequencing.

#### Evidence from Weekly Reports:

> "Precast NCR's — Column corbels reinforcement RFI 765. Column cut this weekend to ship to Texas A&M for load testing. Metro waffle load test to be completed by 1/31/23..."
>
> — *[Taylor Fab1] Weekly Reports for Week of 0123.pdf*

> "Heldenfels FAB Columns — HEI currently repairing and recasting columns. Numerous FAB columns not yet released for fabrication..."
>
> — *[Taylor Fab1] Weekly Reports for Week of 0123.pdf*

> "FIZ Precast Columns — Drawings not issued for approval yet. 2 columns now issued for approval..."
>
> — *[Taylor Fab1] Weekly Reports for Week of 0123.pdf*

---

## Root Cause Analysis

| Root Cause | Evidence | Impact on Schedule |
|------------|----------|-------------------|
| **Delayed Design Finalization** | SUP equipment loads changing design; L3/L4 roof beams still evolving in March | New task sequences added as designs finalized |
| **FIZ Retrofit Decision** | "Secai/SAS made decision to retrofit" (Mar 2023) | Fundamentally changed structural scope |
| **Production Acceleration** | Raker production 6/wk → 15-18/wk | More granular task tracking required |
| **Quality/Rework** | HEI "repairing and recasting columns"; NCRs for corbel reinforcement | Additional engineering and rework tasks |
| **Subcontractor Additions** | HEI, Infinity, BERG Drywall mobilized | Workforce tracking tasks added |

---

## Key Insight

The 220% schedule growth from October 2022 to June 2023 was **NOT primarily from new square footage of construction**. Rather, it resulted from:

1. **Scope Maturation** — High-level summary schedule → detailed task-level schedule
2. **Design Finalization** — Support Buildings and FIZ decisions requiring task decomposition
3. **Production Tracking** — Accelerated fabrication rates requiring granular sequencing
4. **Quality Management** — Precast defects creating engineering and rework tasks
5. **Resource Mobilization** — New subcontractors requiring task-level tracking

**The October 2022 baseline was a summary-level schedule, not a complete project plan.** The growth reflects progressive elaboration as scope was detailed, designs were finalized, and production was accelerated.

---

## Post-June 2023 Growth Events (Data Gap)

Weekly reports end in June 2023, but additional growth events occurred:

| Date | Growth | Tasks | Notes |
|------|--------|-------|-------|
| Oct 2023 | +82% | 3,716 → 6,764 | No weekly report coverage |
| Jul 2025 | +66% | 7,392 → 12,245 | No weekly report coverage |

These later growth events likely represent similar patterns (interior finishes elaboration), but we lack weekly report narrative to confirm root causes.

---

## Recommendations

1. **For delay analysis:** Use a later baseline (e.g., June 2023 at ~2,700 tasks) that includes more complete scope rather than the October 2022 summary schedule

2. **For scope analysis:** Track growth by trade category — the Trade Activity Code analysis shows Interior Finishes grew from 6% to 44% of schedule duration

3. **For root cause attribution:** Cross-reference specific RFIs and submittals mentioned in weekly reports with schedule task additions

---

## Data File References

| Data Source | Location |
|-------------|----------|
| YATES Schedule Versions | `data/primavera/processed/task.csv`, `xer_files.csv` |
| Weekly Reports (Raw) | `data/raw/weekly_reports/*.pdf` |
| Weekly Reports (Parsed) | `data/weekly_reports/tables/*.csv` |
| Key Issues | `data/weekly_reports/tables/key_issues.csv` |
| Work Progress | `data/weekly_reports/tables/work_progressing.csv` |

---

*Analysis prepared by MXI for Samsung Taylor FAB1 delay analysis project.*
