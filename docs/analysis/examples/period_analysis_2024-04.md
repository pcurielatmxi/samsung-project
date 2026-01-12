# Schedule Delay Attribution Analysis
## Period: February 9, 2024 → April 8, 2024

**Analysis Date:** January 2026
**Analyst:** MXI
**Data Source:** YATES P6 Schedule Snapshots (file_id 51 → 52)

---

## Executive Summary

During this 2-month period, the project end date slipped **108 days** (from August 6, 2024 to November 23, 2024). This is the largest single-period slip observed in the project timeline.

**Key Finding:** This is a classic **execution delay pattern**, not schedule compression. 81% of affected tasks show FORWARD_PUSH (delays propagating forward through the network), indicating that work took longer than planned rather than deadlines being tightened.

**Primary Causes Identified:**
1. Design changes and RFIs (IMPACT tasks)
2. Procurement delays (VIP Room glass - 4-week lead time issue)
3. Systemic elevator steel installation delays
4. Significant schedule restructuring (2,639 new predecessor relationships)

---

## Step 1: Project-Level Impact Assessment

### Raw Metrics

| Metric | Value |
|--------|-------|
| Previous Snapshot Date | February 9, 2024 |
| Current Snapshot Date | April 8, 2024 |
| Previous Project End | August 6, 2024 |
| Current Project End | November 23, 2024 |
| **Project Slippage** | **+108 days** |
| Tasks Compared | 4,597 common tasks |
| Tasks Added | 616 new tasks |
| Tasks Removed | 229 tasks |

### How to Interpret

The **project_slippage_days = +108** tells us the project end date moved 108 days later. This is a significant slip - more than 3 months in a single update period.

The fact that 616 tasks were added suggests scope growth or schedule decomposition occurred during this period. This is a red flag that warrants investigation.

**Question raised:** Why were 616 tasks added? Are these new scope items, or existing work being broken into more detail?

---

## Step 2: Float Driver Analysis

### Raw Metrics

| Float Driver | Task Count | Percentage |
|--------------|------------|------------|
| FORWARD_PUSH | 3,717 | 80.9% |
| BACKWARD_PULL | 729 | 15.9% |
| MIXED | 19 | 0.4% |
| NONE | 132 | 2.9% |

### How to Interpret

**FORWARD_PUSH (80.9%)** means that for most tasks, the float decreased because the early finish date moved later (not because the late finish moved earlier). This happens when:
- A task's own duration increased
- A predecessor delayed, pushing this task's start later

**BACKWARD_PULL (15.9%)** means some tasks had their late dates pulled earlier, but this is a minority. If this were dominant (like the Nov 2022 period at 75%), it would indicate schedule compression from deadline pressure.

**Conclusion:** This period shows **execution delays**, not schedule compression. Work is taking longer than planned.

**Why this matters:**
- If BACKWARD_PULL dominated → investigate if deadlines were changed or if the schedule is being compressed unrealistically
- Since FORWARD_PUSH dominates → investigate why tasks are taking longer (productivity, rework, scope changes, resource issues)

---

## Step 3: Enhanced Category Analysis

### Raw Metrics

| Category | Task Count | Description |
|----------|------------|-------------|
| COMPLETED_OK | 2,968 | Finished on time |
| INHERITED_LOGIC_CHANGE | 564 | Delayed by new predecessors |
| INHERITED_FROM_PRED | 563 | Delayed by upstream work |
| DUAL_SQUEEZE | 180 | Compressed from both directions |
| WAITING_OK | 133 | Waiting, no impact |
| ACTIVE_OK | 132 | In progress, on track |
| CAUSE_PLUS_INHERITED | 33 | Both own delay and inherited |
| SQUEEZED_FROM_SUCC | 23 | Pulled by downstream |

### How to Interpret

**COMPLETED_OK (2,968 tasks):** Good news - the majority of compared tasks finished on schedule. This means the delays are concentrated, not pervasive.

**INHERITED_LOGIC_CHANGE (564 tasks):** This is significant. These tasks were delayed because **new predecessor relationships were added**. This happens when:
- New work items were inserted before existing tasks
- The scheduler identified coordination requirements that weren't previously captured
- RFIs or design changes created new dependencies

**INHERITED_FROM_PRED (563 tasks):** These tasks are delayed because their existing predecessors delayed. They are "victims" of upstream delays, not causes.

**CAUSE_PLUS_INHERITED (33 tasks):** These are critical - they both caused delay AND received delay. These tasks are in trouble zones where multiple issues compound.

**Conclusion:** The high LOGIC_CHANGE count (564 tasks) suggests significant schedule restructuring, likely due to design changes or coordination issues discovered during execution.

**Question raised:** What drove the 2,639 new predecessor relationships? Were these RFIs, change orders, or coordination issues?

---

## Step 4: Root Cause Tracing

### Raw Metrics

| Metric | Value |
|--------|-------|
| Root Cause Tasks | 666 |
| Propagated Tasks | 642 |
| Cause Type: LOGIC_CHANGE | 897 (68%) |
| Cause Type: DURATION | 242 (18%) |
| Cause Type: UNKNOWN | 169 (13%) |

### How to Interpret

**Root cause tasks (666)** originated the delays. These are the tasks where the delay started, before propagating to others.

**Propagated tasks (642)** inherited delays from root causes. These are victims, not causes.

**LOGIC_CHANGE (68%)** - The majority of root causes are classified as logic changes, meaning new predecessors were added that caused the delay. This is consistent with the high INHERITED_LOGIC_CHANGE count in the category analysis.

**DURATION (18%)** - These are tasks where the task's own duration growth explains the delay. These are execution issues.

**Conclusion:** The delay pattern is primarily driven by schedule restructuring (new dependencies), with some execution issues (duration growth).

**Question raised:** Are the LOGIC_CHANGE root causes legitimate scope additions, or are they workarounds for problems not being properly documented?

---

## Step 5: Top Delay Contributors Analysis

### Top 10 Own-Delay Contributors

| Rank | Task Code | Task Name | Own Delay | Inherited | Float Δ | Category |
|------|-----------|-----------|-----------|-----------|---------|----------|
| 1 | FIZ.WEST.2311 | IMPACT [C29/S12] - RFI 6488 - Added Structural Steel | +56d | +57d | -121d | CAUSE_PLUS_INHERITED |
| 2 | INT.1500 | Submittal and Fabrication - VIP Room Glass Window (4-week lead time) | +46d | +57d | -106d | CAUSE_PLUS_INHERITED |
| 3 | CN.SWB5.6330 | IMPACT - CCD 184 - Corrugated Metal Panels | +33d | +129d | -209d | INHERITED_LOGIC_CHANGE |
| 4 | CN.SWA1.6600 | IMPACT [S.TIA-9.5] - Bent Plate Cutout GL14 | +24d | +57d | +260d | CAUSE_PLUS_INHERITED |
| 5 | CN.SEB4.190 | PH1 - Install Elevator Steel - SWA-5 - Elevator 01B | +22d | +48d | -28d | INHERITED_LOGIC_CHANGE |
| 6 | CN.SEB4190 | PH1 - Install Elevator Steel - SWA-5 - Elevator 01 | +19d | +29d | -28d | INHERITED_LOGIC_CHANGE |
| 7 | CN.SWB54670 | Install Elevator Steel - SWB-5 - All Levels | +18d | +55d | -61d | INHERITED_LOGIC_CHANGE |
| 8 | CN.SEB3.1340 | AD Waterproofing - SEB3 - GL 25-28 | +18d | -12d | +10d | DUAL_SQUEEZE |
| 9 | CN.SEA4.1340 | AD Waterproofing - SEA4 - GL 9-6 | +15d | +52d | -46d | INHERITED_LOGIC_CHANGE |
| 10 | CN.SWA5.1521 | PH1 - Install Elevator Steel - SWA-5 - Elevator 01A | +15d | +51d | -77d | INHERITED_LOGIC_CHANGE |

### How to Interpret Each Column

**Own Delay:** Days the task's duration increased beyond what was planned in the previous snapshot. A +56d own_delay means this task now takes 56 days longer than it did in February.

**Inherited:** Days of delay pushed onto this task by its predecessors. A +57d inherited means this task's start was pushed 57 days later by upstream delays.

**Float Δ:** Change in total float. A -121d means this task lost 121 days of schedule buffer. Negative values indicate increased schedule risk.

**Category:**
- CAUSE_PLUS_INHERITED = Task both caused delay AND received delay (compounding problem)
- INHERITED_LOGIC_CHANGE = Delayed because new predecessors were added

### Pattern Recognition

**Pattern 1: IMPACT Tasks**
Tasks with "IMPACT" in their name appear repeatedly:
- FIZ.WEST.2311: RFI 6488 - Structural Steel (+56d)
- CN.SWB5.6330: CCD 184 - Corrugated Metal Panels (+33d)
- CN.SWA1.6600: Bent Plate Cutout (+24d)

**What this means:** "IMPACT" tasks in P6 typically represent change orders, RFIs, or design modifications. The presence of multiple high-delay IMPACT tasks indicates design changes are a primary driver of the 108-day slip.

**Question raised:** What are RFI 6488 and CCD 184? These specific changes need investigation to understand the root cause.

**Pattern 2: Elevator Steel Cluster**
Multiple elevator-related tasks appear:
- CN.SEB4.190: Elevator 01B (+22d)
- CN.SEB4190: Elevator 01 (+19d)
- CN.SWB54670: Elevator SWB-5 (+18d)
- CN.SWA5.1521: Elevator 01A (+15d)

**What this means:** When multiple related tasks delay simultaneously with similar delay values, it suggests a systemic issue:
- Common resource constraint (same crew, same equipment)
- Design issue affecting all elevators
- Predecessor issue affecting the entire scope

**Question raised:** Why did multiple elevator steel tasks delay? Is this a design issue, resource issue, or coordination issue?

**Pattern 3: Procurement Issue**
Task INT.1500 stands out:
- Name: "VIP Room Glass Window (4-week lead time)"
- Own delay: +46 days
- Category: CAUSE_PLUS_INHERITED

**What this means:** The task name explicitly mentions "4-week lead time," suggesting procurement timing. A 46-day delay on a procurement task indicates:
- Material was ordered late
- Supplier delayed delivery
- Submittals were not approved in time
- Original lead time estimate was wrong

**Question raised:** Was the 4-week lead time accurate? When was the submittal approved? When was the material ordered vs. needed?

---

## Step 6: Investigation Checklist

Based on the analysis above, the following items require investigation:

### Investigation Item 1: RFI 6488 (Structural Steel)

**Task:** FIZ.WEST.2311
**Impact:** +56 days own delay, 121 days float consumed

**Why this needs investigation:**
- This is the #1 delay contributor
- Task name indicates it's an IMPACT (change order/RFI)
- "Added Structural Steel" suggests scope addition
- The combination of high own_delay (+56d) AND high inherited (+57d) means this task both caused problems and was affected by upstream issues

**Documents to collect:**
- [ ] RFI 6488 submission and response
- [ ] Structural drawings before/after change
- [ ] Cost impact documentation
- [ ] Any associated change order

**Questions to answer:**
1. What triggered RFI 6488?
2. Was this a design error, owner request, or field condition?
3. How much additional steel was required?
4. Who is responsible for the delay - designer, owner, or contractor?

---

### Investigation Item 2: CCD 184 (Corrugated Metal Panels)

**Task:** CN.SWB5.6330
**Impact:** +33 days own delay, +129 days inherited, 209 days float consumed

**Why this needs investigation:**
- Task shows massive inherited delay (+129d) indicating it's at the end of a long delay chain
- The CCD (likely "Contract Change Directive") number suggests formal change order
- This task consumed 209 days of float - extremely significant

**Documents to collect:**
- [ ] CCD 184 documentation
- [ ] Corrugated metal panel submittals
- [ ] Predecessor task analysis (what caused +129d inherited?)

**Questions to answer:**
1. What was CCD 184?
2. Why did predecessors delay this task by 129 days?
3. Is the 33-day own_delay due to the CCD or execution issues?

---

### Investigation Item 3: Elevator Steel Delays

**Tasks:** CN.SEB4.190, CN.SEB4190, CN.SWB54670, CN.SWA5.1521
**Impact:** +15 to +22 days own delay each, multiple elevators affected

**Why this needs investigation:**
- Multiple elevator tasks delayed simultaneously suggests systemic issue
- Similar delay values (15-22 days) across different elevators indicates common cause
- All are INHERITED_LOGIC_CHANGE, meaning new predecessors were added

**Documents to collect:**
- [ ] Elevator steel fabrication submittals
- [ ] Elevator steel delivery logs
- [ ] Crew assignment records for this period
- [ ] New predecessor relationships added (what tasks were inserted?)

**Questions to answer:**
1. Were new predecessors added to all elevator tasks? What were they?
2. Was there a design change affecting elevator steel?
3. Was there a resource constraint (crew, equipment)?
4. Was elevator steel fabrication delayed?

---

### Investigation Item 4: VIP Room Glass Procurement

**Task:** INT.1500
**Impact:** +46 days own delay, 106 days float consumed

**Why this needs investigation:**
- Task name explicitly mentions "4-week lead time"
- 46-day delay suggests lead time was underestimated or procurement was late
- This is a submittal/fabrication task, so the delay will cascade to installation

**Documents to collect:**
- [ ] VIP Room glass submittal log
- [ ] Submittal approval dates
- [ ] Purchase order date vs. required-on-site date
- [ ] Supplier delivery confirmation

**Questions to answer:**
1. When was the submittal approved?
2. Was the 4-week lead time based on supplier confirmation or assumption?
3. When was the glass actually delivered vs. planned?
4. Who selected/specified this glass?

---

### Investigation Item 5: Schedule Restructuring (2,639 New Relationships)

**Impact:** 564 tasks categorized as INHERITED_LOGIC_CHANGE

**Why this needs investigation:**
- 2,639 new predecessor relationships is massive restructuring
- 1,520 relationships were also removed
- This suggests major schedule revision, not incremental updates

**Documents to collect:**
- [ ] Schedule narrative from this period
- [ ] List of new predecessor relationships (from analysis output)
- [ ] Any schedule recovery plan documentation
- [ ] Meeting minutes discussing schedule changes

**Questions to answer:**
1. Was there a formal schedule re-baseline during this period?
2. Were the new predecessors due to discovered coordination needs or scope changes?
3. Is this legitimate schedule refinement or "schedule manipulation"?

---

## Step 7: Conclusions

### Primary Causes of 108-Day Slip

| Cause | Evidence | Contribution |
|-------|----------|--------------|
| **Design Changes (RFIs/CCDs)** | Multiple IMPACT tasks as top delayers; RFI 6488, CCD 184 | High |
| **Schedule Restructuring** | 2,639 new relationships; 68% of root causes are LOGIC_CHANGE | High |
| **Procurement Issues** | VIP Glass 46-day delay; lead time mentioned in task name | Medium |
| **Elevator Steel Systemic Issue** | 4 elevator tasks delayed 15-22 days each | Medium |

### Accountability Assessment

Based on the analysis, the 108-day slip appears to have multiple contributing factors:

1. **Design Changes (RFIs):** If RFI 6488 was due to design errors, the designer may bear responsibility. If due to owner-requested changes, the owner may bear responsibility.

2. **Procurement:** If the VIP glass delay was due to late ordering or unrealistic lead times, this may be contractor responsibility.

3. **Schedule Restructuring:** The massive logic changes need explanation. If these represent legitimate coordination needs discovered during execution, they may be contractor responsibility. If they represent scope additions, responsibility depends on the source.

### Risk Assessment

The 180 tasks in DUAL_SQUEEZE category are high-risk - they're being compressed from both directions and have minimal buffer. These should be monitored closely in subsequent periods.

---

## Appendix: Methodology Notes

### How Float Driver is Calculated

```
float_change = float_curr - float_prev
early_push = early_end_curr - early_end_prev  (positive = pushed later)
late_pull = late_end_prev - late_end_curr     (positive = pulled earlier)

if early_push > late_pull + 1 day:
    float_driver = FORWARD_PUSH
elif late_pull > early_push + 1 day:
    float_driver = BACKWARD_PULL
elif early_push > 0 or late_pull > 0:
    float_driver = MIXED
else:
    float_driver = NONE
```

### How Root Cause is Determined

A task is marked as ROOT_CAUSE if:
1. Its own_delay explains ≥80% of its float decrease, OR
2. Its constraint was tightened, OR
3. It has new predecessors that explain the delay

Tasks that don't meet these criteria but have float decrease are marked as PROPAGATED and traced upstream to find the actual root cause.

### Data Sources Used

- P6 Schedule Export (file_id 51): February 9, 2024 snapshot
- P6 Schedule Export (file_id 52): April 8, 2024 snapshot
- Relationship data from taskpred.csv
- Task data from task.csv

---

*Analysis performed using schedule_slippage_analysis.py v2.0*
*Methodology documented in docs/analysis/delay_attribution_methodology.md*
