# Samsung Taylor FAB1 Performance Analysis

**Prepared for:** Samsung Leadership | **Prepared by:** MXI | **Date:** December 2025

---

## Executive Summary

Samsung has engaged MXI to conduct a **data-driven analysis** of the Taylor FAB1 project's schedule delays and labor consumption.

### Central Question

**What is the impact of quality issues and inefficiencies on the project's schedule and labor consumption?**

This analysis examines four dimensions:

1. **Scope Evolution** — How much scope was added due to rework or coordination issues?

2. **Delay Attribution** — How much schedule impact resulted from quality and performance issues?

3. **Resource Consumption** — How much labor was consumed and what factors drove consumption?

4. **Quality Impact** — What quality issues occurred and what rework did they drive?

| Question | How We Will Answer |
|----------|-------------------|
| **1. Scope Evolution** | Compare 66 YATES schedule snapshots to track task growth over time. Cross-reference with weekly reports to categorize additions (rework, coordination issues, design changes). Reconcile against change orders when available. |
| **2. Delay Attribution** | Track completion date movement across snapshots. Correlate with 1,108 documented issues from weekly reports. Identify which activities and trades were involved when delays occurred. |
| **3. Resource Consumption** | Track labor hours by contractor, trade, and date. Correlate labor consumption with documented issues and rework tasks. Quantify labor during problem periods. |
| **4. Quality Impact** | Compile NCR and inspection reports by trade and location. Link quality issues to rework tasks in schedule. Quantify rework duration and labor consumed. Supplement with MXI field documentation. |

**Why This Matters:** This analysis provides Samsung with a factual understanding of project performance, enabling informed decisions for the remainder of the project. All findings will be traceable to contemporaneous source documents.

**Data Sources:** 66 YATES schedule versions, 37 weekly progress reports, 10,789 RFI/submittal entries, 13,205 labor records, TBM daily work plans, quality documentation (NCRs, inspections, MXI field observations), and contract documents (bid documents, RFP, change orders — pending collection).

---

## Analysis Goals & Status

Each goal follows standard data analysis phases:

| Phase | Description |
|-------|-------------|
| **1. Collection** | Gather and inventory source data |
| **2. Preparation** | Parse, clean, normalize, and validate data |
| **3. Exploration** | Initial analysis to understand patterns and quality |
| **4. Analysis** | Deep investigation, correlation, root cause identification |
| **5. Conclusions** | Findings, attribution, quantified impacts |
| **6. Reporting** | Dashboards, reports, evidence packages |

---

### Goal 1: Scope Evolution Analysis
**Purpose:** Quantify how the project scope changed from baseline to current, distinguishing Samsung-directed changes from contractor-caused growth (rework, coordination failures, planning deficiencies).

**Measurable Outcomes:**
- Baseline task count vs. current task count by contractor/trade
- Percentage breakdown of scope growth by root cause category
- List of contractor-caused scope additions with cost/schedule impact

| Data Source | Collection | Preparation | Exploration | Analysis | Conclusions | Reporting |
|-------------|:----------:|:-----------:|:-----------:|:--------:|:-----------:|:---------:|
| YATES Schedules (66 versions) | ✅ | ✅ | ✅ | 🔄 | ⬜ | ⬜ |
| Weekly Reports - Scope Changes | ✅ | ✅ | ✅ | 🔄 | ⬜ | ⬜ |
| RFI/Submittal Logs | ✅ | ✅ | 🔄 | ⬜ | ⬜ | ⬜ |
| Bid Documents / RFP | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| Change Orders | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |

---

### Goal 2: Delay Attribution Analysis
**Purpose:** Identify root causes of schedule delays, determine which contractors' activities drove critical path slippage, and distinguish excusable delays from contractor-caused delays.

**Measurable Outcomes:**
- Timeline of delay events with days of impact
- Delay attribution by contractor (days and percentage)
- Critical path evolution showing when/why completion date moved
- Concurrent delay assessment

| Data Source | Collection | Preparation | Exploration | Analysis | Conclusions | Reporting |
|-------------|:----------:|:-----------:|:-----------:|:--------:|:-----------:|:---------:|
| YATES Schedules - Critical Path | ✅ | ✅ | 🔄 | ⬜ | ⬜ | ⬜ |
| Weekly Reports - Issues (1,108) | ✅ | ✅ | ✅ | 🔄 | ⬜ | ⬜ |
| Activity Code Analysis | ✅ | ✅ | ✅ | 🔄 | ⬜ | ⬜ |
| Weather Records | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |

---

### Goal 3: Resource Consumption Analysis
**Purpose:** Quantify labor consumption by contractor, trade, and location, and correlate with documented issues and rework.

**Measurable Outcomes:**
- Total labor hours by contractor and trade over time
- Labor consumption correlated with documented issues
- Rework labor hours quantified (repeated work in same locations)
- Labor spikes tied to specific problem periods

| Data Source | Collection | Preparation | Exploration | Analysis | Conclusions | Reporting |
|-------------|:----------:|:-----------:|:-----------:|:--------:|:-----------:|:---------:|
| Primavera Resource Assignments | ✅ | ✅ | 🔄 | ⬜ | ⬜ | ⬜ |
| Weekly Reports - Manpower | ✅ | ✅ | ✅ | ⬜ | ⬜ | ⬜ |
| Labor Detail Records | ✅ | ✅ | 🔄 | ⬜ | ⬜ | ⬜ |
| TBM Daily Work Plans | ✅ | ✅ | ✅ | ⬜ | ⬜ | ⬜ |
| Contractor Invoices/Cost Data | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |

---

### Goal 4: Quality Impact Analysis
**Purpose:** Document quality issues by trade and location, and quantify the rework they drove.

**Measurable Outcomes:**
- Quality issues documented by trade and location
- Rework tasks linked to quality issues
- Schedule delay days attributed to quality issues
- Labor hours consumed by quality-driven rework

| Data Source | Collection | Preparation | Exploration | Analysis | Conclusions | Reporting |
|-------------|:----------:|:-----------:|:-----------:|:--------:|:-----------:|:---------:|
| NCR Reports | 🔄 | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| Inspection Reports | 🔄 | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| Weekly Reports - Quality Issues | ✅ | ✅ | ✅ | 🔄 | ⬜ | ⬜ |
| MXI Field Documentation | 🔄 | 🔄 | ⬜ | ⬜ | ⬜ | ⬜ |
| Rework Task Identification | ✅ | ✅ | 🔄 | ⬜ | ⬜ | ⬜ |

---

### Supporting Analysis: WBS Taxonomy Classification
**Purpose:** Standardized classification system enabling consistent tracking across all data sources and roll-up reporting by building, level, trade, and contractor.

**Status:** ✅ Complete — 333,560 tasks classified (99.95% coverage)

---

## Status Legend

| Symbol | Meaning |
|:------:|---------|
| ✅ | Complete |
| 🔄 | In Progress |
| ⬜ | Not Started |

---

## Next Priority Actions

1. Obtain and process NCR and Inspection reports
2. Complete schedule evolution analysis across snapshots
3. Finalize delay attribution methodology
4. Analyze labor consumption patterns by contractor/trade
5. Develop Power BI executive dashboards

---

*Status updated: December 2025*
