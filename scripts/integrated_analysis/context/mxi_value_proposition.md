# MXI Value Proposition for SECAI

**Prepared for:** SECAI Cost Management
**Date:** January 2026
**Engagement:** Forensic Schedule & Data Analysis

---

## Executive Summary

MXI's forensic analysis services provide SECAI with **data-driven leverage** to negotiate contractor claims. Our engagement fee of ~$240K (2 months) delivers potential savings of **$1M-$5M+** through rigorous claims analysis:

1. **Claims Analysis & Discrepancy Identification** - Forensic review of Yates' $100M+ pending claims to identify overlapping charges, duplications, and misattributed costs
2. **Cancelled Inspection Backcharges** - Data-supported backcharges for Yates-caused inspection cancellations

**Conservative ROI Estimate: 4:1 to 20:1**

---

## Value Opportunity #1: Claims Analysis & Discrepancy Identification

### The Exposure

Yates has submitted significant change order and EOT claims:

| Category | Amount | Status |
|----------|--------|--------|
| SAS/SEC Approved | $108,284,628 | Executed |
| SAS/SEC CCD Approved | $61,003,687 | Executed |
| **SECAI Reviewed (not approved)** | **$36,744,003** | Under Review |
| **SECAI In Review** | **$63,452,091** | Under Review |
| To Be Submitted | $6,133,523 | Pending |
| **Total Under Review/Pending** | **$106,329,617** | Target for Analysis |

Yates' EOT claim attributes **728 days to SECAI** and **0 days to Yates**.

### MXI Analysis Objectives

MXI's forensic data analysis will systematically review the pending claims to identify:

| Analysis Type | Description | Example Findings |
|---------------|-------------|------------------|
| **Overlap Detection** | IOCCs covering same scope under different justifications | Acceleration costs claimed under both "night shift" and "extended duration" |
| **Double-Dipping** | Same costs charged through multiple line items | GC overhead in both GR/GC line AND individual IOCC markups |
| **Misattribution** | SECAI-attributed delays actually caused by Yates/subs | Quality failures causing rework delays attributed to "design changes" |
| **Excessive Markup** | Compounded percentages beyond contract terms | Fee applied to already-marked-up subcontractor changes |
| **Unsupported Claims** | Claims lacking adequate documentation | Time extensions without corresponding schedule impact |

### Potential Savings Estimate

| Reduction Rate | Savings (on $100M under review) | Assessment |
|----------------|--------------------------------|------------|
| **1%** | **$1.0M** | Highly achievable - basic duplicate detection |
| **2%** | **$2.0M** | Achievable - overlap + misattribution findings |
| **3%** | **$3.0M** | Likely - comprehensive discrepancy analysis |
| **5%** | **$5.0M** | Possible - significant quality-to-delay correlation findings |

**Rationale for 1-5% Range:**
- The claims include 300+ individual IOCCs - overlap is statistically likely
- Acceleration claims ($15M+) often overlap with extended duration claims
- Quality failures (1,000+ inspection failures) correlate with rework costs being claimed
- GC markup structure (Fee + Subguard + Bond + Misc = 8.76%) compounds across claim layers

### Specific Analysis Deliverables

| Deliverable | Description |
|-------------|-------------|
| **IOCC Cross-Reference Matrix** | Map IOCCs to identify scope overlap |
| **Delay-Quality Correlation Report** | Link quality failures to claimed delays |
| **Acceleration Audit** | Reconcile night shift/acceleration claims against schedule progress |
| **Markup Verification** | Validate fee application against contract terms |
| **Schedule Impact Validation** | Independent P6 analysis of delay causation |

---

## Value Opportunity #2: Cancelled Inspection Backcharges

### Yates Cancelled Inspections

MXI has consolidated quality inspection data from PSI (Construction Hive) system:

| Metric | Count |
|--------|-------|
| Total PSI Inspections | 6,309 |
| Yates Cancelled Inspections | 111 |

### Backcharge Calculation

At **$700 per cancelled inspection** (SECAI QC standard rate):

| Category | Count | Backcharge Amount |
|----------|-------|-------------------|
| Yates Cancelled Inspections | 111 | **$77,700** |

*Note: This is Yates direct only. If Yates subcontractors (Berg, MK Marlow, etc.) are included under Yates' responsibility, the total increases to 282 cancellations ($197,400).*

---

## MXI Data Assets Supporting Analysis

MXI has consolidated and normalized data enabling cross-source verification:

| Data Source | Records | Analysis Value |
|-------------|---------|----------------|
| P6 Schedules | 66 snapshots, 964K tasks | Independent delay causation analysis |
| RABA/PSI Quality | 15.7K inspections | Quality failure attribution |
| Change Order Docs | $275M in claims | IOCC-level detail for overlap detection |
| ProjectSight Labor | 857K entries | Verify extended duration claims |
| Weekly Reports | 37 reports | Contemporaneous documentation |
| Narratives | 108 documents | Claims statements for cross-reference |

### Key Analytical Capabilities

| Capability | Application |
|------------|-------------|
| **Schedule Slippage Analysis** | Quantify delays by task, challenge attribution |
| **Float Consumption Tracking** | Identify where Yates consumed schedule buffer |
| **Quality-to-Schedule Correlation** | Link inspection failures to rework delays |
| **IOCC Text Analysis** | Semantic search across claim descriptions |
| **Cost Category Mapping** | Classify IOCCs by root cause |

---

## Engagement ROI Summary

| Component | Amount |
|-----------|--------|
| **MXI Engagement Cost** | $240,000 |
| | |
| **Conservative Savings (1% claim reduction)** | $1,000,000 |
| **+ Cancelled Inspection Backcharges** | $77,700 |
| **Conservative Total** | **$1,077,700** |
| **Conservative ROI** | **4.5:1** |
| | |
| **Moderate Savings (3% claim reduction)** | $3,000,000 |
| **+ Cancelled Inspection Backcharges** | $77,700 |
| **Moderate Total** | **$3,077,700** |
| **Moderate ROI** | **12.8:1** |
| | |
| **Optimistic Savings (5% claim reduction)** | $5,000,000 |
| **+ Cancelled Inspection Backcharges** | $77,700 |
| **Optimistic Total** | **$5,077,700** |
| **Optimistic ROI** | **21:1** |

---

## Additional Scope: Field Tracking (Idle Time)

*Note: Not part of current forensic analysis scope but available from MXI field tracking services.*

Idle time identification can further support:
- Challenging productivity assumptions in acceleration claims
- Demonstrating subcontractor inefficiency
- Supporting counter-arguments for extended duration claims

---

## Recommended Immediate Actions

1. **Claims Discrepancy Analysis** - MXI to systematically review pending IOCCs for overlap, duplication, and misattribution

2. **Schedule Causation Review** - Independent analysis of BRG's delay attribution methodology using MXI P6 snapshot data

3. **Quality-Delay Correlation** - Map quality failures to specific delay claims to identify Yates-caused impacts

4. **Cancelled Inspection Documentation** - Compile detailed listing of 111 Yates cancelled inspections with dates and supporting data

---

## Source Data

All figures derived from MXI-consolidated data:
- Schedule data: `processed/primavera/`
- Quality data: `processed/raba/`, `processed/psi/`
- Claims data: `context/claims/MASTER_SUMMARY.md`
- Labor data: `processed/projectsight/`

---

*Prepared by MXI for SECAI internal use. Savings estimates are conservative projections based on typical claims analysis outcomes.*
