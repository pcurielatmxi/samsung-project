# Weekly Reports Analysis Findings

**Generated:** 2025-12-08
**Source:** 37 Weekly Report PDFs (Aug 2022 - Jun 2023)

## Executive Summary

Weekly reports from Yates Construction provide narrative summaries of project status, documenting key issues, work progress, and procurement status throughout the Taylor FAB1 construction project.

### Data Extracted

**Narrative Sections** (pages 1-25):

| Table | Records | Description |
|-------|---------|-------------|
| `weekly_reports.csv` | 37 | Report metadata |
| `key_issues.csv` | 1,108 | Open issues and blockers |
| `work_progressing.csv` | 1,039 | Work progress by discipline |
| `procurement.csv` | 374 | Buyout/procurement items |

**Addendum Data Dumps** (pages 25+):

| Table | Records | Description |
|-------|---------|-------------|
| `addendum_files.csv` | 37 | Addendum extraction metadata |
| `addendum_rfi_log.csv` | 3,849 | RFI entries from ProjectSight export |
| `addendum_submittal_log.csv` | 6,940 | Submittal entries from ProjectSight export |
| `addendum_manpower.csv` | 613 | Daily labor hours by company |
| `labor_detail.csv` | 13,205 | Individual worker entries (name, classification, trade, hours) |

### Coverage

- **Date Range:** 2022-08-21 to 2023-06-12 (10 months)
- **Primary Author:** Glen Young (Project Executive) - 29 of 37 reports
- **Other Authors:** Mark Hammond (Quality Director), Dongsoo Kim (Project Manager)

---

## Issue Analysis

### Issue Categories

Based on keyword analysis of 1,108 extracted issues:

| Category | Count | % of Issues | Notes |
|----------|-------|-------------|-------|
| Precast/Columns | 317 | 28.6% | Highest category - column production, truss setting |
| Concrete/Slabs | 275 | 24.8% | Slab pours, rebar issues |
| Steel/Erection | 230 | 20.8% | Steel erection, crane access |
| Procurement | 175 | 15.8% | Buyout delays, LOI issues |
| Delays/Schedule | 129 | 11.6% | Explicit schedule delays |
| Design Issues | 113 | 10.2% | Drawing quality, RFIs |
| Fabrication | 85 | 7.7% | Production rate issues |
| Weather | 26 | 2.3% | Weather-related delays |
| Safety | 8 | 0.7% | Safety incidents |

### Key Contractors Mentioned in Issues

| Entity | Mentions | Role |
|--------|----------|------|
| **SECAI** | 118 | Owner's Engineering (most mentioned) |
| Baker | 58 | Concrete contractor |
| HEI/Heldenfels | 56 | Precast fabricator |
| Patriot | 35 | Misc steel |
| SNS | 31 | Steel erector |
| Jacobs | 20 | Design engineer |
| Cobb | 20 | Mechanical/utilities |
| Tindall | 17 | Precast fabricator |

### Issues Over Time

| Month | Issues | Trend |
|-------|--------|-------|
| Aug 2022 | 27 | Project ramp-up |
| Sep 2022 | 73 | Increasing |
| Oct 2022 | 59 | |
| Nov 2022 | 42 | Holiday slowdown |
| Dec 2022 | 61 | |
| Jan 2023 | 170 | Peak issues |
| Feb 2023 | 140 | High |
| Mar 2023 | 163 | High |
| Apr 2023 | 133 | |
| May 2023 | 164 | High |
| Jun 2023 | 76 | Partial month |

**Observation:** Issue count peaked in Jan-Mar 2023, suggesting this was a critical period with many concurrent problems.

---

## Recurring Themes

### 1. Precast Column Production Issues
- HEI/Heldenfels column fabrication consistently mentioned as bottleneck
- Column repairs and recasting causing delays
- "Precast Columns – They should have caught up more last week"

### 2. Design and Drawing Quality
- Multiple mentions of "poor drawing quality"
- RFI backlog and revision delays
- Jacobs (design engineer) mentioned frequently in design-related issues

### 3. Procurement Delays
- LOI (Letter of Intent) delays holding up subcontractor mobilization
- Critical change orders pending approval
- "Still awaiting written LOI, nonetheless Yates is going to issue Contract..."

### 4. Slab Sequence Constraints
- Slab pours interdependent with precast/steel erection
- "Slabs need to be poured out to not impede Precast and steel erection"
- Crane access and loading constraints on slabs

### 5. SUP Building Delays
- SUP (Support) buildings frequently mentioned as blocking FAB work
- "FAB Building standalone without SUP Buildings being erected and braced to first"

---

## Sample Issues (Verbatim)

### Design Issues
> "Met with SECAI and Jacobs 12/5 – to discuss packages and poor drawing quality and drawing issuances"

### Fabrication Delays
> "Precast Columns – They should have caught up more last week and I suspect concrete now driver"

### Procurement
> "Still awaiting written LOI, nonetheless Yates is going to issue Contract to Baker to keep work proceeding"

### Schedule
> "Slabs completed 2/17 – Obviously not making this due primarily to lost week of cold weather"

---

## Recommendations for Further Analysis

1. **Cross-reference with Primavera schedules** - Correlate issues with schedule delays in XER data
2. **Track issue resolution** - Same issues appear across multiple weeks; build resolution timeline
3. **Quantify delay impacts** - Link issues mentioning specific milestones to schedule variance
4. **Contractor performance** - Analyze which contractors are most frequently mentioned in issues
5. **Design issue tracking** - Extract RFI numbers mentioned for design delay analysis

---

## Addendum Data Analysis

### Addendum Structure

Weekly reports contain 5 addendums with ProjectSight data dumps:
- **ADDENDUM #001: SCHEDULE** - Primavera printouts (not parsed - redundant with XER files)
- **ADDENDUM #002: MANPOWER REPORT** - Daily labor hours by company
- **ADDENDUM #003: RFI LOG** - Request for Information tracking
- **ADDENDUM #004: SUBMITTAL LOG** - Submittal status tracking
- **ADDENDUM #005: CHANGE ORDER LOG** - Change order tracking (minimal data)

### Labor Hours Summary (from addendum_manpower.csv)

| Company | Total Hours | Workers | Notes |
|---------|-------------|---------|-------|
| S - ZAVALA FRANCISCO | 46,723 | - | Concrete/rebar |
| W.G. YATES & SONS CONSTRUCTION CO | 41,302 | - | General Contractor |
| GROUT TECH INC | 14,121 | - | Grouting |
| BRAZOS URETHANE INC | 9,898 | - | Insulation |
| COBB MECHANICAL CO | 8,046 | - | Mechanical |
| PREFERRED DALLAS, LLC | 5,449 | - | - |
| A H BECK FOUNDATION CO INC | 4,428 | - | Foundations |

**Total**: 27 unique companies tracked, 613 daily records

### RFI Summary (from addendum_rfi_log.csv)

- 3,849 RFI entries across all reports
- RFI numbers follow pattern `[Y_RFI#NNNN]`
- Includes created date and due date when available
- Note: Same RFIs appear in multiple weekly reports (cumulative log)

### Submittal Summary (from addendum_submittal_log.csv)

- 6,940 submittal entries across all reports
- Submittal numbers follow pattern `[Y_SBMT#N]`
- Note: Cumulative log - same submittals repeat across reports

---

## Labor Detail Analysis

### Individual Worker Data (from labor_detail.csv)

| Company | Total Hours | Unique Workers |
|---------|-------------|----------------|
| W & W Steel LLC | 31,704 | 104 |
| W.G. Yates & Sons | 18,972 | 34 |
| Grout Tech Inc | 15,993 | 76 |
| Baker Concrete | 14,965 | 343 |
| Brazos Urethane | 12,450 | 78 |
| F D Thomas Inc | 6,786 | 123 |
| Cobb Mechanical | 4,535 | 42 |
| Preferred Dallas | 4,411 | 69 |
| A H Beck Foundation | 3,252 | 15 |
| Patriot Erectors | 473 | 8 |

**Total**: 113,540 hours tracked across 880 unique workers (35/37 files)

### By Classification

| Classification | Hours | Workers |
|----------------|-------|---------|
| Worker | 27,029 | 156 |
| Journeyman | 11,070 | 222 |
| Superintendent | 9,643 | 44 |
| Labor | 3,915 | 50 |
| Foreman | 3,621 | 53 |
| Safety Personnel | 3,527 | 13 |
| Rodbuster | 3,526 | 115 |

### Parsing Notes

- Uses **bounding box approach** with PyMuPDF to reconstruct table rows from PDF
- Groups text by Y coordinate (3px tolerance) to form rows
- Extracts: name, classification, trade (CSI code), hours per worker
- 100% date coverage via filename parsing

---

## Data Quality Notes

- 4 reports missing author identification (older format)
- Some reports extracted fewer items due to format variations
- Content truncated at 500 characters per item
- Parser extracts first ~25 pages only (narrative section)
- **Addendum extraction**: Uses PyMuPDF (fitz) for fast text extraction (~23 seconds for all 37 files)
- **Manpower data**: Company-level daily totals only; individual worker entries not parsed due to PDF column interleaving
