# XER File Overlap Analysis

**Generated:** 2025-12-05
**Files Analyzed:** 48 XER files
**Total Task Records:** 964,002

## Project Context

**Project:** Samsung Austin Semiconductor Chip Manufacturing Facility (Taylor, TX - FAB1)
- **Owner:** Samsung
- **Owner's Engineering Arm:** SECAI (Samsung Engineering Construction America Inc.)
- **General Contractor:** Yates Construction

## Executive Summary

The 48 XER files represent **two parallel schedule perspectives** of the same project:

1. **SECAI Schedule (Owner)** - 47 files: Samsung/SECAI's detailed schedule with evolving versions from Oct 2022 - Jun 2025
2. **SAMSUNG-TFAB1 Schedule (GC)** - 1 file: Yates Construction's schedule perspective (current)

The low task code overlap (0.1%) between these perspectives is due to **different task coding conventions**, not different project scope.

## Key Findings

### 1. Version Evolution Evidence

| Metric | Value |
|--------|-------|
| Task codes in multiple files | 66.5% |
| Consecutive file overlap | 95-100% typically |
| Task growth | ~7K (2022) → ~32K (2025) |

### 2. File Groups Identified

| Group | Date Range | Files | Task Range | Description |
|-------|------------|-------|------------|-------------|
| 1 | Oct 2022 - Dec 2023 | 23 | 7K-11K | SECAI - Early project, consistent evolution |
| 2 | Jul-Sep 2023 | 3 | 20K-66K | SECAI - Large interim export (anomaly) |
| 3 | Nov 2023 - Jun 2024 | 7 | 24K-31K | SECAI - Growth phase |
| 4 | May 2024 - Jun 2025 | 14 | 29K-32K | SECAI - Mature phase, stable |
| 5 | Nov 2025 | 1 | 12K | **GC (Yates) Schedule** |

### 3. Two Schedule Perspectives Explained

The `SAMSUNG-TFAB1-11-20-25- Live-3.xer` file represents the **GC (Yates) schedule**, which is a parallel perspective to the SECAI owner schedule:

| Aspect | SECAI (Owner) | SAMSUNG-TFAB1 (GC) |
|--------|---------------|---------------------|
| Maintained by | Samsung/SECAI | Yates Construction |
| Role | Owner's engineering schedule | General Contractor's schedule |
| Task count | ~32K (detailed) | ~12K (summarized) |
| Historical files | 47 versions | 1 file |

**Task Code Prefix Comparison:**

| GC Schedule (Yates) | Count | Owner Schedule (SECAI) | Count |
|---------------------|-------|------------------------|-------|
| CN | 6,321 | TE0 | 6,269 |
| FAB | 3,145 | TM-009 | 4,272 |
| ZX- | 1,390 | TM0 | 4,222 |
| INT | 336 | TA0 | 3,438 |
| EJ- | 177 | SAM | 2,221 |

The low overlap (0.1%, 44 codes) is expected - these are different organizations' views of the same project with different task coding conventions.

### 4. Anomalies in SECAI Schedule Series

| File | Date | Tasks | Issue |
|------|------|-------|-------|
| Taylor FAB1-SECAI Schedule update_data date 7-30-23.xer | 2023-07-30 | 66,706 | Likely master program export (includes other projects?) |
| Aug.11.2023 T-PJT Level.3 Schedule R0.0 (1).xer | 2023-08-11 | 6,178 | Level 3 detail (less granular summary) |

## Task Count by File (Chronological)

| Date | Tasks | Filename |
|------|-------|----------|
| 2022-10-17 | 7,165 | SECAI Schedule update 10-17-22.xer |
| 2022-10-31 | 7,827 | SECAI revised plan(DD 10.31.2022, revised FFU TO Nov. 30. 2023).xer |
| 2022-10-31 | 7,756 | schedule update data date 10-31-22.xer |
| 2022-10-31 | 7,968 | SECAI Updated Schedule (DD 10.31.2022, FFU TO 1.19.2024).xer |
| 2022-11-22 | 7,968 | 4. SECAI Updated Schedule Nov.22.2022.xer |
| 2022-12-19 | 8,215 | Schedule update 12-19-22- Data date .xer |
| 2022-12-19 | 8,203 | T1 Project-SECAI schedule update-20221219.xer |
| 2023-01-29 | 8,230 | T1 Project-SECAI schedule update-20230129.xer |
| 2023-02-12 | 10,102 | T1 Project-SECAI schedule update-20230212.xer |
| 2023-02-26 | 10,240 | T1 Project-SECAI schedule update-20230226.xer |
| 2023-03-12 | 10,246 | T1 Project-SECAI schedule update-20230312.xer |
| 2023-03-26 | 10,481 | T1 Project-SECAI schedule update-20230326.xer |
| 2023-04-09 | 11,545 | T1 Project-SECAI schedule update-20230409.xer |
| 2023-04-23 | 11,258 | T1 Project-SECAI schedule update-20230423.xer |
| 2023-05-07 | 11,312 | T1 Project-SECAI schedule update-20230507.xer |
| 2023-05-21 | 11,393 | T1 Project-SECAI schedule update-20230521.xer |
| 2023-06-04 | 11,398 | T1 Project-SECAI schedule update-20230604.xer |
| 2023-06-18 | 11,461 | T1 Project-SECAI schedule update-20230618.xer |
| 2023-07-02 | 11,506 | T1 Project-SECAI schedule update-20230702.xer |
| 2023-07-30 | 66,706 | Taylor FAB1-SECAI Schedule update_data date 7-30-23.xer |
| 2023-08-11 | 6,178 | Aug.11.2023 T-PJT Level.3 Schedule R0.0 (1).xer |
| 2023-08-27 | 20,704 | T1 Project-SECAI schedule update-20230827.xer |
| 2023-09-24 | 21,712 | T1 Project-SECAI schedule update-20230924.xer |
| 2023-11-12 | 26,786 | T1 Project-SECAI Schedule update-Data Date 11.12.2023.xer |
| 2023-11-30 | 7,827 | 3. SECAI revised plan(FFU TO Nov. 30. 2023).xer |
| 2023-11-30 | 7,827 | 4. SECAI Original Schedule(FFU 11.30.2023).xer |
| 2023-12-30 | 8,179 | 3. SECAI Re-baseline(FFU 12.30.2023).xer |
| 2024-01-12 | 26,779 | SECAI TAYLOR FAB1 1.12.24 SCHEDULE UPDATE.xer |
| 2024-02-09 | 24,353 | T1 Project-SECAI Schedule Update-Data Date 02-09-2024.xer |
| 2024-03-08 | 24,563 | T1 Project-SECAI Schedule Update Data Date 03-08-2024.xer |
| 2024-04-05 | 27,848 | T1 Project -SECAI Schedule Update Data Date 04-05-2024.xer |
| 2024-05-03 | 25,789 | T1 Project -SECAI Schedule Update Data Date 05-03-2024.xer |
| 2024-05-31 | 29,635 | T1 Project -SECAI Schedule Update Data Date 05-31-2024.xer |
| 2024-06-28 | 31,460 | T1 Project -SECAI Schedule Update Data Date 06-28-2024_r1.xer |
| 2024-07-26 | 29,048 | T1 Project -SECAI Schedule Update Data Date 07-26-2024.xer |
| 2024-08-23 | 29,180 | T1 Project -SECAI Schedule Update Data Date 08-23-2024.xer |
| 2024-09-20 | 29,941 | T1 Project -SECAI Schedule Update Data Date 09-20-2024.xer |
| 2024-10-18 | 31,720 | T1 Project-SECAI Schedule Update Data Date 10-18-2024.xer |
| 2024-11-15 | 31,561 | T1 Project-SECAI Schedule Update Data Date 11-15-2024.xer |
| 2024-12-13 | 32,297 | T1 Project-SECAI Schedule Update Data Date 12-13-2024.xer |
| 2025-01-10 | 32,317 | T1 Project-SECAI Schedule Update Data Date 1-10-2025.xer |
| 2025-02-07 | 32,123 | T1 Project-SECAI Schedule Update Data Date 2-7-2025.xer |
| 2025-03-07 | 32,269 | T1 Project-SECAI Schedule Update Data Date 3-7-2025.xer |
| 2025-04-04 | 32,711 | T1 Project-SECAI Schedule Update Data Date 4-4-2025.xer |
| 2025-05-02 | 32,521 | T1 Project-SECAI Schedule Update Data Date 5-2-2025.xer |
| 2025-05-30 | 32,616 | T1 Project-SECAI Schedule Update Data Date 5-30-2025.xer |
| 2025-06-27 | 32,645 | T1 Project-SECAI Schedule Update Data Date 6-27-2025 (1).xer |
| 2025-11-20 | 12,433 | SAMSUNG-TFAB1-11-20-25- Live-3.xer * |

\* = Current file

## Consecutive File Overlap

| From | To | Overlap | Added | Removed | % Shared |
|------|-----|---------|-------|---------|----------|
| 2022-10-17 | 2022-10-31 | 6,766 | 1,061 | 399 | 94.4% |
| 2022-10-31 | 2022-10-31 | 7,629 | 127 | 198 | 97.5% |
| 2022-10-31 | 2022-10-31 | 7,603 | 365 | 153 | 98.0% |
| 2022-10-31 | 2022-11-22 | 7,968 | 0 | 0 | 100.0% |
| 2022-11-22 | 2022-12-19 | 7,965 | 247 | 3 | 100.0% |
| 2022-12-19 | 2022-12-19 | 8,196 | 4 | 16 | 99.8% |
| 2022-12-19 | 2023-01-29 | 8,073 | 157 | 127 | 98.5% |
| 2023-01-29 | 2023-02-12 | 8,208 | 1,894 | 22 | 99.7% |
| 2023-02-12 | 2023-02-26 | 10,081 | 159 | 21 | 99.8% |
| 2023-02-26 | 2023-03-12 | 10,163 | 83 | 77 | 99.2% |
| 2023-03-12 | 2023-03-26 | 10,240 | 241 | 6 | 99.9% |
| 2023-03-26 | 2023-04-09 | 10,352 | 606 | 129 | 98.8% |
| 2023-04-09 | 2023-04-23 | 10,829 | 429 | 129 | 98.8% |
| 2023-04-23 | 2023-05-07 | 11,174 | 138 | 84 | 99.3% |
| 2023-05-07 | 2023-05-21 | 11,294 | 99 | 18 | 99.8% |
| 2023-05-21 | 2023-06-04 | 11,339 | 59 | 54 | 99.5% |
| 2023-06-04 | 2023-06-18 | 11,393 | 68 | 5 | 100.0% |
| 2023-06-18 | 2023-07-02 | 11,403 | 103 | 58 | 99.5% |
| 2023-07-02 | 2023-07-30 | 8,637 | 31,596 | 2,869 | **75.1%** |
| 2023-07-30 | 2023-08-11 | 3,968 | 2,210 | 36,265 | **9.9%** |
| 2023-08-11 | 2023-08-27 | 3,114 | 17,590 | 3,064 | **50.4%** |
| 2023-08-27 | 2023-09-24 | 15,158 | 6,553 | 5,546 | 73.2% |
| 2023-09-24 | 2023-11-12 | 17,921 | 8,864 | 3,790 | 82.5% |
| 2023-11-12 | 2023-11-30 | 3,648 | 4,179 | 23,137 | **13.6%** |
| 2023-11-30 | 2023-11-30 | 7,827 | 0 | 0 | 100.0% |
| 2023-11-30 | 2023-12-30 | 7,696 | 483 | 131 | 98.3% |
| 2023-12-30 | 2024-01-12 | 2,989 | 23,790 | 5,190 | **36.5%** |
| 2024-01-12 | 2024-02-09 | 22,334 | 2,019 | 4,445 | 83.4% |
| 2024-02-09 | 2024-03-08 | 23,661 | 902 | 692 | 97.2% |
| 2024-03-08 | 2024-04-05 | 23,972 | 3,876 | 591 | 97.6% |
| 2024-04-05 | 2024-05-03 | 24,507 | 1,282 | 3,341 | 88.0% |
| 2024-05-03 | 2024-05-31 | 25,171 | 4,463 | 618 | 97.6% |
| 2024-05-31 | 2024-06-28 | 28,962 | 2,498 | 672 | 97.7% |
| 2024-06-28 | 2024-07-26 | 27,963 | 1,084 | 3,497 | 88.9% |
| 2024-07-26 | 2024-08-23 | 28,458 | 722 | 589 | 98.0% |
| 2024-08-23 | 2024-09-20 | 28,389 | 1,552 | 791 | 97.3% |
| 2024-09-20 | 2024-10-18 | 29,789 | 1,920 | 152 | 99.5% |
| 2024-10-18 | 2024-11-15 | 31,269 | 292 | 440 | 98.6% |
| 2024-11-15 | 2024-12-13 | 30,687 | 1,592 | 874 | 97.2% |
| 2024-12-13 | 2025-01-10 | 31,515 | 776 | 764 | 97.6% |
| 2025-01-10 | 2025-02-07 | 32,096 | 27 | 195 | 99.4% |
| 2025-02-07 | 2025-03-07 | 32,086 | 183 | 37 | 99.9% |
| 2025-03-07 | 2025-04-04 | 32,169 | 499 | 100 | 99.7% |
| 2025-04-04 | 2025-05-02 | 32,476 | 45 | 192 | 99.4% |
| 2025-05-02 | 2025-05-30 | 32,519 | 97 | 2 | 100.0% |
| 2025-05-30 | 2025-06-27 | 32,615 | 30 | 1 | 100.0% |
| 2025-06-27 | 2025-11-20 | 44 | 12,389 | 32,601 | **0.1%** |

## Shared Task Codes (Current vs Previous)

Only 44 task codes are shared between the current file (Nov 2025) and the previous file (Jun 2025):

| Task Code | Task Name |
|-----------|-----------|
| A1220 | DRILL/ EPOXY DOWELS |
| A1270 | CIP OAC PADS (17-19) |
| A1320 | INSTALL TROUGH INSULATION |
| A1350 | WATERPROOF PRIMER & MEMBRANE @ TROUGHS |
| A1380 | PH1 - MATERIAL DELIVERY & LAYDOWN SETUP - CLADDING STEEL |
| A1440 | MEP EQUIPMENT LOADING BY SECAI |
| A1460 | MEP EQUIPMENT LOADING BY SECAI |
| A1470 | METAL STUD FRAMING DRYWALL AND FINISHES - CLOSING THE OPENING |
| A1480 | MEP EQUIPMENT LOADING BY SECAI |
| A1490 | METAL STUD FRAMING DRYWALL AND FINISHES - CLOSING THE OPENING |
| A2000 | ALL YATES DOOR FRAME INSPECTION - FAB110110B |
| A2020 | IMPACT - SECAI MEP TRADE DAMAGE - DOOR FRAME RE-ALIGNMENT |
| A2080 | LEVEL 3 COMPLETION - 11/22 Target - TCO 3 |
| A2100 | LEVEL 4 DS WEST COMPLETION - 11/22 Target - TCO 3 |
| A2130 | LEVEL 1 SUE DS COMPLETION - 12/05 Target- TCO 4 |
| ... | (44 total) |

## Recommendations

1. **Track Both Perspectives**: Maintain both SECAI (owner) and GC (Yates) schedules as complementary views of the same project

2. **Add Schedule Type to Manifest**: Consider adding a `schedule_type` field to distinguish:
   - `owner` for SECAI files
   - `gc` for SAMSUNG-TFAB1/Yates files

3. **Obtain Historical GC Schedules**: If available, historical Yates schedule exports would enable similar version tracking for the GC perspective

4. **Cross-Reference Analysis**: Future analysis could attempt to map tasks between owner and GC schedules based on:
   - Task names/descriptions
   - WBS structure
   - Date ranges
   - Location/area codes

## Methodology

- Overlap calculated using Jaccard similarity on task codes
- File groups identified using >50% task code similarity threshold
- Analysis performed on 964,002 task records across 48 files
