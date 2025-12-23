# RABA Scraper - Remaining Errors Report

**Generated:** 2025-12-23 08:45

## Summary

| Metric | Count |
|--------|-------|
| Successful Downloads | 1016 |
| Empty Dates | 253 |
| **Remaining Errors** | **63** |

## Root Cause

All 63 remaining errors are **single-record dates**. The RABA system does not render the "Batch Checked Reports Into One PDF" button when search results contain only 1 record.

This is a UI limitation in RABA - the batch download feature requires 2+ records.

## Errors by Month

| Month | Errors |
|-------|--------|
| 2022-06 | 1 |
| 2022-10 | 1 |
| 2022-11 | 2 |
| 2023-02 | 1 |
| 2023-05 | 1 |
| 2023-07 | 1 |
| 2023-08 | 1 |
| 2023-10 | 2 |
| 2023-11 | 2 |
| 2023-12 | 2 |
| 2024-01 | 2 |
| 2024-02 | 3 |
| 2024-03 | 2 |
| 2024-04 | 2 |
| 2024-05 | 3 |
| 2024-06 | 4 |
| 2024-07 | 9 |
| 2024-08 | 2 |
| 2024-09 | 1 |
| 2024-10 | 3 |
| 2024-11 | 8 |
| 2024-12 | 2 |
| 2025-01 | 1 |
| 2025-04 | 1 |
| 2025-07 | 1 |
| 2025-09 | 1 |
| 2025-10 | 1 |
| 2025-11 | 1 |
| 2025-12 | 2 |
| **Total** | **63** |

## Action Items

### Option 1: Manual Download (Recommended for 63 records)
Download each single-record report manually from RABA:
1. Navigate to View Reports
2. Set date filter to the specific date
3. Click Search
4. Click directly on the Assignment ID link to open the report
5. Save/Print as PDF

### Option 2: Implement Individual Report Download
Modify the scraper to handle single-record dates differently:
- Detect when record_count == 1
- Instead of batch download, click the Assignment ID link directly
- Handle the popup/new tab that opens with the individual report
- Save the PDF

### Option 3: Skip Single-Record Dates
If single-record dates are low priority:
- Mark them as "skipped" in manifest
- Document that batch download only works for 2+ records

## All Error Dates

| Date | Assignment Count |
|------|------------------|
| 2022-06-01 | 1 |
| 2022-10-17 | 1 |
| 2022-11-06 | 1 |
| 2022-11-21 | 1 |
| 2023-02-05 | 1 |
| 2023-05-27 | 1 |
| 2023-07-16 | 1 |
| 2023-08-06 | 1 |
| 2023-10-21 | 1 |
| 2023-10-23 | 1 |
| 2023-11-05 | 1 |
| 2023-11-19 | 1 |
| 2023-12-22 | 1 |
| 2023-12-30 | 1 |
| 2024-01-17 | 1 |
| 2024-01-21 | 1 |
| 2024-02-18 | 1 |
| 2024-02-20 | 1 |
| 2024-02-25 | 1 |
| 2024-03-22 | 1 |
| 2024-03-23 | 1 |
| 2024-04-13 | 1 |
| 2024-04-15 | 1 |
| 2024-05-05 | 1 |
| 2024-05-11 | 1 |
| 2024-05-25 | 1 |
| 2024-06-01 | 1 |
| 2024-06-08 | 1 |
| 2024-06-15 | 1 |
| 2024-06-30 | 1 |
| 2024-07-04 | 1 |
| 2024-07-06 | 1 |
| 2024-07-07 | 1 |
| 2024-07-14 | 1 |
| 2024-07-20 | 1 |
| 2024-07-21 | 1 |
| 2024-07-27 | 1 |
| 2024-07-28 | 1 |
| 2024-07-30 | 1 |
| 2024-08-03 | 1 |
| 2024-08-25 | 1 |
| 2024-09-01 | 1 |
| 2024-10-11 | 1 |
| 2024-10-14 | 1 |
| 2024-10-26 | 1 |
| 2024-11-02 | 1 |
| 2024-11-14 | 1 |
| 2024-11-15 | 1 |
| 2024-11-18 | 1 |
| 2024-11-21 | 1 |
| 2024-11-23 | 1 |
| 2024-11-25 | 1 |
| 2024-11-27 | 1 |
| 2024-12-03 | 1 |
| 2024-12-07 | 1 |
| 2025-01-04 | 1 |
| 2025-04-26 | 1 |
| 2025-07-26 | 1 |
| 2025-09-07 | 1 |
| 2025-10-25 | 1 |
| 2025-11-21 | 1 |
| 2025-12-03 | 1 |
| 2025-12-05 | 1 |
