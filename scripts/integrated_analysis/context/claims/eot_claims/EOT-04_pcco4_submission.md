# EOT-04: Extension of Time with PCCO #04

## Summary
Yates' fourth and most comprehensive Extension of Time submission, submitted August 13, 2025 in conjunction with PCCO #04. Includes BRG Expert Report on Schedule Progress with detailed critical path delay analysis attributing delays primarily to SECAI.

## Key Details

| Field | Value |
|-------|-------|
| **Number** | EOT-04 |
| **Date Submitted** | 2025-08-13 |
| **Date Executed** | Pending |
| **Associated PCCO** | PCCO #04 |
| **Open/Pending CO Amount** | $106,329,616 |
| **Days Requested** | 739 days (728 SECAI, 0 Yates, 11 excusable) |
| **Days Granted** | Pending |
| **Status** | Submitted |

> **Source for CO amounts:** `raw/narratives/CO#4 - Extension of Time - BRG Expert Report - combined w Exhibits - Aug 2025.pdf`, **Page 1015**

## Background
Following three prior EOT submissions (EOT-01 through EOT-03), Yates submitted the most detailed extension of time claim with supporting expert analysis from BRG. This submission responds to SECAI's ongoing review of prior EOT claims and includes comprehensive critical path analysis.

## Scope / Claims
- Contractual entitlement to time extensions and GC compensation
- No Yates concurrent delays claimed
- SECAI sole responsibility for:
  - MEP coordination under their control
  - Control of all Trade Contractors
  - Preparation and adherence to unified consolidated project schedule

## Key Delay Periods (from BRG Report)

| Period | Dates | Critical Path Delay | Days | SECAI | Yates | Excusable |
|--------|-------|---------------------|------|-------|-------|-----------|
| 12 | 2/21/25-3/26/25 | Elevator installation delays | 93 | 93 | 0 | 0 |
| 13 | 3/26/25-4/3/25 | Elevator 22 installation | 1 | 1 | 0 | 0 |
| 14 | 4/4/25-5/2/25 | Elevator 1 installation | 66 | 66 | 0 | 0 |
| 15 | 5/3/25-5/22/25 | Trestle & Canopy N32 coordination | (43) | (43) | 0 | 0 |
| 16 | 5/23/25-6/5/25 | Trestle & Canopy N32 coordination | 42 | 42 | 0 | 0 |
| 17 | 6/6/25-7/2/25 | Shaft wall framing GL33 | (16) | (16) | 0 | 0 |

## Parties Involved
- **Claimant:** W.G. Yates & Sons Construction Company
- **Respondent:** Samsung E&C America, Inc. (SECAI)
- **Expert:** BRG (Berkeley Research Group) - Schedule Analysis

## Related Documents
Source documents in raw data:
- `raw/narratives/Yates Extension of Time # 04 – Submission in Conjunction with PCCO # 04.pdf`
- `raw/narratives/CO#4 - Extension of Time - BRG Expert Report - combined w Exhibits - Aug 2025.pdf` (934 chunks)
- `raw/narratives/BRG Preliminary Expert Report August 2025.pdf`

Find via embeddings:
```bash
python -m scripts.narratives.embeddings search "EOT 04 PCCO extension time" --type eot_claim --limit 10
```

## Prior EOT History

| EOT | Date | Associated CO | Status |
|-----|------|---------------|--------|
| EOT-01 | Pre-2024 | CO-01 | Executed - Extended SC to July 2, 2025 |
| EOT-02 | 2024-08-27 | CO-02 | General Conditions request |
| EOT-03 | 2025-02-10 | CO-03/PCCO-03 | Submitted with Preliminary Expert Report |
| EOT-04 | 2025-08-13 | CO-04/PCCO-04 | Current submission |

## Impact on Analysis

### Schedule Data (P6)
- Critical path analysis in BRG report can be validated against P6 snapshots
- Delay periods align with schedule update data dates
- Elevator and coordination delays should be visible in P6 activity variances

### Labor Hours (ProjectSight, TBM)
- General Conditions compensation claim implies extended site overhead
- Look for labor consumption during delay periods
- Compare claimed delay periods with actual labor on affected areas

### Quality Metrics (RABA, PSI)
- Waffle coating issues referenced in EOT claims
- Elevator installation quality inspections
- Coordination issues may correlate with quality failures

## Notes
- Prior submissions (EOT-01 through EOT-03) were under SECAI review with limited substantive feedback
- April 28, 2025 EOT meeting addressed in `2025.04.30 - OFFICIAL NOTICE - Yates Response to LT-0793`
- Yates claims over 1,000 fragnets identifying project delays in contemporaneous schedule
- Full concurrency analysis for Elevator 1B issue documented in BRG report
