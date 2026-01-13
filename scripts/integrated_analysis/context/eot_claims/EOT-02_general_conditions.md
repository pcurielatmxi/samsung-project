# EOT-02: General Conditions Extension Request

## Summary
Yates' second Extension of Time claim, submitted August 27, 2024 requesting General Conditions compensation due to schedule delays. Subsequently incorporated into CO-02 executed November 25, 2024.

## Key Details

| Field | Value |
|-------|-------|
| **Number** | EOT-02 |
| **Date Submitted** | August 27, 2024 |
| **Date Executed** | November 25, 2024 (with CO-02) |
| **Associated CO** | CO-02 |
| **Days Requested** | TBD |
| **Days Granted** | Incorporated in CO-02 |
| **Status** | Executed |

## Background
Following SECAI's July 5, 2024 letter providing instructions on how to file EOT claims (LT-0468), Yates submitted EOT-02 requesting General Conditions compensation for schedule delays through mid-2024.

## SECAI EOT Filing Requirements (LT-0468)
Per SECAI's July 2024 instructions:
1. Chronologically ordered list of delay events in narrative format
2. Evidence establishing entitlement to time extension
3. Evidence establishing GC costs
4. Burden of proof rests on Contractor

## Scope / Claims
From the August 27, 2024 submission letter:

> "Yates is pleased to provide the following data in support of the extension of time request for General Conditions due to schedule delays that occurred on the construction project known as Samsung Taylor FAB1 Semiconductor Fabrication Project"

Key claims:
- Extended General Conditions costs
- Schedule delays through August 2024
- Site overhead compensation

## Key Dates & Milestones

| Date | Event |
|------|-------|
| July 5, 2024 | SECAI issues LT-0468 (EOT filing instructions) |
| Aug 27, 2024 | EOT-02 submitted to Mr. Haewan Lee |
| Aug 29, 2024 | Formal submission recorded |
| Nov 25, 2024 | Incorporated in CO-02 execution |

## Parties Involved
- **Claimant:** W.G. Yates & Sons Construction Company
- **Respondent:** Samsung E&C America, Inc. (SECAI)
- **SECAI Contact:** Mr. Haewan Lee, Project Manager

## Related Documents
Source documents in raw data:
- `raw/narratives/BRG Expert Schedule Report/EXHIBIT - XXXX - 2024-08-29 - Yates Subitted EOT CO 2.pdf` (10 pages)
- `raw/narratives/BRG Expert Schedule Report/EXHIBIT 4 - SCT-TFAB-LT-0468 - SECAIs Letter to DSI on How to File EOT Claim.pdf` (3 pages)
- `raw/narratives/BRG Expert Schedule Report/BRG Expert report Exhibits - EOT -Aug 2025/EXHIBIT_39_Executed CO 2 T1-TA001-CO-002_YATES Change of Contract_r6_r Combined Package 2024.11.25.pdf`

Find via embeddings:
```bash
python -m scripts.narratives.embeddings search "EOT request General Conditions August 2024" --type eot_claim --limit 10
python -m scripts.narratives.embeddings search "LT-0468 EOT claim filing instructions" --limit 10
python -m scripts.narratives.embeddings search "Haewan Lee extension time" --limit 10
```

## Impact on Analysis

### Schedule Data (P6)
- Covers delays through August 2024
- Review P6 updates from Q2-Q3 2024 for delay drivers
- Compare claimed delays with P6 critical path slippage

### Labor Hours (ProjectSight, TBM)
- GC compensation claim indicates extended site presence
- Analyze monthly labor hours through August 2024
- Compare with baseline staffing assumptions

### Quality Metrics
- Review quality inspection rates through August 2024
- Identify any quality-driven delays in this period

## Notes
- First formal EOT following SECAI's filing procedure guidance
- No expert report attached (unlike EOT-03 and EOT-04)
- Sets precedent for subsequent submissions
- Successfully incorporated into executed CO-02

