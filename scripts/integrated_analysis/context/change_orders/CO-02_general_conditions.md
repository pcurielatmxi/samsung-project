# CO-02: General Conditions & Additional Scope

## Summary
Yates' second bilateral change order with SECAI, executed November 25, 2024. Includes additional scope items, schedule extension compensation, and General Conditions cost claims.

## Key Details

| Field | Value |
|-------|-------|
| **Number** | CO-02 |
| **Document ID** | T1-TA001-CO-002 |
| **Date Submitted** | Aug 27, 2024 (EOT-02) |
| **Date Executed** | November 25, 2024 |
| **Associated EOT** | EOT-02 |
| **Schedule of Values** | $899,225,403.28 |
| **Prior Contract Value** | $859,608,599.81 (CO-01) |
| **Incremental Change** | +$39,616,803.47 |
| **Status** | Executed |

## Background
Following CO-01, Yates submitted EOT-02 on August 27, 2024 requesting General Conditions compensation due to schedule delays. This was subsequently incorporated into the CO-02 package which was executed in late November 2024.

## Scope / Claims

### Cost Breakdown by Category

*Note: MXI analysis - categorized from individual line items*

| Category | Amount | % of Change | Description |
|----------|--------|-------------|-------------|
| General Conditions Extension | ~$15M | ~38% | Extended site overhead through 2024 |
| Acceleration/Night Shifts | ~$12M | ~30% | Night shift premiums, schedule recovery |
| Design Changes/RFIs | ~$8M | ~20% | Scope additions from RFIs and design changes |
| Coordination/Rework | ~$5M | ~12% | Trade stacking, cleanroom coating issues |

### Key Line Items

| Item | Description | Subcontractor | Amount |
|------|-------------|---------------|--------|
| TA001-388 | Level 03 SUP Cleanroom PC Coating (concurrent work areas) | Cherry Coatings | $552,821 |
| TA001-254(3) | C&B Extended GC Due to Material Installation Delays | Cook & Boardman | $1,099,381 |
| TA001-385 | Scaffold for Level 2 L/31 | Various | $25,616 |
| TA001-386 | Goal Post Header reinstall at L3 M27 | Various | $14,538 |
| TA001-389 | FDT/Rolling Plains July 4th holiday pay | FD Thomas/Rolling Plains | $47,443 |
| TA001-390 | VOC levels mitigation | Various | TBD |

### Trade Contractors with Claims

| Subcontractor | Claim Types |
|---------------|-------------|
| Baker Concrete | Night shifts, design changes, acceleration |
| Berg Group | Blockout corrections, QC issues |
| Patriot | Night shift operations |
| Cherry Coatings | Cleanroom coating rework |
| Rolling Plains | Extended duration |
| FD Thomas | Extended duration |
| Cook & Boardman | Material delay impacts |

## EOT-02 Request (August 27, 2024)

From the EOT-02 submission letter:
> "Yates is pleased to provide the following data in support of the extension of time request for General Conditions due to schedule delays that occurred on the construction project known as Samsung Taylor FAB1 Semiconductor Fabrication Project."

The submission was addressed to Mr. Haewan Lee, Project Manager, Samsung E&C America, Inc.

## Key Dates & Milestones

| Date | Event |
|------|-------|
| July 5, 2024 | SECAI issues EOT claim filing instructions (LT-0468) |
| Aug 27, 2024 | Yates submits EOT-02 request |
| Nov 25, 2024 | CO-02 executed |

## Parties Involved
- **Contractor:** W.G. Yates & Sons Construction Company
- **Prime:** Samsung E&C America, Inc. (SECAI)

## Related Documents
Source documents in raw data:
- `raw/narratives/BRG Expert Schedule Report/BRG Expert report Exhibits - EOT -Aug 2025/EXHIBIT_39_Executed CO 2 T1-TA001-CO-002_YATES Change of Contract_r6_r Combined Package 2024.11.25.pdf` (44 pages)
- `raw/narratives/BRG Expert Schedule Report/EXHIBIT - XXXX - 2024-08-29 - Yates Subitted EOT CO 2.pdf` (10 pages)

Find via embeddings:
```bash
python -m scripts.narratives.embeddings search "CO-02 T1-TA001-CO-002 executed November 2024" --limit 10
python -m scripts.narratives.embeddings search "EOT request General Conditions August 2024" --type eot_claim --limit 10
```

## Impact on Analysis

### Schedule Data (P6)
- General Conditions claim implies project was delayed through 2024
- Review P6 updates from Q2-Q4 2024 for delay drivers

### Labor Hours (ProjectSight, TBM)
- Extended GC costs indicate prolonged site presence
- Analyze labor hours by month to quantify extended duration impact

### Quality Metrics
- VOC levels issue referenced (TA001-390)
- Level 03 SUP Cleanroom coating issues

## Notes
- Bilateral change order (mutually executed)
- Incorporated EOT-02 time extension request
- Significant trade contractor pass-through costs
- Multiple items related to cleanroom coating and coordination issues

