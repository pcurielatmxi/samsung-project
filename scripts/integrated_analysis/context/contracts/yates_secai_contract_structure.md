# Yates-SECAI Contract Structure

**Last Updated:** 2026-01-15
**Contract ID:** T1-TA001

---

## Summary

The Yates-SECAI subcontract for Taylor FAB1 is a **Cost Plus Fee with GMP (Guaranteed Maximum Price)** structure based on AIA standard documents. Yates' management/overhead costs are reimbursed on a cost-plus basis with a fee percentage, while subcontractor scope work is generally contracted as lump sum and passed through to Samsung.

---

## Contract Type

| Attribute | Value | Source |
|-----------|-------|--------|
| **Contract Form** | AIA A201-2017 (General Conditions) | EXHIBIT_01, EXHIBIT_33 |
| **Payment Basis** | Cost Plus Fee with GMP | AIA A102-2017 reference in contract |
| **Contract ID** | T1-TA001 | All Change Order documents |

### AIA Documents Used

| Document | Title | Purpose |
|----------|-------|---------|
| AIA A201-2017 | General Conditions of the Contract for Construction | General terms and conditions |
| AIA A102-2017 | Standard Form of Agreement (Cost Plus Fee with GMP) | Basis of payment structure |

> "§ 9.2 Schedule of Values As set forth in Section 12.1.5 of the AIA Document A102-2017, Exhibit C sets forth a Schedule of Values that allocates values to various portions of the Work."
>
> — *SECAI-Yates Subcontract (EXHIBIT_01), page 25*

---

## Payment Structure

### Fee and Markup Components

| Component | Rate | Purpose | Source |
|-----------|------|---------|--------|
| **GC Fee** | 2.95% | General Contractor's fee | CO documents |
| **Miscellaneous** | 3.11% | Project-related costs | CO documents |
| **Gap Insurance** | 0.35% | Insurance coverage | CO documents |
| **Subguard** | 1.5% | Subcontractor default insurance | CO documents |
| **P&P Bond** | 0.85% | Performance & Payment bond | CO documents |

*Source: Schedule of Values in EXHIBIT_38, EXHIBIT_39, EXHIBIT_40*

### Cost Categories

| Category | Payment Basis | Description |
|----------|---------------|-------------|
| **Yates GC/Management** | Cost Plus Fee | Staff, supervision, GC overhead - reimbursed based on actuals plus fee |
| **General Conditions (GR/GC)** | Cost Plus (time-based) | Site overhead, temporary facilities - tied to project duration |
| **Subcontractor Base Scope** | Lump Sum (pass-through) | Fixed-price subcontracts; Yates passes through cost + markups |
| **Subcontractor Changes** | Lump Sum or T&M | Change orders may be fixed price or time & materials |

---

## Yates GC Costs (Cost Plus Fee)

Yates' general conditions and management costs are reimbursed on a **cost-plus basis**, meaning they are tied to actual expenditures plus the 2.95% fee.

### Evidence

1. **Extended General Conditions Claims**: Yates claims additional GC costs when project duration extends, demonstrating costs are time-based:

> "Contractor must also provide evidence to establish the alleged general conditions costs... the burden of proof rests on the Contractor."
>
> — *SECAI Letter on EOT Claims (EXHIBIT 4), July 2024*

2. **Monthly GC Line Items**: The Schedule of Values shows GC costs tracked monthly (e.g., DAILY CLEANUP, SURVEY, SITE FACILITIES) with cumulative amounts.

3. **Fee Applied to Subtotal**: The 2.95% fee is applied as a percentage on top of base costs.

### GC Cost Categories (from Schedule of Values)

| SOV Line | Description | Tracking |
|----------|-------------|----------|
| 1.1 | Site Temporary Facilities | Monthly actuals |
| 1.2 | Temporary Utilities | Monthly actuals |
| 1.6 | Scaffolding/Equipment | Monthly actuals |
| 1.9 | Survey/Layout | Monthly actuals |
| 1.11 | Daily Cleanup | Monthly actuals |

---

## Subcontractor Costs (Lump Sum)

Subcontractor scope work is generally contracted on a **lump sum basis**. Yates holds fixed-price subcontracts with its trade subcontractors and passes through these costs to Samsung with markups.

### Evidence

1. **Fixed Subcontract Values**: Baker Concrete shown with fixed subcontract amount:

| Subcontractor | Subcontract Value | Source |
|---------------|-------------------|--------|
| Baker Concrete | $46,783,750.00 | CO #04 Submission |
| Rolling Plains | $4,995,446.21 | CO #04 Submission |
| Cook & Boardman | $5,192,782.00 | CO #04 Submission |

*Source: 2025.08.15 - Yates CO # 04 Submission to SECAI.pdf*

2. **Cost Confirmation Process**: The contract references "Cost Confirmation" for major scope packages:

> "1.2 1st Original Contract $786,604,641.00 - Include Cost Confirmation (Con'c/PC/Steel/Metal Panel/Roofing)"
>
> — *CO #01 Schedule of Values (EXHIBIT_38)*

"Cost Confirmation" is industry terminology for locking in subcontractor pricing at a fixed amount.

3. **Subguard Insurance (1.5%)**: This insurance specifically covers Yates' risk if a lump-sum subcontractor defaults. Cost-plus subcontracts would not require such insurance since there would be no fixed-price commitment at risk.

4. **Formal Bid Process**: Weekly reports reference bid solicitation and proposals from subcontractors, consistent with competitive lump-sum procurement.

---

## Exceptions: T&M Work

Some subcontractor work is explicitly handled on a **Time & Materials (T&M)** basis, typically for acceleration or unplanned scope:

| IOCC | Description | Contractor | Amount | Basis |
|------|-------------|------------|--------|-------|
| IOCC-190 | Foundation Acceleration by Baker | Baker | $431,403 | T&M |
| Various | Night Shift Extensions | Baker | ~$3.1M | T&M |

### T&M Controls

T&M work typically operates under Letter of Intent (LOI) with **Not-to-Exceed (NTE)** amounts:

> "Need to get official LOI released for Baker this week as they'll reach NTE amount of $4M T&M LOI previously issued"
>
> — *Weekly Report, August 2022*

---

## Contract Value Progression

| Stage | Amount | Description | Source |
|-------|--------|-------------|--------|
| LNTP (LOI) | $676,184,973 | Letter of Intent - includes WWAFCO novation, GC Fee, GR/GC | EXHIBIT_38 |
| 1st Original Contract | $786,604,641 | Cost Confirmation packages (Concrete/PC/Steel/Panel/Roofing) | EXHIBIT_38 |
| CO-01 SOV | $859,608,599.81 | First bilateral change order | EXHIBIT_38 |
| CO-02 SOV | $899,225,403.28 | Second bilateral change order | EXHIBIT_39 |
| CO-03 SOV | $920,220,199.13 | Third bilateral change order | EXHIBIT_40 |

---

## Contractual Hierarchy

```
Samsung (Owner)
    └── SECAI (Prime Contractor / Construction Manager)
            └── Yates (General Contractor - Building Package)
                    └── Trade Subcontractors (50+)
                            ├── Baker Concrete (Lump Sum)
                            ├── W&W Steel (Lump Sum)
                            ├── Berg Drywall (Lump Sum)
                            └── ... etc.
```

**Key Relationships:**
- **Samsung → SECAI**: Prime contract (structure unknown from available documents)
- **SECAI → Yates**: Cost Plus Fee with GMP (AIA A102/A201)
- **Yates → Subcontractors**: Generally Lump Sum (some T&M for changes)

---

## Implications for Analysis

### Labor Hours Analysis
- Yates GC staff hours are directly reimbursable - extended duration = higher GC costs
- Subcontractor labor hours are embedded in lump sum prices - not directly visible to Samsung
- T&M work hours should be separately tracked and verified

### Change Order Analysis
- IOCCs typically represent subcontractor lump sum change prices
- GC markup percentages (Fee, Subguard, Bond) are applied on top
- Extended GC claims require proof of actual costs incurred

### Delay Claims
- Time extensions entitle Yates to extended GC costs (compensable if SECAI-caused)
- Subcontractor delay costs may be passed through if contractually justified
- §8.3.2 governs delay compensation entitlement

---

## Source Documents

| Document | Path | Description |
|----------|------|-------------|
| SECAI-Yates Subcontract | `raw/narratives/BRG Expert Schedule Report/.../EXHIBIT_01_*.pdf` | Executed contract (redacted) |
| SECAI Prime Contract | `raw/narratives/BRG Expert Schedule Report/.../EXHIBIT_33_*.pdf` | SECAI-Samsung contract reference |
| CO #01 Package | `raw/narratives/BRG Expert Schedule Report/.../EXHIBIT_38_*.pdf` | First change order with SOV |
| CO #02 Package | `raw/narratives/BRG Expert Schedule Report/.../EXHIBIT_39_*.pdf` | Second change order |
| CO #03 Package | `raw/narratives/BRG Expert Schedule Report/.../EXHIBIT_40_*.pdf` | Third change order |

---

## Embeddings Search

```bash
# Find contract provisions
python -m scripts.narratives.embeddings search "AIA A201 A102 contract" --limit 10

# Find GC cost structure
python -m scripts.narratives.embeddings search "GC Fee general conditions reimbursable" --limit 10

# Find subcontractor pricing
python -m scripts.narratives.embeddings search "Baker subcontract value amount" --limit 10

# Find T&M work
python -m scripts.narratives.embeddings search "T&M time materials NTE" --limit 10
```

---

## Revision History

| Date | Version | Author | Changes |
|------|---------|--------|---------|
| 2026-01-15 | 1.0 | MXI | Initial document - contract structure analysis |
