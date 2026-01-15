# Context Documents

This folder contains free-form context documents that provide essential background for integrated analysis. These documents capture contractual, legal, and procedural context that informs interpretation of the quantitative data.

---

## Folder Structure

```
context/
├── README.md                     # This file - organization guidelines
├── claims/                       # Change Orders and EOT claims
│   ├── MASTER_SUMMARY.md         # Consolidated view of all COs and EOTs
│   ├── change_orders/            # Individual CO documentation
│   │   └── CO-XX_*.md
│   └── eot_claims/               # Individual EOT documentation
│       └── EOT-XX_*.md
├── contracts/                    # Contract documents, structure, amendments
│   └── yates_secai_contract_structure.md
├── correspondence/               # (Future) Letters, notices between parties
├── experts/                      # (Future) Expert reports, methodologies
└── schedule/                     # (Future) Schedule basis, milestones
```

**Start here:** [claims/MASTER_SUMMARY.md](claims/MASTER_SUMMARY.md) - Consolidated view of all Change Orders and EOT claims

---

## Data Traceability Guidelines

All context documents must clearly distinguish between:
1. **Sourced data** - Values directly from source documents
2. **MXI analysis** - Our interpretation, categorization, or calculation

### Marking Sourced Data

When citing specific values from source documents, use one of these formats:

**Inline citation:**
```markdown
The Schedule of Values was $859,608,599.81 (EXHIBIT_40, page 4).
```

**Block quotation for exact text:**
```markdown
> "W.G. Yates & Sons Construction Company ('Yates') provides the following
> executive summary in conjunction with the attached Preliminary Expert Report..."
>
> — *EOT-03 Submission Letter, February 10, 2025*
```

**Table with source column:**
```markdown
| Item | Amount | Source |
|------|--------|--------|
| CO-01 SOV | $859,608,599.81 | EXHIBIT_40 |
| CO-02 SOV | $899,225,403.28 | EXHIBIT_40 |
```

### Marking MXI Analysis

When presenting analysis, categorization, or calculations not directly from source documents:

**Note format (preferred for tables):**
```markdown
### Cost Breakdown by Category

*Note: MXI analysis - categorized from individual IOCCs*

| Category | Amount | % of Total |
|----------|--------|------------|
| Design Changes | ~$110M | ~40% |
```

**Inline format:**
```markdown
The delays appear to fall into four categories (MXI analysis): design changes,
acceleration, added scope, and coordination failures.
```

**Section header format:**
```markdown
## MXI Analysis: Delay Attribution

Based on our review of the IOCCs and delay periods...
```

### Calculations and Aggregations

Calculations derived from sourced values are acceptable without MXI marking, but show the math:

```markdown
| CO | Schedule of Values | Incremental Change |
|----|--------------------|--------------------|
| CO-01 | $859,608,599.81 | +$72,003,958.81 |

*Incremental = CO-01 SOV - Original Contract ($786,604,641.00)*
```

---

## Document Quotation Guidelines

### Direct Quotations

Use block quotes for verbatim text from source documents:

```markdown
From the BRG Expert Report (August 2025):

> "The Contemporaneous Period Analysis is an observational technique since it
> does not involve the insertion or deletion of delays, but instead is based
> on observing the behavior of the network from update to update."
>
> — *BRG Expert Report, p. 15*
```

### Paraphrasing

When summarizing, indicate the source:

```markdown
According to the EOT-04 submission, Yates claims 739 days of delay, with 728
days attributed to SECAI and 0 days to Yates (BRG Expert Report, Table 3).
```

### Source Document References

Always include the source path for traceability:

```markdown
## Related Documents

| Document | Path | Pages |
|----------|------|-------|
| Executed CO-03 | `raw/narratives/BRG Expert Schedule Report/.../EXHIBIT_40_*.pdf` | 58 |
| BRG Expert Report | `raw/narratives/CO#4 - Extension of Time - BRG Expert Report*.pdf` | 934 |
```

### Embeddings Search Reference

Include search commands to help others find related content:

```bash
# Find this document's source material
python -m scripts.narratives.embeddings search "CO-03 Schedule of Values" --limit 5
```

---

## Document Templates

### Claims Documents (CO/EOT)

See [claims/MASTER_SUMMARY.md](claims/MASTER_SUMMARY.md) for the format used in CO/EOT documentation.

Key sections:
- **Key Details table** - All amounts must be sourced
- **Scope/Claims** - Separate sourced items from MXI categorization
- **Related Documents** - Include source paths and embeddings search

### General Context Document Template

```markdown
# [Document Title]

## Summary
Brief 2-3 sentence summary.

## Key Facts

| Fact | Value | Source |
|------|-------|--------|
| ... | ... | Document name, page |

## Background
Context with inline citations.

## MXI Analysis: [Topic]
*Our interpretation/categorization of the source material*

Analysis content...

## Source Documents

| Document | Path |
|----------|------|
| ... | `raw/...` |

## Embeddings Search
```bash
python -m scripts.narratives.embeddings search "relevant query"
```
```

---

## Naming Conventions

### By Category

| Category | Pattern | Example |
|----------|---------|---------|
| Change Orders | `CO-{NN}_{description}.md` | `CO-04_pcco_submission.md` |
| EOT Claims | `EOT-{NN}_{description}.md` | `EOT-04_pcco4_submission.md` |
| Contracts | `{type}_{date}_{description}.md` | `amendment_2024-05_scope_change.md` |
| Correspondence | `{date}_{from}_{to}_{subject}.md` | `2025-04-30_yates_secai_lt0793_response.md` |
| Expert Reports | `{expert}_{date}_{topic}.md` | `brg_2025-08_schedule_analysis.md` |

### General Rules
- Use lowercase with underscores
- Include dates in ISO format (YYYY-MM-DD) where relevant
- Keep descriptions short but meaningful

---

## Adding New Context Categories

To add a new category:

1. **Create the subfolder** under `context/`
2. **Add to this README** in the folder structure section
3. **Create a category README** (optional) with category-specific guidelines
4. **Update CLAUDE.md** in `scripts/integrated_analysis/` if significant

Example for adding correspondence:
```bash
mkdir -p context/correspondence
# Create context/correspondence/README.md with guidelines
# Add first document following naming convention
```

---

## Cross-References

Link related documents using relative paths:

```markdown
See [CO-02](../change_orders/CO-02_general_conditions.md) for the executed change order.

This EOT was submitted with [PCCO-04](../change_orders/CO-04_pcco_submission.md).
```

---

## Revision History

Track significant changes to context documents:

```markdown
## Revision History

| Date | Version | Author | Changes |
|------|---------|--------|---------|
| 2025-01-13 | 1.0 | MXI | Initial document |
| 2025-01-14 | 1.1 | MXI | Added verified SOV amounts |
```
