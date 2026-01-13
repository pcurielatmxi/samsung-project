# Context Documents

This folder contains free-form context documents that provide essential background for integrated analysis. These documents capture contractual, legal, and procedural context that informs interpretation of the quantitative data.

**Start here:** [MASTER_SUMMARY.md](MASTER_SUMMARY.md) - Consolidated view of all Change Orders and EOT claims

## Folder Structure

```
context/
├── README.md                 # This file - organization guidelines
├── MASTER_SUMMARY.md         # Master index of all COs and EOTs
├── change_orders/            # Change Order (CO) documentation
│   ├── CO-01_initial_extension.md
│   ├── CO-02_general_conditions.md
│   ├── CO-03_schedule_adjustment.md
│   └── CO-04_pcco_submission.md
└── eot_claims/               # Extension of Time (EOT) claims
    ├── EOT-01_initial_extension.md
    ├── EOT-02_general_conditions.md
    ├── EOT-03_preliminary_expert.md
    └── EOT-04_pcco4_submission.md
```

## Document Naming Convention

### Change Orders
```
CO-{number}_{short_description}.md
```
Examples:
- `CO-01_initial_scope.md`
- `CO-02_general_conditions.md`
- `CO-03_schedule_adjustment.md`
- `CO-04_eot_submission.md`

### EOT Claims
```
EOT-{number}_{short_description}.md
```
Examples:
- `EOT-01_precast_delays.md`
- `EOT-02_general_conditions.md`
- `EOT-03_schedule_extension.md`
- `EOT-04_final_claim.md`

## Document Template

Each document should follow this structure:

```markdown
# {CO/EOT}-{number}: {Title}

## Summary
Brief 2-3 sentence summary of the change order or EOT claim.

## Key Details

| Field | Value |
|-------|-------|
| **Number** | CO-XX / EOT-XX |
| **Date Submitted** | YYYY-MM-DD |
| **Date Executed** | YYYY-MM-DD (if applicable) |
| **Associated PCCO** | PCCO #XX (if applicable) |
| **Amount** | $X,XXX,XXX (if applicable) |
| **Days Requested** | XX days (for EOT) |
| **Days Granted** | XX days (for EOT) |
| **Status** | Submitted / Under Review / Executed / Disputed |

## Background
Context and circumstances leading to this CO/EOT.

## Scope / Claims
- Detailed description of what the CO covers or what the EOT claims
- Bullet points for clarity
- Reference specific exhibits or source documents

## Key Dates & Milestones
| Date | Event |
|------|-------|
| YYYY-MM-DD | Event description |

## Parties Involved
- **Claimant:** (e.g., Yates)
- **Respondent:** (e.g., SECAI/Samsung)
- **Expert/Consultant:** (e.g., BRG)

## Related Documents
Links to source documents in the raw data:
- `raw/narratives/{filename}` - Description
- Embedding search: `python -m scripts.narratives.embeddings search "{query}" --type eot_claim`

## Impact on Analysis
How this CO/EOT affects interpretation of:
- Schedule data (P6)
- Labor hours (ProjectSight, TBM)
- Quality metrics (RABA, PSI)

## Notes
Additional context, disputes, or unresolved questions.
```

## Relationship to Data Sources

These context documents connect to the quantitative data:

| Context | Data Sources Affected |
|---------|----------------------|
| **CO scope changes** | P6 task additions, labor consumption |
| **EOT delay claims** | P6 critical path, schedule slippage |
| **Disputed responsibility** | Quality failures, rework attribution |
| **Contract milestones** | Substantial completion dates |

## Usage in Analysis

1. **Before analysis:** Review relevant CO/EOT context
2. **During analysis:** Reference context when interpreting anomalies
3. **In reports:** Cite CO/EOT documents for delay attribution

## Finding Source Documents

Use embeddings search to find relevant source documents:

```bash
# Search for EOT claims
python -m scripts.narratives.embeddings search "extension of time" --type eot_claim

# Search for specific CO
python -m scripts.narratives.embeddings search "Change Order CO-03" --author Yates

# Search for delay events
python -m scripts.narratives.embeddings search "elevator delay critical path"
```

## Versioning

If a CO/EOT has multiple submissions or revisions:
- Use `_v1`, `_v2` suffix or date in filename
- Document revision history in the "Notes" section
- Keep only the most current analysis but reference prior versions

## Cross-References

Link related documents:
- CO-04 may reference EOT-04 (submitted together)
- EOT claims may reference multiple prior COs
- Use markdown links: `See [CO-02](../change_orders/CO-02_general_conditions.md)`
