# TBM Analysis Narrative Generation Prompt

You are analyzing TBM (Tool Box Meeting) data for the Samsung Taylor FAB1 project. Generate professional narrative content for each report category based on the provided data.

## Context

TBM Analysis reports compare planned workforce (from contractor TBM submissions) against field-verified observations. The goal is to identify:
- Planning accuracy (were committed workers present?)
- Idle time waste (workers observed not working)
- Documentation gaps (locations with zero verification)
- Process issues (timing, coordination)

## Input Data Structure

The input JSON contains:
- `metrics`: Calculated KPIs and aggregated data
- `tasks_for_narrative`: Individual field observations with inspector notes
- `tbm_summary`: Contractor-level summary data

## Output Requirements

Generate a JSON response with this structure:

```json
{
  "categories": [
    {
      "number": 1,
      "title": "TBM Planning & Documentation Performance",
      "content": [...]
    },
    ...
  ]
}
```

## Content Types

Each category's `content` array can contain:

### Text Paragraph
```json
{"type": "text", "text": "Narrative paragraph explaining the findings..."}
```

### Alert Box
```json
{"type": "alert", "text": "KEY FINDING: Important message", "level": "warning"}
```
Levels: "critical" (red), "warning" (orange), "success" (green)

### Subheader
```json
{"type": "subheader", "text": "1.1 Subsection Title"}
```

### Bullet List
```json
{"type": "bullet_list", "items": ["Point 1", "Point 2", "Point 3"]}
```

### Data Table
```json
{"type": "table", "data": [["Header1", "Header2"], ["Value1", "Value2"]]}
```

### Task Observation
```json
{
  "type": "task",
  "task_id": "1234",
  "location": "K-13",
  "floor": "Level 3",
  "status": "IDLE TIME",
  "observation": "Three workers were observed standing idle waiting for materials.",
  "inspector": "Nahomi Cedeno",
  "analysis": "IDLE - 3 workers × 1 hour × $65/hr = $195 waste. Workers standing idle waiting for materials."
}
```

## Category Guidelines

### Category 1: TBM Planning & Documentation Performance
- Overall accuracy assessment
- Compare planned vs verified totals
- Note any timing issues with TBM submission
- Discuss verification window constraints

### Category 2: Zero Verification Locations
- List locations where TBM committed workers but 0 were verified
- Group by contractor
- Include table: Floor | Location | TBM | Verified
- Note implications for planning accuracy

### Category 3: Idle Worker Observations - ACTUAL COST
- **CRITICAL**: Calculate actual dollar waste
- Formula: Idle Workers × 1 hour × $65/hr = Cost
- Include task observations with inspector quotes
- Group by contractor
- Sum total idle cost
- Add cost summary table

### Category 4: High Verification Locations - Best Practices
- Locations where verified > planned (>100% accuracy)
- These demonstrate proper TBM implementation
- Note that over-verification may indicate worker mobility

### Category 5: Key Findings
- Bullet list of key metrics
- Format: "METRIC: value (context)"
- Include: accuracy, idle cost, zero verification count, projections

### Category 6: Contractor Performance Comparison
- Side-by-side metrics per contractor
- Include performance tables
- Note relative strengths/weaknesses

### Category 7: TBM Process Limitations
- Time constraints affecting verification
- Methodology notes
- Factors that may skew results

### Category 8: Conclusions & Recommendations
- Primary issues identified
- Specific recommendations
- Use bullet lists for clarity

## Tone and Style

- Professional, objective language
- Quantitative focus - include numbers
- Quote inspector observations directly
- Use "verified" not "actual" for field counts
- Calculate costs explicitly: "X workers × 1 hour × $65/hr = $Y"
- Bold key metrics in text
- Be concise but complete

## Example Task Observation

Input observation from Fieldwire:
```
"Nahomi Cedeno: This column was not listed in the TBM. During an area inspection, three Berg workers were present in the work area. All three workers were observed standing idle waiting for materials."
```

Output:
```json
{
  "type": "task",
  "task_id": "1689",
  "location": "K-13",
  "floor": "Level 3",
  "status": "IDLE TIME",
  "observation": "This column was not listed in the TBM. During an area inspection, three Berg workers were present in the work area. All three workers were observed standing idle waiting for materials.",
  "inspector": "Nahomi Cedeno",
  "analysis": "IDLE - 3 workers × 1 hour × $65/hr = $195 waste. Workers standing idle waiting for materials. Note: Location not on TBM indicates documentation gap."
}
```

## Output Format

Return ONLY valid JSON. No markdown code blocks, no explanations, just the JSON object.
