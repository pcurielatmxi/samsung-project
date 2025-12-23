# TBM Report Narrative Prompts

## Report Objectives & Communication Goals

### Primary Objective
Document and quantify contractor workforce management performance to support contractual discussions and process improvements.

### Communication Goals

1. **Objective Documentation** - Present facts without accusation
   - Focus on what was observed, not why it happened
   - Use passive voice where appropriate to avoid assigning blame
   - Let the data speak for itself

2. **Quantifiable Impact** - Translate observations into dollars
   - Every idle observation = measurable waste
   - Every zero-verification location = documentation gap
   - Monthly projections show cumulative impact

3. **Constructive Framing** - Position for improvement
   - Acknowledge process constraints (TBM timing)
   - Highlight what's working (high verification locations)
   - Provide actionable recommendations

4. **Professional Tone** - Suitable for contractual discussions
   - Formal but accessible language
   - No inflammatory language
   - Evidence-based conclusions only

---

## Section-by-Section Prompts

### Executive Summary (NEW)

**Purpose:** High-level overview for executives who may only read this section.

**Prompt:**
```
You are writing an executive summary for a TBM (Tool Box Meeting) Analysis Report.
This report documents daily contractor workforce verification on a Samsung semiconductor construction project.

Write a 3-4 sentence executive summary that:
1. States the overall verification accuracy and how it compares to the 80% target
2. Highlights the key cost impact (idle time waste)
3. Notes the primary documentation gap (zero verification locations)
4. Ends with a forward-looking statement about process improvement needs

Tone: Professional, objective, suitable for inclusion in contractual correspondence.
Do NOT use inflammatory language or assign blame.
Focus on facts and quantifiable impact.
```

**Context Data:**
```json
{
  "report_date": "December 19, 2025",
  "lpi": 19,
  "lpi_target": 80,
  "idle_time_cost": 455,
  "monthly_projection": 12,
  "zero_verification_count": 3,
  "total_locations": 17,
  "contractor_metrics": [...]
}
```

---

### Category 1: TBM Planning & Documentation Performance

**Purpose:** Open the detailed analysis with context on overall performance.

**Prompt:**
```
You are writing the opening paragraph for the "TBM Planning & Documentation Performance" section.

Write 2-3 sentences that:
1. State the overall Labor Productivity Index (LPI) - this is the % of committed workers actually verified in the field
2. Compare contractor performance (which had higher accuracy)
3. Note the gap between actual performance and the 80% target

Important context:
- LPI = (Verified Workers / Planned Workers) × 100
- Low LPI does NOT mean workers weren't working - it means they couldn't be verified during the available time window
- TBM = Tool Box Meeting, the daily workforce planning document contractors submit

Tone: Factual, objective. Do not assign blame for low numbers.
```

---

### Category 2: Zero Verification Locations

**Purpose:** Document locations where workers were planned but none were verified.

**Prompt:**
```
You are writing an introduction for the "Zero Verification Locations" section.

These are locations where the TBM (daily workforce plan) showed worker commitments,
but field verification found zero workers present.

Write 2-3 sentences that:
1. State how many locations had zero verification and what % of total this represents
2. Explain what this metric means (planning/documentation gaps, NOT necessarily worker absence)
3. Note that this is a documentation concern, not necessarily a performance issue

CRITICAL: Do NOT imply workers were absent or not working. Zero verification can mean:
- Inspectors couldn't reach the location in time
- Workers were reassigned but TBM wasn't updated
- Workers arrived/left outside verification window

Tone: Neutral, explanatory.
```

---

### Category 3: Idle Worker Observations - ACTUAL COST

**Purpose:** Document observed idle time and its cost impact.

**Prompt:**
```
You are writing an introduction for the "Idle Worker Observations" section.

This section documents workers who were PHYSICALLY OBSERVED idle during field verification.
Unlike zero-verification (documentation issue), idle observations are ACTUAL OBSERVED WASTE.

Write 2-3 sentences that:
1. State the number of idle observations and total worker-hours affected
2. Quantify the daily cost at $65/hour labor rate
3. Emphasize this is observed waste, not inferred from documentation

CRITICAL context for each observation:
- Inspector physically saw workers not actively working
- Observations include context (waiting for materials, standing idle, etc.)
- Cost calculation: idle_workers × 1 hour × $65/hr

Tone: Factual, emphasizing the quantifiable nature of this waste.
```

**Per-Observation Analysis Prompt:**
```
You are writing a brief analysis for an idle worker observation.

Given the field observation text from the inspector, write 1-2 sentences that:
1. Classify the idle type (WAITING for materials/approval, STANDING IDLE, NO MANPOWER found, etc.)
2. State the cost calculation: X workers × 1 hour × $65/hr = $Y
3. If the observation text mentions a specific cause (materials, approval, equipment), note it

Example outputs:
- "WAITING - MATERIALS: 3 workers observed waiting for drywall delivery. Cost: 3 × $65 = $195 waste."
- "NO MANPOWER: Location inspected but committed workers not found in area. Documentation gap."
- "PARTIAL IDLE: 2 of 4 workers actively working, 2 standing idle. Cost: 2 × $65 = $130 waste."

Tone: Analytical, brief.
```

---

### Category 4: High Verification Locations

**Purpose:** Highlight positive examples where verification exceeded commitments.

**Prompt:**
```
You are writing an introduction for the "High Verification Locations" section.

These locations showed >100% accuracy - more workers were verified than committed in the TBM.
This is generally POSITIVE, showing either additional workforce deployment or good worker mobility.

Write 2-3 sentences that:
1. Acknowledge these locations as examples of proper TBM implementation
2. Explain what >100% accuracy means (more verified than planned)
3. Note this could indicate additional workforce or workers moving from adjacent areas

Tone: Positive, recognizing good performance.
```

---

### Category 7: TBM Process Limitations

**Purpose:** Provide context on systemic issues affecting verification accuracy.

**Prompt:**
```
You are writing the "Root Cause Analysis" for the Process Limitations section.

This section explains WHY verification rates may be low - not to excuse poor performance,
but to provide context for fair interpretation of the data.

Write 2-3 sentences that:
1. Acknowledge the low overall verification rate
2. Explain that timing constraints (late TBM submission) limit verification window
3. Note that actual contractor performance may be better than metrics suggest

Key timing context:
- TBM should be submitted by 8:00 AM
- Workers typically leave by 2:30-4:00 PM
- Late TBM submission = insufficient time to verify all locations
- Inspectors cannot be everywhere at once

Tone: Balanced, explanatory. Not making excuses, but providing necessary context.
```

---

### Category 8: Conclusions & Recommendations

**Purpose:** Synthesize findings and provide actionable recommendations.

**Issues Analysis Prompt:**
```
You are identifying the primary issues from the TBM analysis data.

Based on the data provided, identify 3-5 key issues. Prioritize:
1. Quantifiable waste (idle time cost)
2. Documentation gaps (zero verification rate)
3. Process issues (timing constraints)
4. Patterns from observation text (waiting for materials, approval bottlenecks)

Format each issue as a brief statement with quantification where possible.
Example: "Idle time represents $455/day in documented waste"

Tone: Direct, factual.
```

**Recommendations Prompt:**
```
You are writing recommendations based on TBM analysis findings.

Write 4-6 actionable recommendations that:
1. Start with the most CRITICAL process fix (usually TBM submission timing)
2. Address specific issues found in the data (idle patterns, documentation gaps)
3. Include quantified impact where possible (monthly projection)
4. Are realistic and actionable

Mark the most critical recommendation with "CRITICAL:" prefix.

Format: Brief, action-oriented bullet points.
Tone: Constructive, solution-focused.
```

---

## Fallback Templates

When Claude is unavailable, use these data-driven templates:

### Executive Summary Fallback
```python
f"TBM verification for {report_date} shows {lpi}% overall accuracy against the 80% target. "
f"Field verification documented ${idle_time_cost:,.0f} in idle time waste across {idle_count} observations. "
f"{zero_verification_count} locations ({zero_verification_rate}%) showed zero verification, indicating documentation gaps. "
f"Process improvements, particularly earlier TBM submission, would enable more comprehensive verification."
```

### Category 1 Fallback
```python
f"TBM verification shows {lpi}% overall accuracy against an 80% target. "
f"{better_contractor} leads with {better_accuracy}% accuracy. "
f"The gap between actual and target performance reflects both documentation challenges and verification time constraints."
```

### Category 3 Fallback
```python
f"Field verification identified {idle_count} idle worker observations totaling {idle_hours} worker-hours. "
f"This represents ACTUAL OBSERVED WASTE - ${idle_cost:,.0f} at ${labor_rate}/hr. "
f"Unlike zero-verification locations (documentation issue), these are workers physically observed not working."
```

---

## Output Requirements

All generated narratives must:

1. **Be plain text** - No markdown, no bullet points, no formatting
2. **Be concise** - 2-4 sentences maximum per section
3. **Include numbers** - Quantify everything possible
4. **Avoid blame** - Focus on what, not who
5. **Be professional** - Suitable for contractual correspondence
