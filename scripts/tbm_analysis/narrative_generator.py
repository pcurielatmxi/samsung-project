#!/usr/bin/env python3
"""
TBM Narrative Generator
=======================

Generates narrative content for specific report sections using Claude Code.
Takes structured data and produces professional analysis text.

See prompts/narrative_prompts.md for prompt design rationale and communication goals.

Usage:
    python narrative_generator.py --input report_content.json --output report_with_narratives.json
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Optional


# =============================================================================
# PROMPT TEMPLATES
# =============================================================================

# Output requirements appended to all prompts
OUTPUT_REQUIREMENTS = """
OUTPUT REQUIREMENTS:
- Plain text only (no markdown, no bullets, no formatting)
- 2-4 sentences maximum
- Include specific numbers from the data
- Professional, objective tone suitable for contractual correspondence
- Do NOT assign blame or use inflammatory language
"""


PROMPT_EXECUTIVE_SUMMARY = """You are writing an executive summary for a TBM (Tool Box Meeting) Analysis Report.
This report documents daily contractor workforce verification on a Samsung semiconductor construction project.

Write a 3-4 sentence executive summary that:
1. States the overall verification accuracy (LPI) and how it compares to the 80% target
2. Highlights the key cost impact (idle time waste, monthly projection)
3. Notes the primary documentation gap (zero verification locations)
4. Ends with a constructive statement about process improvement needs

Tone: Professional, objective, suitable for inclusion in contractual correspondence.
Focus on facts and quantifiable impact.
"""


PROMPT_CAT1_OPENING = """You are writing the opening paragraph for the "TBM Planning & Documentation Performance" section.

Write 2-3 sentences that:
1. State the overall Labor Productivity Index (LPI) - this is the % of committed workers actually verified in the field
2. Compare contractor performance (which had higher accuracy)
3. Note the gap between actual performance and the 80% target

Important context:
- LPI = (Verified Workers / Planned Workers) × 100
- Low LPI does NOT mean workers weren't working - it means they couldn't be verified during the available time window
- The target is 80% verification accuracy

Tone: Factual, objective. Do not assign blame for low numbers.
"""


PROMPT_CAT2_INTRO = """You are writing an introduction for the "Zero Verification Locations" section.

These are locations where the TBM (daily workforce plan) showed worker commitments,
but field verification found zero workers present.

Write 2-3 sentences that:
1. State how many locations had zero verification and what % of total this represents
2. Explain what this metric means (planning/documentation gaps, NOT necessarily worker absence)
3. Note that this is a documentation concern, not necessarily a performance issue

CRITICAL: Do NOT imply workers were absent or not working. Zero verification can result from:
- Inspectors couldn't reach the location in time
- Workers were reassigned but TBM wasn't updated
- Workers arrived/left outside verification window

Tone: Neutral, explanatory.
"""


PROMPT_CAT3_INTRO = """You are writing an introduction for the "Idle Worker Observations" section.

This section documents workers who were PHYSICALLY OBSERVED idle during field verification.
Unlike zero-verification (documentation issue), idle observations are ACTUAL OBSERVED WASTE.

Write 2-3 sentences that:
1. State the number of idle observations and total worker-hours affected
2. Quantify the daily cost impact
3. Emphasize this is observed waste, not inferred from documentation

Tone: Factual, emphasizing the quantifiable nature of this waste.
"""


PROMPT_CAT4_INTRO = """You are writing an introduction for the "High Verification Locations" section.

These locations showed >100% accuracy - more workers were verified than committed in the TBM.
This is generally POSITIVE, showing either additional workforce deployment or good worker mobility.

Write 2-3 sentences that:
1. Acknowledge these locations as examples of proper TBM implementation
2. Explain what >100% accuracy means (more verified than planned)
3. Note this could indicate additional workforce or workers moving from adjacent areas

Tone: Positive, recognizing good performance.
"""


PROMPT_CAT7_ROOT_CAUSE = """You are writing the "Root Cause Analysis" for the Process Limitations section.

This section explains WHY verification rates may be low - not to excuse poor performance,
but to provide context for fair interpretation of the data.

Write 2-3 sentences that:
1. Acknowledge the low overall verification rate from the data
2. Explain that timing constraints limit the verification window
3. Note that actual contractor performance may be better than metrics suggest due to these constraints

Key timing context:
- TBM should be submitted by 8:00 AM for adequate verification time
- Workers typically leave between 2:30-4:00 PM
- Late TBM submission = insufficient time to verify all locations
- Inspectors cannot physically reach all locations in limited time

Tone: Balanced, explanatory. Not making excuses, but providing necessary context.
"""


PROMPT_CAT8_ISSUES = """You are identifying the primary issues from TBM analysis data.

Based on the data provided, identify 2-4 key issues. Prioritize:
1. Quantifiable waste (idle time cost)
2. Documentation gaps (zero verification rate)
3. Process patterns from observation text

Format as brief factual statements. Include numbers where available.

Tone: Direct, factual.
"""


PROMPT_CAT8_RECOMMENDATIONS = """You are writing recommendations based on TBM analysis findings.

Write 3-5 actionable recommendations that:
1. Start with the most CRITICAL process fix (TBM submission timing)
2. Address specific issues found in the data
3. Include quantified monthly impact
4. Are realistic and actionable

Mark the most critical recommendation with "CRITICAL:" prefix.

Tone: Constructive, solution-focused.
"""


# =============================================================================
# NARRATIVE GENERATION FUNCTIONS
# =============================================================================

def generate_narrative(prompt: str, context: str, max_sentences: int = 4) -> Optional[str]:
    """
    Call Claude Code in non-interactive mode to generate narrative.

    Args:
        prompt: The specific instruction for what to generate
        context: The data context to analyze
        max_sentences: Maximum sentences (included in output requirements)

    Returns:
        Generated narrative text or None if failed
    """
    full_prompt = f"""{prompt}

DATA:
{context}

{OUTPUT_REQUIREMENTS}"""

    try:
        result = subprocess.run(
            ['claude', '-p', full_prompt],
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode == 0:
            # Clean up the output - remove any markdown or extra formatting
            output = result.stdout.strip()
            # Remove common markdown artifacts
            output = output.replace('**', '').replace('*', '').replace('`', '')
            return output
        else:
            print(f"  Claude error: {result.stderr}")
            return None

    except subprocess.TimeoutExpired:
        print("  Claude timed out")
        return None
    except FileNotFoundError:
        print("  Claude not found - using fallback")
        return None


def generate_executive_summary(content: dict) -> Optional[str]:
    """Generate executive summary narrative."""

    context = json.dumps({
        'report_date': content['report_date_formatted'],
        'contractor_group': content['contractor_group'],
        'lpi': content['lpi'],
        'lpi_target': content['lpi_target'],
        'idle_time_cost': content['idle_time_cost'],
        'idle_time_hours': content['idle_time_hours'],
        'monthly_projection': content['monthly_projection'],
        'zero_verification_count': content['zero_verification_count'],
        'zero_verification_rate': content['zero_verification_rate'],
        'total_locations': content['total_locations'],
        'total_tasks': content['total_tasks'],
        'contractors': content['contractors'],
    }, indent=2)

    narrative = generate_narrative(PROMPT_EXECUTIVE_SUMMARY, context)

    if not narrative:
        # Fallback
        narrative = (
            f"TBM verification for {content['report_date_formatted']} shows {content['lpi']:.0f}% overall accuracy "
            f"against the 80% target. Field verification documented ${content['idle_time_cost']:,.0f} in idle time waste "
            f"across {len(content['idle_observations'])} observations, projecting to ${content['monthly_projection']:.0f}K monthly. "
            f"{content['zero_verification_count']} locations ({content['zero_verification_rate']:.0f}%) showed zero verification, "
            f"indicating documentation gaps. Process improvements, particularly earlier TBM submission, would enable more comprehensive verification."
        )

    return narrative


def generate_cat1_narrative(content: dict) -> dict:
    """Generate Category 1: TBM Planning & Documentation Performance."""

    context = json.dumps({
        'lpi': content['lpi'],
        'lpi_target': content['lpi_target'],
        'contractor_metrics': content['contractor_metrics'],
        'total_tasks': content['total_tasks'],
        'total_planned': content['total_planned'],
        'total_verified': content['total_verified'],
        'zero_verification_count': content['zero_verification_count'],
    }, indent=2)

    opening = generate_narrative(PROMPT_CAT1_OPENING, context)

    if not opening:
        # Fallback - find better performing contractor
        better = max(content['contractor_metrics'], key=lambda x: x['accuracy'])
        opening = (
            f"TBM verification shows {content['lpi']:.0f}% overall accuracy against the 80% target. "
            f"{better['name']} leads with {better['accuracy']:.0f}% accuracy. "
            f"The gap between actual and target performance reflects both documentation challenges and verification time constraints."
        )

    return {
        'opening': opening,
        'timing_impact': None,  # Could add TBM receipt time if available in data
    }


def generate_cat2_narrative(content: dict) -> dict:
    """Generate Category 2: Zero Verification Locations."""

    # Count by contractor
    by_contractor = {}
    for loc in content['zero_verification_locations']:
        company = loc['company']
        by_contractor[company] = by_contractor.get(company, 0) + 1

    context = json.dumps({
        'zero_verification_count': content['zero_verification_count'],
        'total_locations': content['total_locations'],
        'zero_verification_rate': content['zero_verification_rate'],
        'by_contractor': by_contractor,
        'zero_verification_locations': content['zero_verification_locations'][:5],  # Sample
    }, indent=2)

    intro = generate_narrative(PROMPT_CAT2_INTRO, context)

    if not intro:
        intro = (
            f"The following {content['zero_verification_count']} locations had TBM commitments but zero field verification, "
            f"representing {content['zero_verification_rate']:.0f}% of all locations. This indicates planning/documentation gaps "
            f"where committed workers could not be verified during the available inspection window."
        )

    return {
        'intro': intro,
    }


def generate_cat3_narrative(content: dict) -> dict:
    """Generate Category 3: Idle Worker Observations - ACTUAL COST."""

    # Analyze observation patterns
    idle_obs = content['idle_observations']
    observation_texts = [o.get('observation_text', '').lower() for o in idle_obs]

    patterns = {
        'waiting_materials': sum(1 for t in observation_texts if 'material' in t or 'delivery' in t),
        'waiting_approval': sum(1 for t in observation_texts if 'approval' in t or 'authorization' in t),
        'waiting_equipment': sum(1 for t in observation_texts if 'equipment' in t or 'tool' in t),
        'no_manpower': sum(1 for o in idle_obs if o.get('status') == 'NO MANPOWER'),
        'standing_idle': sum(1 for t in observation_texts if 'idle' in t or 'standing' in t),
    }

    context = json.dumps({
        'idle_count': len(idle_obs),
        'idle_time_hours': content['idle_time_hours'],
        'idle_time_cost': content['idle_time_cost'],
        'labor_rate': content['labor_rate'],
        'monthly_projection': content['monthly_projection'],
        'summary_by_contractor': content['idle_summary_by_contractor'],
        'patterns': patterns,
        'sample_observations': [
            {'status': o['status'], 'observation': o.get('observation_text', '')[:100]}
            for o in idle_obs[:3]
        ],
    }, indent=2)

    intro = generate_narrative(PROMPT_CAT3_INTRO, context)

    if not intro:
        intro = (
            f"Field verification identified {len(idle_obs)} idle worker observations totaling "
            f"{content['idle_time_hours']:.0f} worker-hours. This represents ACTUAL OBSERVED WASTE - "
            f"${content['idle_time_cost']:,.0f} at ${content['labor_rate']}/hr. "
            f"Unlike zero-verification locations (documentation issue), these are workers physically observed not working."
        )

    return {
        'methodology': f"CALCULATION METHODOLOGY: Idle time cost based on 1-hour idle duration per observation. Worker counts from field verification. Labor rate: ${content['labor_rate']}/hr.",
        'intro': intro,
    }


def generate_cat4_narrative(content: dict) -> dict:
    """Generate Category 4: High Verification Locations."""

    high_ver = content['high_verification_locations']

    context = json.dumps({
        'high_verification_count': len(high_ver),
        'examples': high_ver[:5] if high_ver else [],
        'total_locations': content['total_locations'],
    }, indent=2)

    intro = generate_narrative(PROMPT_CAT4_INTRO, context)

    if not intro:
        if high_ver:
            intro = (
                f"Several locations showed excellent verification rates above 100%, demonstrating proper TBM implementation. "
                f"These {len(high_ver)} locations had more workers verified than committed, indicating either additional "
                f"workforce deployment or workers moving from adjacent areas to assist."
            )
        else:
            intro = (
                "No locations showed verification rates above 100% for this report period. "
                "This indicates workforce was deployed as planned without significant over-staffing or mobility between areas."
            )

    return {
        'intro': intro,
        'note': "Note: Accuracy >100% indicates more workers verified than committed in TBM, suggesting either additional workforce deployment or workers from adjacent areas.",
    }


def generate_cat7_narrative(content: dict) -> dict:
    """Generate Category 7: TBM Process Limitations."""

    context = json.dumps({
        'lpi': content['lpi'],
        'lpi_target': content['lpi_target'],
        'zero_verification_rate': content['zero_verification_rate'],
        'zero_verification_count': content['zero_verification_count'],
        'total_locations': content['total_locations'],
        'total_tasks': content['total_tasks'],
    }, indent=2)

    root_cause = generate_narrative(PROMPT_CAT7_ROOT_CAUSE, context)

    if not root_cause:
        root_cause = (
            f"The verification rates documented in this report ({content['lpi']:.0f}% overall, "
            f"{content['zero_verification_count']} locations with 0% verification) are partially attributable "
            f"to verification time constraints. When TBM is received late, inspectors have insufficient time "
            f"to verify all {content['total_locations']} committed locations before workers depart."
        )

    return {
        'root_cause': root_cause,
        'caveats': [
            "Unverified locations may reflect time constraints, not actual worker absence",
            "Idle time observations limited to areas inspectors could physically reach",
            "Some high-verification locations may indicate concentrated verification effort",
            "Zero-verification locations may be due to inability to reach remote areas in time",
            "Actual contractor performance may be better than reported metrics suggest",
        ],
        'improvements': [
            "TBM submission deadline: 8:00 AM daily (provides 6+ hour verification window)",
            "Minimum verification window: 6 hours from TBM receipt to worker departure",
            "Worker commitment: If in TBM, workers must remain on site until 4:00 PM minimum",
            "Real-time updates: Contractors must notify MXI immediately of schedule changes",
        ],
    }


def generate_cat8_narrative(content: dict) -> dict:
    """Generate Category 8: Conclusions & Recommendations."""

    # Analyze patterns from idle observations
    idle_obs = content['idle_observations']
    observation_texts = [o.get('observation_text', '').lower() for o in idle_obs]

    waiting_materials = sum(1 for t in observation_texts if 'material' in t or 'delivery' in t)
    waiting_approval = sum(1 for t in observation_texts if 'approval' in t or 'authorization' in t or 'waiting' in t)
    waiting_equipment = sum(1 for t in observation_texts if 'equipment' in t or 'tool' in t)

    # Build issues list
    issues = []
    if content['zero_verification_count'] > 0:
        issues.append(f"TBM Documentation: {content['zero_verification_rate']:.0f}% of locations show zero verification")
    if content['idle_time_cost'] > 0:
        issues.append(f"Idle time represents actual quantifiable waste (${content['idle_time_cost']:,.0f}/day)")
    if waiting_materials > 0:
        issues.append("Material/Equipment Delays: Workers observed idle waiting for deliveries")
    if waiting_approval > 0:
        issues.append("Approval Bottlenecks: Workers observed waiting for authorization to proceed")

    # Build recommendations
    recommendations = [
        "CRITICAL: Require TBM submission by 8:00 AM to allow proper verification planning",
        "Establish minimum notice period: 6 hours between TBM submission and verification requirement",
    ]

    if waiting_materials > 0:
        recommendations.append("Improve material logistics: Address material delivery timing to reduce worker idle time")
    if waiting_approval > 0:
        recommendations.append("Streamline approval process: Reduce worker wait time for work authorization")

    recommendations.extend([
        "Enhance TBM updates: Require real-time location updates when workers are reassigned",
        f"Continue monitoring idle time: ${content['monthly_projection']:.0f}K monthly projection requires mitigation",
    ])

    return {
        'issues': issues,
        'recommendations': recommendations,
    }


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def add_narratives(content: dict, use_claude: bool = True) -> dict:
    """Add all narrative content to report data.

    Args:
        content: Report content dictionary
        use_claude: If True, attempt Claude generation; if False, use fallbacks only

    Returns:
        Content dictionary with narratives added
    """
    print("Generating narratives...")

    if use_claude:
        print("  Generating executive summary...")
        exec_summary = generate_executive_summary(content)
    else:
        # Fallback executive summary
        exec_summary = (
            f"TBM verification for {content['report_date_formatted']} shows {content['lpi']:.0f}% overall accuracy "
            f"against the 80% target. Field verification documented ${content['idle_time_cost']:,.0f} in idle time waste "
            f"projecting to ${content['monthly_projection']:.0f}K monthly. "
            f"{content['zero_verification_count']} locations showed zero verification."
        )

    content['narratives'] = {
        'executive_summary': exec_summary,
        'cat1': generate_cat1_narrative(content) if use_claude else _fallback_cat1(content),
        'cat2': generate_cat2_narrative(content) if use_claude else _fallback_cat2(content),
        'cat3': generate_cat3_narrative(content) if use_claude else _fallback_cat3(content),
        'cat4': generate_cat4_narrative(content) if use_claude else _fallback_cat4(content),
        'cat7': generate_cat7_narrative(content) if use_claude else _fallback_cat7(content),
        'cat8': generate_cat8_narrative(content),  # Cat8 is data-driven, not AI-generated
    }

    print("  Category 1: TBM Performance - done")
    print("  Category 2: Zero Verification - done")
    print("  Category 3: Idle Observations - done")
    print("  Category 4: High Verification - done")
    print("  Category 7: Process Limitations - done")
    print("  Category 8: Conclusions - done")

    return content


def _fallback_cat1(content: dict) -> dict:
    """Fallback for Category 1 when Claude unavailable."""
    better = max(content['contractor_metrics'], key=lambda x: x['accuracy'])
    return {
        'opening': (
            f"TBM verification shows {content['lpi']:.0f}% overall accuracy against the 80% target. "
            f"{better['name']} leads with {better['accuracy']:.0f}% accuracy."
        ),
        'timing_impact': None,
    }


def _fallback_cat2(content: dict) -> dict:
    """Fallback for Category 2."""
    return {
        'intro': (
            f"The following {content['zero_verification_count']} locations had TBM commitments but zero field verification, "
            f"representing {content['zero_verification_rate']:.0f}% of all locations."
        ),
    }


def _fallback_cat3(content: dict) -> dict:
    """Fallback for Category 3."""
    return {
        'methodology': f"CALCULATION METHODOLOGY: Idle time cost based on 1-hour idle duration per observation. Labor rate: ${content['labor_rate']}/hr.",
        'intro': (
            f"Field verification identified {len(content['idle_observations'])} idle worker observations. "
            f"This represents ACTUAL WASTE - ${content['idle_time_cost']:,.0f}."
        ),
    }


def _fallback_cat4(content: dict) -> dict:
    """Fallback for Category 4."""
    return {
        'intro': "Several locations showed excellent verification rates, demonstrating proper TBM implementation.",
        'note': "Note: Accuracy >100% indicates more workers verified than committed in TBM.",
    }


def _fallback_cat7(content: dict) -> dict:
    """Fallback for Category 7."""
    return {
        'root_cause': (
            f"The low verification rates ({content['lpi']:.0f}% overall) may be partially "
            f"attributable to verification time constraints."
        ),
        'caveats': [
            "Unverified locations may reflect time constraints, not actual absence",
            "Idle time observations limited to accessible areas",
            "Actual contractor performance may be better than reported metrics suggest",
        ],
        'improvements': [
            "TBM submission deadline: 8:00 AM daily",
            "Minimum verification window: 6 hours from TBM receipt",
            "Real-time updates when workers reassigned",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description='Generate TBM report narratives')
    parser.add_argument('--input', required=True, help='Input report_content.json path')
    parser.add_argument('--output', help='Output path (default: same as input with _narratives suffix)')
    parser.add_argument('--skip-claude', action='store_true', help='Skip Claude, use fallback narratives only')

    args = parser.parse_args()

    # Load content
    input_path = Path(args.input)
    with open(input_path) as f:
        content = json.load(f)

    print(f"Loaded: {input_path}")

    # Generate narratives
    use_claude = not args.skip_claude
    if not use_claude:
        print("Using fallback narratives (Claude skipped)")

    content = add_narratives(content, use_claude=use_claude)

    # Save output
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.parent / 'report_content_with_narratives.json'

    with open(output_path, 'w') as f:
        json.dump(content, f, indent=2)

    print(f"\nSaved: {output_path}")


if __name__ == '__main__':
    main()
