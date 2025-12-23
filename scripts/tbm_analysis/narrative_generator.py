#!/usr/bin/env python3
"""
TBM Narrative Generator
=======================

Generates narrative content for specific report sections using Claude Code.
Takes structured data and produces professional analysis text.

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
# NARRATIVE GENERATION FUNCTIONS
# =============================================================================

def generate_narrative(prompt: str, context: str) -> Optional[str]:
    """
    Call Claude Code in non-interactive mode to generate narrative.

    Args:
        prompt: The specific instruction for what to generate
        context: The data context to analyze

    Returns:
        Generated narrative text or None if failed
    """
    full_prompt = f"""{prompt}

DATA:
{context}

OUTPUT REQUIREMENTS:
- Professional, objective tone
- Quantitative focus with specific numbers
- 2-4 sentences maximum
- No markdown formatting
- Plain text only"""

    try:
        result = subprocess.run(
            ['claude', '-p', full_prompt],
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode == 0:
            return result.stdout.strip()
        else:
            print(f"Claude error: {result.stderr}")
            return None

    except subprocess.TimeoutExpired:
        print("Claude timed out")
        return None
    except FileNotFoundError:
        print("Claude not found - using fallback")
        return None


def generate_cat1_narrative(content: dict) -> dict:
    """Generate Category 1: TBM Planning & Documentation Performance."""

    # Build context
    context = json.dumps({
        'lpi': content['lpi'],
        'lpi_target': content['lpi_target'],
        'contractor_metrics': content['contractor_metrics'],
        'total_tasks': content['total_tasks'],
        'zero_verification_count': content['zero_verification_count'],
    }, indent=2)

    # Opening paragraph
    opening = generate_narrative(
        "Write an opening assessment of TBM verification performance. Compare overall accuracy to target, note which contractor performed better.",
        context
    )

    if not opening:
        # Fallback
        better = max(content['contractor_metrics'], key=lambda x: x['accuracy'])
        opening = f"TBM verification shows {content['lpi']}% overall accuracy against an 80% target. {better['name']} leads with {better['accuracy']}% accuracy."

    return {
        'opening': opening,
        'timing_impact': None,  # Could add TBM receipt time analysis if available
    }


def generate_cat2_narrative(content: dict) -> dict:
    """Generate Category 2: Zero Verification Locations."""

    context = json.dumps({
        'zero_verification_count': content['zero_verification_count'],
        'total_locations': content['total_locations'],
        'zero_verification_rate': content['zero_verification_rate'],
        'by_contractor': {
            c['contractor']: len([z for z in content['zero_verification_locations'] if z['company'] == c['contractor']])
            for c in content['idle_summary_by_contractor']
        }
    }, indent=2)

    intro = generate_narrative(
        "Write a brief intro explaining what zero verification locations mean and their significance for TBM planning accuracy.",
        context
    )

    if not intro:
        intro = f"The following {content['zero_verification_count']} locations had TBM commitments but zero field verification. This represents planning/documentation gaps requiring attention."

    return {
        'intro': intro,
    }


def generate_cat3_narrative(content: dict) -> dict:
    """Generate Category 3: Idle Worker Observations - ACTUAL COST."""

    context = json.dumps({
        'idle_count': len(content['idle_observations']),
        'idle_time_hours': content['idle_time_hours'],
        'idle_time_cost': content['idle_time_cost'],
        'labor_rate': content['labor_rate'],
        'summary_by_contractor': content['idle_summary_by_contractor'],
    }, indent=2)

    intro = generate_narrative(
        "Write a brief intro explaining that these are actual observed idle workers representing quantifiable waste.",
        context
    )

    if not intro:
        intro = f"Field verification identified {len(content['idle_observations'])} idle worker observations. This represents ACTUAL WASTE - quantifiable cost of ${content['idle_time_cost']:,.0f}."

    return {
        'methodology': f"CALCULATION METHODOLOGY: Idle time cost based on 1-hour idle duration per observation. Worker counts from field verification. Labor rate: ${content['labor_rate']}/hr.",
        'intro': intro,
    }


def generate_cat4_narrative(content: dict) -> dict:
    """Generate Category 4: High Verification Locations."""

    context = json.dumps({
        'high_verification_count': len(content['high_verification_locations']),
        'examples': content['high_verification_locations'][:5],
    }, indent=2)

    intro = generate_narrative(
        "Write a brief intro explaining that these locations showed excellent verification rates above 100%, demonstrating proper TBM implementation.",
        context
    )

    if not intro:
        intro = "Several locations showed excellent verification rates, demonstrating proper TBM implementation and workforce documentation."

    return {
        'intro': intro,
        'note': "Note: Accuracy >100% indicates more workers verified than committed in TBM, suggesting either additional workforce deployment or workers from adjacent areas.",
    }


def generate_cat7_narrative(content: dict) -> dict:
    """Generate Category 7: TBM Process Limitations."""

    context = json.dumps({
        'zero_verification_rate': content['zero_verification_rate'],
        'total_locations': content['total_locations'],
        'lpi': content['lpi'],
    }, indent=2)

    root_cause = generate_narrative(
        "Write a brief root cause analysis explaining why verification rates may be low, focusing on systemic issues like timing constraints.",
        context
    )

    if not root_cause:
        root_cause = f"The low verification rates documented in this report ({content['lpi']}% overall, {content['zero_verification_count']} locations with 0% verification) may be partially attributable to verification time constraints."

    return {
        'root_cause': root_cause,
        'caveats': [
            "Unverified locations may reflect time constraints, not actual absence",
            "Idle time observations limited to accessible areas",
            "Some high-verification locations may indicate concentrated verification",
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
    waiting_materials = len([o for o in idle_obs if 'material' in o.get('observation_text', '').lower()])
    waiting_approval = len([o for o in idle_obs if 'approval' in o.get('observation_text', '').lower()])
    waiting_equipment = len([o for o in idle_obs if 'equipment' in o.get('observation_text', '').lower()])

    issues = []
    if waiting_materials > 0:
        issues.append("Material/Equipment Delays: Workers idle waiting for deliveries")
    if waiting_approval > 0:
        issues.append("Approval Bottlenecks: Workers waiting for authorization to proceed")
    if content['zero_verification_count'] > 0:
        issues.append(f"TBM Documentation: {content['zero_verification_rate']}% of locations show zero verification")
    if content['idle_time_cost'] > 0:
        issues.append(f"Idle time represents actual quantifiable waste (${content['idle_time_cost']:,.0f}/day)")

    recommendations = [
        "CRITICAL: Require TBM submission by 8:00 AM to allow proper verification planning",
        "Establish minimum notice period: 6 hours between TBM submission and verification requirement",
    ]

    if waiting_materials > 0:
        recommendations.append("Improve material logistics: Address material delivery delays")
    if waiting_approval > 0:
        recommendations.append("Streamline approval process: Reduce waiting time for work authorization")

    recommendations.extend([
        "Enhance TBM updates: Real-time location updates when workers reassigned",
        f"Continue monitoring idle time: ${content['monthly_projection']}K monthly projection requires mitigation",
    ])

    return {
        'issues': issues,
        'recommendations': recommendations,
    }


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def add_narratives(content: dict) -> dict:
    """Add all narrative content to report data."""

    print("Generating narratives...")

    content['narratives'] = {
        'cat1': generate_cat1_narrative(content),
        'cat2': generate_cat2_narrative(content),
        'cat3': generate_cat3_narrative(content),
        'cat4': generate_cat4_narrative(content),
        'cat7': generate_cat7_narrative(content),
        'cat8': generate_cat8_narrative(content),
    }

    print("  Category 1: TBM Performance - done")
    print("  Category 2: Zero Verification - done")
    print("  Category 3: Idle Observations - done")
    print("  Category 4: High Verification - done")
    print("  Category 7: Process Limitations - done")
    print("  Category 8: Conclusions - done")

    return content


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
    if args.skip_claude:
        print("Using fallback narratives (Claude skipped)")
    content = add_narratives(content)

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
