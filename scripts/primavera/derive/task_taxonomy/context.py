"""
Task Context Builder for Task Taxonomy

Builds a combined DataFrame with all columns needed for field inference:
- Task data (task_id, task_name, wbs_id)
- WBS hierarchy (tier_2 through tier_6, wbs_name)
- Activity codes (z_trade, z_bldg, z_level, z_sub_contractor)
- TaskClassifier inference (scope, phase, building, level, impact fields)
"""

import sys
from pathlib import Path

import pandas as pd

# Add project root for src imports
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.classifiers.task_classifier import TaskClassifier


def build_activity_code_lookup(
    taskactv_df: pd.DataFrame,
    actvcode_df: pd.DataFrame,
    actvtype_df: pd.DataFrame
) -> dict:
    """
    Build lookup table for activity codes per task.

    Returns:
        Dict: task_id -> {
            'Z-TRADE': value,
            'Z-BLDG': value,
            'Z-LEVEL': value,
            'Z-SUB CONTRACTOR': value,
        }
    """
    # Build actvtype lookup: actv_code_type_id -> actv_code_type
    type_lookup = dict(zip(actvtype_df['actv_code_type_id'], actvtype_df['actv_code_type']))

    # Build actvcode lookup: actv_code_id -> actv_code_name
    code_lookup = dict(zip(actvcode_df['actv_code_id'], actvcode_df['actv_code_name']))

    # Build actvcode to type lookup: actv_code_id -> actv_code_type
    code_to_type = {}
    for _, row in actvcode_df.iterrows():
        actv_code_id = row['actv_code_id']
        type_id = row['actv_code_type_id']
        if type_id in type_lookup:
            code_to_type[actv_code_id] = type_lookup[type_id]

    # Build task -> activity codes lookup
    task_actv_lookup = {}
    code_types_of_interest = {'Z-TRADE', 'Z-BLDG', 'Z-LEVEL', 'Z-SUB CONTRACTOR'}

    for _, row in taskactv_df.iterrows():
        task_id = row['task_id']
        actv_code_id = row['actv_code_id']

        # Get the code type and value
        code_type = code_to_type.get(actv_code_id)
        code_value = code_lookup.get(actv_code_id)

        if code_type in code_types_of_interest and code_value:
            if task_id not in task_actv_lookup:
                task_actv_lookup[task_id] = {}
            task_actv_lookup[task_id][code_type] = code_value

    return task_actv_lookup


def build_task_context(
    tasks_df: pd.DataFrame,
    wbs_df: pd.DataFrame,
    taskactv_df: pd.DataFrame,
    actvcode_df: pd.DataFrame,
    actvtype_df: pd.DataFrame,
    verbose: bool = True
) -> pd.DataFrame:
    """
    Build combined task context with all columns needed for inference.

    Merges:
    - Task data (task_id, task_name, wbs_id)
    - WBS hierarchy (tier_2 through tier_6, wbs_name)
    - Activity codes (z_trade, z_bldg, z_level, z_sub_contractor)
    - TaskClassifier inference (scope, phase, building, level, impact fields)

    Args:
        tasks_df: Task table from P6
        wbs_df: WBS table with tier columns
        taskactv_df: Task activity code assignments
        actvcode_df: Activity code definitions
        actvtype_df: Activity code types
        verbose: Print progress messages

    Returns:
        DataFrame with columns:
            task_id, task_name, wbs_id, wbs_name,
            tier_2, tier_3, tier_4, tier_5, tier_6,
            z_trade, z_bldg, z_level, z_sub_contractor,
            scope, scope_desc, phase, phase_desc,
            building_inferred, level_inferred,
            loc_type, loc_type_desc, loc_id, label,
            impact_code, impact_type, impact_type_desc,
            attributed_to, attributed_to_desc,
            root_cause, root_cause_desc
    """
    if verbose:
        print("Building task context...")

    # Build activity code lookup
    if verbose:
        print("  Building activity code lookups...")
    task_actv_lookup = build_activity_code_lookup(taskactv_df, actvcode_df, actvtype_df)

    if verbose:
        has_trade = sum(1 for v in task_actv_lookup.values() if 'Z-TRADE' in v)
        has_bldg = sum(1 for v in task_actv_lookup.values() if 'Z-BLDG' in v)
        has_level = sum(1 for v in task_actv_lookup.values() if 'Z-LEVEL' in v)
        has_sub = sum(1 for v in task_actv_lookup.values() if 'Z-SUB CONTRACTOR' in v)
        print(f"    Z-TRADE: {has_trade:,} tasks")
        print(f"    Z-BLDG: {has_bldg:,} tasks")
        print(f"    Z-LEVEL: {has_level:,} tasks")
        print(f"    Z-SUB CONTRACTOR: {has_sub:,} tasks")

    # Prepare WBS columns
    wbs_cols = ['wbs_id', 'wbs_name', 'tier_2', 'tier_3', 'tier_4', 'tier_5', 'tier_6']
    available_wbs_cols = [c for c in wbs_cols if c in wbs_df.columns]
    wbs_subset = wbs_df[available_wbs_cols].copy()

    # Merge tasks with WBS
    if verbose:
        print("  Merging tasks with WBS hierarchy...")
    context = tasks_df[['task_id', 'task_name', 'wbs_id']].merge(
        wbs_subset,
        on='wbs_id',
        how='left'
    )

    # Add activity codes as columns
    if verbose:
        print("  Adding activity codes...")
    context['z_trade'] = context['task_id'].map(
        lambda x: task_actv_lookup.get(x, {}).get('Z-TRADE')
    )
    context['z_bldg'] = context['task_id'].map(
        lambda x: task_actv_lookup.get(x, {}).get('Z-BLDG')
    )
    context['z_level'] = context['task_id'].map(
        lambda x: task_actv_lookup.get(x, {}).get('Z-LEVEL')
    )
    context['z_sub_contractor'] = context['task_id'].map(
        lambda x: task_actv_lookup.get(x, {}).get('Z-SUB CONTRACTOR')
    )

    # Run TaskClassifier on all tasks
    if verbose:
        print("  Running TaskClassifier inference...")
    classifier = TaskClassifier()

    # Get WBS name lookup for classification context
    wbs_names = wbs_df.set_index('wbs_id')['wbs_name'].to_dict()

    classifications = []
    total = len(context)
    for idx, row in context.iterrows():
        if verbose and idx % 10000 == 0 and idx > 0:
            print(f"    Processed {idx:,}/{total:,} ({idx/total*100:.1f}%)")

        task_name = str(row.get('task_name', ''))
        wbs_name = wbs_names.get(row['wbs_id'], '')
        classification = classifier.classify_task(task_name, wbs_name)
        classifications.append(classification)

    if verbose:
        print(f"    Processed {total:,}/{total:,} (100%)")

    # Add classification columns
    classification_df = pd.DataFrame(classifications)

    # Rename building/level to avoid collision with final output
    classification_df = classification_df.rename(columns={
        'building': 'building_inferred',
        'level': 'level_inferred',
    })

    # Concatenate (same index)
    context = pd.concat([context.reset_index(drop=True), classification_df], axis=1)

    if verbose:
        print(f"  Built context for {len(context):,} tasks")
        print(f"  Columns: {list(context.columns)}")

    return context
