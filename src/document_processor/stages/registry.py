"""
Stage type registry.

Maps stage type names to implementation classes.
"""

from pathlib import Path
from typing import Dict, Type

from .base import BaseStage
from .llm_stage import LLMStage
from .script_stage import ScriptStage
from ..config import StageConfig


# Registry mapping type names to stage classes
STAGE_TYPES: Dict[str, Type[BaseStage]] = {
    "llm": LLMStage,
    "script": ScriptStage,
}


def create_stage(config: StageConfig, config_dir: Path) -> BaseStage:
    """
    Factory function to create stage instances.

    Args:
        config: Stage configuration
        config_dir: Path to config directory

    Returns:
        Instantiated stage object

    Raises:
        ValueError: If stage type is unknown
    """
    stage_class = STAGE_TYPES.get(config.type)
    if stage_class is None:
        valid_types = ", ".join(STAGE_TYPES.keys())
        raise ValueError(
            f"Unknown stage type '{config.type}' for stage '{config.name}'. "
            f"Valid types: {valid_types}"
        )

    return stage_class(config, config_dir)
