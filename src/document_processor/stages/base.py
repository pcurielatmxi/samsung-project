"""
Abstract base class for pipeline stages.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Any, Dict

from ..config import StageConfig


@dataclass
class StageResult:
    """Result of processing a single file through a stage."""
    success: bool
    result: Optional[Any] = None
    error: Optional[str] = None
    usage: Optional[dict] = None
    duration_ms: int = 0
    retryable: bool = True


@dataclass
class FileTask:
    """A file to be processed through the pipeline."""
    source_path: Path
    relative_path: Path  # Relative to input_dir
    output_dir: Path
    stem: str

    def get_stage_output(self, stage: StageConfig) -> Path:
        """Get output path for a specific stage."""
        return self.output_dir / stage.folder_name / f"{self.stem}{stage.output_suffix}"

    def get_stage_error(self, stage: StageConfig) -> Path:
        """Get error marker path for a specific stage."""
        return self.output_dir / stage.folder_name / f"{self.stem}{stage.error_suffix}"

    def get_stage_input(self, stage: StageConfig, prior_stage: Optional[StageConfig]) -> Path:
        """
        Get input path for a stage.

        Stage 0: Returns source_path (original input file)
        Stage N: Returns output from stage N-1
        """
        if prior_stage is None:
            return self.source_path
        return self.get_stage_output(prior_stage)

    def stage_status(
        self,
        stage: StageConfig,
        prior_stage: Optional[StageConfig]
    ) -> str:
        """
        Get status for a specific stage.

        Returns:
            "completed" - Output file exists
            "failed" - Error file exists
            "blocked" - Prior stage not completed
            "pending" - Ready to process
        """
        output = self.get_stage_output(stage)
        error = self.get_stage_error(stage)

        if output.exists():
            return "completed"
        if error.exists():
            return "failed"

        # Check if blocked by prior stage
        if prior_stage is not None:
            prior_output = self.get_stage_output(prior_stage)
            if not prior_output.exists():
                return "blocked"

        return "pending"


class BaseStage(ABC):
    """Abstract base class for pipeline stages."""

    def __init__(self, config: StageConfig, config_dir: Path):
        """
        Initialize stage.

        Args:
            config: Stage configuration
            config_dir: Path to config directory (for loading scripts/prompts)
        """
        self.config = config
        self.config_dir = config_dir

    @property
    def name(self) -> str:
        """Stage name."""
        return self.config.name

    @abstractmethod
    async def process(
        self,
        task: FileTask,
        input_path: Path,
    ) -> StageResult:
        """
        Process a single file.

        Args:
            task: The file task to process
            input_path: Path to input file (source or prior stage output)

        Returns:
            StageResult with success/failure and output data
        """
        pass

    def get_output_path(self, task: FileTask) -> Path:
        """Get output path for this stage."""
        return task.get_stage_output(self.config)

    def get_error_path(self, task: FileTask) -> Path:
        """Get error marker path for this stage."""
        return task.get_stage_error(self.config)
