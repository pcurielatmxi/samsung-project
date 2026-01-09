"""
Aggregate stage implementation.

Runs once after all per-file stages complete, processing all outputs together.
Used for consolidation, summary generation, or cross-file analysis.
"""

import importlib.util
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional, List

from ..config import StageConfig, PipelineConfig


@dataclass
class AggregateResult:
    """Result of running an aggregate stage."""
    success: bool
    error: Optional[str] = None
    files_processed: int = 0
    output_files: List[str] = None
    duration_ms: int = 0

    def __post_init__(self):
        if self.output_files is None:
            self.output_files = []


class AggregateStage:
    """
    Aggregate processing stage.

    Unlike per-file stages, aggregate stages run once after all files
    have been processed through prior stages. They receive the entire
    output directory and produce aggregate outputs (CSVs, summaries, etc.).

    Expected function signature:
        def aggregate(
            input_dir: Path,      # Prior stage output directory
            output_dir: Path,     # This stage's output directory
            config: PipelineConfig,
        ) -> AggregateResult
    """

    def __init__(
        self,
        stage_config: StageConfig,
        pipeline_config: PipelineConfig,
        config_dir: Path,
    ):
        self.stage_config = stage_config
        self.pipeline_config = pipeline_config
        self.config_dir = config_dir

        if not stage_config.script:
            raise ValueError(f"Aggregate stage '{stage_config.name}' requires a script path")
        if not stage_config.function:
            raise ValueError(f"Aggregate stage '{stage_config.name}' requires a function name")

        self._module = None
        self._function: Optional[Callable] = None

    def _load_script(self) -> None:
        """Dynamically load the Python script and function."""
        if self._module is not None:
            return

        script_path = self.config_dir / self.stage_config.script

        if not script_path.exists():
            raise FileNotFoundError(f"Script not found: {script_path}")

        try:
            spec = importlib.util.spec_from_file_location(
                f"aggregate_{self.stage_config.name}",
                script_path,
            )
            if spec is None or spec.loader is None:
                raise ImportError(f"Cannot load script: {script_path}")

            self._module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self._module)

            self._function = getattr(self._module, self.stage_config.function, None)
            if self._function is None:
                raise AttributeError(
                    f"Function '{self.stage_config.function}' not found in {script_path}"
                )

        except Exception as e:
            raise ImportError(f"Failed to load script {script_path}: {e}")

    def get_input_dir(self, prior_stage: StageConfig) -> Path:
        """Get the input directory (prior stage's output folder)."""
        return self.pipeline_config.output_dir / prior_stage.folder_name

    def get_output_dir(self) -> Path:
        """Get this stage's output directory."""
        return self.pipeline_config.output_dir / self.stage_config.folder_name

    def run(self, prior_stage: StageConfig) -> AggregateResult:
        """
        Run the aggregate stage.

        Args:
            prior_stage: The stage whose outputs will be aggregated

        Returns:
            AggregateResult with success/failure and output info
        """
        start_time = time.time()

        try:
            # Ensure script is loaded
            self._load_script()

            # Get directories
            input_dir = self.get_input_dir(prior_stage)
            output_dir = self.get_output_dir()

            # Create output directory if needed
            output_dir.mkdir(parents=True, exist_ok=True)

            # Verify input directory exists
            if not input_dir.exists():
                return AggregateResult(
                    success=False,
                    error=f"Input directory not found: {input_dir}",
                )

            # Call the aggregate function
            try:
                result = self._function(
                    input_dir=input_dir,
                    output_dir=output_dir,
                    config=self.pipeline_config,
                )

                # If function returns AggregateResult, use it
                if isinstance(result, AggregateResult):
                    result.duration_ms = int((time.time() - start_time) * 1000)
                    return result

                # Otherwise, wrap the result
                duration_ms = int((time.time() - start_time) * 1000)
                return AggregateResult(
                    success=True,
                    files_processed=result.get("files_processed", 0) if isinstance(result, dict) else 0,
                    output_files=result.get("output_files", []) if isinstance(result, dict) else [],
                    duration_ms=duration_ms,
                )

            except Exception as e:
                duration_ms = int((time.time() - start_time) * 1000)
                return AggregateResult(
                    success=False,
                    error=f"Aggregate execution failed: {e}",
                    duration_ms=duration_ms,
                )

        except FileNotFoundError as e:
            return AggregateResult(
                success=False,
                error=str(e),
            )
        except (ImportError, AttributeError) as e:
            return AggregateResult(
                success=False,
                error=str(e),
            )
        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            return AggregateResult(
                success=False,
                error=str(e),
                duration_ms=duration_ms,
            )
