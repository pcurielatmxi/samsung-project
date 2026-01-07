"""
Python script stage implementation.

Dynamically loads and executes Python scripts for custom processing.
"""

import importlib.util
import json
import time
from pathlib import Path
from typing import Any, Callable, Optional

from .base import BaseStage, StageResult, FileTask
from ..config import StageConfig


class ScriptStage(BaseStage):
    """
    Python script processing stage.

    Dynamically imports a Python script from the config directory
    and calls a specified function for each file.

    Expected function signature:
        def process_record(input_data: dict, source_path: Path) -> dict
    """

    def __init__(self, config: StageConfig, config_dir: Path):
        super().__init__(config, config_dir)
        if not config.script:
            raise ValueError(f"Script stage '{config.name}' requires a script path")
        if not config.function:
            raise ValueError(f"Script stage '{config.name}' requires a function name")

        self._module = None
        self._function: Optional[Callable] = None

    def _load_script(self) -> None:
        """Dynamically load the Python script and function."""
        if self._module is not None:
            return

        script_path = self.config_dir / self.config.script

        if not script_path.exists():
            raise FileNotFoundError(f"Script not found: {script_path}")

        try:
            spec = importlib.util.spec_from_file_location(
                f"postprocess_{self.config.name}",
                script_path,
            )
            if spec is None or spec.loader is None:
                raise ImportError(f"Cannot load script: {script_path}")

            self._module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self._module)

            self._function = getattr(self._module, self.config.function, None)
            if self._function is None:
                raise AttributeError(
                    f"Function '{self.config.function}' not found in {script_path}"
                )

        except Exception as e:
            raise ImportError(f"Failed to load script {script_path}: {e}")

    async def process(
        self,
        task: FileTask,
        input_path: Path,
    ) -> StageResult:
        """
        Process a file with the Python script.

        Reads JSON from prior stage, calls the processing function,
        and returns the result.
        """
        start_time = time.time()

        try:
            # Ensure script is loaded
            self._load_script()

            # Read input JSON
            try:
                with open(input_path, "r", encoding="utf-8") as f:
                    input_data = json.load(f)
            except Exception as e:
                return StageResult(
                    success=False,
                    error=f"Failed to read input file: {e}",
                    retryable=False,
                )

            # Call the processing function
            try:
                result = self._function(
                    input_data=input_data,
                    source_path=task.source_path,
                )
            except Exception as e:
                return StageResult(
                    success=False,
                    error=f"Script execution failed: {e}",
                    retryable=True,
                )

            duration_ms = int((time.time() - start_time) * 1000)

            return StageResult(
                success=True,
                result=result,
                duration_ms=duration_ms,
            )

        except FileNotFoundError as e:
            return StageResult(
                success=False,
                error=str(e),
                retryable=False,
            )
        except (ImportError, AttributeError) as e:
            return StageResult(
                success=False,
                error=str(e),
                retryable=False,
            )
        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            return StageResult(
                success=False,
                error=str(e),
                duration_ms=duration_ms,
                retryable=True,
            )
