"""
Configuration loading and validation for N-stage document processing pipeline.

Expects a config folder with:
  - config.json: Main configuration with stages array
  - {stage}_prompt.txt: Prompt files for LLM stages
  - {stage}_qc_prompt.txt: Optional QC prompt files
  - schema.json: Optional JSON schema for structured output
"""

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Literal


@dataclass
class StageConfig:
    """Configuration for a single pipeline stage."""
    name: str
    type: Literal["llm", "script"]
    index: int  # 0-based position in pipeline

    # LLM stage fields
    model: Optional[str] = None
    prompt: Optional[str] = None
    prompt_file: Optional[str] = None
    schema: Optional[dict] = None
    schema_file: Optional[str] = None
    qc_prompt: Optional[str] = None
    qc_prompt_file: Optional[str] = None
    enhance_prompt: Optional[str] = None
    enhance_prompt_file: Optional[str] = None

    # Script stage fields
    script: Optional[str] = None
    function: Optional[str] = None

    @property
    def folder_name(self) -> str:
        """Generate numbered folder name (e.g., '1.extract', '2.format')."""
        return f"{self.index + 1}.{self.name}"

    @property
    def output_suffix(self) -> str:
        """Output file suffix (e.g., '.extract.json')."""
        return f".{self.name}.json"

    @property
    def error_suffix(self) -> str:
        """Error file suffix (e.g., '.extract.error.json')."""
        return f".{self.name}.error.json"

    @property
    def has_qc(self) -> bool:
        """Whether this stage has quality checking enabled."""
        return self.qc_prompt is not None

    @property
    def has_enhance(self) -> bool:
        """Whether this stage has enhancement prompt configured."""
        return self.enhance_prompt is not None


@dataclass
class PipelineConfig:
    """Complete pipeline configuration."""
    config_dir: Path
    input_dir: Path
    output_dir: Path
    stages: List[StageConfig]
    concurrency: int = 5
    file_extensions: List[str] = field(default_factory=lambda: [".pdf"])

    # Quality check settings
    qc_batch_size: int = 10
    qc_failure_threshold: float = 0.10
    qc_min_samples: int = 10

    def get_stage(self, name: str) -> Optional[StageConfig]:
        """Get stage by name."""
        for stage in self.stages:
            if stage.name == name:
                return stage
        return None

    def get_prior_stage(self, stage: StageConfig) -> Optional[StageConfig]:
        """Get the stage before this one (for input chaining)."""
        if stage.index == 0:
            return None
        return self.stages[stage.index - 1]

    def validate(self) -> List[str]:
        """Validate configuration. Returns list of errors (empty if valid)."""
        errors = []

        # Check input directory
        if not self.input_dir.exists():
            errors.append(f"Input directory not found: {self.input_dir}")
        elif not self.input_dir.is_dir():
            errors.append(f"Input path is not a directory: {self.input_dir}")

        # Check stages
        if not self.stages:
            errors.append("No stages defined")

        for stage in self.stages:
            if stage.type == "llm":
                if not stage.prompt:
                    errors.append(f"Stage '{stage.name}': LLM stage missing prompt")
            elif stage.type == "script":
                if not stage.script:
                    errors.append(f"Stage '{stage.name}': Script stage missing script path")
                if not stage.function:
                    errors.append(f"Stage '{stage.name}': Script stage missing function name")

        # Check QC settings
        if self.qc_failure_threshold < 0 or self.qc_failure_threshold > 1:
            errors.append(f"qc_failure_threshold must be 0-1, got {self.qc_failure_threshold}")

        return errors


class ConfigValidationError(Exception):
    """Raised when config validation fails."""
    def __init__(self, errors: List[str]):
        self.errors = errors
        super().__init__(f"Config validation failed: {'; '.join(errors)}")


def windows_to_wsl_path(windows_path: str) -> str:
    """
    Convert Windows path to WSL2 path.

    Examples:
        "C:\\Users\\foo" -> "/mnt/c/Users/foo"
        "/mnt/c/Users/foo" -> "/mnt/c/Users/foo" (unchanged)
    """
    if not windows_path:
        return windows_path

    # Normalize path separators
    normalized = windows_path.replace('\\', '/')

    # Already a Unix/WSL path
    if normalized.startswith('/'):
        return normalized

    # Convert Windows drive letter (C:/... -> /mnt/c/...)
    if len(normalized) >= 2 and normalized[1] == ':':
        drive_letter = normalized[0].lower()
        rest_of_path = normalized[2:].lstrip('/')
        return f'/mnt/{drive_letter}/{rest_of_path}'

    return normalized


def expand_env_vars(path_str: str) -> str:
    """
    Expand environment variables in path string.
    Supports ${VAR_NAME} syntax.
    Converts Windows paths to WSL2 if needed.
    """
    def replace_var(match):
        var_name = match.group(1)
        value = os.environ.get(var_name, match.group(0))
        return windows_to_wsl_path(value)

    return re.sub(r'\$\{(\w+)\}', replace_var, path_str)


def _load_stage(stage_data: dict, index: int, config_dir: Path) -> StageConfig:
    """Load a single stage configuration."""
    stage_type = stage_data.get("type", "llm")
    name = stage_data.get("name", f"stage{index + 1}")

    stage = StageConfig(
        name=name,
        type=stage_type,
        index=index,
    )

    if stage_type == "llm":
        stage.model = stage_data.get("model", "gemini-3-flash-preview")
        stage.prompt_file = stage_data.get("prompt_file")
        stage.schema_file = stage_data.get("schema_file")
        stage.qc_prompt_file = stage_data.get("qc_prompt_file")
        stage.enhance_prompt_file = stage_data.get("enhance_prompt_file")

        # Load prompt
        if stage.prompt_file:
            prompt_path = config_dir / stage.prompt_file
            if prompt_path.exists():
                stage.prompt = prompt_path.read_text(encoding="utf-8").strip()
            else:
                raise FileNotFoundError(f"Prompt file not found for stage '{name}': {prompt_path}")

        # Load schema
        if stage.schema_file:
            schema_path = config_dir / stage.schema_file
            if schema_path.exists():
                with open(schema_path, "r", encoding="utf-8") as f:
                    stage.schema = json.load(f)
            else:
                raise FileNotFoundError(f"Schema file not found for stage '{name}': {schema_path}")

        # Load QC prompt
        if stage.qc_prompt_file:
            qc_path = config_dir / stage.qc_prompt_file
            if qc_path.exists():
                stage.qc_prompt = qc_path.read_text(encoding="utf-8").strip()
            # QC is optional, don't raise if missing

        # Load enhance prompt
        if stage.enhance_prompt_file:
            enhance_path = config_dir / stage.enhance_prompt_file
            if enhance_path.exists():
                stage.enhance_prompt = enhance_path.read_text(encoding="utf-8").strip()
            # Enhancement is optional, don't raise if missing

    elif stage_type == "script":
        stage.script = stage_data.get("script")
        stage.function = stage_data.get("function", "process_record")

    return stage


def load_config(config_dir: str | Path) -> PipelineConfig:
    """
    Load and validate pipeline configuration from a config folder.

    Args:
        config_dir: Path to config folder

    Returns:
        PipelineConfig object

    Raises:
        ConfigValidationError: If validation fails
        FileNotFoundError: If required files are missing
    """
    config_dir = Path(config_dir).resolve()

    if not config_dir.exists():
        raise FileNotFoundError(f"Config directory not found: {config_dir}")

    # Load main config.json
    config_file = config_dir / "config.json"
    if not config_file.exists():
        raise FileNotFoundError(f"config.json not found in {config_dir}")

    with open(config_file, "r", encoding="utf-8") as f:
        config_data = json.load(f)

    # Load stages
    stages_data = config_data.get("stages", [])
    if not stages_data:
        raise ConfigValidationError(["No stages defined in config.json"])

    stages = []
    for i, stage_data in enumerate(stages_data):
        stage = _load_stage(stage_data, i, config_dir)
        stages.append(stage)

    # Resolve directories with env var expansion
    input_dir_str = expand_env_vars(config_data.get("input_dir", ""))
    output_dir_str = expand_env_vars(config_data.get("output_dir", ""))

    input_dir = Path(input_dir_str)
    output_dir = Path(output_dir_str)

    if not input_dir.is_absolute():
        input_dir = (config_dir / input_dir).resolve()
    if not output_dir.is_absolute():
        output_dir = (config_dir / output_dir).resolve()

    # Build config
    config = PipelineConfig(
        config_dir=config_dir,
        input_dir=input_dir,
        output_dir=output_dir,
        stages=stages,
        concurrency=config_data.get("concurrency", 5),
        file_extensions=config_data.get("file_extensions", [".pdf"]),
        qc_batch_size=config_data.get("qc_batch_size", 10),
        qc_failure_threshold=config_data.get("qc_failure_threshold", 0.10),
        qc_min_samples=config_data.get("qc_min_samples", 10),
    )

    # Validate
    errors = config.validate()
    if errors:
        raise ConfigValidationError(errors)

    return config


def print_config(config: PipelineConfig) -> None:
    """Print configuration summary."""
    print("=" * 60)
    print("Pipeline Configuration")
    print("=" * 60)
    print(f"Config dir:   {config.config_dir}")
    print(f"Input dir:    {config.input_dir}")
    print(f"Output dir:   {config.output_dir}")
    print(f"Concurrency:  {config.concurrency}")
    print(f"Extensions:   {config.file_extensions}")
    print()
    print("Quality Check Settings:")
    print(f"  Batch size:  {config.qc_batch_size}")
    print(f"  Threshold:   {config.qc_failure_threshold * 100:.0f}%")
    print(f"  Min samples: {config.qc_min_samples}")
    print()
    print(f"Stages ({len(config.stages)}):")
    for stage in config.stages:
        indicators = []
        if stage.has_qc:
            indicators.append("QC")
        if stage.has_enhance:
            indicators.append("ENHANCE")
        indicator_str = f" [{', '.join(indicators)}]" if indicators else ""
        if stage.type == "llm":
            print(f"  {stage.folder_name}: {stage.type} ({stage.model}){indicator_str}")
            if stage.prompt:
                print(f"    Prompt: {stage.prompt[:60]}...")
            if stage.schema:
                print(f"    Schema: {len(stage.schema.get('properties', {}))} properties")
        else:
            print(f"  {stage.folder_name}: {stage.type} ({stage.script}::{stage.function})")
    print("=" * 60)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python config.py <config_dir>")
        sys.exit(1)

    try:
        config = load_config(sys.argv[1])
        print_config(config)
    except (FileNotFoundError, ConfigValidationError) as e:
        print(f"ERROR: {e}")
        sys.exit(1)
