"""
Configuration loading and validation for document processing pipeline.

Expects a config folder with:
  - config.json: Main configuration
  - extract_prompt.txt: Stage 1 extraction prompt
  - format_prompt.txt: Stage 2 formatting prompt (optional)
  - schema.json: JSON schema for Stage 2 output
"""

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List


@dataclass
class Stage1Config:
    """Stage 1 (Gemini extraction) configuration."""
    model: str = "gemini-3-flash-preview"
    prompt: str = ""
    prompt_file: str = "extract_prompt.txt"


@dataclass
class Stage2Config:
    """Stage 2 (Claude formatting) configuration."""
    model: str = "haiku"
    prompt: str = ""
    prompt_file: str = "format_prompt.txt"
    schema: dict = field(default_factory=dict)
    schema_file: str = "schema.json"


@dataclass
class PipelineConfig:
    """Complete pipeline configuration."""
    config_dir: Path
    input_dir: Path
    output_dir: Path
    stage1: Stage1Config
    stage2: Stage2Config
    concurrency: int = 5
    file_extensions: List[str] = field(default_factory=lambda: [".pdf"])

    def validate(self) -> List[str]:
        """Validate configuration. Returns list of errors (empty if valid)."""
        errors = []

        # Check input directory
        if not self.input_dir.exists():
            errors.append(f"Input directory not found: {self.input_dir}")
        elif not self.input_dir.is_dir():
            errors.append(f"Input path is not a directory: {self.input_dir}")

        # Check prompts are loaded
        if not self.stage1.prompt:
            errors.append("Stage 1 prompt is empty")
        if not self.stage2.prompt:
            errors.append("Stage 2 prompt is empty")

        # Check schema is loaded
        if not self.stage2.schema:
            errors.append("Stage 2 schema is empty")

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
        "D:\\Data" -> "/mnt/d/Data"
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

    # Return as-is if no conversion needed
    return normalized


def expand_env_vars(path_str: str) -> str:
    """
    Expand environment variables in path string.
    Supports ${VAR_NAME} syntax.
    Converts Windows paths to WSL2 if needed.

    Examples:
        "${WINDOWS_DATA_DIR}/raw/raba" where WINDOWS_DATA_DIR="C:\\Users\\...\\Data"
        -> "/mnt/c/Users/.../Data/raw/raba"
    """
    # Replace ${VAR} patterns with environment variable values
    def replace_var(match):
        var_name = match.group(1)
        value = os.environ.get(var_name, match.group(0))
        # Convert Windows paths to WSL2
        return windows_to_wsl_path(value)

    return re.sub(r'\$\{(\w+)\}', replace_var, path_str)


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

    # Parse stage configs
    stage1_data = config_data.get("stage1", {})
    stage2_data = config_data.get("stage2", {})

    stage1 = Stage1Config(
        model=stage1_data.get("model", "gemini-3-flash-preview"),
        prompt_file=stage1_data.get("prompt_file", "extract_prompt.txt"),
    )

    stage2 = Stage2Config(
        model=stage2_data.get("model", "haiku"),
        prompt_file=stage2_data.get("prompt_file", "format_prompt.txt"),
        schema_file=stage2_data.get("schema_file", "schema.json"),
    )

    # Load Stage 1 prompt
    prompt_file = config_dir / stage1.prompt_file
    if not prompt_file.exists():
        raise FileNotFoundError(f"Stage 1 prompt file not found: {prompt_file}")
    stage1.prompt = prompt_file.read_text(encoding="utf-8").strip()

    # Load Stage 2 prompt
    format_prompt_file = config_dir / stage2.prompt_file
    if not format_prompt_file.exists():
        raise FileNotFoundError(f"Stage 2 prompt file not found: {format_prompt_file}")
    stage2.prompt = format_prompt_file.read_text(encoding="utf-8").strip()

    # Load Stage 2 schema
    schema_file = config_dir / stage2.schema_file
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_file}")
    with open(schema_file, "r", encoding="utf-8") as f:
        stage2.schema = json.load(f)

    # Resolve input/output directories (relative to config_dir or absolute)
    # Expand environment variables first
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
        stage1=stage1,
        stage2=stage2,
        concurrency=config_data.get("concurrency", 5),
        file_extensions=config_data.get("file_extensions", [".pdf"]),
    )

    # Validate
    errors = config.validate()
    if errors:
        raise ConfigValidationError(errors)

    return config


def print_config(config: PipelineConfig) -> None:
    """Print configuration summary."""
    print("=" * 60, flush=True)
    print("Pipeline Configuration", flush=True)
    print("=" * 60, flush=True)
    print(f"Config dir:  {config.config_dir}", flush=True)
    print(f"Input dir:   {config.input_dir}", flush=True)
    print(f"Output dir:  {config.output_dir}", flush=True)
    print(f"Concurrency: {config.concurrency}", flush=True)
    print(f"Extensions:  {config.file_extensions}", flush=True)
    print(flush=True)
    print("Stage 1 (Extract):", flush=True)
    print(f"  Model:  {config.stage1.model}", flush=True)
    print(f"  Prompt: {config.stage1.prompt[:80]}...", flush=True)
    print(flush=True)
    print("Stage 2 (Format):", flush=True)
    print(f"  Model:  {config.stage2.model}", flush=True)
    print(f"  Prompt: {config.stage2.prompt[:80]}...", flush=True)
    print(f"  Schema: {len(config.stage2.schema.get('properties', {}))} properties", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python config.py <config_dir>", flush=True)
        sys.exit(1)

    try:
        config = load_config(sys.argv[1])
        print_config(config)
    except (FileNotFoundError, ConfigValidationError) as e:
        print(f"ERROR: {e}", flush=True)
        sys.exit(1)
