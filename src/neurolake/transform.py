"""Run the dbt project against a lake directory."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


class DbtError(RuntimeError):
    pass


def dbt_project_dir() -> Path:
    env = os.environ.get("NEUROLAKE_DBT_DIR")
    return Path(env) if env else Path(__file__).resolve().parents[2] / "dbt"


def run_dbt(lake: str | Path, args: list[str]) -> None:
    """Run dbt in a subprocess so its DuckDB connection is closed when it exits."""
    project = dbt_project_dir()
    dbt = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")
    env = {**os.environ, "NEUROLAKE_LAKE": str(Path(lake).resolve())}
    cmd = [dbt, *args, "--project-dir", str(project), "--profiles-dir", str(project)]
    proc = subprocess.run(cmd, env=env)
    if proc.returncode != 0:
        raise DbtError(f"dbt {' '.join(args)} failed with exit code {proc.returncode}")
