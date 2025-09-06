from __future__ import annotations
from textwrap import dedent
from pathlib import Path


def render_sbatch(job_name: str, out_dir: Path, array: str, cpus: int, time: str = "24:00:00", mem: str = "16G", partition: str = "standard", cmd: str = "echo hello") -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    script = dedent(f"""
    #!/bin/bash
    #SBATCH --job-name={job_name}
    #SBATCH --partition={partition}
    #SBATCH --time={time}
    #SBATCH --mem={mem}
    #SBATCH --cpus-per-task={cpus}
    #SBATCH --array={array}
    #SBATCH --output={out_dir}/%x-%A_%a.out

    set -euo pipefail
    {cmd}
    """)
    path = out_dir / f"{job_name}.slurm"
    path.write_text(script)
    return path
