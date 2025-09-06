from textwrap import dedent
from pathlib import Path

def render_slurm(job_name: str, out_dir: Path, cmd: str, *, partition: str = "standard", time: str = "4:00:00", mem: str = "8G", ntasks: int = 1, cpus_per_task: int = 4) -> str:
  script = dedent(f"""
  #!/bin/bash
  #SBATCH --job-name={job_name}
  #SBATCH --partition={partition}
  #SBATCH --time={time}
  #SBATCH --mem={mem}
  #SBATCH --ntasks={ntasks}
  #SBATCH --cpus-per-task={cpus_per_task}
  #SBATCH --output={out_dir}/%x-%j.out
  set -euo pipefail
  {cmd}
  """)
  return script
  
