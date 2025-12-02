"""
Step 2 orchestrator (stub): read genome map, create tasks, and dry-run print.
"""
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List
import typer

from scifi_demux.utils.state import add_or_get_task
from scifi_demux.io_utils import resolve_layout_path

step2_app = typer.Typer(add_completion=False)

def register_map_row(state, group: str, genome: str, ref_path: str) -> Dict[str, Any]:
    task_id = f"step2:group:{group}:genome:{genome}"
    task = add_or_get_task(state, task_id, kind="step2", group=group, genome=genome)
    step_keys = ["index", "map"] + [f"clean_{i}" for i in range(1, 9)]
    for k in step_keys:
        task.setdefault("steps", {}).setdefault(k, {"status": "pending"})
    task.setdefault("params", {})["ref_path"] = ref_path
    return task


def _discover_step2_fastqs_from_step1(
    work_root: Path,
    sample: str,
) -> List[Path]:
    """
    Given the step1 work_root and a sample name, locate the FASTQs to use as
    input for step2.

    Priority:
      1) <work_root>/<sample>.fastq(.gz)
      2) <work_root>/sample/combined/*.fastq*
    """
    direct = work_root / f"{sample}.fastq"
    direct_gz = work_root / f"{sample}.fastq.gz"

    if direct.exists():
        return [direct]
    if direct_gz.exists():
        return [direct_gz]

    # Fallback: combined directory from layout
    layout = resolve_layout_path(library=sample, work_root=work_root)
    combined_dir = layout.sample_dir / "combined"

    if not combined_dir.exists():
        raise FileNotFoundError(
            f"Could not find FASTQs for sample={sample}. "
            f"Tried {direct}, {direct_gz}, and combined dir {combined_dir}"
        )

    fastqs = sorted(combined_dir.glob("*.fastq*"))
    if not fastqs:
        raise FileNotFoundError(
            f"No FASTQs found in combined dir: {combined_dir}"
        )

    return fastqs
