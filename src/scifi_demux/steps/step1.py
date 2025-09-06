"""
Step 1 orchestrator (stub): register steps and provide dry-run planning.
Wire to your real scripts in subsequent commits.
"""
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any

from scifi_demux.utils.state import add_or_get_task, mark_task_step, save_state

def plan_step1(state, library: str) -> Dict[str, Any]:
    task_id = f"step1:library:{library}"
    task = add_or_get_task(state, task_id, kind="step1", library=library)
    for key in ["umi_r1", "umi_r3", "cutadapt", "demux", "merge", "qc"]:
        task.setdefault("steps", {}).setdefault(key, {"status": "pending"})
    return task
