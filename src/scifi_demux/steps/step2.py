"""
Step 2 orchestrator (stub): read genome map, create tasks, and dry-run print.
"""
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any

from scifi_demux.utils.state import add_or_get_task

def register_map_row(state, group: str, genome: str, ref_path: str) -> Dict[str, Any]:
    task_id = f"step2:group:{group}:genome:{genome}"
    task = add_or_get_task(state, task_id, kind="step2", group=group, genome=genome)
    step_keys = ["index", "map"] + [f"clean_{i}" for i in range(1, 9)]
    for k in step_keys:
        task.setdefault("steps", {}).setdefault(k, {"status": "pending"})
    task.setdefault("params", {})["ref_path"] = ref_path
    return task
