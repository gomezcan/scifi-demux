from __future__ import annotations
from pathlib import Path
import json
from typing import Dict, Any, Iterable

STATE_PATH_DEFAULT = Path("scifi_demux_state.json")


def ensure_state(path: Path) -> Dict[str, Any]:
    if path.exists():
        return load_state(path)
    s = {"version": 1, "tasks": []}
    save_state(s, path)
    return s


def load_state(path: Path) -> Dict[str, Any]:
    with open(path, "r") as fh:
        return json.load(fh)


def save_state(state: Dict[str, Any], path: Path = STATE_PATH_DEFAULT) -> None:
    path.write_text(json.dumps(state, indent=2))


def _find_task(state: Dict[str, Any], task_id: str) -> Dict[str, Any] | None:
    for t in state.get("tasks", []):
        if t.get("id") == task_id:
            return t
    return None


def add_or_get_task(state: Dict[str, Any], task_id: str, **attrs) -> Dict[str, Any]:
    t = _find_task(state, task_id)
    if t is None:
        t = {"id": task_id, **attrs, "steps": {}}
        state.setdefault("tasks", []).append(t)
    else:
        t.update({k: v for k, v in attrs.items() if k not in t})
    return t


def mark_task_step(state: Dict[str, Any], task_id: str, step: str, status: str, **meta) -> None:
    t = _find_task(state, task_id)
    if t is None:
        raise KeyError(f"Unknown task {task_id}")
    steps = t.setdefault("steps", {})
    steps.setdefault(step, {})
    steps[step]["status"] = status
    if meta:
        steps[step].update(meta)


def iter_tasks(state: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    return list(state.get("tasks", []))
