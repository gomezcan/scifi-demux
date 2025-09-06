from __future__ import annotations
from pathlib import Path
import json, hashlib, os, tempfile, shutil
from typing import Any

def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(path.parent)) as tmp:
        tmp.write(text)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)

def atomic_write_json(path: Path, obj: Any, indent: int = 2) -> None:
    atomic_write_text(path, json.dumps(obj, indent=indent))

def sha1_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as fh:
        while True:
            b = fh.read(chunk)
            if not b: break
            h.update(b)
    return h.hexdigest()

def write_ok(target: Path, meta: dict) -> None:
    ok = target.with_suffix(target.suffix + ".ok.json") if target.suffix else Path(str(target) + ".ok.json")
    atomic_write_json(ok, meta)

def has_ok(target: Path) -> bool:
    ok = target.with_suffix(target.suffix + ".ok.json") if target.suffix else Path(str(target) + ".ok.json")
    return ok.exists()
