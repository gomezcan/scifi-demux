# src/scifi_demux/io_utils.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, Union, List
import shutil

# --- packaged data resolution ---
def data_path(name: str) -> Path:
    """
    Locate packaged data robustly:
      1) Prefer filesystem path relative to this module (works in editable + wheel installs).
      2) Fall back to importlib.resources using the parent package.
    """
    pkg_root = Path(__file__).resolve().parent  # .../scifi_demux
    fs = pkg_root / "__data__" / name
    if fs.exists():
        return fs
    try:
        from importlib.resources import files as _files
        return Path(_files("scifi_demux") / "__data__" / name)
    except Exception as e:
        raise FileNotFoundError(
            f"Could not locate data file '{name}' (looked at {fs}); fallback failed: {e}"
        )

def resolve_tn5_bcs(user_path: Optional[str]) -> Path:
    if user_path and user_path.lower() != "builtin":
        return Path(user_path).resolve()
    return data_path("tn5_bcs.txt")

def resolve_whitelist(user_path: Optional[str]) -> Path:
    if user_path and user_path.lower() != "builtin":
        return Path(user_path).resolve()
    return data_path("737K-cratac-v1.txt")

def resolve_layout_path(layout: Optional[Union[str, Path]]) -> Path:
    """
    None or 'builtin' -> packaged 96-well layout; else use the given path.
    """
    if layout is None or str(layout).lower() == "builtin":
        return data_path("96well_Tn5_bc_layout.txt")
    return Path(layout)

# --- filesystem helpers used by CLI / renaming ---
def ensure_dir(d: Path) -> Path:
    d.mkdir(parents=True, exist_ok=True)
    return d

def find_fastqs(root: Path) -> List[Path]:
    """
    Recursively find FASTQ files under `root`.
    """
    exts = (".fastq", ".fq", ".fastq.gz", ".fq.gz")
    out: List[Path] = []
    for p in root.rglob("*"):
        name = p.name.lower()
        if name.endswith(exts):
            out.append(p)
    return sorted(out)

def atomic_symlink(src: Path, dst: Path) -> None:
    """
    Create/replace a symlink at `dst` pointing to `src` atomically-ish.
    If `dst` exists (file/link), it is removed first.
    """
    dst = Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        if dst.exists() or dst.is_symlink():
            dst.unlink()
    except FileNotFoundError:
        pass
    dst.symlink_to(src)

def copy_or_link(src: Path, dst: Path, mode: str = "link") -> None:
    """
    Copy or symlink `src` -> `dst`. `mode`: 'link' (default) or 'copy'.
    """
    src = Path(src); dst = Path(dst)
    if mode == "copy":
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
    else:
        atomic_symlink(src, dst)
