from pathlib import Path
from typing import Iterable, Optional, Union
from importlib.resources import files
import shutil

def data_path(name: str) -> Path:
    return Path(files("scifi_demux.__data__") / name)

def resolve_tn5_bcs(user_path: Optional[str]) -> Path:
    if user_path and user_path.lower() != "builtin":
        return Path(user_path).resolve()
    return data_path("tn5_bcs.txt")

def resolve_whitelist(user_path: Optional[str]) -> Path:
    if user_path and user_path.lower() != "builtin":
        return Path(user_path).resolve()
    return data_path("737K-cratac-v1.txt")

def ensure_dir(d: Path) -> Path:
  d.mkdir(parents=True, exist_ok=True)
  return d

def find_fastqs(root: Path) -> list[Path]:
  exts = (".fastq", ".fq", ".fastq.gz", ".fq.gz")
  return [p for p in sorted(root.rglob("*")) if p.suffix.lower() in (".gz", ".fastq", ".fq") and p.name.endswith(exts)]

def atomic_symlink(src: Path, dst: Path) -> None:
  dst.parent.mkdir(parents=True, exist_ok=True)
  if dst.exists() or dst.is_symlink():
    dst.unlink()
    dst.symlink_to(src)

def copy_or_link(src: Path, dst: Path, mode: str = "link") -> None:
  if mode == "copy":
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
  else:
    atomic_symlink(src, dst)

def resolve_layout_path(layout: Optional[Union[str, Path]]) -> Path:
    """
    Return a concrete path to the layout file.
    - None or "builtin" -> packaged 96-well layout
    - any other string/Path -> as-is (expanded to Path)
    """
    if layout is None or str(layout).lower() == "builtin":
        return data_path("96well_Tn5_bc_layout.txt")
    return Path(layout)
