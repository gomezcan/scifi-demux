from pathlib import Path
from typing import Iterable
import shutil

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
