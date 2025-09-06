from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass
from typing import Iterable
from .io_utils import copy_or_link
from .logging_utils import get_logger

log = get_logger(__name__)

@dataclass
class RenamePlan:
  src: Path
  dst: Path

def plan_renames(fastqs: Iterable[Path], scheme: str = "{plate}_{well}_{read}{ext}", *, plate: str, well: str) -> list[RenamePlan]:
  plans = []
  for fq in fastqs:
    read = "R1" if "R1" in fq.name else "R2" if "R2" in fq.name else "UNK"
    ext = ".fastq.gz" if fq.suffixes[-2:] == [".fastq", ".gz"] else fq.suffix
    out = fq.parent / scheme.format(plate=plate, well=well, read=read, ext=ext)
    plans.append(RenamePlan(src=fq, dst=out))
  return plans

def apply_renames(plans: list[RenamePlan], mode: str = "link", dry_run: bool = False) -> None:
  for p in plans:
    log.info(f"{mode}: {p.src} -> {p.dst}")
    if not dry_run:
      copy_or_link(p.src, p.dst, mode=mode)
