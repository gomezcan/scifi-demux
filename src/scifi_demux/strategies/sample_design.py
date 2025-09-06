from __future__ import annotations
from pathlib import Path
from typing import Dict
from ..config import Design
from ..io_utils import ensure_dir, copy_or_link
from ..logging_utils import get_logger

log = get_logger(__name__)

def _well_aliases(design: Design) -> Dict[str, str]:
  """Map 'Plate:Well' -> sample name"""
  mapping: Dict[str, str] = {}
  for s in design.samples:
    for w in s.wells:
      mapping[w] = s.name
  return mapping

def demux_by_samples(fastqs: list[Path], design: Design, out: Path, mode: str = "link", dry_run: bool = False) -> None:
  out = ensure_dir(out)
  aliases = _well_aliases(design)
  for fq in fastqs:
    tokens = fq.stem.split("_")
    well_id = next((t for t in tokens if ":" in t), None)
    if not well_id or well_id not in aliases:
      log.warning(f"No sample mapping for {fq.name}; skipping")
      continue
    sample = aliases[well_id]
    dst_dir = out / sample
    dst = dst_dir / fq.name
    log.info(f"{mode}: {fq} -> {dst}")
    if not dry_run:
      copy_or_link(fq, dst, mode=mode)



