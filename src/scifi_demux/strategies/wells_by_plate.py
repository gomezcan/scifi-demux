from __future__ import annotations
from pathlib import Path
from typing import Iterable
from ..config import Design
from ..io_utils import ensure_dir, copy_or_link
from ..logging_utils import get_logger

log = get_logger(__name__)

def demux_by_wells(fastqs: list[Path], design: Design, out: Path, mode: str = "link", dry_run: bool = False) -> None:
  out = ensure_dir(out)
  # Very light placeholder logic: route files into per‑well folders using well tokens in filenames
  for fq in fastqs:
    # Expect filename to include "Plate:Well" or similar; adapt to your pre‑rename scheme
    tokens = fq.stem.split("_")
    well_id = next((t for t in tokens if ":" in t), None)
    if not well_id:
      log.warning(f"Cannot infer well from {fq.name}; skipping")
      continue
    dst_dir = out / well_id.replace(":", "/")
    dst = dst_dir / fq.name
    log.info(f"{mode}: {fq} -> {dst}")
    if not dry_run:
      copy_or_link(fq, dst, mode=mode)
