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
    """
    Demultiplex FASTQ files by sample based on a design file.
    
    This function organizes FASTQ files into sample-specific directories based on
    well identifiers found in filenames and a mapping provided by the design object.
    
    Args:
        fastqs: List of Path objects pointing to FASTQ files to be demultiplexed
        design: Design object containing sample-to-well mapping information
        out: Output directory where samples will be organized
        mode: File operation mode - "link" to create symlinks, "copy" to copy files
        dry_run: If True, only log actions without performing file operations
    
    Returns:
        None
    """
    # Ensure output directory exists (create if it doesn't)
    out = ensure_dir(out)
    
    # Get mapping of well identifiers to sample names from design
    aliases = _well_aliases(design)
    
    # Process each FASTQ file
    for fq in fastqs:
        # Split filename (without extension) by underscores to extract tokens
        tokens = fq.stem.split("_")
        
        # Find the token containing a colon (assumed to be the well identifier)
        # Example: "A:1" or "B:12" - common format for well positions
        well_id = next((t for t in tokens if ":" in t), None)
        
        # Skip file if no well identifier found or if it's not in our mapping
        if not well_id or well_id not in aliases:
            log.warning(f"No sample mapping for {fq.name}; skipping")
            continue
        
        # Get sample name corresponding to this well identifier
        sample = aliases[well_id]
        
        # Create destination path: output_dir/sample_name/filename.fastq
        dst_dir = out / sample
        dst = dst_dir / fq.name
        
        # Log the operation that would be performed
        log.info(f"{mode}: {fq} -> {dst}")
        
        # Only perform file operations if not in dry-run mode
        if not dry_run:
            # Either create symlink or copy file based on specified mode
            copy_or_link(fq, dst, mode=mode)
