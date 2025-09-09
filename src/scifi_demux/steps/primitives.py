# src/scifi_demux/steps/primitives.py
from __future__ import annotations
from pathlib import Path
from typing import Optional, List
import shutil, subprocess, tempfile, os
from scifi_demux.demux_core import demux_split_barcodes
import re
from typing import Dict, List, Tuple

def _which_or_raise(*bins: str):
    missing = [b for b in bins if shutil.which(b) is None]
    if missing:
        raise RuntimeError(f"Missing required executables: {', '.join(missing)}")

def _run(cmd: List[str], env=None):
    print("[CMD]", " ".join(map(str, cmd)))
    subprocess.run(cmd, check=True, env=env)

def umi_extract_pair(
    read_keep: Path,             # keep this read’s sequence (R1 or R3)
    mate_in: Path,               # mate used as --stdin (R3 or R2)
    out_fastq_gz: Path,
    umi_pattern: str = "NNNNNNNNNNNNNNNN",
    threads: int = 8,
    do_chunking: bool = False,
    chunks: int = 20,
):
    """Mirror your umi_tools extract logic (optionally chunk with seqkit)."""
    _which_or_raise("umi_tools", "pigz")
    out_fastq_gz.parent.mkdir(parents=True, exist_ok=True)

    if not do_chunking:
        tmp_out = out_fastq_gz.with_suffix("")  # uncompressed
        _run([
            "umi_tools", "extract",
            f"--bc-pattern={umi_pattern}",
            f"--stdin={str(mate_in)}",
            f"--read2-in={str(read_keep)}",
            f"--read2-out={str(tmp_out)}",
        ])
        with open(out_fastq_gz, "wb") as fout:
            subprocess.run(["pigz", "-p", str(threads), "-c", str(tmp_out)], check=True, stdout=fout)
        try: tmp_out.unlink()
        except FileNotFoundError: pass
        return

    # large-file path: split → per-chunk extract → concat → gzip
    _which_or_raise("seqkit", "parallel")
    with tempfile.TemporaryDirectory() as tdir:
        chunks_dir = Path(tdir) / "chunks"; chunks_dir.mkdir()
        _run(["seqkit", "split2", "--by-part", str(chunks), "-j", str(threads),
              "-O", str(chunks_dir), "-1", str(read_keep), "-2", str(mate_in)])
        tmp_concat = Path(tdir) / "concat.fastq"
        with open(tmp_concat, "wb") as cat:
            # pick up both R1- and R3-named chunks robustly
            for keep_chunk in sorted(chunks_dir.glob("*_R1*.fastq.gz")) + sorted(chunks_dir.glob("*_R3*.fastq.gz")):
                mate_chunk = None
                if "_R1." in keep_chunk.name:
                    mate_chunk = Path(str(keep_chunk).replace("_R1.", "_R3."))
                elif "_R3." in keep_chunk.name:
                    mate_chunk = Path(str(keep_chunk).replace("_R3.", "_R2."))
                if not mate_chunk or not mate_chunk.exists():
                    raise FileNotFoundError(f"Mate chunk not found for {keep_chunk}")
                tmp_chunk = Path(tdir) / (keep_chunk.stem + ".bc1.fastq")
                _run([
                    "umi_tools", "extract",
                    f"--bc-pattern={umi_pattern}",
                    f"--stdin={str(mate_chunk)}",
                    f"--read2-in={str(keep_chunk)}",
                    f"--read2-out={str(tmp_chunk)}",
                ])
                with open(tmp_chunk, "rb") as fin:
                    shutil.copyfileobj(fin, cat)
        with open(out_fastq_gz, "wb") as fout:
            subprocess.run(["pigz", "-p", str(threads), "-c", str(tmp_concat)], check=True, stdout=fout)

def cutadapt_append_tn5_to_name(
    r1_in: Path, r3_in: Path,
    r1_out: Path, r3_out: Path,
    threads: int = 4,
):
    """Append split Tn5 halves into read name; trim 5/5; clip adapters."""
    _which_or_raise("cutadapt")
    r1_out.parent.mkdir(parents=True, exist_ok=True); r3_out.parent.mkdir(parents=True, exist_ok=True)
    _run([
        "cutadapt",
        "-e", "0.2",
        "--pair-filter=any",
        "-j", str(threads),
        "--rename", "{id}_{r1.cut_prefix}_{r2.cut_prefix} {comment}",
        "-u", "5", "-U", "5",
        "-g", "AGATGTGTATAAGAGACAG",
        "-G", "AGATGTGTATAAGAGACAG",
        "-o", str(r1_out),
        "-p", str(r3_out),
        str(r1_in), str(r3_in),
    ])

def demux_by_split_bc(layout_file: Path, sample_well_map: Path, input_fastq_gz: Path, out_dir: Path):
    """Backwards-compatible shim that calls the in-package demux function."""
    demux_split_barcodes(layout_file, input_fastq_gz, sample_well_map, output_dir=out_dir)


_PART_RE = re.compile(r"^part_(\d+)_R([13])\.bc1\.bc2_(.+)\.fastq\.gz$")

def _scan_demux_parts(corr_dir: Path) -> Dict[str, Dict[str, List[Path]]]:
    """
    Return {sample: {"R1": [parts...], "R3": [parts...]}} discovered under corr_dir.
    """
    by_sample: Dict[str, Dict[str, List[Path]]] = {}
    for p in corr_dir.glob("part_*_R*.bc1.bc2_*.fastq.gz"):
        m = _PART_RE.match(p.name)
        if not m:
            continue
        chunk_id, read, sample = int(m.group(1)), f"R{m.group(2)}", m.group(3)
        d = by_sample.setdefault(sample, {"R1": [], "R3": []})
        d[read].append(p)
    # sort parts by numeric chunk id embedded in filename
    def _key(path: Path) -> int:
        m = _PART_RE.match(path.name)
        return int(m.group(1)) if m else 0
    for sample in by_sample:
        by_sample[sample]["R1"].sort(key=_key)
        by_sample[sample]["R3"].sort(key=_key)
    return by_sample


def merge_demuxed_chunks(
    corr_dir: Path,
    out_dir: Path,
    *,
    overwrite: bool = True,
    keep_parts: bool = True,
) -> Dict[str, Dict[str, Dict[str, int]]]:
    """
    Merge per-sample chunked FASTQs produced by demux into final gz files.

    Inputs:
      corr_dir: directory containing files like 'part_001_R1.bc1.bc2_<S>.fastq.gz'
      out_dir:  where to write '<S>_R1.bc1.bc2.fastq.gz' and '<S>_R3.bc1.bc2.fastq.gz'
      overwrite: if True, replace existing outputs
      keep_parts: if False, delete chunk files after successful merge

    Returns:
      summary[sample][read] = {"parts": N, "bytes": total_bytes}
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    by_sample = _scan_demux_parts(corr_dir)
    if not by_sample:
        raise RuntimeError(f"No demux parts found under {corr_dir}")

    summary: Dict[str, Dict[str, Dict[str, int]]] = {}

    for sample, reads in by_sample.items():
        sample_sum: Dict[str, Dict[str, int]] = {}
        for read in ("R1", "R3"):
            parts = reads.get(read, [])
            if not parts:
                continue
            dst = out_dir / f"{sample}_{read}.bc1.bc2.fastq.gz"
            if dst.exists() and not overwrite:
                raise FileExistsError(f"{dst} exists and overwrite=False")
            # Concatenate gzip members (valid & stream-safe)
            total = 0
            with open(dst, "wb") as fout:
                for part in parts:
                    with open(part, "rb") as fin:
                        total += shutil.copyfileobj(fin, fout) or 0
            sample_sum[read] = {"parts": len(parts), "bytes": int(total)}
            if not keep_parts:
                for part in parts:
                    try: part.unlink()
                    except FileNotFoundError: pass
        if sample_sum:
            summary[sample] = sample_sum

    return summary
    



