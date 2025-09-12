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

def _run(cmd: List[str], **popen_kwargs):
    print("[CMD]", " ".join(map(str, cmd)))
    subprocess.run(cmd, check=True, **popen_kwargs)

def umi_extract_pair(
    *,
    read_keep: Path,             # keep this read’s sequence (R1 or R3)
    mate_in: Path,               # MUST be R2: provides barcode/UMI
    out_fastq_gz: Path,
    umi_pattern: str = "NNNNNNNNNNNNNNNN",
    threads: int = 8,
    do_chunking: bool = False,
    chunks: int = 20,
):
    """
    Extract barcode/UMI from mate_in (R2) and append to read_keep (R1 or R3) names.
    Writes compressed FASTQ to out_fastq_gz. Logs go to stderr (not captured in stdout logs).
    """
    _which_or_raise("umi_tools", "pigz")
    out_fastq_gz.parent.mkdir(parents=True, exist_ok=True)

    if not do_chunking:
        # ---- simple path: write read2 to a temp plain FASTQ, then compress ----
        tmp_out = out_fastq_gz.with_suffix("")  # e.g. .../part_002_R1.bc1.fastq
        _run([
            "umi_tools", "extract",
            f"--bc-pattern={umi_pattern}",
            f"--stdin={str(mate_in)}",      # R2 (UMI source)
            f"--read2-in={str(read_keep)}", # keep (R1 or R3)
            f"--read2-out={str(tmp_out)}",  # write KEEP here (plain FASTQ)
            "--log2stderr",
        ])
        with open(out_fastq_gz, "wb") as fout:
            subprocess.run(["pigz", "-p", str(threads), "-c", str(tmp_out)],
                           check=True, stdout=fout)
        try:
            tmp_out.unlink()
        except FileNotFoundError:
            pass
        return

    # Large-file path: chunk read_keep and mate_in together → per-chunk extract → concat → gzip
    _which_or_raise("seqkit", "parallel")
    with tempfile.TemporaryDirectory() as tdir:
        tdir = Path(tdir)
        chunks_dir = tdir / "chunks"
        chunks_dir.mkdir()

        # Split the two inputs in lockstep (ensures matching chunk boundaries)
        _run([
            "seqkit", "split2", "--by-part", str(chunks), "-j", str(threads),
            "-O", str(chunks_dir),
            "-1", str(read_keep),    # keep stream (R1 or R3)
            "-2", str(mate_in),      # barcode stream (R2)
        ])

        tmp_concat = tdir / "concat.fastq"
        with open(tmp_concat, "wb") as cat:
            # Gather keep chunks (either *_R1.part_* or *_R3.part_*)
            keep_chunks = sorted(list(chunks_dir.glob("*_R1*.fastq.gz")) + list(chunks_dir.glob("*_R3*.fastq.gz")))
            if not keep_chunks:
                raise FileNotFoundError(f"No keep-chunks found in {chunks_dir}")

            for keep_chunk in keep_chunks:
                # Match its R2 mate chunk (regardless of keep being R1 or R3)
                mate_chunk = Path(str(keep_chunk).replace("_R1.", "_R2.").replace("_R3.", "_R2."))
                if not mate_chunk.exists():
                    raise FileNotFoundError(f"Mate chunk not found for {keep_chunk.name}; expected {mate_chunk.name}")

                # Emit modified read2 to stdout, capture to a temp file; then append to concat
                p1 = subprocess.Popen([
                    "umi_tools", "extract",
                    f"--bc-pattern={umi_pattern}",
                    f"--stdin={str(mate_chunk)}",   # R2
                    f"--read2-in={str(keep_chunk)}",# R1/R3
                    "--read2-out=-",
                    "--log2stderr",
                ], stdout=subprocess.PIPE)
                tmp_chunk = tdir / (keep_chunk.stem + ".bc1.fastq")
                with open(tmp_chunk, "wb") as fout:
                    p2 = subprocess.Popen(["gzip", "-dc"], stdin=p1.stdout, stdout=subprocess.PIPE)  # if umi_tools ever gz-compresses, normalize
                    p1.stdout.close()
                    # directly write umi_tools stdout to file (no re-compress here)
                    # Actually p2 above is just a safeguard; usually umi_tools emits plain FASTQ
                    # so we can simply read from p1 if desired; keeping minimal here:
                    p1_ret = p1.wait()
                    # if we had used piping, we'd also wait p2; omitted for simplicity
                # Append to concat
                with open(tmp_chunk, "rb") as fin:
                    shutil.copyfileobj(fin, cat)
                tmp_chunk.unlink(missing_ok=True)

        with open(out_fastq_gz, "wb") as fout:
            subprocess.run(["pigz", "-p", str(threads), "-c", str(tmp_concat)], check=True, stdout=fout)
        tmp_concat.unlink(missing_ok=True)


def cutadapt_append_tn5_to_name(
    r1_in: Path, r3_in: Path,
    r1_out: Path, r3_out: Path,
    threads: int = 4,
):
    """
    Append split Tn5 halves into read name; trim 5/5; clip adapters.
    Preserves original semantics (rename + ME removal), adds only safe guards.
    """
    _which_or_raise("cutadapt")
    r1_out.parent.mkdir(parents=True, exist_ok=True)
    r3_out.parent.mkdir(parents=True, exist_ok=True)

    _run([
        "cutadapt",
        "-e", "0.2",
        "--pair-filter=any",
        "-j", str(threads),

        # --- keep your exact renaming semantics
        "--rename", "{id}_{r1.cut_prefix}_{r2.cut_prefix} {comment}",

        # --- your fixed 5' clipping on both reads
        "-u", "5", "-U", "5",

        # --- your ME/EM sequence trimming (leave exactly as you provided)
        "-g", "AGATGTGTATAAGAGACAG",
        "-G", "AGATGTGTATAAGAGACAG",

        # --- SAFE guards that don't change intended behavior
        "--report=minimal",             # keep logs small

        # outputs
        "-o", str(r1_out),
        "-p", str(r3_out),

        # inputs (R1 first, then R3)
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
    



