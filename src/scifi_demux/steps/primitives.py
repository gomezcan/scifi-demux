# src/scifi_demux/steps/primitives.py
from __future__ import annotations

import gzip
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List

from scifi_demux.demux_core import demux_split_barcodes


# ----------------------------- utils -----------------------------

def _which_or_raise(*bins: str) -> None:
    missing = [b for b in bins if shutil.which(b) is None]
    if missing:
        raise RuntimeError(f"Missing required executables: {', '.join(missing)}")


def _run(cmd: List[str], **popen_kwargs) -> None:
    print("[CMD]", " ".join(map(str, cmd)))
    subprocess.run(cmd, check=True, **popen_kwargs)


def _count_fastq_reads(fq: Path) -> int:
    """
    Count reads in a FASTQ(.gz) by counting lines. Assumes 4 lines per record.
    Streams gzip to avoid RAM spikes.
    """
    opener = gzip.open if str(fq).endswith(".gz") else open
    line_count = 0
    try:
        with opener(fq, "rt", encoding="utf-8", errors="replace") as fh:
            for line_count, _ in enumerate(fh, start=1):
                pass
    except FileNotFoundError:
        return 0
    if line_count == 0:
        return 0
    return line_count // 4


def _write_json(p: Path, obj: dict) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2) + "\n")


# --------------------------- primitives --------------------------

def umi_extract_pair(
    *,
    read_keep: Path,             # keep this read’s sequence (R1 or R3)
    mate_in: Path,               # MUST be R2: provides barcode/UMI
    out_fastq_gz: Path,
    umi_pattern: str = "NNNNNNNNNNNNNNNN",
    threads: int = 8,
    do_chunking: bool = False,
    chunks: int = 20,
) -> Dict[str, int]:
    """
    Extract barcode/UMI from mate_in (R2) and append to read_keep (R1 or R3) names.
    Writes compressed FASTQ to out_fastq_gz. UMI-tools logs are redirected to a file
    to keep SLURM logs clean.

    Returns:
        {"reads_in": int, "reads_out": int}
    """
    _which_or_raise("umi_tools", "pigz")
    out_fastq_gz.parent.mkdir(parents=True, exist_ok=True)

    # temp plain-FASTQ (umi_tools writes plain; we compress afterward)
    tmp_out = out_fastq_gz.with_suffix("")  # strip .gz if present
    if tmp_out.suffix != ".fastq":
        tmp_out = tmp_out.with_suffix(".fastq")

    log_path = out_fastq_gz.with_name(out_fastq_gz.name + ".umi.log")

    if not do_chunking:
        # simple path
        _run([
            "umi_tools", "extract",
            f"--bc-pattern={umi_pattern}",
            f"--stdin={str(mate_in)}",           # R2 (UMI source)
            f"--read2-in={str(read_keep)}",      # keep (R1 or R3)
            f"--read2-out={str(tmp_out)}",       # write KEEP here (plain FASTQ)
            "-S", "/dev/null",                   # discard read1 stream
            "--log", str(log_path),
            "--log2stderr",                      # log to file + stderr flag present but log file wins
        ])
        # compress
        with open(out_fastq_gz, "wb") as fout:
            subprocess.run(["pigz", "-p", str(threads), "-c", str(tmp_out)], check=True, stdout=fout)
        try:
            tmp_out.unlink()
        except FileNotFoundError:
            pass
        reads_out = _count_fastq_reads(out_fastq_gz)
        reads_in = _count_fastq_reads(read_keep)
        return {"reads_in": reads_in, "reads_out": reads_out}

    # large-file path: chunk then per-chunk extract, concat, compress
    _which_or_raise("seqkit", "parallel")
    with tempfile.TemporaryDirectory() as tdir_:
        tdir = Path(tdir_)
        chunks_dir = tdir / "chunks"
        chunks_dir.mkdir(parents=True, exist_ok=True)

        # Split the two inputs in lockstep
        _run([
            "seqkit", "split2", "--by-part", str(chunks), "-j", str(threads),
            "-O", str(chunks_dir),
            "-1", str(read_keep),    # keep stream (R1 or R3)
            "-2", str(mate_in),      # barcode stream (R2)
        ])

        # Concatenate plain FASTQ parts emitted by umi_tools into tmp_concat
        tmp_concat = tdir / "concat.fastq"
        with open(tmp_concat, "wb") as cat:
            keep_chunks = sorted(list(chunks_dir.glob("*_R1.part_*.fastq.gz")) + list(chunks_dir.glob("*_R3.part_*.fastq.gz")))
            if not keep_chunks:
                raise FileNotFoundError(f"No keep-chunks found in {chunks_dir}")

            for keep_chunk in keep_chunks:
                # find the mate chunk (R2) for this keep chunk
                mate_chunk = Path(str(keep_chunk).replace("_R1.part_", "_R2.part_").replace("_R3.part_", "_R2.part_"))
                if not mate_chunk.exists():
                    raise FileNotFoundError(f"Mate chunk not found for {keep_chunk.name}; expected {mate_chunk.name}")

                tmp_chunk = tdir / (keep_chunk.stem + ".bc1.fastq")
                _run([
                    "umi_tools", "extract",
                    f"--bc-pattern={umi_pattern}",
                    f"--stdin={str(mate_chunk)}",      # R2
                    f"--read2-in={str(keep_chunk)}",   # R1/R3
                    f"--read2-out={str(tmp_chunk)}",   # plain FASTQ
                    "-S", "/dev/null",
                    "--log", str(log_path),
                    "--log2stderr",
                ])
                # append to concat
                with open(tmp_chunk, "rb") as fin:
                    shutil.copyfileobj(fin, cat)
                tmp_chunk.unlink(missing_ok=True)

        with open(out_fastq_gz, "wb") as fout:
            subprocess.run(["pigz", "-p", str(threads), "-c", str(tmp_concat)], check=True, stdout=fout)
        tmp_concat.unlink(missing_ok=True)

    reads_out = _count_fastq_reads(out_fastq_gz)
    reads_in = _count_fastq_reads(read_keep)
    return {"reads_in": reads_in, "reads_out": reads_out}


def cutadapt_append_tn5_to_name(
    r1_in: Path, r3_in: Path,
    r1_out: Path, r3_out: Path,
    threads: int = 4,
) -> Dict[str, int]:
    """
    Append split Tn5 halves into read name; fixed 5' clipping; adapter trim.
    Returns read counts from outputs for QC aggregation.
    """
    _which_or_raise("cutadapt")
    r1_out.parent.mkdir(parents=True, exist_ok=True)
    r3_out.parent.mkdir(parents=True, exist_ok=True)

    json_log = r1_out.with_suffix("").with_suffix(".cutadapt.json")

    _run([
        "cutadapt",
        "-e", "0.2",
        "--pair-filter=any",
        "-j", str(threads),

        # rename semantics: include cut prefixes from both reads
        "--rename", "{id}_{r1.cut_prefix}_{r2.cut_prefix} {comment}",

        # fixed 5' clipping
        "-u", "5", "-U", "5",

        # ME trimming (keep exactly as desired)
        "-g", "AGATGTGTATAAGAGACAG",
        "-G", "AGATGTGTATAAGAGACAG",

        "--report=minimal",
        f"--json={str(json_log)}",

        "-o", str(r1_out),
        "-p", str(r3_out),

        # inputs
        str(r1_in), str(r3_in),
    ])

    # parse JSON if available; still count outputs as source of truth
    out_r1 = _count_fastq_reads(r1_out)
    out_r3 = _count_fastq_reads(r3_out)
    return {"reads_out_r1": out_r1, "reads_out_r3": out_r3}


def demux_by_split_bc(layout_file: Path, sample_well_map: Path, input_fastq_gz: Path, out_dir: Path) -> Dict[str, Dict[str, int]]:
    """
    Backwards-compatible shim calling the demux core, then counting per-sample outputs.

    Returns:
        { "<sample>": { "r1_reads": int, "r3_reads": int, "passed": int } }
    """
    demux_split_barcodes(layout_file, input_fastq_gz, sample_well_map, output_dir=out_dir)

    per_sample: Dict[str, Dict[str, int]] = {}
    for smp_dir in sorted(out_dir.glob("*")):
        if not smp_dir.is_dir():
            continue
        r1 = next(smp_dir.glob("*_R1.bc1.bc2.fastq.gz"), None)
        r3 = next(smp_dir.glob("*_R3.bc1.bc2.fastq.gz"), None)
        r1c = _count_fastq_reads(r1) if r1 else 0
        r3c = _count_fastq_reads(r3) if r3 else 0
        per_sample[smp_dir.name] = {"r1_reads": r1c, "r3_reads": r3c, "passed": min(r1c, r3c)}
    return per_sample


# ------------------------ demux parts → merge --------------------

_PART_RE = re.compile(r"^part_(\d+)_R([13])\.bc1\.bc2_(.+)\.fastq\.gz$")


def _scan_demux_parts(corr_dir: Path) -> Dict[str, Dict[str, List[Path]]]:
    """
    Return {sample: {"R1": [parts...], "R3": [parts...]}} discovered under corr_dir.
    Expected filenames: part_###_R[13].bc1.bc2_<sample>.fastq.gz
    """
    by_sample: Dict[str, Dict[str, List[Path]]] = {}
    for p in corr_dir.glob("part_*_R*.bc1.bc2_*.fastq.gz"):
        m = _PART_RE.match(p.name)
        if not m:
            continue
        read = f"R{m.group(2)}"
        sample = m.group(3)
        d = by_sample.setdefault(sample, {"R1": [], "R3": []})
        d[read].append(p)

    # sort parts by numeric chunk id
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

            # Concatenate gzip members correctly and track bytes
            total_bytes = 0
            with open(dst, "wb") as fout:
                for part in parts:
                    with open(part, "rb") as fin:
                        # copyfileobj returns None; manually count bytes
                        buf = fin.read(1024 * 1024)
                        while buf:
                            fout.write(buf)
                            total_bytes += len(buf)
                            buf = fin.read(1024 * 1024)

            sample_sum[read] = {"parts": len(parts), "bytes": int(total_bytes)}

            if not keep_parts:
                for part in parts:
                    try:
                        part.unlink()
                    except FileNotFoundError:
                        pass

        if sample_sum:
            summary[sample] = sample_sum

    return summary
