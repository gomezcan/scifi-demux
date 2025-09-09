# src/scifi_demux/steps/primitives.py
from __future__ import annotations
from pathlib import Path
from typing import Optional, List
import shutil, subprocess, tempfile, os
from scifi_demux.demux_core import demux_split_barcodes  # add


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

