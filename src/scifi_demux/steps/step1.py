from __future__ import annotations
from pathlib import Path
from typing import Optional
import os, subprocess, shutil  # add shutil
from scifi_demux.utils.fs import ensure_dir, has_ok, write_ok, atomic_write_text
from scifi_demux.io_utils import data_path
from scifi_demux.io_utils import resolve_layout_path
import time, json
from datetime import datetime

from scifi_demux.steps.primitives import (
    umi_extract_pair,              
    cutadapt_append_tn5_to_name,   
    demux_by_split_bc,             
    merge_demuxed_chunks,          
)

PLAN_NAME = "run_plan.step1.chunks.tsv"

# step1.py (key excerpts)

PLAN_NAME = "run_plan.step1.chunks.tsv"

def _raw_fastqs(raw_dir: Path, library: str) -> tuple[Path, Path, Path]:
    r1 = raw_dir / f"{library}_R1.fastq.gz"
    r2 = raw_dir / f"{library}_R2.fastq.gz"
    r3 = raw_dir / f"{library}_R3.fastq.gz"
    missing = [p for p in (r1,r2,r3) if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing raw FASTQs: {' '.join(map(str, missing))}")
    return r1, r2, r3

def plan_chunks(raw_dir: Path, library: str, work_root: Path, chunks: int) -> Path:
    r1, r2, r3 = _raw_fastqs(raw_dir, library)
    chunks_dir = ensure_dir(work_root / "chunks_raw")
    plan_path = work_root / PLAN_NAME

    # Split R1/R3 paired (keeps mates aligned)
    if not any(chunks_dir.glob(f"{library}_R1.part_*.fastq.gz")):
        subprocess.run([
            "seqkit", "split2", "--by-part", str(chunks),
            "-1", str(r1), "-2", str(r3),
            "-O", str(chunks_dir)
        ], check=True)

    # Split R2 alone into the same number of parts (counts are equal → boundaries align)
    if not any(chunks_dir.glob(f"{library}_R2.part_*.fastq.gz")):
        subprocess.run([
            "seqkit", "split", "-p", str(chunks),
            str(r2), "-O", str(chunks_dir)
        ], check=True)

    lines = ["#chunk_id\tlibrary\tr1_raw_chunk\tr2_raw_chunk\tr3_raw_chunk\tout_root"]

    r1_parts = sorted(chunks_dir.glob(f"{library}_R1.part_*.fastq.gz"))
    r2_parts = sorted(chunks_dir.glob(f"{library}_R2.part_*.fastq.gz"))
    r3_parts = sorted(chunks_dir.glob(f"{library}_R3.part_*.fastq.gz"))

    if not (len(r1_parts) == len(r2_parts) == len(r3_parts) == chunks):
        raise RuntimeError(f"Chunk counts mismatch R1={len(r1_parts)} R2={len(r2_parts)} R3={len(r3_parts)} (expected {chunks})")

    for i, (r1p, r2p, r3p) in enumerate(zip(r1_parts, r2_parts, r3_parts), start=1):
        lines.append(f"{i}\t{library}\t{r1p}\t{r2p}\t{r3p}\t{work_root}")

    atomic_write_text(plan_path, "\n".join(lines) + "\n")
    return plan_path

def worker_chunk(plan: Path, idx: int, layout: Optional[str], design: Optional[Path], mode: str = "local") -> None:
    rows = [ln.strip() for ln in plan.read_text().splitlines() if ln.strip() and not ln.startswith("#")]
    if idx > len(rows):
        raise IndexError(f"array_id {idx} > plan rows {len(rows)}")

    # Now 6 cols: cid, lib, r1, r2, r3, work_root
    chunk_id_s, library, r1_raw, r2_raw, r3_raw, work_root_s = rows[idx-1].split("\t")
    chunk_id  = int(chunk_id_s); work_root = Path(work_root_s)
    sent_dir  = ensure_dir(work_root / "_sentinels")

    bc1_dir    = ensure_dir(work_root / "bc1")
    bc1bc2_dir = ensure_dir(work_root / "bc1bc2")
    corr_dir   = ensure_dir(work_root / "Corrected")

    r1_bc1 = bc1_dir    / f"part_{chunk_id:03d}_R1.bc1.fastq.gz"
    r3_bc1 = bc1_dir    / f"part_{chunk_id:03d}_R3.bc1.fastq.gz"
    r1_bc2 = bc1bc2_dir / f"part_{chunk_id:03d}_R1.bc1.bc2.fastq.gz"
    r3_bc2 = bc1bc2_dir / f"part_{chunk_id:03d}_R3.bc1.bc2.fastq.gz"

    threads = int(os.environ.get("SLURM_CPUS_PER_TASK") or os.environ.get("NSLOTS") or 1) if mode == "hpc" else 1

    # --- Sentinels: pass *stem* to write_ok (it adds .ok.json)
    umi_ok   = sent_dir / f"chunk_{chunk_id:03d}.umi"
    cut_ok   = sent_dir / f"chunk_{chunk_id:03d}.cutadapt"
    demux_ok = sent_dir / f"chunk_{chunk_id:03d}.demux"

    # 1) UMI: attach R2’s 10x BC/UMI to BOTH R1 and R3
    if not (sent_dir / (umi_ok.name + ".ok.json")).exists():
        # R1 <- (barcodes from) R2
        umi_extract_pair(read_keep=Path(r1_raw), mate_in=Path(r2_raw), out_fastq_gz=r1_bc1, threads=threads)
        # R3 <- (barcodes from) R2
        umi_extract_pair(read_keep=Path(r3_raw), mate_in=Path(r2_raw), out_fastq_gz=r3_bc1, threads=threads)
        write_ok(umi_ok, {"chunk": chunk_id, "step": "umi", "threads": threads})

    # 2) Cutadapt (safe defaults; do not discard)
    if not (sent_dir / (cut_ok.name + ".ok.json")).exists():
        cutadapt_append_tn5_to_name(r1_in=r1_bc1, r3_in=r3_bc1, r1_out=r1_bc2, r3_out=r3_bc2, threads=min(threads, 8))
        write_ok(cut_ok, {"chunk": chunk_id, "step": "cutadapt", "threads": threads})

    # 3) Demux (requires design + layout)
    if not (sent_dir / (demux_ok.name + ".ok.json")).exists():
        layout_path = resolve_layout_path(layout)
        if not design:
            raise ValueError("demux requires --design")
        demux_by_split_bc(layout_file=layout_path, sample_well_map=design, input_fastq_gz=r1_bc2, out_dir=corr_dir)
        demux_by_split_bc(layout_file=layout_path, sample_well_map=design, input_fastq_gz=r3_bc2, out_dir=corr_dir)
        write_ok(demux_ok, {"chunk": chunk_id, "step": "demux", "threads": threads})


def run_step1_local(library: str, raw_dir: Path, design: Optional[Path], layout: str, chunks: int, parallel_jobs: int) -> None:
    work_root = Path(f"{library}_work")
    plan = plan_chunks(raw_dir=raw_dir, library=library, work_root=work_root, chunks=chunks)
    # Run all chunk workers with GNU parallel (1 thread per chunk)
    cmd = f"seq 1 {chunks} | parallel -j {parallel_jobs} scifi-demux step1 worker-chunk --plan {plan} --array-id {{}} --mode local"
    if design:
        cmd += f" --design {design}"
    if layout:
        cmd += f" --layout {layout}"
    subprocess.run(cmd, shell=True, check=True)
    # Merge when done
    merge_library(library=library, work_root=work_root)


def merge_library(library: str, work_root: Path) -> None:
    # Ensure all demux sentinels exist before merging
    plan = work_root / PLAN_NAME
    rows = [ln for ln in plan.read_text().splitlines() if ln.strip() and not ln.startswith("#")]
    sent_dir = work_root / "_sentinels"
    missing = []
    for ln in rows:
        cid = int(ln.split("\t", 1)[0])
        if not (sent_dir / f"chunk_{cid:03d}.demux.ok.json").exists():
            missing.append(cid)
    if missing:
        raise RuntimeError(f"Cannot merge: missing demux sentinels for chunks: {missing}")

    corr_dir = work_root / "Corrected"
    out_dir = work_root / "combined"
    summary = merge_demuxed_chunks(corr_dir=corr_dir, out_dir=out_dir, overwrite=True, keep_parts=False)
    print(f"[merge] wrote {len(summary)} samples to {out_dir}")

# ---------- helpers for progress ----------
def _expected_chunk_ids(work_root: Path) -> list[int]:
    plan = work_root / PLAN_NAME
    if not plan.exists():
        return []
    rows = [ln.strip() for ln in plan.read_text().splitlines() if ln.strip() and not ln.startswith("#")]
    return [int(ln.split("\t", 1)[0]) for ln in rows]

def report_missing_chunks(work_root: Path) -> list[int]:
    expected = _expected_chunk_ids(work_root)
    sent = work_root / "_sentinels"
    missing: list[int] = []
    for cid in expected:
        if not (sent / f"chunk_{cid:03d}.demux.ok.json").exists():
            missing.append(cid)
    return missing

def _parse_duration_to_sec(s: str | None) -> int | None:
    if s in (None, "", "auto"): return None
    s = s.strip().lower()
    if s == "0": return 0
    if s[-1] in "smhd":
        mult = {'s':1,'m':60,'h':3600,'d':86400}[s[-1]]
        return int(float(s[:-1]) * mult)
    return int(float(s))

def _detect_scheduler_timelimit_sec() -> int | None:
    job_id = os.environ.get("SLURM_JOB_ID")
    if not job_id:
        return None
    try:
        out = subprocess.check_output(["scontrol","show","job",job_id], stderr=subprocess.DEVNULL).decode()
        for tok in out.split():
            if tok.startswith("TimeLimit="):
                val = tok.split("=",1)[1]
                if val == "UNLIMITED": return None
                hh, mm, ss = map(int, val.split(":"))
                return hh*3600 + mm*60 + ss
    except Exception:
        return None

def _scan_counts(work_root: Path) -> dict:
    total_ids = _expected_chunk_ids(work_root)
    sent = work_root / "_sentinels"
    umi = sum(1 for cid in total_ids if (sent / f"chunk_{cid:03d}.umi.ok.json").exists())
    cut = sum(1 for cid in total_ids if (sent / f"chunk_{cid:03d}.cutadapt.ok.json").exists())
    dem = sum(1 for cid in total_ids if (sent / f"chunk_{cid:03d}.demux.ok.json").exists())
    missing = [cid for cid in total_ids if not (sent / f"chunk_{cid:03d}.demux.ok.json").exists()]
    return {"total": len(total_ids), "umi": umi, "cut": cut, "dem": dem, "missing": missing}

def _write_progress(work_root: Path, library: str, poll_interval: int, max_wait_sec: int | None, state: str, msg: str, started_ts: float) -> None:
    ctrl = ensure_dir(work_root / "_control")
    snap = ctrl / "progress.json"
    nd = ctrl / "progress.ndjson"
    counts = _scan_counts(work_root)
    now = time.time()
    obj = {
        "stage": "step1",
        "library": library,
        "work_root": str(work_root),
        "times": {
            "started_at": datetime.utcfromtimestamp(started_ts).isoformat()+"Z",
            "updated_at": datetime.utcfromtimestamp(now).isoformat()+"Z",
            "elapsed_sec": int(now - started_ts),
        },
        "poll": {"interval_sec": poll_interval, "max_wait_sec": max_wait_sec},
        "counts": {
            "total": counts["total"], "umi": counts["umi"], "cut": counts["cut"], "dem": counts["dem"],
            "missing": len(counts["missing"]), "missing_indices": counts["missing"],
        },
        "state": state,
        "message": msg,
    }
    atomic_write_text(snap, json.dumps(obj, indent=2))
    try:
        with open(nd, "a") as fh:
            fh.write(json.dumps(obj) + "\n")
    except Exception:
        pass

def wait_and_maybe_merge(library: str, work_root: Path, poll_interval: int = 60, max_wait: str = "auto") -> None:
    started = time.time()
    max_wait_sec = _parse_duration_to_sec(max_wait)
    if max_wait_sec is None:
        max_wait_sec = _detect_scheduler_timelimit_sec()
    while True:
        counts = _scan_counts(work_root)
        if counts["total"] > 0 and counts["dem"] >= counts["total"]:
            _write_progress(work_root, library, poll_interval, max_wait_sec, "merging", "All chunks complete; merging", started)
            merge_library(library, work_root)
            _write_progress(work_root, library, poll_interval, max_wait_sec, "qc", "Merge complete; running MultiQC", started)
            # QC: use repo config if present; otherwise vanilla
            cfg = Path("qc/multiqc_scifi.yaml")
            run_multiqc(work_root, config=cfg if cfg.exists() else None)
            _write_progress(work_root, library, poll_interval, max_wait_sec, "complete", "QC complete", started)
            return
        msg = f"{counts['dem']}/{counts['total']} chunks complete; missing={','.join(map(str, counts['missing']))}" if counts["total"] else "waiting for plan"
        _write_progress(work_root, library, poll_interval, max_wait_sec, "waiting", msg, started)
        if max_wait_sec and (time.time() - started) >= max_wait_sec:
            _write_progress(work_root, library, poll_interval, max_wait_sec, "timeout", "Timed out waiting for chunks", started)
            raise TimeoutError("Reached max-wait while waiting for chunk completion")
        time.sleep(poll_interval)


# --- QC hook (MultiQC) ---
def run_multiqc(work_root: Path, *, config: Optional[Path] = None, out_subdir: str = "qc/report") -> None:
    """
    Run MultiQC over the library workspace. Non-fatal on absence/failure.
    Looks in work_root and subdirs; writes report to work_root/<out_subdir>.
    """
    out_dir = work_root / out_subdir
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = ["multiqc", "--outdir", str(out_dir), str(work_root)]
    if config:
        cmd[1:1] = ["--config", str(config)]
    try:
        subprocess.run(cmd, check=True)
        print(f"[multiqc] wrote report to {out_dir}")
    except FileNotFoundError:
        print("[multiqc] multiqc not found on PATH; skipping QC")
    except subprocess.CalledProcessError as e:
        print(f"[multiqc] multiqc failed with exit {e.returncode}; continuing")
        
# ---------- run_step1_hpc that supports follow ----------
def run_step1_hpc(
    library: str,
    raw_dir: Path,
    design: Optional[Path],
    layout: str | None,
    chunks: int,
    *,
    follow: bool,
    poll_interval: int,
    max_wait: str,
) -> Path:
    work_root = Path(f"{library}_work")
    plan = plan_chunks(raw_dir=raw_dir, library=library, work_root=work_root, chunks=chunks)
    # user submits the array externally; with --follow we poll and then merge
    if follow:
        wait_and_maybe_merge(library=library, work_root=work_root, poll_interval=poll_interval, max_wait=max_wait)
    return plan

