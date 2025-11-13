# src/scifi_demux/steps/step1.py
from __future__ import annotations

import csv
import json
import os
import subprocess
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from scifi_demux.io_utils import resolve_layout_path
from scifi_demux.utils.fs import ensure_dir, write_ok, atomic_write_text

from .primitives import (
    umi_extract_pair,
    cutadapt_append_tn5_to_name,
    demux_by_split_bc,
    merge_demuxed_chunks,
    _count_fastq_reads,   # used for QC aggregation
    _write_json,          # used for QC aggregation
)

PLAN_NAME = "run_plan.step1.chunks.tsv"


# ---------------------------- planning ----------------------------

def _raw_fastqs(raw_dir: Path, library: str) -> tuple[Path, Path, Path]:
    r1 = raw_dir / f"{library}_R1.fastq.gz"
    r2 = raw_dir / f"{library}_R2.fastq.gz"
    r3 = raw_dir / f"{library}_R3.fastq.gz"
    missing = [p for p in (r1, r2, r3) if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing raw FASTQs: {' '.join(map(str, missing))}")
    return r1, r2, r3


def plan_chunks(raw_dir: Path, library: str, work_root: Path, chunks: int) -> Path:
    r1, r2, r3 = _raw_fastqs(raw_dir, library)
    work_root.mkdir(parents=True, exist_ok=True)
    plan_path = work_root / PLAN_NAME

    # Output subdirs with lockstep splits
    r12_dir = ensure_dir(work_root / "chunks_r12")  # split2(R1,R2)
    r32_dir = ensure_dir(work_root / "chunks_r32")  # split2(R3,R2)

    # 1) split R1+R2 together
    if not any(r12_dir.glob(f"{library}_R1.part_*.fastq.gz")):
        subprocess.run([
            "seqkit", "split2", "--by-part", str(chunks), "-j", "4",
            "-1", str(r1), "-2", str(r2),
            "-O", str(r12_dir),
        ], check=True)

    # 2) split R3+R2 together
    if not any(r32_dir.glob(f"{library}_R3.part_*.fastq.gz")):
        subprocess.run([
            "seqkit", "split2", "--by-part", str(chunks), "-j", "4",
            "-1", str(r3), "-2", str(r2),
            "-O", str(r32_dir),
        ], check=True)

    # Gather parts
    r1_parts   = sorted(r12_dir.glob(f"{library}_R1.part_*.fastq.gz"))
    r2r1_parts = sorted(r12_dir.glob(f"{library}_R2.part_*.fastq.gz"))  # R2 aligned to R1
    r3_parts   = sorted(r32_dir.glob(f"{library}_R3.part_*.fastq.gz"))
    r2r3_parts = sorted(r32_dir.glob(f"{library}_R2.part_*.fastq.gz"))  # R2 aligned to R3

    if not (len(r1_parts) == len(r2r1_parts) == len(r3_parts) == len(r2r3_parts) == chunks):
        raise RuntimeError(
            f"Chunk count mismatch: R1={len(r1_parts)} R2(R1)={len(r2r1_parts)} "
            f"R3={len(r3_parts)} R2(R3)={len(r2r3_parts)} expected={chunks}"
        )

    # Plan lines: chunk_id, library, r1, r2-for-r1, r3, r2-for-r3, out_root
    lines = ["#chunk_id\tlibrary\tr1_raw_chunk\tr2_for_r1\tr3_raw_chunk\tr2_for_r3\tout_root"]
    for i, (r1p, r2r1p, r3p, r2r3p) in enumerate(zip(r1_parts, r2r1_parts, r3_parts, r2r3_parts), start=1):
        lines.append(f"{i}\t{library}\t{r1p}\t{r2r1p}\t{r3p}\t{r2r3p}\t{work_root}")

    atomic_write_text(plan_path, "\n".join(lines) + "\n")
    return plan_path


# ---------------------------- worker -----------------------------

def worker_chunk(plan: Path, idx: int, layout: Optional[str], design: Optional[Path], mode: str = "local") -> None:
    rows = [ln.strip() for ln in plan.read_text().splitlines() if ln.strip() and not ln.startswith("#")]
    if idx > len(rows):
        raise IndexError(f"array_id {idx} > plan rows {len(rows)}")

    # 7 cols: cid, lib, r1, r2-for-r1, r3, r2-for-r3, work_root
    chunk_id_s, library, r1_raw, r2_for_r1, r3_raw, r2_for_r3, work_root_s = rows[idx - 1].split("\t")
    chunk_id = int(chunk_id_s)
    work_root = Path(work_root_s)

    sent_dir = ensure_dir(work_root / "_sentinels")
    metrics_dir = ensure_dir(work_root / "_control" / "metrics")

    bc1_dir = ensure_dir(work_root / "bc1")
    bc1bc2_dir = ensure_dir(work_root / "bc1bc2")
    corr_dir = ensure_dir(work_root / "Corrected")

    r1_bc1 = bc1_dir / f"part_{chunk_id:03d}_R1.bc1.fastq.gz"
    r3_bc1 = bc1_dir / f"part_{chunk_id:03d}_R3.bc1.fastq.gz"
    r1_bc2 = bc1bc2_dir / f"part_{chunk_id:03d}_R1.bc1.bc2.fastq.gz"
    r3_bc2 = bc1bc2_dir / f"part_{chunk_id:03d}_R3.bc1.bc2.fastq.gz"

    threads = int(os.environ.get("SLURM_CPUS_PER_TASK") or os.environ.get("NSLOTS") or 1) if mode == "hpc" else 1

    umi_ok   = sent_dir / f"chunk_{chunk_id:03d}.umi"
    cut_ok   = sent_dir / f"chunk_{chunk_id:03d}.cutadapt"
    demux_ok = sent_dir / f"chunk_{chunk_id:03d}.demux"

    # 1) UMI attach
    if not (sent_dir / (umi_ok.name + ".ok.json")).exists():
        r1_stats = umi_extract_pair(read_keep=Path(r1_raw), mate_in=Path(r2_for_r1), out_fastq_gz=r1_bc1, threads=threads)
        r3_stats = umi_extract_pair(read_keep=Path(r3_raw), mate_in=Path(r2_for_r3), out_fastq_gz=r3_bc1, threads=threads)
        write_ok(umi_ok, {"chunk": chunk_id, "step": "umi", "threads": threads})
        # metrics
        _write_json(metrics_dir / f"chunk_{chunk_id:03d}.umi.json", {"r1": r1_stats, "r3": r3_stats})

    # 2) Cutadapt rename/clip/trim
    if not (sent_dir / (cut_ok.name + ".ok.json")).exists():
        ca_stats = cutadapt_append_tn5_to_name(r1_in=r1_bc1, r3_in=r3_bc1, r1_out=r1_bc2, r3_out=r3_bc2, threads=min(threads, 8))
        write_ok(cut_ok, {"chunk": chunk_id, "step": "cutadapt", "threads": threads})
        _write_json(metrics_dir / f"chunk_{chunk_id:03d}.cutadapt.json", ca_stats)

    # 3) Demux per sample (requires design)
    if not (sent_dir / (demux_ok.name + ".ok.json")).exists():
        if not design:
            raise ValueError("demux requires --design")
        layout_path = resolve_layout_path(layout)

        # Run demux separately for R1 and R3 chunk outputs.
        # demux_by_split_bc writes per-sample chunked files into corr_dir and returns per-sample counts.
        demux_r1 = demux_by_split_bc(layout_file=layout_path, sample_well_map=design, input_fastq_gz=r1_bc2, out_dir=corr_dir)
        demux_r3 = demux_by_split_bc(layout_file=layout_path, sample_well_map=design, input_fastq_gz=r3_bc2, out_dir=corr_dir)
        write_ok(demux_ok, {"chunk": chunk_id, "step": "demux", "threads": threads})

        # Merge demux metrics from R1/R3:
        # demux_* dicts have the same keys; keep per-sample totals and min-pairs (passed) if present.
        merged: dict = {}
        keys = set(demux_r1.keys()) | set(demux_r3.keys())
        for k in keys:
            r1c = demux_r1.get(k, {})
            r3c = demux_r3.get(k, {})
            merged[k] = {
                "r1_reads": int(r1c.get("r1_reads", 0)),
                "r3_reads": int(r3c.get("r3_reads", 0)),
                "passed": int(min(r1c.get("r1_reads", 0), r3c.get("r3_reads", 0))),
            }
        _write_json(metrics_dir / f"chunk_{chunk_id:03d}.demux.json", merged)


# ---------------------------- local orchestration ----------------------------

def run_step1_local(library: str, raw_dir: Path, design: Optional[Path], layout: str, chunks: int, parallel_jobs: int) -> None:
    work_root = Path(f"{library}_work")
    plan = plan_chunks(raw_dir=raw_dir, library=library, work_root=work_root, chunks=chunks)

    # Fan-out with GNU parallel (one proc per chunk)
    cmd = f"seq 1 {chunks} | parallel -j {parallel_jobs} scifi-demux step1 worker-chunk --plan {plan} --array-id {{}} --mode local"
    if design:
        cmd += f" --design {design}"
    if layout:
        cmd += f" --layout {layout}"
    subprocess.run(cmd, shell=True, check=True)

    # Merge when done
    merge_library(library=library, work_root=work_root)


# ---------------------------- merge + QC aggregation ----------------------------

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

    # Merge demux parts → combined
    corr_dir = work_root / "Corrected"
    out_dir = work_root / "combined"
    summary = merge_demuxed_chunks(corr_dir=corr_dir, out_dir=out_dir, overwrite=True, keep_parts=False)
    print(f"[merge] wrote {len(summary)} samples to {out_dir}")

    # -------- Aggregate metrics into QC summaries --------
    metrics_dir = work_root / "_control" / "metrics"
    qc_dir = work_root / "qc" / "summary"
    qc_dir.mkdir(parents=True, exist_ok=True)

    lib_raw_r1 = lib_raw_r3 = 0
    lib_umi_r1_out = lib_umi_r3_out = 0
    lib_cut_r1_out = lib_cut_r3_out = 0
    per_sample = defaultdict(lambda: {"r1": 0, "r3": 0, "passed": 0})

    # Totals from split parts listed in the plan
    for ln in rows:
        _, _, r1p, _, r3p, _, _ = ln.split("\t")
        lib_raw_r1 += _count_fastq_reads(Path(r1p))
        lib_raw_r3 += _count_fastq_reads(Path(r3p))

    # Per-chunk UMI and cutadapt totals
    for p in sorted(metrics_dir.glob("chunk_*.umi.json")):
        d = json.loads(p.read_text())
        lib_umi_r1_out += int(d.get("r1", {}).get("reads_out", 0))
        lib_umi_r3_out += int(d.get("r3", {}).get("reads_out", 0))
    for p in sorted(metrics_dir.glob("chunk_*.cutadapt.json")):
        d = json.loads(p.read_text())
        lib_cut_r1_out += int(d.get("reads_out_r1", 0))
        lib_cut_r3_out += int(d.get("reads_out_r3", 0))

    # Per-chunk demux assigned/passed per sample
    for p in sorted(metrics_dir.glob("chunk_*.demux.json")):
        d = json.loads(p.read_text())
        for smp, v in d.items():
            per_sample[smp]["r1"] += int(v.get("r1_reads", 0))
            per_sample[smp]["r3"] += int(v.get("r3_reads", 0))
            per_sample[smp]["passed"] += int(v.get("passed", 0))

    # Library-level TSV
    lib_tsv = qc_dir / "library_counts.tsv"
    with lib_tsv.open("w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["library", "stage", "reads_R1", "reads_R3", "notes"])
        w.writerow([library, "raw", lib_raw_r1, lib_raw_r3, "from split parts"])
        w.writerow([library, "umi_attached", lib_umi_r1_out, lib_umi_r3_out, "post umi_tools extract"])
        w.writerow([library, "cutadapt_named", lib_cut_r1_out, lib_cut_r3_out, "post cutadapt rename/trim"])
        assigned_r1 = sum(v["r1"] for v in per_sample.values())
        assigned_r3 = sum(v["r3"] for v in per_sample.values())
        passed_pairs = sum(v["passed"] for v in per_sample.values())
        w.writerow([library, "demux_assigned", assigned_r1, assigned_r3, "sum of sample outputs"])
        w.writerow([library, "demux_passed_pairs", passed_pairs, passed_pairs, "paired min(R1,R3)"])

    # Sample-level TSV
    smp_tsv = qc_dir / "sample_counts.tsv"
    with smp_tsv.open("w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["sample", "reads_R1", "reads_R3", "passed_pairs"])
        for smp in sorted(per_sample):
            w.writerow([smp, per_sample[smp]["r1"], per_sample[smp]["r3"], per_sample[smp]["passed"]])

    # New: per-sample read pairs and fraction of library (for MultiQC barplots)
    pairs_tsv = qc_dir / "sample_pairs.tsv"
    fracs_tsv = qc_dir / "sample_pairs_fraction.tsv"
    
    # define library “pair” count as min(R1,R3) at raw stage
    lib_pairs_raw = min(lib_raw_r1, lib_raw_r3) if (lib_raw_r1 and lib_raw_r3) else 0

    with pairs_tsv.open("w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["sample", "read_pairs"])
        for smp in sorted(per_sample):
            w.writerow([smp, per_sample[smp]["passed"]])

    with fracs_tsv.open("w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["sample", "fraction"])
        for smp in sorted(per_sample):
            passed = per_sample[smp]["passed"]
            frac = passed / lib_pairs_raw if lib_pairs_raw > 0 else 0.0
            w.writerow([smp, f"{frac:.6f}"])

    
    # Combined JSON
    _write_json(qc_dir / "counts.json", {
        "library": library,
        "library_counts": {
            "raw": {"R1": lib_raw_r1, "R3": lib_raw_r3},
            "umi_attached": {"R1": lib_umi_r1_out, "R3": lib_umi_r3_out},
            "cutadapt_named": {"R1": lib_cut_r1_out, "R3": lib_cut_r3_out},
            "demux_assigned": {"R1": assigned_r1, "R3": assigned_r3},
            "demux_passed_pairs": {"pairs": passed_pairs},
        },
        "samples": per_sample,
    })

    # Tiny README
    (qc_dir / "README.md").write_text(
        f"# Step 1 counts — {library}\n\n"
        f"- Library totals: `library_counts.tsv`\n"
        f"- Per-sample totals: `sample_counts.tsv`\n"
        f"- JSON: `counts.json`\n"
    )


# ---------------------------- progress + waiting ----------------------------

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
    if s in (None, "", "auto"):
        return None
    s = s.strip().lower()
    if s == "0":
        return 0
    if s[-1] in "smhd":
        mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}[s[-1]]
        return int(float(s[:-1]) * mult)
    return int(float(s))


def _detect_scheduler_timelimit_sec() -> int | None:
    job_id = os.environ.get("SLURM_JOB_ID")
    if not job_id:
        return None
    try:
        out = subprocess.check_output(["scontrol", "show", "job", job_id], stderr=subprocess.DEVNULL).decode()
        for tok in out.split():
            if tok.startswith("TimeLimit="):
                val = tok.split("=", 1)[1]
                if val == "UNLIMITED":
                    return None
                hh, mm, ss = map(int, val.split(":"))
                return hh * 3600 + mm * 60 + ss
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
            "started_at": datetime.utcfromtimestamp(started_ts).isoformat() + "Z",
            "updated_at": datetime.utcfromtimestamp(now).isoformat() + "Z",
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
            cfg = Path("qc/multiqc_scifi.yaml")
            run_multiqc(work_root, config=cfg if cfg.exists() else None)
            _write_progress(work_root, library, poll_interval, max_wait_sec, "complete", "QC complete", started)
            return
        msg = (
            f"{counts['dem']}/{counts['total']} chunks complete; missing={','.join(map(str, counts['missing']))}"
            if counts["total"] else
            "waiting for plan"
        )
        _write_progress(work_root, library, poll_interval, max_wait_sec, "waiting", msg, started)
        if max_wait_sec and (time.time() - started) >= max_wait_sec:
            _write_progress(work_root, library, poll_interval, max_wait_sec, "timeout", "Timed out waiting for chunks", started)
            raise TimeoutError("Reached max-wait while waiting for chunk completion")
        time.sleep(poll_interval)


# ---------------------------- MultiQC hook ----------------------------

def run_multiqc(work_root: Path, *, config: Optional[Path] = None, out_subdir: str = "qc/report") -> None:
    """
    Run MultiQC over the library workspace. Non-fatal on absence/failure.
    Writes to work_root/<out_subdir>.
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


# -------------------- counts-only aggregation (no merge) -----------------
def _aggregate_counts_only(library: str, work_root: Path) -> Path:
    """
    Recompute QC summaries from existing per-chunk metrics and the plan.
    Does NOT merge or touch FASTQs. Writes TSV/JSON into qc/summary/.
    Returns the qc/summary directory path.
    """
    plan = work_root / PLAN_NAME
    if not plan.exists():
        raise FileNotFoundError(f"Plan not found: {plan}")

    qc_dir = work_root / "qc" / "summary"
    qc_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir = work_root / "_control" / "metrics"

    rows = [ln for ln in plan.read_text().splitlines() if ln.strip() and not ln.startswith("#")]

    # library accumulators
    lib_raw_r1 = lib_raw_r3 = 0
    lib_umi_r1_out = lib_umi_r3_out = 0
    lib_cut_r1_out = lib_cut_r3_out = 0
    per_sample = defaultdict(lambda: {"r1": 0, "r3": 0, "passed": 0})

    # raw from split parts listed in plan
    for ln in rows:
        # cid  lib  r1p  r2forr1  r3p  r2forr3  out_root
        parts = ln.split("\t")
        if len(parts) < 7:
            continue
        _, _, r1p, _, r3p, _, _ = parts
        lib_raw_r1 += _count_fastq_reads(Path(r1p))
        lib_raw_r3 += _count_fastq_reads(Path(r3p))

    # per-chunk UMI + cutadapt
    for p in sorted(metrics_dir.glob("chunk_*.umi.json")):
        try:
            d = json.loads(p.read_text())
            lib_umi_r1_out += int(d.get("r1", {}).get("reads_out", 0))
            lib_umi_r3_out += int(d.get("r3", {}).get("reads_out", 0))
        except Exception:
            pass

    for p in sorted(metrics_dir.glob("chunk_*.cutadapt.json")):
        try:
            d = json.loads(p.read_text())
            lib_cut_r1_out += int(d.get("reads_out_r1", 0))
            lib_cut_r3_out += int(d.get("reads_out_r3", 0))
        except Exception:
            pass

    # per-chunk demux assigned/passed per sample
    for p in sorted(metrics_dir.glob("chunk_*.demux.json")):
        try:
            d = json.loads(p.read_text())
            for smp, v in d.items():
                per_sample[smp]["r1"] += int(v.get("r1_reads", 0))
                per_sample[smp]["r3"] += int(v.get("r3_reads", 0))
                per_sample[smp]["passed"] += int(v.get("passed", 0))
        except Exception:
            pass

    # library-level TSV
    lib_tsv = qc_dir / "library_counts.tsv"
    with lib_tsv.open("w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["library", "stage", "reads_R1", "reads_R3", "notes"])
        w.writerow([library, "raw", lib_raw_r1, lib_raw_r3, "from split parts"])
        w.writerow([library, "umi_attached", lib_umi_r1_out, lib_umi_r3_out, "post umi_tools extract"])
        w.writerow([library, "cutadapt_named", lib_cut_r1_out, lib_cut_r3_out, "post cutadapt rename/trim"])
        assigned_r1 = sum(v["r1"] for v in per_sample.values())
        assigned_r3 = sum(v["r3"] for v in per_sample.values())
        passed_pairs = sum(v["passed"] for v in per_sample.values())
        w.writerow([library, "demux_assigned", assigned_r1, assigned_r3, "sum of sample outputs"])
        w.writerow([library, "demux_passed_pairs", passed_pairs, passed_pairs, "paired min(R1,R3)"])

    # sample-level TSV
    smp_tsv = qc_dir / "sample_counts.tsv"
    with smp_tsv.open("w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["sample", "reads_R1", "reads_R3", "passed_pairs"])
        for smp in sorted(per_sample):
            w.writerow([smp, per_sample[smp]["r1"], per_sample[smp]["r3"], per_sample[smp]["passed"]])

    # New: per-sample read pairs and fraction of library (for MultiQC)
    pairs_tsv = qc_dir / "sample_pairs.tsv"
    fracs_tsv = qc_dir / "sample_pairs_fraction.tsv"
    lib_pairs_raw = min(lib_raw_r1, lib_raw_r3) if (lib_raw_r1 and lib_raw_r3) else 0

    with pairs_tsv.open("w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["sample", "read_pairs"])
        for smp in sorted(per_sample):
            w.writerow([smp, per_sample[smp]["passed"]])

    with fracs_tsv.open("w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["sample", "fraction"])
        for smp in sorted(per_sample):
            passed = per_sample[smp]["passed"]
            frac = passed / lib_pairs_raw if lib_pairs_raw > 0 else 0.0
            w.writerow([smp, f"{frac:.6f}"])

    # machine-readable JSON
    _write_json(qc_dir / "counts.json", {
        "library": library,
        "library_counts": {
            "raw": {"R1": lib_raw_r1, "R3": lib_raw_r3},
            "umi_attached": {"R1": lib_umi_r1_out, "R3": lib_umi_r3_out},
            "cutadapt_named": {"R1": lib_cut_r1_out, "R3": lib_cut_r3_out},
            "demux_assigned": {"R1": assigned_r1, "R3": assigned_r3},
            "demux_passed_pairs": {"pairs": passed_pairs},
        },
        "samples": per_sample,
    })

    # lightweight readme
    (qc_dir / "README.md").write_text(
        f"# Step 1 counts — {library}\n\n"
        f"- Library totals: `library_counts.tsv`\n"
        f"- Per-sample totals: `sample_counts.tsv`\n"
        f"- JSON: `counts.json`\n"
    )
    return qc_dir


# ---------------------------- HPC helper ----------------------------

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
    """
    Prepare plan. If follow=True, block and monitor sentinels, merge, and run QC.
    Array workers should invoke `scifi-demux step1 worker-chunk ...` using this plan.
    """
    work_root = Path(f"{library}_work")
    plan = plan_chunks(raw_dir=raw_dir, library=library, work_root=work_root, chunks=chunks)
    if follow:
        wait_and_maybe_merge(library=library, work_root=work_root, poll_interval=poll_interval, max_wait=max_wait)
    return plan
