from __future__ import annotations
from pathlib import Path
from typing import Optional
import os, subprocess
from scifi_demux.utils.fs import ensure_dir, has_ok, write_ok, atomic_write_text
from scifi_demux.io_utils import data_path

PLAN_NAME = "run_plan.step1.chunks.tsv"


def _raw_fastqs(raw_dir: Path, library: str) -> tuple[Path, Path]:
    r1 = raw_dir / f"{library}_R1.fastq.gz"
    r3 = raw_dir / f"{library}_R3.fastq.gz"
    if not r1.exists() or not r3.exists():
        raise FileNotFoundError(f"Missing raw FASTQs: {r1} {r3}")
    return r1, r3


def plan_chunks(raw_dir: Path, library: str, work_root: Path, chunks: int) -> Path:
    r1, r3 = _raw_fastqs(raw_dir, library)
    chunks_dir = ensure_dir(work_root / "chunks_raw")
    plan_path = work_root / PLAN_NAME
    # Split raw FASTQs into N parts using seqkit split2 (paired by part count)
    # part names: part_001_R1.fq.gz and part_001_R3.fq.gz
    if not any(chunks_dir.glob("part_*_R1.fastq.gz")):
        subprocess.run([
            "seqkit", "split2", "--by-part", str(chunks), "-1", str(r1), "-2", str(r3), "-O", str(chunks_dir)
        ], check=True)
    # Create plan TSV
    lines = ["#chunk_id	library	r1_raw_chunk	r3_raw_chunk	out_root"]
    pairs = sorted(chunks_dir.glob("part_*_R1.fastq.gz"))
    for i, r1p in enumerate(pairs, start=1):
        r3p = chunks_dir / r1p.name.replace("_R1", "_R3")
        lines.append(f"{i}	{library}	{r1p}	{r3p}	{work_root}")
    atomic_write_text(plan_path, "
".join(lines) + "
)
    return plan_path


def worker_chunk(plan: Path, idx: int, layout: str, design: Optional[Path], mode: str = "local") -> None:
    # Read Nth (1-based) row
    rows = [ln.strip() for ln in plan.read_text().splitlines() if ln.strip() and not ln.startswith("#")]
    if idx > len(rows):
        raise IndexError(f"array_id {idx} > plan rows {len(rows)}")
    chunk_id, library, r1_raw, r3_raw, work_root = rows[idx-1].split("	")
    chunk_id = int(chunk_id)
    work_root = Path(work_root)
    sent_dir = ensure_dir(work_root / "_sentinels")

    # Paths for intermediates
    bc1_dir     = ensure_dir(work_root / "bc1")
    bc1bc2_dir  = ensure_dir(work_root / "bc1bc2")
    corr_dir    = ensure_dir(work_root / "Corrected")

    r1_bc1 = bc1_dir / f"part_{chunk_id:03d}_R1.bc1.fastq.gz"
    r3_bc1 = bc1_dir / f"part_{chunk_id:03d}_R3.bc1.fastq.gz"
    r1_bc2 = bc1bc2_dir / f"part_{chunk_id:03d}_R1.bc1.bc2.fastq.gz"
    r3_bc2 = bc1bc2_dir / f"part_{chunk_id:03d}_R3.bc1.bc2.fastq.gz"

    threads = int(os.environ.get("SLURM_CPUS_PER_TASK") or os.environ.get("NSLOTS") or 1) if mode == "hpc" else 1
    # 1) UMI
    umi_ok = sent_dir / f"chunk_{chunk_id:03d}.umi.ok.json"
    if not umi_ok.exists():
        # R1 keep, mate=R3
        umi_extract_pair(read_keep=Path(r1_raw), mate_in=Path(r3_raw), out_fastq_gz=r1_bc1, threads=threads)
        # R3 keep, mate=R2 if available (your R3 SLURM did this)
        r2_raw = Path(str(r3_raw).replace("_R3.", "_R2."))
        if r2_raw.exists():
            umi_extract_pair(read_keep=Path(r3_raw), mate_in=r2_raw, out_fastq_gz=r3_bc1, threads=threads)
        else:
            if not r3_bc1.exists():
                shutil.copyfile(r3_raw, r3_bc1)
        write_ok(umi_ok, {"chunk": chunk_id, "step": "umi", "threads": threads})

    # 2) Cutadapt
    cut_ok = sent_dir / f"chunk_{chunk_id:03d}.cutadapt.ok.json"
    if not cut_ok.exists():
        cutadapt_append_tn5_to_name(r1_in=r1_bc1, r3_in=r3_bc1, r1_out=r1_bc2, r3_out=r3_bc2, threads=min(threads, 8))
        write_ok(cut_ok, {"chunk": chunk_id, "step": "cutadapt", "threads": threads})

    # 3) Demux for R1 and R3 (paired outputs)
    demux_ok = sent_dir / f"chunk_{chunk_id:03d}.demux.ok.json"
    if not demux_ok.exists():
        layout_path = resolve_layout_path(layout)  # None/"builtin" -> packaged 96-well layout
        if not design:
            raise ValueError("demux requires --design (sample→wells mapping)")
        # run for R1 and R3 so merge can write paired per-sample fastqs
        demux_by_split_bc(layout_file=layout_path, sample_well_map=design, input_fastq_gz=r1_bc2, out_dir=corr_dir)
        demux_by_split_bc(layout_file=layout_path, sample_well_map=design, input_fastq_gz=r3_bc2, out_dir=corr_dir)
        write_ok(demux_ok, {"chunk": chunk_id, "step": "demux", "threads": threads})


def merge_library(library: str, work_root: Path) -> None:
    # Ensure all demux sentinels exist
    sent_dir = work_root / "_sentinels"
    demux_sents = list(sent_dir.glob("chunk_*.demux.ok.json"))
    if not demux_sents:
        raise RuntimeError("No demux sentinels found; have the workers run?")
    # Your existing merge script expects per-group chunk files in Corrected/
    subprocess.run(["bash", "3_merge_fastq.ATAC.sh", str(work_root / "Corrected")], check=True)


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


def run_step1_hpc(library: str, raw_dir: Path, design: Optional[Path], layout: str, chunks: int, threads_per_chunk: int, array_limit: int, submit: bool, partition: str, time: str, mem: str) -> None:
    work_root = Path(f"{library}_work")
    plan = plan_chunks(raw_dir=raw_dir, library=library, work_root=work_root, chunks=chunks)
    # Render a simple sbatch that runs the worker for each chunk
    array = f"1-{chunks}%{array_limit}" if array_limit else f"1-{chunks}"
    sbatch = f"""#!/bin/bash
#SBATCH --job-name=step1-{library}
#SBATCH --partition={partition}
#SBATCH --time={time}
#SBATCH --mem={mem}
#SBATCH --cpus-per-task={threads_per_chunk}
#SBATCH --array={array}
#SBATCH --output=_logs/step1-{library}-%A_%a.out
set -euo pipefail
scifi-demux step1 worker-chunk --plan {plan} --mode hpc --layout {layout} {'--design '+str(design) if design else ''}
"""
    sb_path = work_root / "step1_worker.sbatch"
    atomic_write_text(sb_path, sbatch)
    print(f"Wrote: {sb_path}")
    # Merge script (dependent)
    merge_sh = work_root / "step1_merge.sh"
    atomic_write_text(merge_sh, f"#!/bin/bash
set -euo pipefail
scifi-demux step1 merge --library {library} --work-root {work_root}
")
    os.chmod(merge_sh, 0o755)
    print(f"Wrote: {merge_sh}")
    if submit:
        # Submit array, then submit merge with dependency
        out = subprocess.check_output(["sbatch", str(sb_path)])
        job_id = out.decode().strip().split()[-1]
        print(f"Submitted array job: {job_id}")
        dep = f"afterok:{job_id}"
        out2 = subprocess.check_output(["sbatch", "--dependency", dep, "--job-name", f"merge-{library}", "--output", f"_logs/merge-{library}-%j.out", str(merge_sh)])
        print(out2.decode().strip())
    else:
        print(f"To run: sbatch {sb_path}")
        print(f"Then, after it completes: {merge_sh}")
