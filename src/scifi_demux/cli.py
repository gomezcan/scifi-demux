# src/scifi_demux/cli.py
from __future__ import annotations
import os
from pathlib import Path
from typing import Optional, List
import typer
from rich.table import Table
from rich.console import Console

# logging
try:
    from .utils.logging import setup_logging  # new location
except ImportError:
    from .logging_utils import setup_logging  # fallback (older layout)

# legacy simple commands
from .io_utils import find_fastqs, ensure_dir
from .renaming import plan_renames, apply_renames
from .demux import run_demux

# state + step1/step2
from .utils.state import (
    STATE_PATH_DEFAULT,
    load_state,
    save_state,
    ensure_state,
    add_or_get_task,
    iter_tasks,
)
from .steps.step1 import (
    plan_chunks,
    run_step1_local,
    run_step1_hpc,
    worker_chunk,
    report_missing_chunks,
    merge_library,
)

app = typer.Typer(add_completion=False, help="scifi-ATAC FASTQ renaming & demultiplexing wrapper")
console = Console()


@app.callback()
def _main(verbose: int = typer.Option(0, "-v", count=True, help="-v/-vv for more logs")):
    setup_logging(verbose)


# ------------------------------------------------------------------------------------
# Simple/legacy commands (kept for convenience)
# ------------------------------------------------------------------------------------
@app.command()
def rename(
    fastq_dir: Path = typer.Option(..., exists=True, file_okay=False, readable=True, help="Input FASTQ directory"),
    plate: str = typer.Option(..., help="Plate identifier, e.g., PlateA"),
    well: str = typer.Option(..., help="Well identifier, e.g., A01"),
    out: Path = typer.Option(..., help="Output directory for renamed files"),
    scheme: str = typer.Option("{plate}_{well}_{read}{ext}", help="Filename scheme"),
    mode: str = typer.Option("link", help="'link' (symlink) or 'copy'"),
    dry_run: bool = typer.Option(False, help="Print actions without writing"),
):
    fastqs = find_fastqs(fastq_dir)
    plans = plan_renames(fastqs, scheme=scheme, plate=plate, well=well)
    ensure_dir(out)
    plans = [p.__class__(p.src, out / p.dst.name) for p in plans]
    apply_renames(plans, mode=mode, dry_run=dry_run)


demux_app = typer.Typer(help="Demultiplex FASTQs using different strategies")
app.add_typer(demux_app, name="demux")


@demux_app.command("wells-by-plate")
def demux_wells_by_plate(
    fastq_dir: Path = typer.Option(..., exists=True, dir_okay=True),
    plate_map: Path = typer.Option(..., exists=True, help="YAML with plates.wells"),
    out: Path = typer.Option(...),
    mode: str = typer.Option("link"),
    dry_run: bool = typer.Option(False),
):
    run_demux("wells-by-plate", fastq_dir, plate_map, out, mode=mode, dry_run=dry_run)


@demux_app.command("sample-design")
def demux_sample_design(
    fastq_dir: Path = typer.Option(..., exists=True, dir_okay=True),
    design: Path = typer.Option(..., exists=True, help="YAML with samples.wells mapping"),
    out: Path = typer.Option(...),
    mode: str = typer.Option("link"),
    dry_run: bool = typer.Option(False),
):
    run_demux("sample-design", fastq_dir, design, out, mode=mode, dry_run=dry_run)


# ------------------------------------------------------------------------------------
# Status
# ------------------------------------------------------------------------------------
@app.command()
def status(state: Path = typer.Option(STATE_PATH_DEFAULT, help="Path to pipeline state JSON")):
    s = load_state(state)
    table = Table(title="scifi-demux status")
    table.add_column("ID", overflow="fold")
    table.add_column("Kind")
    table.add_column("Info", overflow="fold")
    table.add_column("Progress")
    for t in iter_tasks(s):
        kind = t.get("kind", "?")
        if kind == "step1":
            info = f"library={t.get('library')}"
            steps = t.get("steps", {})
        else:
            info = f"group={t.get('group')} genome={t.get('genome')}"
            steps = t.get("steps", {})
        done = sum(1 for v in steps.values() if v.get("status") == "done")
        total = max(len(steps), 1)
        table.add_row(t.get("id", "?"), kind, info, f"{done}/{total}")
    console.print(table)


# ------------------------------------------------------------------------------------
# Step 1 (UMI → cutadapt → demux → merge)
# ------------------------------------------------------------------------------------
step1_app = typer.Typer(help="Step 1: UMI → cutadapt → demux (chunk worker), then merge")
app.add_typer(step1_app, name="step1")


@step1_app.command("plan")
def step1_plan(
    library: str = typer.Option(...),
    raw_dir: Path = typer.Option(..., exists=True, file_okay=False),
    chunks: int = typer.Option(..., help="Number of chunks to split into"),
):
    work_root = Path(f"{library}_work")
    plan = plan_chunks(raw_dir=raw_dir, library=library, work_root=work_root, chunks=chunks)
    typer.echo(str(plan))


@step1_app.command("run")
def step1_run(
    library: str = typer.Option(..., help="Library / FASTQ prefix"),
    raw_dir: Path = typer.Option(Path("."), help="Dir with {lib}_R1.fastq.gz & {lib}_R3.fastq.gz"),
    design: Optional[Path] = typer.Option(None, help="PlateDesign_*.txt; omit for per-well outputs"),
    layout: str = typer.Option("builtin", help="Tn5 layout file or 'builtin'"),
    mode: str = typer.Option("local", help="local|hpc"),
    # local fan-out
    threads: int = typer.Option(8, help="LOCAL: number of chunks & parallel workers"),
    # hpc planning/following
    chunks: Optional[int] = typer.Option(None, help="HPC: total chunks (defaults to --threads if omitted)"),
    follow: bool = typer.Option(False, help="HPC: poll for completion and auto-merge when done (no job submission)"),
    poll_interval: int = typer.Option(60, help="HPC: seconds between progress checks (default: 60)"),
    max_wait: str = typer.Option("auto", help="HPC: maximum wait time (e.g., 12h, 3600s). 'auto' = use scheduler job time if detectable; 0 = unlimited"),
):
    setup_logging(1)
    if mode == "local":
        return run_step1_local(
            library=library,
            raw_dir=raw_dir,
            design=design,
            layout=layout,
            chunks=threads,
            parallel_jobs=threads,
        )
    elif mode == "hpc":
        if chunks is None:
            chunks = threads
        plan_path = run_step1_hpc(
            library=library,
            raw_dir=raw_dir,
            design=design,
            layout=layout,
            chunks=chunks,
            follow=follow,
            poll_interval=poll_interval,
            max_wait=max_wait,
        )
        console.print(f"[bold]Planned[/]: {plan_path}")
        console.print(
            "Launch your array jobs separately. Each task runs:\n  "
            f"scifi-demux step1 worker-chunk --plan {plan_path} --mode hpc"
            + (f" --design {design}" if design else "")
        )
    else:
        raise typer.BadParameter("mode must be 'local' or 'hpc'")


@step1_app.command("worker-chunk")
def step1_worker_chunk(
    plan: Path = typer.Option(..., exists=True, help="run_plan.step1.chunks.tsv"),
    array_id: int = typer.Option(-1, help="1-based row index; if -1, read from env (SLURM/PBS/SGE/LSF)"),
    layout: Optional[str] = typer.Option(None, help="Path or 'builtin' (default)"),
    design: Optional[Path] = typer.Option(None),
    mode: str = typer.Option("local", help="local|hpc (affects threading policy)"),
):
    if array_id < 0:
        for var in ("SLURM_ARRAY_TASK_ID", "PBS_ARRAYID", "SGE_TASK_ID", "LSB_JOBINDEX", "ARRAY_ID"):
            if var in os.environ:
                array_id = int(os.environ[var]); break
    if array_id < 1:
        raise typer.BadParameter("array_id not provided and no known ARRAY env var found")
    worker_chunk(plan=plan, idx=array_id, layout=layout, design=design, mode=mode)


@step1_app.command("check")
def step1_check(work_root: Path = typer.Option(..., help="<LIB>_work directory")):
    missing = report_missing_chunks(work_root)
    if missing:
        console.print(f"[red]Missing demux sentinels for chunks[/]: {', '.join(map(str, missing))}")
        raise typer.Exit(1)
    console.print("[green]All chunk demux sentinels present. Safe to merge.")


@step1_app.command("merge")
def step1_merge(
    library: str = typer.Option(..., help="Library / FASTQ prefix"),
    work_root: Path = typer.Option(..., exists=True, help="<LIB>_work directory"),
):
    merge_library(library=library, work_root=work_root)


@step1_app.command("missing-indices")
def step1_missing_indices(work_root: Path = typer.Option(..., help="<LIB>_work directory")):
    missing = report_missing_chunks(work_root)
    if not missing:
        return
    typer.echo(",".join(str(i) for i in missing))


# ------------------------------------------------------------------------------------
# Step 2 (Map + Clean) — placeholders wired to state
# ------------------------------------------------------------------------------------
step2_app = typer.Typer(help="Step 2: genome index resolve → map → clean (8 sub-steps)")
app.add_typer(step2_app, name="step2")


@step2_app.command("plan")
def step2_plan(
    genome_map: Path = typer.Option(..., exists=True, help="TSV: sample_base, target_genome, ref_path"),
    state: Path = typer.Option(STATE_PATH_DEFAULT),
):
    s = ensure_state(state)
    lines: List[str] = []
    with open(genome_map) as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            cols = ln.split("\t") if "\t" in ln else ln.split()
            if len(cols) < 3:
                raise typer.BadParameter(f"Bad line (expect 3 cols): {ln}")
            group, genome, ref_path = cols[0], cols[1], cols[2]
            task_id = f"step2:group:{group}:genome:{genome}"
            task = add_or_get_task(s, task_id, kind="step2", group=group, genome=genome)
            step_keys = ["index", "map"] + [f"clean_{i}" for i in range(1, 9)]
            for k in step_keys:
                task.setdefault("steps", {}).setdefault(k, {"status": "pending"})
            task.setdefault("params", {})["ref_path"] = ref_path
            lines.append(f"{group}\t{genome}\t{ref_path}")
    save_state(s, state)
    plan_path = genome_map.parent / "run_plan.map.tsv"
    plan_path.write_text("\n".join(lines) + "\n")
    console.print(f"[bold]Planned[/]: {len(lines)} mapping rows → {plan_path}")


@step2_app.command("run")
def step2_run(
    mode: str = typer.Option("local", help="local|hpc"),
    threads_per_task: int = typer.Option(24, min=1),
    state: Path = typer.Option(STATE_PATH_DEFAULT),
    dry_run: bool = typer.Option(True),
):
    s = ensure_state(state)
    pending = [
        t for t in iter_tasks(s)
        if t.get("kind") == "step2"
        and t.get("steps", {}).get("map", {}).get("status") != "done"
    ]
    console.print(f"[bold]Step 2[/] mode={mode} threads={threads_per_task} dry_run={dry_run}")
    console.print(f"Pending mapping tasks: {len(pending)}")
    if dry_run:
        console.print("[yellow]Dry-run mode: showing first 10 pending tasks[/]")
        for t in pending[:10]:
            console.print(f" - {t['id']} ref={t.get('params',{}).get('ref_path','?')}")
        if len(pending) > 10:
            console.print(f" - ... and {len(pending) - 10} more tasks")
        console.print("[yellow]Use --dry-run False to execute these tasks[/]")
    else:
        console.print("Execution wiring will call into steps/step2.py (to be filled next)")
