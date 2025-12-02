from __future__ import annotations
import os
from pathlib import Path
from typing import Optional, List
import typer
from rich.table import Table
from rich.console import Console

from scifi_demux.utils.logging import setup_logging

from scifi_demux.utils.state import (
    STATE_PATH_DEFAULT,
    load_state,
    save_state,
    ensure_state,
    add_or_get_task,
    iter_tasks,
)
from scifi_demux.steps.step1 import (
    plan_chunks,
    run_step1_local,
    run_step1_hpc,
    worker_chunk,
    report_missing_chunks,
    merge_library,
)

app = typer.Typer(add_completion=False, help="scifi‑ATAC: Step 1 (demux) and Step 2 (map+clean) with resume")
console = Console()

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
    wait_and_maybe_merge,
)

app = typer.Typer(
    add_completion=False,
    help="scifi-ATAC FASTQ renaming & demultiplexing wrapper",
)
console = Console()


@app.callback()
def _main(verbose: int = typer.Option(0, "-v", count=True, help="-v/-vv for more logs")):
    setup_logging(verbose)


# ------------------------------------------------------------------------------------
# Simple/legacy commands (kept for convenience)
# ------------------------------------------------------------------------------------
@app.command()
def rename(
    fastq_dir: Path = typer.Option(
        ...,
        exists=True,
        file_okay=False,
        readable=True,
        help="Input FASTQ directory",
    ),
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
# helpers
# ------------------------------------------------------------------------------------


def _read_array_id_from_env() -> int | None:
    for var in ("SLURM_ARRAY_TASK_ID", "PBS_ARRAYID", "SGE_TASK_ID", "LSB_JOBINDEX", "ARRAY_ID"):
        v = os.environ.get(var)
        if v:
            try:
                return int(v)
            except ValueError:
                pass
    return None


def _discover_step2_fastqs_from_step1(work_root: Path, sample: str) -> List[Path]:
    """
    Given a step1 work_root and sample/library name, locate input FASTQs for step2.

    Priority:
      1) <work_root>/<sample>.fastq(.gz)
      2) <work_root>/combined/*.fastq*          (current scifi-demux v2 layout)
      3) <work_root>/sample/combined/*.fastq*   (fallback for older layouts)
    """
    # Direct files in work_root (rare, but keep the option)
    direct = work_root / f"{sample}.fastq"
    direct_gz = work_root / f"{sample}.fastq.gz"

    if direct.exists():
        return [direct]
    if direct_gz.exists():
        return [direct_gz]

    tried_dirs: List[str] = []

    # v2: combined directly under work_root
    combined_v2 = work_root / "combined"
    if combined_v2.exists():
        fastqs = sorted(combined_v2.glob("*.fastq*"))
        if fastqs:
            return fastqs
        tried_dirs.append(str(combined_v2))

    # fallback: older layout with sample/combined
    combined_legacy = work_root / "sample" / "combined"
    if combined_legacy.exists():
        fastqs = sorted(combined_legacy.glob("*.fastq*"))
        if fastqs:
            return fastqs
        tried_dirs.append(str(combined_legacy))

    if not tried_dirs:
        tried_dirs = [str(combined_v2), str(combined_legacy)]

    raise FileNotFoundError(
        f"Could not find FASTQs for sample={sample}. "
        f"Tried {direct}, {direct_gz}, and combined dirs: {', '.join(tried_dirs)}"
    )


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
    work_root: Optional[Path] = typer.Option(
        None,
        help="Work directory; defaults to '<library>_work'",
    ),
):
    # default to "<library>_work" if not provided
    if work_root is None:
        work_root = Path(f"{library}_work")

    work_root.mkdir(parents=True, exist_ok=True)
    plan = plan_chunks(raw_dir=raw_dir, library=library, work_root=work_root, chunks=chunks)
    typer.echo(str(plan))


def _validate_layout(value: str) -> str:
    # Accept the sentinel keyword
    if value == "builtin":
        return "builtin"
    # Otherwise require an existing file
    p = Path(value)
    if not p.exists() or not p.is_file():
        raise typer.BadParameter(f"--layout must be 'builtin' or a readable file. Not found: {value}")
    return str(p.resolve())


@step1_app.command("run")
def step1_run(
    library: str = typer.Option(..., help="Library / FASTQ prefix"),
    raw_dir: Path = typer.Option(
        ...,
        exists=True,
        file_okay=False,
        dir_okay=True,
        help="Dir with {library}_R[1,2,3].fastq.gz",
    ),
    design: Optional[Path] = typer.Option(
        None,
        help="PlateDesign_*.txt; omit for per-well outputs",
    ),
    layout: str = typer.Option(
        "builtin",
        callback=_validate_layout,  # accepts 'builtin' or resolves a readable file path
        help="Tn5 layout: 'builtin' (sci-fi-ATAC default) or path to a layout file",
        show_default=True,
    ),
    mode: str = typer.Option("local", help="local|hpc"),
    threads: int = typer.Option(
        8,
        help="LOCAL: parallel jobs; HPC: default chunk count if --chunks omitted",
    ),
    chunks: Optional[int] = typer.Option(
        None,
        help="HPC: total chunks (defaults to --threads)",
    ),
    follow: bool = typer.Option(
        False,
        help="LOCAL: merge/QC after fan-out; HPC: follow handled by array merge task",
    ),
    poll_interval: int = typer.Option(60, help="Seconds between progress checks"),
    max_wait: str = typer.Option(
        "auto",
        help="Max wait (e.g., 12h, 3600s). 'auto'=scheduler timelimit; 0=unlimited",
    ),
    work_root: Optional[Path] = typer.Option(
        None,
        help="Work directory; defaults to '<library>_work'",
    ),
):
    setup_logging(1)

    # normalize work_root
    if work_root is None:
        work_root = Path(f"{library}_work")
    work_root.mkdir(parents=True, exist_ok=True)

    # validate optional inputs
    if design is not None and not design.exists():
        raise typer.BadParameter(f"--design not found: {design}")

    # LOCAL mode: use run_step1_local (GNU parallel fan-out)
    if mode == "local":
        total_chunks = chunks if chunks is not None else max(1, threads)
        run_step1_local(
            library=library,
            raw_dir=raw_dir,
            design=design,
            layout=layout,
            chunks=total_chunks,
            parallel_jobs=threads,
        )
        if follow:
            wait_and_maybe_merge(
                library=library,
                work_root=Path(f"{library}_work"),
                poll_interval=poll_interval,
                max_wait=max_wait,
            )
        return

    # HPC array mode
    total_chunks = chunks if chunks is not None else max(1, threads)
    if total_chunks < 1:
        raise typer.BadParameter("--chunks/--threads must be >= 1")

    plan_path = work_root / "run_plan.step1.chunks.tsv"
    if not plan_path.exists():
        plan_path = plan_chunks(
            raw_dir=raw_dir,
            library=library,
            work_root=work_root,
            chunks=total_chunks,
        )
    else:
        plan_path = plan_path.resolve()

    array_id = _read_array_id_from_env()  # 1-based SLURM_ARRAY_TASK_ID expected
    if array_id is None:
        typer.echo("[warn] No array env detected; running all chunks serially then merging.")
        for idx in range(1, total_chunks + 1):
            worker_chunk(plan=plan_path, idx=idx, layout=layout, design=design, mode="hpc")
        wait_and_maybe_merge(
            library=library,
            work_root=work_root,
            poll_interval=poll_interval,
            max_wait=max_wait,
        )
        return

    # Roles: 1..N workers, N+1 merge/QC
    if 1 <= array_id <= total_chunks:
        worker_chunk(plan=plan_path, idx=array_id, layout=layout, design=design, mode="hpc")
        return
    if array_id == total_chunks + 1:
        wait_and_maybe_merge(
            library=library,
            work_root=work_root,
            poll_interval=poll_interval,
            max_wait=max_wait,
        )
        return

    raise typer.BadParameter(
        f"Array index {array_id} out of range for chunks={total_chunks}. "
        f"Submit as --array=1-{total_chunks+1} so the last task merges.",
    )


@step1_app.command("worker-chunk")
def step1_worker_chunk(
    plan: Path = typer.Option(..., exists=True, help="run_plan.step1.chunks.tsv"),
    array_id: int = typer.Option(
        -1,
        help="1-based row index; if -1, read from env (SLURM/PBS/SGE/LSF)",
    ),
    layout: Optional[str] = typer.Option(None, help="Path or 'builtin' (default)"),
    design: Optional[Path] = typer.Option(None),
    mode: str = typer.Option("local", help="local|hpc (affects threading policy)"),
):
    if array_id < 0:
        env_id = _read_array_id_from_env()
        if env_id is None:
            raise typer.BadParameter("array_id not provided and no known ARRAY env var found")
        array_id = env_id
    if array_id < 1:
        raise typer.BadParameter("array_id must be >= 1")
    worker_chunk(plan=plan, idx=array_id, layout=layout, design=design, mode=mode)


@step1_app.command("check")
def step1_check(work_root: Path = typer.Option(..., help="<LIB>_work directory")):
    missing = report_missing_chunks(work_root)
    if missing:
        console.print(
            "[red]Missing demux sentinels for chunks[/]: "
            + ", ".join(map(str, missing))
        )
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


@step1_app.command("report")
def step1_report(
    library: str = typer.Option(...),
    work_root: Path = typer.Option(..., exists=True),
):
    # re-aggregate without re-merging
    from scifi_demux.steps.step1 import _aggregate_counts_only

    _aggregate_counts_only(library=library, work_root=work_root)
    console.print(f"[bold green]Wrote[/] counts to {work_root}/qc/summary")


# ------------------------------------------------------------------------------------
# Step 2 (Map + Clean)
# ------------------------------------------------------------------------------------
step2_app = typer.Typer(help="Step 2: genome index resolve → map → clean (8 sub-steps)")
app.add_typer(step2_app, name="step2")


@step2_app.command("plan")
def step2_plan(
    genome_map: Path = typer.Option(
        ...,
        exists=True,
        help="TSV: sample_base, target_genome, ref_path[, sample_path]",
    ),
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
                raise typer.BadParameter(f"Bad line (expect ≥3 cols): {ln}")
            group, genome, ref_path = cols[0], cols[1], cols[2]
            sample_path = cols[3] if len(cols) > 3 else None

            task_id = f"step2:group:{group}:genome:{genome}"
            task = add_or_get_task(
                s,
                task_id,
                kind="step2",
                group=group,
                genome=genome,
            )
            step_keys = ["index", "map"] + [f"clean_{i}" for i in range(1, 9)]
            for k in step_keys:
                task.setdefault("steps", {}).setdefault(k, {"status": "pending"})

            params = task.setdefault("params", {})
            params["ref_path"] = ref_path
            if sample_path is not None:
                params["sample_path"] = sample_path

            # keep all columns in the plan file
            if sample_path is not None:
                lines.append(f"{group}\t{genome}\t{ref_path}\t{sample_path}")
            else:
                lines.append(f"{group}\t{genome}\t{ref_path}")
    save_state(s, state)
    plan_path = genome_map.parent / "run_plan.map.tsv"
    plan_path.write_text("\n".join(lines) + "\n")
    console.print(f"[bold]Planned[/]: {len(lines)} mapping rows → {plan_path}")


@step2_app.command("run")
def step2_run(
    mode: str = typer.Option("local", help="local|hpc"),
    threads_per_task: int = typer.Option(24, min=1),
    outdir: Path = typer.Option(..., help="Output directory"),
    state: Path = typer.Option(STATE_PATH_DEFAULT),
    dry_run: bool = typer.Option(True),
    sample: str = typer.Option(..., help="Sample/library name for FASTQ discovery"),
    from_step1_work_root: Optional[Path] = typer.Option(
        None,
        "--from-step1-work-root",
        exists=True,
        file_okay=False,
        dir_okay=True,
        help=(
            "Path to the step1 work_root directory for this library. "
            "If provided, FASTQs will be auto-discovered for `sample`."
        ),
    ),
    mapq_min: int = typer.Option(20, help="Minimum MAPQ to keep (default: 20)"),
):
    """
    Step 2 orchestrator (currently stub):

      - Lists pending step2 mapping tasks from the state file.
      - Optionally discovers input FASTQs produced by step1 for a given sample
        when --from-step1-work-root is supplied, using:
          1) <work_root>/<sample>.fastq(.gz)
          2) <work_root>/combined/*.fastq*          (v2 layout)
          3) <work_root>/sample/combined/*.fastq*   (legacy layout)

    The actual mapping/cleaning execution will be wired to steps/step2.py
    in a later iteration. The `mapq_min` option is accepted now so the CLI
    is stable once the core implementation is added.
    """
    # Discover FASTQs from step1 workspace if requested
    if from_step1_work_root is not None:
        fastqs = _discover_step2_fastqs_from_step1(
            work_root=from_step1_work_root,
            sample=sample,
        )
        console.print(
            f"[bold]Discovered[/] {len(fastqs)} FASTQ(s) for sample='{sample}' "
            f"from step1 work_root={from_step1_work_root}"
        )
        for fq in fastqs:
            console.print(f"  - {fq}")

    s = ensure_state(state)
    pending = [
        t
        for t in iter_tasks(s)
        if t.get("kind") == "step2"
        and t.get("steps", {}).get("map", {}).get("status") != "done"
    ]
    console.print(
        f"[bold]Step 2[/] mode={mode} threads={threads_per_task} "
        f"dry_run={dry_run} outdir={outdir} mapq_min={mapq_min}"
    )
    console.print(f"Pending mapping tasks: {len(pending)}")
    if dry_run:
        console.print("[yellow]Dry-run mode: showing first 10 pending tasks[/]")
        for t in pending[:10]:
            console.print(
                f" - {t['id']} "
                f"ref={t.get('params', {}).get('ref_path', '?')}"
            )
        if len(pending) > 10:
            console.print(f" - ... and {len(pending) - 10} more tasks")
        console.print("[yellow]Use --dry-run False to execute these tasks (once wired).[/]")
    else:
        console.print(
            "[red]Execution not yet implemented[/]: "
            "mapping/cleaning will be wired to steps/step2.py "
            "run_step2_for_sample_genome() in a later revision."
        )

@app.callback()
def _main(verbose: int = typer.Option(0, "-v", count=True, help="-v/-vv for more logs")):
    setup_logging(verbose)

# -----------------------------
# Status
# -----------------------------
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

# -----------------------------
# Step 1 (Demux) unified runner + worker
# -----------------------------
step1_app = typer.Typer(help="Step 1: UMI → cutadapt → demux (chunk worker), then merge")
app.add_typer(step1_app, name="step1")


@step1_app.command("plan")
def step1_plan(
    library: str = typer.Option(...),
    raw_dir: Path = typer.Option(..., exists=True, file_okay=False),
    chunks: int = typer.Option(..., help="Number of chunks to split into"),
    work_root: Path = typer.Option(None, help="Work directory; defaults to '<library>_work'"),
):
    # default to "<library>_work" if not provided
    if work_root is None:
        work_root = Path(f"{library}_work")

    work_root.mkdir(parents=True, exist_ok=True)

    plan = plan_chunks(
        raw_dir=raw_dir,
        library=library,
        work_root=work_root,
        chunks=chunks,
    )

    # print absolute path for robustness in shell scripts
    typer.echo(str(plan.resolve()))

    
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
    follow: bool = typer.Option(False, help="HPC: poll for completion; when all chunks finish, merge and run QC"),
    poll_interval: int = typer.Option(60, help="HPC: seconds between progress checks (default: 60)"),
    max_wait: str = typer.Option("auto", help="HPC: maximum wait time (e.g., 12h, 3600s). 'auto' = use scheduler job time if detectable; 0 = unlimited"),
):
    """Run Step 1 with internal planning.

    LOCAL: split into `threads` chunks, run workers via GNU parallel, then merge.
    HPC:   plan only; print worker command. With --follow, poll sentinels and merge when complete.
    """
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


# -----------------------------
# Step 1: single chunk worker (UMI → cutadapt → demux)
# -----------------------------
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



# -----------------------------
# Step 2 (Map+Clean)
# -----------------------------
step2_app = typer.Typer(help="Step 2: genome index resolve → map → clean (8 sub-steps)")
app.add_typer(step2_app, name="step2")

@step2_app.command("plan")
def step2_plan(
    genome_map: Path = typer.Option(..., exists=True, help="TSV: sample_base, target_genome, ref_path"),
    state: Path = typer.Option(STATE_PATH_DEFAULT),
):
    """
    Plan and initialize step2 mapping tasks based on a genome mapping file.
    
    This command reads a TSV file specifying sample groups, target genomes, and reference paths,
    then creates corresponding tasks in the state management system for downstream processing.
    
    Args:
        genome_map: Path to TSV file with columns: sample_group, target_genome, reference_path
        state: Path to state file for tracking task progress (default: ./state.json)
    
    Raises:
        typer.BadParameter: If any line in genome_map doesn't have exactly 3 columns
        FileNotFoundError: If genome_map file doesn't exist
    """
    # Load or initialize the state tracking object
    s = ensure_state(state)
    
    # Store cleaned lines to create a run plan file
    lines: List[str] = []
    
    # Read and parse the genome mapping TSV file
    with open(genome_map) as fh:
        for ln in fh:
            # Clean and skip empty or comment lines
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            
            # Handle both space and tab separation (prioritize tabs if present)
            cols = ln.split() if "	" not in ln else ln.split("	")
            
            # Validate column count
            if len(cols) < 3:
                raise typer.BadParameter(f"Bad line (expect 3 cols): {ln}")
            
            # Extract the three required columns
            group, genome, ref_path = cols[0], cols[1], cols[2]
            
            # Create a unique task identifier for this group-genome combination
            task_id = f"step2:group:{group}:genome:{genome}"
            
            # Get existing task or create new one in the state system
            task = add_or_get_task(s, task_id, kind="step2", group=group, genome=genome)
            
            # Define the canonical 10 processing steps for this task:
            # 1. index - Create reference index
            # 2. map - Map reads to reference
            # 3. clean_1 through clean_8 - Various cleaning/processing steps
            step_keys = ["index", "map"] + [f"clean_{i}" for i in range(1, 9)]
            
            # Initialize all steps with 'pending' status if they don't exist
            for k in step_keys:
                task.setdefault("steps", {}).setdefault(k, {"status": "pending"})
            
            # Store the reference path in task parameters
            task.setdefault("params", {})["ref_path"] = ref_path
            
            # Store the cleaned line for output plan file
            lines.append(f"{group}	{genome}	{ref_path}")
    
    # Save the updated state with all new tasks
    save_state(s, state)
    
    # Create a run plan file in the same directory as the input genome_map
    plan_path = genome_map.parent / "run_plan.map.tsv"
    plan_path.write_text("\n".join(lines) + "\n")
    
    # Print success message with formatted output
    console.print(f"[bold]Planned[/]: {len(lines)} mapping rows → {plan_path}")

@step2_app.command("run")
def step2_run(
    mode: str = typer.Option("local", help="local|hpc"),
    threads_per_task: int = typer.Option(24, min=1),
    state: Path = typer.Option(STATE_PATH_DEFAULT),
    dry_run: bool = typer.Option(True),
):
    """
    Execute step2 mapping tasks that are in pending status.
    
    This command identifies pending mapping tasks and either previews them (dry-run mode)
    or executes them using the appropriate execution mode (local or HPC).
    
    Args:
        mode: Execution environment - 'local' for local machine, 'hpc' for cluster/slurm
        threads_per_task: Number of CPU threads to allocate per mapping task
        state: Path to state file tracking task progress
        dry_run: If True, only preview tasks without execution; if False, execute tasks
    
    Notes:
        Currently in development phase - dry_run=True by default for safety
    """
    # Load the state tracking object
    s = ensure_state(state)
    
    # Find all step2 tasks where the 'map' step is not marked as 'done'
    pending = [
        t for t in iter_tasks(s) 
        if t.get("kind") == "step2" 
        and t.get("steps", {}).get("map", {}).get("status") != "done"
    ]
    
    # Print execution configuration summary
    console.print(f"[bold]Step 2[/] mode={mode} threads={threads_per_task} dry_run={dry_run}")
    console.print(f"Pending mapping tasks: {len(pending)}")
    
    if dry_run:
        # Dry-run mode: Preview what would be executed without actually running
        console.print("[yellow]Dry-run mode: showing first 10 pending tasks[/]")
        
        # Display details for first 10 pending tasks (to avoid overwhelming output)
        for t in pending[:10]:
            console.print(f" - {t['id']} ref={t.get('params',{}).get('ref_path','?')}")
        
        # Inform user how to proceed with actual execution
        if len(pending) > 10:
            console.print(f" - ... and {len(pending) - 10} more tasks")
        console.print("[yellow]Use --dry-run False to execute these tasks[/]")
        
    else:
        # Execution mode: Actually run the tasks
        console.print("Execution wiring will call into steps/step2.py (to be filled next)")
        
        # Future implementation would include:
        # - For 'local' mode: Direct execution using subprocess or multiprocessing
        # - For 'hpc' mode: Generate and submit SLURM/sbatch jobs
        # - Update task status to 'running' and then 'done' upon completion
        # - Error handling and retry logic for failed tasks

@app.command()
def multiqc(outdir: Path = typer.Option(Path("qc/report"), help="MultiQC output dir")):
    from subprocess import run
    outdir.mkdir(parents=True, exist_ok=True)
    cmd = ["multiqc", "--config", "qc/multiqc_scifi.yaml", "--outdir", str(outdir), "."]
    console.print("Running: " + " ".join(cmd))
    run(cmd, check=False)

if __name__ == "__main__":
    app()
