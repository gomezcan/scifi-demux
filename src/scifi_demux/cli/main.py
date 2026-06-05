# src/scifi_demux/cli/main.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, List

import typer
from rich.console import Console
from rich.table import Table

# logging
try:
    from scifi_demux.utils.logging import setup_logging  # new location
except ImportError:
    from scifi_demux.logging_utils import setup_logging  # fallback (older layout)

# legacy simple commands
from scifi_demux.io_utils import find_fastqs, ensure_dir, resolve_whitelist, resolve_tn5_bcs
from scifi_demux.renaming import plan_renames, apply_renames
from scifi_demux.config import Design
from scifi_demux.strategies import demux_by_wells, demux_by_samples


# state + step1/step2
from scifi_demux.utils.state import (
    STATE_PATH_DEFAULT,
    load_state,
    save_state,
    ensure_state,
    add_or_get_task,
    mark_task_step,
    iter_tasks,
)
from scifi_demux.steps.step1 import (
    plan_chunks,
    run_step1_local,
    worker_chunk,
    report_missing_chunks,
    merge_library,
    wait_and_maybe_merge,
)
from scifi_demux.steps.step2 import (
    run_step2_for_sample_genome,  # reserved for future wiring
)

app = typer.Typer(
    add_completion=False,
    help="scifi-ATAC FASTQ renaming, demultiplexing, mapping, and cleanup",
)
console = Console()


# ------------------------------------------------------------------------------------
# Common helpers
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
      1)   <work_root>/<sample>.fastq(.gz)
      1.5) <work_root>/*.fastq*                  (Phase 2 merged layout)
      2)   <work_root>/combined/*.fastq*          (current scifi-demux v2 layout)
      3)   <work_root>/sample/combined/*.fastq*   (fallback for older layouts)
    """
    direct = work_root / f"{sample}.fastq"
    direct_gz = work_root / f"{sample}.fastq.gz"

    if direct.exists():
        return [direct]
    if direct_gz.exists():
        return [direct_gz]

    # Phase 2 merged layout: FASTQs directly in work_root
    flat_fqs = sorted(work_root.glob("*.fastq*"))
    if flat_fqs:
        return flat_fqs

    tried_dirs: List[str] = [str(work_root)]

    combined_v2 = work_root / "combined"
    if combined_v2.exists():
        fastqs = sorted(combined_v2.glob("*.fastq*"))
        if fastqs:
            return fastqs
        tried_dirs.append(str(combined_v2))

    combined_legacy = work_root / "sample" / "combined"
    if combined_legacy.exists():
        fastqs = sorted(combined_legacy.glob("*.fastq*"))
        if fastqs:
            return fastqs
        tried_dirs.append(str(combined_legacy))

    if len(tried_dirs) == 1:
        tried_dirs.extend([str(combined_v2), str(combined_legacy)])

    raise FileNotFoundError(
        f"Could not find FASTQs for sample={sample}. "
        f"Tried {direct}, {direct_gz}, and dirs: {', '.join(tried_dirs)}"
    )


def _load_plan_row(plan_path: Path, row_idx: int) -> dict:
    """Read 1-indexed row from run_plan.map.tsv (skip comments/blanks)."""
    rows = [
        ln.strip()
        for ln in plan_path.read_text().splitlines()
        if ln.strip() and not ln.startswith("#")
    ]
    if row_idx < 1 or row_idx > len(rows):
        raise IndexError(
            f"Row index {row_idx} out of range (plan has {len(rows)} rows)"
        )
    cols = rows[row_idx - 1].split("\t")
    result = {"group": cols[0], "genome": cols[1], "ref_path": cols[2]}
    if len(cols) > 3:
        result["sample_path"] = cols[3]
    return result


def _count_plan_rows(plan_path: Path) -> int:
    """Count non-comment, non-empty lines in a plan TSV."""
    return len([
        ln for ln in plan_path.read_text().splitlines()
        if ln.strip() and not ln.startswith("#")
    ])


def _resolve_threads_hpc(mode: str, cli_threads: int) -> int:
    """In HPC mode, prefer $SLURM_CPUS_PER_TASK or $NSLOTS; fall back to cli_threads."""
    if mode == "hpc":
        env_val = os.environ.get("SLURM_CPUS_PER_TASK") or os.environ.get("NSLOTS")
        if env_val:
            try:
                return int(env_val)
            except ValueError:
                pass
    return cli_threads


# ------------------------------------------------------------------------------------
# Root callback
# ------------------------------------------------------------------------------------
@app.callback()
def _main(verbose: int = typer.Option(0, "-v", count=True, help="-v/-vv for more logs")):
    setup_logging(verbose)


# ------------------------------------------------------------------------------------
# Simple / legacy commands
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


def run_demux(
    strategy: str,
    fastq_dir: Path,
    design_yaml: Path,
    out: Path,
    mode: str = "link",
    dry_run: bool = False,
) -> None:
    """Dispatch demux to the appropriate strategy."""
    fastqs = find_fastqs(fastq_dir)
    if not fastqs:
        raise FileNotFoundError(f"No FASTQs found under {fastq_dir}")
    design = Design.from_yaml(design_yaml)
    if strategy == "wells-by-plate":
        demux_by_wells(fastqs, design, out, mode=mode, dry_run=dry_run)
    elif strategy == "sample-design":
        demux_by_samples(fastqs, design, out, mode=mode, dry_run=dry_run)
    else:
        raise ValueError(f"Unknown demux strategy: {strategy}")


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
    work_root: Optional[Path] = typer.Option(
        None,
        help="Work directory; defaults to '<library>_work'",
    ),
):
    if work_root is None:
        work_root = Path(f"{library}_work")

    work_root.mkdir(parents=True, exist_ok=True)
    plan = plan_chunks(raw_dir=raw_dir, library=library, work_root=work_root, chunks=chunks)
    typer.echo(str(plan.resolve()))


def _validate_layout(value: str) -> str:
    if value == "builtin":
        return "builtin"
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
        callback=_validate_layout,
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

    if work_root is None:
        work_root = Path(f"{library}_work")
    work_root.mkdir(parents=True, exist_ok=True)

    if design is not None and not design.exists():
        raise typer.BadParameter(f"--design not found: {design}")

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

    array_id = _read_array_id_from_env()
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
    from scifi_demux.steps.step1 import _aggregate_counts_only

    _aggregate_counts_only(library=library, work_root=work_root)
    console.print(f"[bold green]Wrote[/] counts to {work_root}/qc/summary")


# ------------------------------------------------------------------------------------
# Step 2 (Map + Clean) – planning + stub executor
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

            if sample_path is not None:
                lines.append(f"{group}\t{genome}\t{ref_path}\t{sample_path}")
            else:
                lines.append(f"{group}\t{genome}\t{ref_path}")
    save_state(s, state)
    plan_path = genome_map.parent / "run_plan.map.tsv"
    plan_path.write_text("\n".join(lines) + "\n")
    console.print(f"[bold]Planned[/]: {len(lines)} mapping rows → {plan_path}")


def _run_step2_single_task(
    plan: Path,
    row_idx: int,
    r1_fqs: List[Path],
    r3_fqs: List[Path],
    outdir: Path,
    whitelist_10x: Path,
    whitelist_tn5: Path,
    threads: int,
    mapq_min: int,
    state_obj: dict,
    state_path: Path,
    resume: bool,
    dry_run: bool,
    picard_max_heap: str = "8g",
) -> None:
    """Run step2 for a single plan row. Updates state on completion."""
    row = _load_plan_row(plan, row_idx)
    group = row["group"]
    genome = row["genome"]
    ref = Path(row["ref_path"])
    task_id = f"step2:group:{group}:genome:{genome}"

    # Resume: skip if all steps already done
    if resume:
        for t in iter_tasks(state_obj):
            if t.get("id") == task_id:
                steps = t.get("steps", {})
                if steps and all(v.get("status") == "done" for v in steps.values()):
                    console.print(f"[dim]Skipping (done)[/] {task_id}")
                    return
                break

    # Match FASTQs for this group (exact prefix to avoid A1 matching A10)
    grp_r1 = [f for f in r1_fqs if f.name.startswith(f"{group}_")]
    grp_r3 = [f for f in r3_fqs if f.name.startswith(f"{group}_")]
    if not grp_r1 or not grp_r3:
        raise FileNotFoundError(
            f"No FASTQs found for group '{group}' in discovered FASTQs. "
            f"Available R1: {[f.name for f in r1_fqs]}, "
            f"Available R3: {[f.name for f in r3_fqs]}. "
            f"Ensure the correct --from-step1-work-root is provided."
        )
    fq_r1 = grp_r1[0]
    fq_r3 = grp_r3[0]

    console.print(
        f"[bold]Running[/] step2 row={row_idx} group={group} genome={genome} threads={threads}"
    )

    run_step2_for_sample_genome(
        sample_id=group,
        genome_target=genome,
        fq_r1=fq_r1,
        fq_r3=fq_r3,
        ref_path=ref,
        out_root=outdir,
        whitelist_10x=whitelist_10x,
        whitelist_tn5=whitelist_tn5,
        threads=threads,
        mapq_min=mapq_min,
        dry_run=dry_run,
        picard_max_heap=picard_max_heap,
    )

    # Mark all sub-steps done in state
    for t in iter_tasks(state_obj):
        if t.get("id") == task_id:
            for step_key in t.get("steps", {}):
                mark_task_step(state_obj, task_id, step_key, "done")
            break
    save_state(state_obj, state_path)
    console.print(f"[green]Completed[/] {task_id}")


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
    picard_max_heap: str = typer.Option(
        "8g",
        help=(
            "JVM max heap for Picard MarkDuplicates, e.g. '8g', '32g', '96g'. "
            "Passed as a CLI arg (NOT JAVA_TOOL_OPTIONS) so the bioconda picard "
            "wrapper's hard-coded default of -Xmx2g is correctly overridden. "
            "Bump for large pools / multi-genome scifi runs (default: 8g)."
        ),
    ),
    plan: Optional[Path] = typer.Option(
        None,
        help="Path to run_plan.map.tsv (auto-discovered if omitted)",
    ),
    resume: bool = typer.Option(
        False,
        help="Skip tasks whose state steps are already all 'done'",
    ),
):
    """
    Step 2 orchestrator: map + clean for pending tasks.

    In LOCAL mode, loops through all plan rows sequentially.
    In HPC mode, each SLURM array task processes a single row from the plan.
    """
    setup_logging(1)

    # -- Locate plan TSV -------------------------------------------------------
    if plan is None:
        for candidate in [Path("run_plan.map.tsv"), outdir / "run_plan.map.tsv"]:
            if candidate.exists():
                plan = candidate
                break
        if plan is None:
            console.print(
                "[red]Could not locate run_plan.map.tsv. "
                "Run 'step2 plan' first or pass --plan.[/]"
            )
            raise typer.Exit(1)

    total_rows = _count_plan_rows(plan)
    threads = _resolve_threads_hpc(mode, threads_per_task)

    # -- Discover FASTQs -------------------------------------------------------
    fastqs: List[Path] = []
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

    # -- Dry-run: show pending tasks and exit ----------------------------------
    if dry_run:
        s = ensure_state(state)
        pending = [
            t
            for t in iter_tasks(s)
            if t.get("kind") == "step2"
            and t.get("steps", {}).get("map", {}).get("status") != "done"
        ]
        console.print(
            f"[bold]Step 2[/] mode={mode} threads={threads} "
            f"dry_run={dry_run} outdir={outdir} mapq_min={mapq_min}"
        )
        console.print(f"Plan rows: {total_rows}, Pending tasks: {len(pending)}")
        console.print("[yellow]Dry-run mode: showing first 10 pending tasks[/]")
        for t in pending[:10]:
            console.print(
                f" - {t['id']} "
                f"ref={t.get('params', {}).get('ref_path', '?')}"
            )
        if len(pending) > 10:
            console.print(f" - ... and {len(pending) - 10} more tasks")
        console.print("[yellow]Use --dry-run False to execute.[/]")
        return

    # -- Execution: validate FASTQs --------------------------------------------
    if from_step1_work_root is None:
        console.print("[red]--from-step1-work-root is required for execution mode[/]")
        raise typer.Exit(1)

    whitelist_10x = resolve_whitelist(None)
    whitelist_tn5 = resolve_tn5_bcs(None)

    r1_fqs = sorted(f for f in fastqs if "_R1" in f.name)
    r3_fqs = sorted(f for f in fastqs if "_R3" in f.name)
    if not r1_fqs or not r3_fqs:
        console.print("[red]Could not find R1/R3 FASTQ pairs[/]")
        raise typer.Exit(1)

    s = ensure_state(state)

    # -- Common kwargs for _run_step2_single_task ------------------------------
    task_kw = dict(
        plan=plan, r1_fqs=r1_fqs, r3_fqs=r3_fqs, outdir=outdir,
        whitelist_10x=whitelist_10x, whitelist_tn5=whitelist_tn5,
        threads=threads, mapq_min=mapq_min, state_obj=s,
        state_path=state, resume=resume, dry_run=False,
        picard_max_heap=picard_max_heap,
    )

    # -- HPC dispatch ----------------------------------------------------------
    if mode == "hpc":
        array_id = _read_array_id_from_env()

        if array_id is None:
            console.print(
                "[warn] No array env detected; running all plan rows serially."
            )
            for row_idx in range(1, total_rows + 1):
                _run_step2_single_task(row_idx=row_idx, **task_kw)
            return

        if 1 <= array_id <= total_rows:
            _run_step2_single_task(row_idx=array_id, **task_kw)
            return

        raise typer.BadParameter(
            f"Array index {array_id} out of range for plan rows={total_rows}. "
            f"Submit as --array=1-{total_rows}."
        )

    # -- Local mode: run all rows serially -------------------------------------
    console.print(
        f"[bold]Step 2[/] mode={mode} threads={threads} "
        f"outdir={outdir} mapq_min={mapq_min} rows={total_rows}"
    )
    for row_idx in range(1, total_rows + 1):
        _run_step2_single_task(row_idx=row_idx, **task_kw)


@step2_app.command("sbatch")
def step2_sbatch(
    plan: Path = typer.Option(..., exists=True, help="run_plan.map.tsv"),
    sample: str = typer.Option(..., help="Sample/library name"),
    from_step1_work_root: Path = typer.Option(..., exists=True, help="Step1 work root"),
    outdir: Path = typer.Option(..., help="Output directory"),
    cpus: int = typer.Option(24, help="CPUs per task"),
    time: str = typer.Option("24:00:00", help="Wall time per task"),
    mem: str = typer.Option("16G", help="Memory per task"),
    partition: str = typer.Option("standard", help="SLURM partition"),
    state: Path = typer.Option(STATE_PATH_DEFAULT),
    mapq_min: int = typer.Option(20),
    picard_max_heap: str = typer.Option(
        "8g",
        help="JVM max heap for Picard MarkDuplicates (default: 8g). Bump for large pools.",
    ),
    job_name: str = typer.Option("step2", help="SLURM job name"),
):
    """Generate a SLURM array submission script for step2."""
    from scifi_demux.exec.hpc import render_sbatch

    total_rows = _count_plan_rows(plan)
    cmd = (
        f"scifi-demux step2 run "
        f"--mode hpc "
        f"--sample {sample} "
        f"--from-step1-work-root {from_step1_work_root.resolve()} "
        f"--outdir {outdir.resolve()} "
        f"--plan {plan.resolve()} "
        f"--state {state} "
        f"--threads-per-task {cpus} "
        f"--mapq-min {mapq_min} "
        f"--picard-max-heap {picard_max_heap} "
        f"--dry-run False "
        f"--resume"
    )
    script_path = render_sbatch(
        job_name=job_name,
        out_dir=outdir / "slurm_logs",
        array=f"1-{total_rows}",
        cpus=cpus,
        time=time,
        mem=mem,
        partition=partition,
        cmd=cmd,
    )
    console.print(f"[bold green]Wrote[/] SLURM script: {script_path}")
    console.print(f"Submit with: sbatch {script_path}")


# ------------------------------------------------------------------------------------
# MultiQC convenience wrapper
# ------------------------------------------------------------------------------------
@app.command()
def multiqc(outdir: Path = typer.Option(Path("qc/report"), help="MultiQC output dir")):
    from subprocess import run

    outdir.mkdir(parents=True, exist_ok=True)
    cmd = ["multiqc", "--config", "qc/multiqc_scifi.yaml", "--outdir", str(outdir), "."]
    console.print("Running: " + " ".join(cmd))
    run(cmd, check=False)


if __name__ == "__main__":
    app()
