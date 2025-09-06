from __future__ import annotations
import sys
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
    mark_task_step,
    add_or_get_task,
    iter_tasks,
)

app = typer.Typer(add_completion=False, help="scifi‑ATAC: Step 1 (demux) and Step 2 (map+clean) with resume")
console = Console()

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
# Step 1 (Demux) — minimal stub: registers a library task now; real execution wired next
# -----------------------------
step1_app = typer.Typer(help="Step 1: UMI → cutadapt → demux → merge")
app.add_typer(step1_app, name="step1")

@step1_app.command("run")
def step1_run(
    library: str = typer.Option(..., help="Library name (prefix of raw FASTQs)"),
    mode: str = typer.Option("local", help="local|hpc"),
    state: Path = typer.Option(STATE_PATH_DEFAULT),
    dry_run: bool = typer.Option(True, help="Plan only for now"),
):
    s = ensure_state(state)
    task_id = f"step1:library:{library}"
    task = add_or_get_task(s, task_id, kind="step1", library=library)
    # Register canonical step keys for progress tracking
    for key in ["umi_r1", "umi_r3", "cutadapt", "demux", "merge", "qc"]:
        task.setdefault("steps", {}).setdefault(key, {"status": "pending"})
    save_state(s, state)
    console.print(f"[bold]Registered Step 1 task[/]: {task_id} (mode={mode}, dry_run={dry_run})")
    console.print("Next: wire actual execution to your scripts in steps/step1.py")


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
    s = ensure_state(state)
    lines: List[str] = []
    with open(genome_map) as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln or ln.startswith("#"): continue
            cols = ln.split() if "	" not in ln else ln.split("	")
            if len(cols) < 3:
                raise typer.BadParameter(f"Bad line (expect 3 cols): {ln}")
            group, genome, ref_path = cols[0], cols[1], cols[2]
            task_id = f"step2:group:{group}:genome:{genome}"
            task = add_or_get_task(s, task_id, kind="step2", group=group, genome=genome)
            # Define the canonical 10 step keys: index + map + clean_1..8
            step_keys = ["index", "map"] + [f"clean_{i}" for i in range(1, 9)]
            for k in step_keys:
                task.setdefault("steps", {}).setdefault(k, {"status": "pending"})
            task.setdefault("params", {})["ref_path"] = ref_path
            lines.append(f"{group}	{genome}	{ref_path}")
    save_state(s, state)
    plan_path = genome_map.parent / "run_plan.map.tsv"
    plan_path.write_text("
".join(lines) + "
")
    console.print(f"[bold]Planned[/]: {len(lines)} mapping rows → {plan_path}")


@step2_app.command("run")
def step2_run(
    mode: str = typer.Option("local", help="local|hpc"),
    threads_per_task: int = typer.Option(24, min=1),
    state: Path = typer.Option(STATE_PATH_DEFAULT),
    dry_run: bool = typer.Option(True),
):
    s = ensure_state(state)
    # Very initial runner: just show how many tasks are pending for 'map'
    pending = [t for t in iter_tasks(s) if t.get("kind") == "step2" and t.get("steps", {}).get("map", {}).get("status") != "done"]
    console.print(f"[bold]Step 2[/] mode={mode} threads={threads_per_task} dry_run={dry_run}")
    console.print(f"Pending mapping tasks: {len(pending)}")
    if dry_run:
        for t in pending[:10]:
            console.print(f" - {t['id']} ref={t.get('params',{}).get('ref_path','?')}")
    else:
        console.print("Execution wiring will call into steps/step2.py (to be filled next)")

@app.command()
def multiqc(outdir: Path = typer.Option(Path("qc/report"), help="MultiQC output dir")):
    from subprocess import run
    outdir.mkdir(parents=True, exist_ok=True)
    cmd = ["multiqc", "--config", "qc/multiqc_scifi.yaml", "--outdir", str(outdir), "."]
    console.print("Running: " + " ".join(cmd))
    run(cmd, check=False)

if __name__ == "__main__":
    app()
