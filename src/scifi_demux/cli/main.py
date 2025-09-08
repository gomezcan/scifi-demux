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
