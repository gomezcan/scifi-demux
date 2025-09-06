from __future__ import annotations
from pathlib import Path
import typer
from typing import Optional
from .logging_utils import setup_logging
from .config import Design
from .io_utils import find_fastqs, ensure_dir
from .renaming import plan_renames, apply_renames
from .demux import run_demux
from .slurm import render_slurm

app = typer.Typer(add_completion=False, help="scifi‑ATAC FASTQ renaming & demultiplexing wrapper")


@app.callback()
def _main(
    verbose: int = typer.Option(0, "-v", count=True, help="-v/-vv for more logs"),
):
    setup_logging(verbose)


@ app.command()
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


@ demux_app.command("wells-by-plate")
def demux_wells_by_plate(
    fastq_dir: Path = typer.Option(..., exists=True, dir_okay=True),
    plate_map: Path = typer.Option(..., exists=True, help="YAML with plates.wells"),
    out: Path = typer.Option(...),
    mode: str = typer.Option("link"),
    dry_run: bool = typer.Option(False),
):
    run_demux("wells-by-plate", fastq_dir, plate_map, out, mode=mode, dry_run=dry_run)


@ demux_app.command("sample-design")
def demux_sample_design(
    fastq_dir: Path = typer.Option(..., exists=True, dir_okay=True),
    design: Path = typer.Option(..., exists=True, help="YAML with samples.wells mapping"),
    out: Path = typer.Option(...),
    mode: str = typer.Option("link"),
    dry_run: bool = typer.Option(False),
):
    run_demux("sample-design", fastq_dir, design, out, mode=mode, dry_run=dry_run)


@ app.command()
def slurm(
    job_name: str = typer.Option("demux", help="Job name"),
    out_dir: Path = typer.Option(..., help="Where to write .slurm"),
    cmd: str = typer.Option(..., help="Command to run on the cluster"),
    partition: str = typer.Option("standard"),
    time: str = typer.Option("4:00:00"),
    mem: str = typer.Option("8G"),
    ntasks: int = typer.Option(1),
    cpus_per_task: int = typer.Option(4),
):
    out_dir.mkdir(parents=True, exist_ok=True)
    script = render_slurm(job_name, out_dir, cmd, partition=partition, time=time, mem=mem, ntasks=ntasks, cpus_per_task=cpus_per_task)
    path = out_dir / f"{job_name}.slurm"
    path.write_text(script)
    typer.echo(str(path))
