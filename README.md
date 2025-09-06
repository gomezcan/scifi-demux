# scifi-demux

**demultiplexing** scripts for scifi‑ATAC FASTQ processing. Provides:

- One command (`scifi-demux ...`) with subcommands for `rename` and `demux`.
- YAML config with validation (Pydantic) for plate layouts, well barcodes, and sample design.
- Strategy plug‑ins so you can drop in custom logic without changing the CLI.
- HPC-friendly features: chunking, SLURM templating, resumable outputs, structured logs.


## Quick start

```bash
uv venv && source .venv/bin/activate # or: python -m venv .venv
pip install -e .
scifi-demux --help
```

Dry run on an example:

```bash
scifi-demux rename \
--fastq-dir /path/raw_fastq \
--plate-map example_configs/design.yaml \
--out /path/renamed --dry-run
```

Demultiplex by wells across plates:

```bash
scifi-demux demux wells-by-plate \
--fastq-dir /path/renamed \
--plate-map example_configs/design.yaml \
--out /path/demux_wells \
--threads 8
```

Demultiplex by sample design (groups wells into samples):

```bash
scifi-demux demux sample-design \
--fastq-dir /path/renamed \
--design example_configs/design.yaml \
--out /path/demux_samples
```
