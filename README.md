# scifi-demux

Chunk-first **demultiplexing** and **mapping/cleaning** for scifi-ATAC FASTQ workflows with:

- One CLI (```scifi-demux …```) and subcommands for **Step 1** (UMI → cutadapt → demux → merge) and **Step 2** (map → clean).
- Optional **design file** to group wells into samples/pools; defaults to per-well outputs if omitted.
- Resumable, **checkpointed** execution (local with GNU parallel; HPC with SLURM arrays).
- Built-in **QC summaries** and a ready MultiQC config.

## Installation

We recommend using conda/mamba so you get the bioinformatics tools (UMI-tools, cutadapt, bwa, samtools, picard, seqkit, GNU parallel, MultiQC) alongside the Python CLI.

```
# HTTPS
git clone https://github.com/gomezcan/scifi-demux
cd scifi-demux
```

```
# or SSH (recommended)
git clone git@github.com:gomezcan/scifi-demux.git
cd scifi-demux
```

### Option A — Fresh environment (recommended)

#### 1) Configure channels (once):

```
conda config --add channels conda-forge
conda config --add channels bioconda
conda config --set channel_priority strict
```

#### 2) Create and activate the env (fast with mamba):

```
# If you have mamba:
mamba create -n scifi-demux \
  python=3.11 umi_tools cutadapt seqkit samtools bwa picard pigz parallel multiqc -y
conda activate scifi-demux
```

```
# With conda:
conda create -n scifi-demux python=3.11 umi_tools cutadapt seqkit samtools bwa picard pigz parallel multiqc -y
conda activate scifi-demux
```

```
# or used environment.yml
conda env create -n scifi-demux -f environment.yml
conda activate scifi-demux
```

#### 3) Install the Python package (editable dev mode):

```
pip install -e .
```

### Option B — Install into an existing conda env
```
conda activate <your-env>
# (optional) add channels once per machine
conda config --add channels conda-forge
conda config --add channels bioconda
conda config --set channel_priority strict

# install (from the repo you cloned)
pip install -e /path/to/ambientmapper
# or straight from GitHub
pip install "git+https://github.com/gomezcan/ambientmapper.git"
```

#### 4) Verify the install
scifi-demux --help
umi_tools --version
cutadapt --version
samtools --version | head -1
bwa 2>&1 | head -1
parallel --version | head -1
multiqc --version



## Quick start

> The CLI is being built in two stages:
>  - "**Step 1**: UMI → cutadapt → (chunk) demux → merge → QC"
>  - "**Step 2**: genome index resolve/build → mapping → 8 cleaning sub-steps → QC"


## Step 1 — plan/run (demux pipeline)
Register a library and plan the run (dry-run shows what will execute):

```bash
scifi-demux step1 run \
  --library SeedlingLib1 \
  --mode local \
  --dry-run
```

Step 1 will produce `{group}_R1.bc1.bc2.fastq.gz` / `{group}_R3.bc1.bc2.fastq.gz` per **group** = sample/pool from the design file, or per-well if no design is supplied.

## Step 2 — plan & run mapping/cleaning
Create a simple TSV describing mapping tasks:
```swift
# sample_base<TAB>target_genome<TAB>ref_path
Pool1	B73	/path/to/indexes/Index_B73_bwa
Pool1	Mo17	/path/to/genomes/Mo17.fa
```

Plan, then (initially) dry-run:

```bash
scifi-demux step2 plan --genome-map example_configs/genome_map_design.tsv

# local: sequential mapping (multi-threaded), cleaning after each mapping
scifi-demux step2 run --mode local --threads-per-task 24 --dry-run

# hpc: SLURM arrays (one task = one row)
# scifi-demux step2 run --mode hpc --threads-per-task 24 --dry-run
```

## Status & resume
```bash
# See progress across all tasks
scifi-demux status

# Resume only pending work (applies to both steps)
# scifi-demux step2 run --mode local --threads-per-task 24 --resume
```

## MultiQC (summary report)
```bash
multiqc --config qc/multiqc_scifi.yaml --outdir qc/report .
```

### Design file formats
- ***Well→sample grouping***: plain text, one line per group (pool), ranges like `A1-12,B1-12`.
  Example:
  ```ngnix
  Pool1	A1-12,B1-12
  Pool2	C1-12,D1-12
  ```
- If **no design** is provided, demux defaults to **per-well** outputs. 


