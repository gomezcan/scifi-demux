# scifi-demux: Renaming & Demultiplexing for sci-fi ATAC-seq

[![tests](https://github.com/gomezcan/scifi-demux/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/gomezcan/scifi-demux/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/gomezcan/scifi-demux/branch/main/graph/badge.svg)](https://codecov.io/gh/gomezcan/scifi-demux)
[![PyPI version](https://img.shields.io/pypi/v/scifi-demux.svg)](https://pypi.org/project/scifi-demux/)
[![Python versions](https://img.shields.io/pypi/pyversions/scifi-demux.svg)](https://pypi.org/project/scifi-demux/)

`scifi-demux` is a typed CLI toolkit to orchestrate **renaming**, **demultiplexing**, and **mapping/cleaning** of sci-fi ATAC-seq FASTQs.  
It wraps existing bioinformatics tools in a reproducible, resumable, QC-aware framework.

- One CLI (`scifi-demux …`) with subcommands for **Step 1** (UMI → cutadapt → demux → merge) and **Step 2** (map → clean).
- Supports **local (GNU parallel)** and **HPC (SLURM array)** execution.
- Uses optional **design files** for pooling/grouping, or defaults to per-well.
- Generates built-in **QC summaries** and integrates with MultiQC.



## Installation

We recommend using conda/mamba so you get the bioinformatics tools (UMI-tools, cutadapt, bwa, samtools, picard, seqkit, GNU parallel, MultiQC) alongside the Python CLI.

```
# HTTPS
git clone https://github.com/gomezcan/scifi-demux
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
pip install -e /path/to/scifi-demux
# or straight from GitHub
pip install "git+https://github.com/gomezcan/scifi-demux.git"
```

#### 4) Verify the install
```
scifi-demux --help
umi_tools --version
cutadapt --version
samtools --version | head -1
bwa 2>&1 | head -1
parallel --version | head -1
multiqc --version
```


## Quick start

> The CLI is being built in two stages:
>  - "**Step 1**: UMI → cutadapt → (chunk) demux → merge → QC"
>  - "**Step 2**: genome index resolve/build → mapping → 8 cleaning sub-steps → QC"


## Step 1 LOCAL mode end-to-end: plan (split library in chunks) + run demux (assigment pools based on designs ) + merge chunks + QC
Register a library and plan the run (dry-run shows what will execute):

Step 1. End-to-end form, with design
```
scifi-demux step1 run \
--mode local \
--library Sample1 \
--raw-dir /path/raw/fastq.gz \  
--design PlateDesign_SampleExample1.txt \
--out /path/demux_samples   \
--threads 8
```

Step 1. End-to-end form, without design
```
scifi-demux step1 run \
--mode local \
--library Sample1 \
--raw-dir /path/raw/fastq.gz \  
--out /path/demux_samples   \
--threads 8
```

Step 1 will produce `{group}_R1.bc1.bc2.fastq.gz` / `{group}_R3.bc1.bc2.fastq.gz` per **group** = sample/pool from the design file, or per-well if no design is supplied.
NOTE: In **local** mode, it creates one chunk per thread and launches the same number of threads as workers in parallel, with each thread processing its own chunk. This helps reduce memory usage and processing time.

## Step 1. HPC mode end-to-en: plan (split library in chunks) +  run demux (assigment pools based on designs ) + merge chunks + QC

```
#!/bin/bash
########## BATCH Lines for Resource Request ##########
#SBATCH --time=8:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=70G
#SBATCH --job-name=Demux_step1
#SBATCH --output=_logs/%x-%j.log
#SBATCH --array=1-50%10 # 50 chunks process 10 at the time

conda activate scifi-demux

# 1 ) get workers plan
scifi-demux step1 plan --library SampleExample1 \
    --raw-dir /path/raw \
    --chunks 50
  
# 2 ) WORKER (array tasks)
scifi-demux step1 worker-chunk \
    --plan SampleExample1_work/run_plan.step1.chunks.tsv \
    --mode hpc \
    --design PlateDesign_SampleExample1.txt \
    --arra-id ${SLURM_ARRAY_TASK_ID}
fi
```

After the array finishes, run the merge/check commands once.

```
# 3) After all array tasks finish, merge (barriered)
scifi-demux step1 merge --library SampleExample1 --work-root SampleExample1_work

# 4) Check completeness before merging
scifi-demux step1 check --library SampleExample1 --work-root SampleExample1_work
```

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


