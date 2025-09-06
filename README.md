# scifi-demux

**demultiplexing** scripts for scifi‑ATAC FASTQ processing. Provides:

- One command (`scifi-demux ...`) with subcommands for `rename` and `demux`.
- YAML config with validation (Pydantic) for plate layouts, well barcodes, and sample design.
- Strategy plug‑ins so you can drop in custom logic without changing the CLI.
- HPC-friendly features: chunking, SLURM templating, resumable outputs, structured logs.

## Installation

We recommend using conda/mamba so you get the bioinformatics tools (UMI-tools, cutadapt, bwa, samtools, picard, seqkit, GNU parallel, MultiQC) alongside the Python CLI.

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
# If you only have conda:
conda create -n scifi-demux \
  python=3.11 umi_tools cutadapt seqkit samtools bwa picard pigz parallel multiqc -y
conda activate scifi-demux
```

#### 3) Install this package (editable dev mode):

```
pip install -e .
```

#### 3) Verify the install
scifi-demux --help
umi_tools --version
cutadapt --version
samtools --version | head -1
bwa 2>&1 | head -1
parallel --version | head -1
multiqc --version


## Quick start

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
