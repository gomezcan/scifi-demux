# src/scifi_demux/steps/step2.py
"""
Step 2 core:
  - Ensure BWA index (build if ref_path is a FASTA).
  - Map paired FASTQs with bwa mem.
  - Run scifi-ATAC cleaning pipeline:
      * Python BC tagging + MAPQ filter + 10x/Tn5 correction
        (replaces legacy 1_1–1_4 scripts)
      * Picard MarkDuplicates (BARCODE_TAG=BC)
      * multi-mapping + BC fix (legacy 1_5_scifi_fixBC.pl)
      * Tn5 BED generation (legacy 1_6_scifi_makeTn5bed.py)
"""

from __future__ import annotations
from pathlib import Path
from typing import List
import subprocess


from scifi_demux.io_utils import legacy_script_path
from scifi_demux.utils.fs import write_ok, has_ok
from scifi_demux.utils.scifi_cleanup import scifi_cleanup_bam
from scifi_demux.utils.scifi_fixBC import process_and_count
from .primitives import _which_or_raise


# ---------------------------------------------------------------------------
# Generic runner
# ---------------------------------------------------------------------------

def _run(cmd: List[str] | str, *, dry_run: bool = False) -> None:
    """
    Helper to run a command (list or shell string).
    """
    if isinstance(cmd, list):
        cmd_str = " ".join(str(x) for x in cmd)
        shell = False
    else:
        cmd_str = cmd
        shell = True

    print(f"[step2] RUN: {cmd_str}")
    if dry_run:
        return

    subprocess.run(cmd, shell=shell, check=True)


# ---------------------------------------------------------------------------
# BWA index helpers
# ---------------------------------------------------------------------------

def _guess_bwa_index_prefix(ref_path: Path) -> Path:
    """
    Heuristic:
      - If ref_path has FASTA-like suffix, treat it as FASTA and use
        prefix = ref_path with the LAST extension stripped (handles .fa, .fasta,
        .fna, and gzipped variants).
      - Otherwise, assume ref_path is already an index prefix.
    """
    fasta_suffixes = {".fa", ".fasta", ".fna", ".fa.gz", ".fasta.gz", ".fna.gz"}
    if any(str(ref_path).endswith(suf) for suf in fasta_suffixes):
        # Strip only the last extension; ok for .fa/.fasta/.fna and .gz
        return Path(str(ref_path).rsplit(".", 1)[0])
    return ref_path


def ensure_bwa_index(ref_path: Path, threads: int = 4, dry_run: bool = False) -> Path:
    """
    Ensure a BWA index exists.

    ref_path:
      - If points to a FASTA, build a BWA index (if missing) with prefix
        guessed by _guess_bwa_index_prefix.
      - If points to an existing index prefix, just return it.

    Returns:
      Path to the index prefix to use in `bwa mem`.
    """
    ref_path = ref_path.resolve()
    prefix = _guess_bwa_index_prefix(ref_path)

    # Check for existing index files (BWA standard extensions)
    index_exts = [".bwt", ".pac", ".ann", ".amb", ".sa"]
    if all((prefix.with_suffix(ext)).exists() for ext in index_exts):
        print(f"[step2] Using existing BWA index prefix: {prefix}")
        return prefix

    # If we get here and ref_path is a FASTA, build the index
    fasta_suffixes = {".fa", ".fasta", ".fna", ".fa.gz", ".fasta.gz", ".fna.gz"}
    is_fasta = any(str(ref_path).endswith(suf) for suf in fasta_suffixes)
    if not is_fasta:
        # User passed something that doesn't look like a FASTA and index files
        # are missing: fail clearly.
        raise FileNotFoundError(
            f"[step2] BWA index files not found for prefix={prefix}. "
            f"ref_path={ref_path} does not look like FASTA, so index cannot be built automatically."
        )

    # Build index
    print(f"[step2] Building BWA index with prefix={prefix} from FASTA={ref_path}")
    cmd = [
        "bwa",
        "index",
        "-p",
        str(prefix),
        str(ref_path),
    ]
    _run(cmd, dry_run=dry_run)

    return prefix


# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------

def run_bwa_mapping(
    sample_id: str,
    genome_target: str,
    fq_r1: Path,
    fq_r3: Path,
    index_prefix: Path,
    out_dir: Path,
    threads: int = 8,
    dry_run: bool = False,
    sent_dir: Path | None = None,
) -> Path:
    """
    Run bwa mem for one sample × genome and write a raw BAM.

    Returns:
      Path to raw BAM: <out_dir>/<sample_id>_<genome_target>_scifiATAC.raw.bam
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    base = f"{sample_id}_{genome_target}_scifiATAC"
    sam_path = out_dir / f"{base}.raw.sam"
    bam_path = out_dir / f"{base}.raw.bam"

    # Sentinel: skip if already mapped
    if sent_dir is not None and has_ok(sent_dir / f"{base}.map"):
        print(f"[step2] Skipping map (sentinel exists): {base}")
        return bam_path

    print(
        f"[step2] Mapping sample={sample_id} to genome={genome_target} using index={index_prefix}"
    )

    # Map to SAM
    cmd_mem = [
        "bwa",
        "mem",
        "-M",
        "-t",
        str(threads),
        str(index_prefix),
        str(fq_r1),
        str(fq_r3),
    ]
    # bwa mem → SAM (shell redirection)
    cmd_mem_shell = " ".join(str(x) for x in cmd_mem) + f" > {sam_path}"
    _run(cmd_mem_shell, dry_run=dry_run)

    # SAM → BAM
    cmd_view = [
        "samtools",
        "view",
        "-@",
        str(threads),
        "-bS",
        str(sam_path),
        "-o",
        str(bam_path),
    ]
    _run(cmd_view, dry_run=dry_run)

    # Clean up SAM
    if not dry_run and sam_path.exists():
        sam_path.unlink()

    if sent_dir is not None and not dry_run:
        write_ok(sent_dir / f"{base}.map", {"step": "map", "output": str(bam_path)})

    return bam_path


# ---------------------------------------------------------------------------
# Cleaning pipeline
# ---------------------------------------------------------------------------

def run_scifi_cleaning_pipeline(
    base: str,
    bam_raw: Path,
    out_bam_dir: Path,
    out_bed_dir: Path,
    whitelist_10x: Path,
    whitelist_tn5: Path,
    threads: int = 8,
    mapq_min: int = 20,
    dry_run: bool = False,
    sent_dir: Path | None = None,
) -> None:
    """
    Run the scifi-ATAC cleaning pipeline for a given base name and raw BAM.

    Python stage:
      - Sort BAM
      - Filter: mapped, MAPQ ≥ mapq_min, proper pairs
      - 10x whitelist check + Tn5 1-mismatch correction
      - Attach BC tag, write <base>.mq{mapq_min}.BC.bam

    Subsequent stages:
      - Picard MarkDuplicates (BARCODE_TAG=BC)
      - scifi_fixBC.process_and_count (multi-mapping + BC fix; writes *_rmdup.mm.bam + *_bc_counts.txt)
      - 1_6_scifi_makeTn5bed.py → BED → sort/uniq → pigz
      - per-BAM read counts and optional cleanup
    """
    out_bam_dir.mkdir(parents=True, exist_ok=True)
    out_bed_dir.mkdir(parents=True, exist_ok=True)

    threads_str = str(threads)

    # Resolve legacy script for Tn5 BED generation
    s_tn5_bed = legacy_script_path("1_6_scifi_makeTn5bed.py")

    # 1) Sort raw BAM from BWA
    bam_sort = out_bam_dir / f"{base}.rawSort.bam"
    if sent_dir is None or not has_ok(sent_dir / f"{base}.sort"):
        cmd_sort = [
            "samtools",
            "sort",
            "-@",
            threads_str,
            "-o",
            str(bam_sort),
            str(bam_raw),
        ]
        _run(cmd_sort, dry_run=dry_run)
        if sent_dir is not None and not dry_run:
            write_ok(sent_dir / f"{base}.sort", {"step": "sort", "output": str(bam_sort)})
    else:
        print(f"[step2] Skipping sort (sentinel exists): {base}")

    # 2) Python barcode cleanup + MAPQ filter → BC-tagged BAM
    bam_bc = out_bam_dir / f"{base}.mq{mapq_min}.BC.bam"
    if sent_dir is None or not has_ok(sent_dir / f"{base}.bc_tag"):
        if dry_run:
            print(
                f"[step2] DRY-RUN: would run scifi_cleanup_bam("
                f"input_bam={bam_sort}, output_bam={bam_bc}, "
                f"whitelist_10x={whitelist_10x}, whitelist_tn5={whitelist_tn5}, "
                f"min_mapq={mapq_min}, threads={threads})"
            )
        else:
            scifi_cleanup_bam(
                input_bam=bam_sort,
                output_bam=bam_bc,
                whitelist_10x=whitelist_10x,
                whitelist_tn5=whitelist_tn5,
                min_mapq=mapq_min,
                threads=threads,
            )
            if sent_dir is not None:
                write_ok(sent_dir / f"{base}.bc_tag", {"step": "bc_tag", "output": str(bam_bc)})
    else:
        print(f"[step2] Skipping bc_tag (sentinel exists): {base}")

    # 3) Remove duplicates with Picard MarkDuplicates
    bam_rmdup = out_bam_dir / f"{base}.mq{mapq_min}.BC.rmdup.bam"
    metrics = out_bam_dir / f"{base}.metrics"
    if sent_dir is None or not has_ok(sent_dir / f"{base}.dedup"):
        cmd_picard = [
            "picard",
            "MarkDuplicates",
            f"I={bam_bc}",
            f"O={bam_rmdup}",
            f"METRICS_FILE={metrics}",
            "REMOVE_DUPLICATES=true",
            "BARCODE_TAG=BC",
            "ASSUME_SORT_ORDER=coordinate",
            "MAX_FILE_HANDLES_FOR_READ_ENDS_MAP=1000",
        ]
        _run(cmd_picard, dry_run=dry_run)
        if sent_dir is not None and not dry_run:
            write_ok(sent_dir / f"{base}.dedup", {"step": "dedup", "output": str(bam_rmdup)})
    else:
        print(f"[step2] Skipping dedup (sentinel exists): {base}")

    # -----------------------------------------------------------
    # 4) fix multi-mapping & BC (Replaces 1_5_scifi_fixBC.pl)
    # -----------------------------------------------------------
    bam_mm = out_bam_dir / f"{base}.mq{mapq_min}.BC.rmdup.mm.bam"
    bc_counts_out = out_bam_dir / f"{base}_bc_counts.txt"
    if sent_dir is None or not has_ok(sent_dir / f"{base}.fixbc"):
        if dry_run:
            print(
                f"[step2] DRY-RUN: would run scifi_fixBC.process_and_count("
                f"input_bam={bam_rmdup}, output_bam={bam_mm}, "
                f"counts_file={bc_counts_out}, library_tag={base}, "
                f"threads={threads})"
            )
        else:
            process_and_count(
                input_bam=str(bam_rmdup),
                output_bam=str(bam_mm),
                counts_file=str(bc_counts_out),
                library_tag=base,   # same as Perl script's last arg
                threads=threads,
                tissue_label="leaf",  # or make this configurable later
            )
            if sent_dir is not None:
                write_ok(sent_dir / f"{base}.fixbc", {"step": "fixbc", "output": str(bam_mm)})
    else:
        print(f"[step2] Skipping fixbc (sentinel exists): {base}")

    # 5) Index final BAM
    if sent_dir is None or not has_ok(sent_dir / f"{base}.index_bam"):
        cmd_index = [
            "samtools",
            "index",
            "-@",
            threads_str,
            str(bam_mm),
        ]
        _run(cmd_index, dry_run=dry_run)
        if sent_dir is not None and not dry_run:
            write_ok(sent_dir / f"{base}.index_bam", {"step": "index_bam", "output": str(bam_mm) + ".bai"})
    else:
        print(f"[step2] Skipping index_bam (sentinel exists): {base}")

    # 6) Make Tn5 BED and compress
    bed_path = out_bed_dir / f"{base}.mq{mapq_min}.tn5.bed"
    if sent_dir is None or not has_ok(sent_dir / f"{base}.tn5bed"):
        cmd_tn5 = (
            f"python {s_tn5_bed} {bam_mm} "
            f"| sort -k1,1 -k2,2n "
            f"| uniq "
            f"> {bed_path}"
        )
        _run(cmd_tn5, dry_run=dry_run)

        cmd_gzip = [
            "pigz",
            "-p",
            threads_str,
            str(bed_path),
        ]
        _run(cmd_gzip, dry_run=dry_run)
        if sent_dir is not None and not dry_run:
            write_ok(sent_dir / f"{base}.tn5bed", {"step": "tn5bed", "output": str(bed_path) + ".gz"})
    else:
        print(f"[step2] Skipping tn5bed (sentinel exists): {base}")

    # 7) Optional read counts
    proper_pairs_txt = out_bam_dir / f"{base}.mq{mapq_min}.BC.rmdup.proper_pairs.txt"
    proper_pairs_mm_txt = out_bam_dir / f"{base}.mq{mapq_min}.BC.rmdup.mm.proper_pairs.txt"
    if sent_dir is None or not has_ok(sent_dir / f"{base}.counts"):
        cmd_count_pp = [
            "samtools",
            "view",
            "-@",
            threads_str,
            "-c",
            str(bam_rmdup),
        ]
        cmd_count_pp_mm = [
            "samtools",
            "view",
            "-@",
            threads_str,
            "-c",
            str(bam_mm),
        ]
        _run(" ".join(str(x) for x in cmd_count_pp) + f" > {proper_pairs_txt}", dry_run=dry_run)
        _run(" ".join(str(x) for x in cmd_count_pp_mm) + f" > {proper_pairs_mm_txt}", dry_run=dry_run)
        if sent_dir is not None and not dry_run:
            write_ok(sent_dir / f"{base}.counts", {"step": "counts"})
    else:
        print(f"[step2] Skipping counts (sentinel exists): {base}")

    # 8) Optional cleanup of intermediates
    # Only delete bam_sort once bc_tag has completed (its sole consumer),
    # so that a resume after bc_tag failure can still find the sorted BAM.
    bc_tag_done = sent_dir is not None and has_ok(sent_dir / f"{base}.bc_tag")
    if not dry_run and bc_tag_done and bam_sort.exists():
        bam_sort.unlink()


# ---------------------------------------------------------------------------
# High-level entry point for one sample × genome
# ---------------------------------------------------------------------------

def run_step2_for_sample_genome(
    sample_id: str,
    genome_target: str,
    fq_r1: Path,
    fq_r3: Path,
    ref_path: Path,
    out_root: Path,
    whitelist_10x: Path,
    whitelist_tn5: Path,
    threads: int = 8,
    mapq_min: int = 20,
    dry_run: bool = False,
) -> None:
    """
    High-level entry point for one (sample, genome) pair.

    - Ensure BWA index from ref_path.
    - Map fq_r1/fq_r3 with bwa mem.
    - Run scifi-ATAC cleanup pipeline.

    Final outputs:

      BAM:
        <out_root>/<sample_id>/
          <sample_id>_<genome_target>_scifiATAC.mq<mapq_min>.BC.rmdup.mm.bam
          + .bai index and *_bc_counts.txt

      BED:
        <out_root>/<sample_id>/bed/
          <sample_id>_<genome_target>_scifiATAC.mq<mapq_min>.tn5.bed.gz
    """
    _which_or_raise("bwa", "samtools", "picard", "pigz")

    out_root = out_root.resolve()
    sample_out_dir = out_root / sample_id
    bam_dir = sample_out_dir
    bed_dir = sample_out_dir / "bed"

    # Sentinel directory for resume support
    sent_dir = bam_dir / "_sentinels"
    sent_dir.mkdir(parents=True, exist_ok=True)

    # 1) ensure index
    index_prefix = ensure_bwa_index(ref_path=ref_path, threads=threads, dry_run=dry_run)

    # 2) mapping
    bam_raw = run_bwa_mapping(
        sample_id=sample_id,
        genome_target=genome_target,
        fq_r1=fq_r1,
        fq_r3=fq_r3,
        index_prefix=index_prefix,
        out_dir=bam_dir,
        threads=threads,
        dry_run=dry_run,
        sent_dir=sent_dir,
    )

    # 3) cleaning pipeline
    base = f"{sample_id}_{genome_target}_scifiATAC"
    run_scifi_cleaning_pipeline(
        base=base,
        bam_raw=bam_raw,
        out_bam_dir=bam_dir,
        out_bed_dir=bed_dir,
        whitelist_10x=whitelist_10x,
        whitelist_tn5=whitelist_tn5,
        threads=threads,
        mapq_min=mapq_min,
        dry_run=dry_run,
        sent_dir=sent_dir,
    )
