# src/scifi_demux/demux_core.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Iterable, Tuple, List
import csv, gzip, os, re

# ----------------------------
# Loading mappings (unchanged semantics)
# ----------------------------

def load_sample_to_wells(file_path: Path | str) -> Dict[str, List[str]]:
    """
    Parse a 'Pool<TAB>Ranges' text file into {sample: [well_id, ...]}.

    Accepts ranges like 'A1-12,B1-12' (case-insensitive rows).
    """
    sample_to_wells: Dict[str, List[str]] = {}
    with open(file_path, "r") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            sample, ranges_str = line.split("\t", 1)
            wells = sample_to_wells.setdefault(sample.strip(), [])
            for r in ranges_str.split(","):
                r = r.strip().replace(" ", "")
                if not r:
                    continue
                row = r[0].upper()
                rest = r[1:]
                if "-" in rest:
                    start_s, end_s = rest.split("-", 1)
                else:
                    start_s = end_s = rest
                start, end = int(start_s), int(end_s)
                if start > end:
                    raise ValueError(f"Start > end in '{r}'")
                wells.extend(f"{row}{i}" for i in range(start, end + 1))
    return sample_to_wells


def load_barcode_layout(layout_file: Path | str) -> Dict[str, str]:
    """
    Load a 96-well Tn5 barcode layout TSV into {WellID -> split_bc}.
    Expects first row to be column headers, first column to be row letters.
    """
    well_to_barcode: Dict[str, str] = {}
    with open(layout_file, "r") as f:
        reader = csv.reader(f, delimiter="\t")
        col_names = next(reader)[1:]
        for row in reader:
            row_label = row[0]
            for i, bc in enumerate(row[1:]):
                well_to_barcode[f"{row_label}{col_names[i]}"] = bc
    return well_to_barcode


def build_well_to_sample(sample_to_wells: Dict[str, List[str]]) -> Dict[str, str]:
    """Invert sample→wells to well→sample."""
    return {w: s for s, wells in sample_to_wells.items() for w in wells}


def build_barcode_to_sample(
    well_to_barcode: Dict[str, str],
    well_to_sample: Dict[str, str],
) -> Dict[str, str]:
    """Map split barcodes to sample names using well→barcode + well→sample."""
    return {bc: well_to_sample[well] for well, bc in well_to_barcode.items() if well in well_to_sample}


# ----------------------------
# Matching logic (unchanged semantics)
# ----------------------------

def mismatch_count_with_N(query: str, ref: str) -> int:
    """Count mismatches across equal-length strings; any 'N' counts as a mismatch."""
    return sum(0 if q == r else 1 for q, r in zip(query, ref))


def best_barcode_match_split(
    query_bc: str,
    all_barcodes: Iterable[str],
    max_mismatch: int = 1,
) -> tuple[str | None, int | None]:
    """
    Match 'LLLLL_RRRRR' to a set of split barcodes, allowing ≤ max_mismatch per half.
    Returns (best_bc, total_mismatch) or (None, None).
    """
    q_left, q_right = query_bc.split("_")
    best_bc, min_total_mis = None, 10**9
    for bc in all_barcodes:
        bc_left, bc_right = bc.split("_")
        mis_left = mismatch_count_with_N(q_left, bc_left)
        mis_right = mismatch_count_with_N(q_right, bc_right)
        if mis_left <= max_mismatch and mis_right <= max_mismatch:
            total_mis = mis_left + mis_right
            if total_mis < min_total_mis:
                best_bc, min_total_mis = bc, total_mis
    return (best_bc, min_total_mis) if best_bc else (None, None)


# ----------------------------
# FASTQ demux (unchanged semantics; writes per-sample files)
# ----------------------------

def process_fastq(
    input_fastq_gz: Path | str,
    barcode_to_sample: Dict[str, str],
    output_dir: Path | str,
    max_mismatch_per_half: int = 1,
) -> dict:
    """
    Stream a gz FASTQ whose read names end with '_LLLLL_RRRRR'.
    - drop reads with >2 Ns in the split barcode
    - correct to the best split barcode with ≤1 mismatch per half
    - write to per-sample gz files; replace original split tag with corrected one in the header
    Returns summary counts.
    """
    os.makedirs(output_dir, exist_ok=True)
    all_barcodes = list(barcode_to_sample.keys())
    sample_names = sorted(set(barcode_to_sample.values()))

    base = os.path.splitext(os.path.splitext(os.path.basename(str(input_fastq_gz)))[0])[0]
    handles = {s: gzip.open(os.path.join(str(output_dir), f"{base}_{s}.fastq.gz"), "wt") for s in sample_names}

    total = n_drop = mis_drop = 0
    counts = {s: 0 for s in sample_names}

    with gzip.open(str(input_fastq_gz), "rt") as fq:
        while True:
            name = fq.readline()
            if not name:
                break
            seq = fq.readline(); plus = fq.readline(); qual = fq.readline()
            name = name.rstrip("\n"); seq = seq.rstrip("\n"); plus = plus.rstrip("\n"); qual = qual.rstrip("\n")
            total += 1

            bc = "_".join(name.split(" ", 1)[0].split("_")[-2:])
            if bc.count("N") > 2:
                n_drop += 1
                continue

            best, mis = best_barcode_match_split(bc, all_barcodes, max_mismatch=max_mismatch_per_half)
            if not best:
                mis_drop += 1
                continue

            s = barcode_to_sample[best]
            handles[s].write(f"{name.replace(bc, best, 1)}\n{seq}\n{plus}\n{qual}\n")
            counts[s] += 1

    for h in handles.values():
        h.close()

    kept = total - n_drop - mis_drop
    return {"base": base, "total": total, "n_drop": n_drop, "mis_drop": mis_drop, "kept": kept, "per_sample": counts}


def demux_split_barcodes(
    layout_file: Path | str,
    input_fastq_gz: Path | str,
    sample_well_map: Path | str,
    output_dir: Path | str | None = None,
    max_mismatch_per_half: int = 1,
) -> dict:
    """
    Convenience one-shot: load mappings → build barcode→sample → process FASTQ.
    """
    well_to_bc = load_barcode_layout(layout_file)
    sample_to_wells = load_sample_to_wells(sample_well_map)
    well_to_sample = build_well_to_sample(sample_to_wells)
    bc_to_sample = build_barcode_to_sample(well_to_bc, well_to_sample)

    out_dir = output_dir if output_dir else os.path.dirname(str(input_fastq_gz))
    return process_fastq(input_fastq_gz, bc_to_sample, out_dir, max_mismatch_per_half=max_mismatch_per_half)
