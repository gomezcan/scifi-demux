# src/scifi_demux/utils/scifi_cleanup.py

import argparse
import sys
from pathlib import Path
from typing import Dict, Set, Tuple, Optional

import pysam

# --------------------------------------------------------------------
# Constants / basic helpers
# --------------------------------------------------------------------

DNA_COMPLEMENT = str.maketrans("ACGTNacgtn", "TGCANtgcan")


def reverse_complement(seq: str) -> str:
    """Fast reverse complement of a DNA sequence."""
    return seq.translate(DNA_COMPLEMENT)[::-1]


# --------------------------------------------------------------------
# Whitelist loading
# --------------------------------------------------------------------

def load_whitelists(tenx_path: Path, tn5_path: Path) -> Tuple[Set[str], Dict[str, Optional[str]]]:
    """
    Load 10x whitelist into a Set (for exact match).

    Load Tn5 whitelist and pre-calculate 1-mismatch variants map.

    For Tn5:
      - tn5_map[valid_seq] = valid_seq (identity)
      - tn5_map[mutant] = valid_seq   (if uniquely 1-mismatch-close to valid_seq)
      - tn5_map[mutant] = None        (if ambiguous: maps to >1 valid_seq)
    """
    print(f"[INFO] Loading 10x whitelist: {tenx_path}", file=sys.stderr)
    tenx_wl: Set[str] = set()
    with tenx_path.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            tenx_wl.add(line.split("\t")[0])

    print(f"[INFO] Loading Tn5 whitelist: {tn5_path}", file=sys.stderr)
    tn5_map: Dict[str, Optional[str]] = {}
    valid_tn5: list[str] = []

    with tn5_path.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Format: name \t sequence \t tag
            parts = line.split("\t")
            if len(parts) >= 2:
                seq = parts[1]
                valid_tn5.append(seq)
                tn5_map[seq] = seq  # identity mapping

    # Generate 1-mismatch lookups for Tn5
    bases = ["A", "C", "G", "T", "N"]
    for valid_seq in valid_tn5:
        for i in range(len(valid_seq)):
            orig = valid_seq[i]
            for b in bases:
                if b == orig:
                    continue
                mutant = valid_seq[:i] + b + valid_seq[i + 1 :]
                if mutant not in tn5_map:
                    tn5_map[mutant] = valid_seq
                else:
                    # If already mapped to a different valid_seq, mark ambiguous
                    if tn5_map[mutant] is not None and tn5_map[mutant] != valid_seq:
                        tn5_map[mutant] = None

    return tenx_wl, tn5_map


# --------------------------------------------------------------------
# Barcode parsing / correction
# --------------------------------------------------------------------

def correct_barcodes(
    raw_bc: str,
    tenx_wl: Set[str],
    tn5_map: Dict[str, Optional[str]],
) -> Tuple[Optional[str], int]:
    """
    Parse and correct scifi-ATAC barcodes.

    Input:
      raw_bc: string containing concatenated 10x + Tn5A + Tn5B segments.
              Assumed layout (26 bp total):
                0–15  : 10x (will be reverse-complemented)
                16–20 : Tn5A
                21–25 : Tn5B

    Returns:
      (corrected_full_bc, status), where:
        corrected_full_bc: 26 bp string or None if correction fails
        status:
          0 = exact, no correction needed
          1 = corrected (Tn5A and/or Tn5B adjusted via tn5_map)
          2 = fail (10x not in whitelist, or Tn5 unresolvable/ambiguous)
    """
    if len(raw_bc) < 26:
        return None, 2

    # Parse segments
    raw_10x = raw_bc[0:16]
    raw_tn5_a = raw_bc[16:21]
    raw_tn5_b = raw_bc[21:26]

    # Legacy logic: reverse complement the 10x portion
    seq_10x = reverse_complement(raw_10x)

    # 1. Check 10x (exact match against 737k set)
    if seq_10x not in tenx_wl:
        return None, 2

    # 2. Check/Correct Tn5 A
    corr_tn5_a = tn5_map.get(raw_tn5_a)
    if not corr_tn5_a:
        return None, 2

    # 3. Check/Correct Tn5 B
    corr_tn5_b = tn5_map.get(raw_tn5_b)
    if not corr_tn5_b:
        return None, 2

    final_bc = seq_10x + corr_tn5_a + corr_tn5_b
    status = 1 if (corr_tn5_a != raw_tn5_a or corr_tn5_b != raw_tn5_b) else 0
    return final_bc, status


# --------------------------------------------------------------------
# Core cleanup function
# --------------------------------------------------------------------

def scifi_cleanup_bam(
    input_bam: Path,
    output_bam: Path,
    whitelist_10x: Path,
    whitelist_tn5: Path,
    min_mapq: int = 20,
    threads: int = 4,
) -> Tuple[int, int, int]:
    """
    Scifi-ATAC pre-dedup cleaning step:

      - Filter reads:
          * mapped
          * mapping_quality >= min_mapq
          * proper pair (FLAG 0x2)
      - Extract concatenated BC from read name suffix
      - Validate 10x part (exact), and 1-mismatch-correct Tn5 parts
      - Attach corrected BC as 'BC' tag
      - Write passing reads to output BAM

    Returns:
      (total_reads, pass_mapq, pass_bc)
    """
    tenx_wl, tn5_map = load_whitelists(whitelist_10x, whitelist_tn5)

    count_total = 0
    count_pass_mq = 0
    count_pass_bc = 0

    print(f"[INFO] Processing BAM: {input_bam}", file=sys.stderr)

    # pysam threads: bgzf compression threads; OK to pass here
    with pysam.AlignmentFile(str(input_bam), "rb", threads=threads) as infile, \
         pysam.AlignmentFile(str(output_bam), "wb", template=infile, threads=threads) as outfile:

        for read in infile:
            count_total += 1

            # 1. Filter: unmapped, low MAPQ, not proper pair
            # Roughly mimics: samtools view -q {min_mapq} -f 3
            if read.is_unmapped or read.mapping_quality < min_mapq or not read.is_proper_pair:
                continue

            count_pass_mq += 1

            # 2. Extract Barcode from read name
            # Expect something like: READID_..._<BCSTRING>
            qname_parts = read.query_name.split("_")
            if len(qname_parts) < 2:
                continue

            raw_bc_seq = qname_parts[-1]

            # 3. Correct barcodes
            corrected_bc, status = correct_barcodes(raw_bc_seq, tenx_wl, tn5_map)
            if not corrected_bc:
                continue

            # 4. Attach BC tag (and optionally status)
            read.set_tag("BC", corrected_bc, value_type="Z")
            # If you want to track status, uncomment:
            # read.set_tag("BS", status, value_type="i")

            # Optionally strip BC suffix from name to save space:
            # read.query_name = "_".join(qname_parts[:-1])

            outfile.write(read)
            count_pass_bc += 1

    print(f"[INFO] Complete.", file=sys.stderr)
    print(f"  Total Reads:           {count_total}", file=sys.stderr)
    print(f"  Passed MapQ/Flags:     {count_pass_mq}", file=sys.stderr)
    print(f"  Passed BC Correction:  {count_pass_bc}", file=sys.stderr)

    return count_total, count_pass_mq, count_pass_bc


# --------------------------------------------------------------------
# CLI entry point (standalone script)
# --------------------------------------------------------------------

def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Scifi-ATAC pre-dedup cleaning")
    parser.add_argument("--input-bam", required=True, type=Path)
    parser.add_argument("--output-bam", required=True, type=Path)
    parser.add_argument("--whitelist-10x", required=True, type=Path)
    parser.add_argument("--whitelist-tn5", required=True, type=Path)
    parser.add_argument("--min-mapq", type=int, default=20)
    parser.add_argument("--threads", type=int, default=4)

    args = parser.parse_args(argv)

    scifi_cleanup_bam(
        input_bam=args.input_bam,
        output_bam=args.output_bam,
        whitelist_10x=args.whitelist_10x,
        whitelist_tn5=args.whitelist_tn5,
        min_mapq=args.min_mapq,
        threads=args.threads,
    )


if __name__ == "__main__":
    main()
