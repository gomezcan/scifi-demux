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


# Orientation of the 10x barcode in the read names relative to the whitelist. Illumina i5 index
# reads are the reverse complement of 737K-cratac-v1 ("revcomp", the legacy behaviour); Element
# AVITI index reads are in whitelist orientation ("forward"). "auto" tests both on the first reads.
TENX_ORIENTATIONS = ("auto", "forward", "revcomp")
AUTO_ORIENT_READS = 100_000   # reads sampled by detect_tenx_orientation
AUTO_ORIENT_MIN = 0.5         # exact-match fraction the better orientation must reach

# --------------------------------------------------------------------
# 10x correction: allow 1 mismatch (unique hit)
# --------------------------------------------------------------------

def correct_10x_one_mismatch(
    seq_10x: str,
    tenx_wl: Set[str],
) -> Tuple[Optional[str], int]:
    """
    Correct a 10x barcode allowing at most one mismatch.

    Returns:
      (corrected_10x, status_code), where:
        corrected_10x: the corrected 10x sequence or None if not resolvable
        status_code:
          0 = exact match
          1 = corrected by 1 mismatch (unique candidate)
          2 = fail (no candidate or ambiguous >1 candidate)
    """
    # Exact match first
    if seq_10x in tenx_wl:
        return seq_10x, 0

    bases = ["A", "C", "G", "T", "N"]
    candidates: list[str] = []

    for i, orig in enumerate(seq_10x):
        for b in bases:
            if b == orig:
                continue
            mutant = seq_10x[:i] + b + seq_10x[i + 1 :]
            if mutant in tenx_wl:
                candidates.append(mutant)
                # If we see >1 candidate, treat as ambiguous
                if len(candidates) > 1:
                    return None, 2

    if not candidates:
        return None, 2

    # unique candidate
    return candidates[0], 1


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

    # Restore identity: valid sequences always map to themselves,
    # even if a 1-mismatch mutant of another valid sequence overwrote them.
    for valid_seq in valid_tn5:
        tn5_map[valid_seq] = valid_seq

    return tenx_wl, tn5_map


# --------------------------------------------------------------------
# Barcode parsing / correction
# --------------------------------------------------------------------

def correct_barcodes(
    raw_bc: str,
    tenx_wl: Set[str],
    tn5_map: Dict[str, Optional[str]],
    tenx_revcomp: bool = True,
) -> Tuple[Optional[str], int]:
    """
    Parse and correct scifi-ATAC barcodes.

    Input:
      raw_bc: string containing concatenated 10x + Tn5A + Tn5B segments.
              Assumed layout (26 bp total):
                0–15  : 10x (reverse-complemented before the whitelist lookup when
                        tenx_revcomp is True; see detect_tenx_orientation)
                16–20 : Tn5A
                21–25 : Tn5B

    Returns:
      (corrected_full_bc, status), where:
        corrected_full_bc: 26 bp string or None if correction fails
        status:
          0 = exact, no correction needed
          1 = corrected (10x and/or Tn5 adjusted)
          2 = fail (10x not in whitelist, or Tn5 unresolvable/ambiguous)
    """
    if len(raw_bc) < 26:
        return None, 2

    # Parse segments
    raw_10x = raw_bc[0:16]
    raw_tn5_a = raw_bc[16:21]
    raw_tn5_b = raw_bc[21:26]

    # Illumina i5 index reads are the reverse complement of the whitelist (legacy behaviour);
    # AVITI Index 1 reads are already in whitelist orientation.
    seq_10x = reverse_complement(raw_10x) if tenx_revcomp else raw_10x

    # 1. Correct 10x (exact + 1-mismatch, unique)
    corrected_10x, status_10x = correct_10x_one_mismatch(seq_10x, tenx_wl)
    if corrected_10x is None:
        return None, 2

    # 2. Check/Correct Tn5 A (exact + 1-mismatch via tn5_map)
    corr_tn5_a = tn5_map.get(raw_tn5_a)
    if not corr_tn5_a:
        return None, 2

    # 3. Check/Correct Tn5 B
    corr_tn5_b = tn5_map.get(raw_tn5_b)
    if not corr_tn5_b:
        return None, 2

    final_bc = corrected_10x + corr_tn5_a + corr_tn5_b

    corrected_any_tn5 = (corr_tn5_a != raw_tn5_a) or (corr_tn5_b != raw_tn5_b)
    status = 0 if (status_10x == 0 and not corrected_any_tn5) else 1

    return final_bc, status


def _raw_tenx_from_qname(qname: str) -> Optional[str]:
    """16 bp 10x segment of a '<id>_<10x>_<Tn5A>_<Tn5B>' read name, or None."""
    parts = qname.split("_")
    if len(parts) < 4 or len(parts[-3]) != 16:
        return None
    return parts[-3]


def detect_tenx_orientation(
    input_bam: Path,
    tenx_wl: Set[str],
    n_reads: int = AUTO_ORIENT_READS,
    min_match: float = AUTO_ORIENT_MIN,
) -> str:
    """
    Decide whether the 10x barcodes in the read names must be reverse-complemented to match
    the whitelist. Samples the first n_reads reads with a parseable barcode and counts exact
    whitelist hits as-is and reverse-complemented. Returns "forward" or "revcomp".

    Raises RuntimeError when the better orientation matches fewer than min_match of the reads,
    so that a wrong whitelist or malformed names fail loudly instead of silently emptying the BAM.
    """
    n = fwd = rc = 0
    with pysam.AlignmentFile(str(input_bam), "rb") as fh:
        for read in fh:
            raw = _raw_tenx_from_qname(read.query_name)
            if raw is None:
                continue
            n += 1
            fwd += raw in tenx_wl
            rc += reverse_complement(raw) in tenx_wl
            if n >= n_reads:
                break
    if n == 0:
        raise RuntimeError(f"[orientation] no read name with a 16 bp 10x segment in {input_bam}")
    f_fwd, f_rc = fwd / n, rc / n
    orientation = "forward" if f_fwd >= f_rc else "revcomp"
    print(
        f"[INFO] 10x orientation: {orientation} (exact whitelist match on {n} reads: "
        f"forward {f_fwd:.1%}, reverse-complement {f_rc:.1%})",
        file=sys.stderr,
    )
    if max(f_fwd, f_rc) < min_match:
        raise RuntimeError(
            f"[orientation] 10x barcodes match the whitelist in neither orientation "
            f"(forward {f_fwd:.1%}, reverse-complement {f_rc:.1%} of {n} reads). "
            f"Check the whitelist and the read names, or force --tenx-orientation."
        )
    return orientation


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
    tenx_orientation: str = "auto",
) -> Tuple[int, int, int]:
    """
    Scifi-ATAC pre-dedup cleaning step:

      - Filter reads:
          * mapped
          * mapping_quality >= min_mapq
          * proper pair (FLAG 0x2)
      - Extract concatenated BC from read name suffix
      - Validate 10x part (exact or unique 1-mismatch, in the orientation given by
        tenx_orientation: 'revcomp' = Illumina i5 index read (legacy), 'forward' = AVITI
        Index 1, 'auto' = detect from the first reads), and 1-mismatch-correct Tn5 parts
      - Attach corrected BC as 'BC' tag
      - Write passing reads to output BAM

    Returns:
      (total_reads, pass_mapq, pass_bc)
    """
    if tenx_orientation not in TENX_ORIENTATIONS:
        raise ValueError(f"tenx_orientation must be one of {TENX_ORIENTATIONS}, got {tenx_orientation!r}")
    tenx_wl, tn5_map = load_whitelists(whitelist_10x, whitelist_tn5)
    if tenx_orientation == "auto":
        tenx_orientation = detect_tenx_orientation(input_bam, tenx_wl)
    else:
        print(f"[INFO] 10x orientation: {tenx_orientation} (forced)", file=sys.stderr)
    tenx_revcomp = tenx_orientation == "revcomp"

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
            if len(qname_parts) < 4:
                continue

            raw_bc_seq = "".join(qname_parts[-3:])

            # 3. Correct barcodes
            corrected_bc, status = correct_barcodes(raw_bc_seq, tenx_wl, tn5_map, tenx_revcomp=tenx_revcomp)
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
    print(f"  Passed BC Correction:  {count_pass_bc}  (10x orientation: {tenx_orientation})", file=sys.stderr)
    if count_pass_mq and count_pass_bc < 0.2 * count_pass_mq:
        print(
            f"[WARNING] only {count_pass_bc / count_pass_mq:.1%} of the MAPQ-passing reads carry a valid "
            f"barcode; check the whitelist and --tenx-orientation",
            file=sys.stderr,
        )

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
    parser.add_argument(
        "--tenx-orientation", choices=TENX_ORIENTATIONS, default="auto",
        help="10x barcode orientation vs the whitelist: auto (detect), forward (AVITI), revcomp (Illumina i5)",
    )

    args = parser.parse_args(argv)

    scifi_cleanup_bam(
        input_bam=args.input_bam,
        output_bam=args.output_bam,
        whitelist_10x=args.whitelist_10x,
        whitelist_tn5=args.whitelist_tn5,
        min_mapq=args.min_mapq,
        threads=args.threads,
        tenx_orientation=args.tenx_orientation,
    )


if __name__ == "__main__":
    main()
