import argparse
import sys
import pysam
from pathlib import Path
from typing import Dict, Set, Tuple, Optional

# Constants
DNA_COMPLEMENT = str.maketrans("ACGTNacgtn", "TGCANtgcan")

def reverse_complement(seq: str) -> str:
    """Fast reverse complement."""
    return seq.translate(DNA_COMPLEMENT)[::-1]

def load_whitelists(tenx_path: Path, tn5_path: Path) -> Tuple[Set[str], Dict[str, Tuple[str, int]]]:
    """
    Load 10x whitelist into a Set (exact match only).
    Load Tn5 whitelist and pre-calculate 1-mismatch variants.
    
    Returns:
        tenx_wl: Set of valid 10x barcodes
        tn5_map: Dict mapping { observed_seq -> (corrected_seq, hamming_distance) }
    """
    print(f"[INFO] Loading 10x whitelist: {tenx_path}", file=sys.stderr)
    tenx_wl = set()
    with open(tenx_path, 'r') as f:
        for line in f:
            if line.strip():
                tenx_wl.add(line.strip().split()[0])

    print(f"[INFO] Loading Tn5 whitelist: {tn5_path}", file=sys.stderr)
    
    # map: sequence -> (corrected_sequence, cost)
    tn5_map: Dict[str, Tuple[str, int]] = {}
    valid_tn5 = []
    
    with open(tn5_path, 'r') as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 2:
                seq = parts[1]
                valid_tn5.append(seq)
                tn5_map[seq] = (seq, 0) # Exact match, cost 0

    # Generate 1-mismatch lookups
    bases = ['A', 'C', 'G', 'T', 'N']
    
    for valid_seq in valid_tn5:
        for i in range(len(valid_seq)):
            for b in bases:
                if b == valid_seq[i]: continue
                
                # Create mutation
                mutant = valid_seq[:i] + b + valid_seq[i+1:]
                
                # COLLISION CHECK:
                # If a mutant is already a VALID sequence (dist=0), don't overwrite it.
                # If a mutant maps to two DIFFERENT valid sequences, mark as ambiguous (None).
                
                if mutant not in tn5_map:
                    tn5_map[mutant] = (valid_seq, 1) # Correction, cost 1
                else:
                    existing_seq, existing_cost = tn5_map[mutant]
                    
                    # If we found a path with LOWER cost (e.g., 0), keep the lower cost one.
                    if existing_cost == 0:
                        continue
                        
                    # If we map to a different valid seq with same cost, it's ambiguous.
                    if existing_seq != valid_seq:
                        tn5_map[mutant] = (None, 99) # Mark as invalid/ambiguous

    # Clean up ambiguous entries
    final_map = {k: v for k, v in tn5_map.items() if v[0] is not None}
    
    return tenx_wl, final_map

def correct_barcodes(
    raw_bc: str, 
    tenx_wl: Set[str], 
    tn5_map: Dict[str, Tuple[str, int]]
) -> Tuple[Optional[str], int]:
    """
    Validates and corrects the barcode construct.
    Structure assumption: 10x (16bp) + Tn5_A (5bp) + Tn5_B (5bp) = 26bp total suffix.
    
    Strict Rule: Total Hamming Distance (Tn5_A + Tn5_B) <= 1.
    """
    # 1. Length Check
    # The read name might contain the full sequence or just the barcode suffix.
    # We assume the barcode is the LAST 26 chars.
    if len(raw_bc) < 26:
        return None, 2

    # Extract Segments
    # Based on legacy logic: 10x is RC'd, Tn5s are forward.
    raw_10x_rc = raw_bc[0:16] # The first 16 bases are the 10x barcode (Reverse Complemented in library)
    raw_tn5_a  = raw_bc[16:21]
    raw_tn5_b  = raw_bc[21:26]
    
    # 2. Check 10x (Exact Match Only)
    # The 10x whitelist is huge (737k). Fuzzy matching here is too slow and risky.
    seq_10x = reverse_complement(raw_10x_rc)
    if seq_10x not in tenx_wl:
        return None, 2 

    # 3. Lookup Tn5 Segments (O(1) lookup)
    res_a = tn5_map.get(raw_tn5_a)
    res_b = tn5_map.get(raw_tn5_b)

    if not res_a or not res_b:
        return None, 2 

    corr_tn5_a, cost_a = res_a
    corr_tn5_b, cost_b = res_b

    # 4. STRICT MUTATION LIMIT
    # We allow max 1 mutation total across the Tn5 pairs.
    total_cost = cost_a + cost_b
    if total_cost > 1:
        return None, 2

    # Construct Final
    final_bc = seq_10x + corr_tn5_a + corr_tn5_b
    
    # Status: 0 = Exact, 1 = Corrected
    status = 1 if total_cost > 0 else 0
    
    return final_bc, status

def main():
    parser = argparse.ArgumentParser(description="Scifi-ATAC Pre-Dedup Cleaning (Strict)")
    parser.add_argument("--input-bam", required=True)
    parser.add_argument("--output-bam", required=True)
    parser.add_argument("--whitelist-10x", required=True)
    parser.add_argument("--whitelist-tn5", required=True)
    parser.add_argument("--min-mapq", type=int, default=20)
    parser.add_argument("--threads", type=int, default=4)
    
    args = parser.parse_args()

    # Load resources
    try:
        tenx_wl, tn5_map = load_whitelists(Path(args.whitelist_10x), Path(args.whitelist_tn5))
    except FileNotFoundError as e:
        sys.exit(f"[ERROR] Could not load whitelists: {e}")

    infile = pysam.AlignmentFile(args.input_bam, "rb", threads=args.threads)
    # Enable header replication
    outfile = pysam.AlignmentFile(args.output_bam, "wb", template=infile, threads=args.threads)

    count_total = 0
    count_pass_mq = 0
    count_pass_bc = 0

    print(f"[INFO] Processing BAM {args.input_bam}...", file=sys.stderr)

    for read in infile:
        count_total += 1
        
        # Legacy Filter 1: MapQ and Proper Pair flag (0x2)
        if read.is_unmapped or read.mapping_quality < args.min_mapq or not read.is_proper_pair:
            continue
            
        count_pass_mq += 1

        # Extract Barcode from Read Name
        # Expected format: @READNAME_SEQ...
        # We split by "_" and take the last chunk.
        try:
            qname_parts = read.query_name.split("_")
            if len(qname_parts) < 2:
                continue
            raw_bc_seq = qname_parts[-1]
        except AttributeError:
            continue

        # Correct
        corrected_bc, status = correct_barcodes(raw_bc_seq, tenx_wl, tn5_map)

        if corrected_bc:
            # Set BC tag
            read.set_tag("BC", corrected_bc, value_type="Z")
            
            # Optional: Clean query name to reduce file size? 
            # Legacy script kept the full name, so we keep it to be safe.
            
            outfile.write(read)
            count_pass_bc += 1

    infile.close()
    outfile.close()

    print(f"[INFO] Complete.", file=sys.stderr)
    print(f"  Total Reads: {count_total}", file=sys.stderr)
    print(f"  Passed MapQ/Flag: {count_pass_mq}", file=sys.stderr)
    print(f"  Passed BC Correction: {count_pass_bc}", file=sys.stderr)

if __name__ == "__main__":
    main()
