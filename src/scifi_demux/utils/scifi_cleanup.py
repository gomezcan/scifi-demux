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

def load_whitelists(tenx_path: Path, tn5_path: Path) -> Tuple[Set[str], Dict[str, str]]:
    """
    Load 10x whitelist into a Set (for exact match).
    Load Tn5 whitelist and pre-calculate 1-mismatch variants map.
    """
    print(f"[INFO] Loading 10x whitelist: {tenx_path}", file=sys.stderr)
    tenx_wl = set()
    with open(tenx_path, 'r') as f:
        for line in f:
            tenx_wl.add(line.strip().split('\t')[0])

    print(f"[INFO] Loading Tn5 whitelist: {tn5_path}", file=sys.stderr)
    # Map valid_barcode -> valid_barcode (identity)
    tn5_map = {} 
    valid_tn5 = []
    
    with open(tn5_path, 'r') as f:
        for line in f:
            # Format: name \t sequence \t tag
            parts = line.strip().split('\t')
            if len(parts) >= 2:
                seq = parts[1]
                valid_tn5.append(seq)
                tn5_map[seq] = seq

    # Generate 1-mismatch lookups for Tn5 (since list is small ~100 items)
    bases = ['A', 'C', 'G', 'T', 'N']
    for valid_seq in valid_tn5:
        for i in range(len(valid_seq)):
            for b in bases:
                if b == valid_seq[i]: continue
                # Create mutation
                mutant = valid_seq[:i] + b + valid_seq[i+1:]
                # Only map if unambiguous (not already mapped to another valid bc)
                if mutant not in tn5_map:
                    tn5_map[mutant] = valid_seq
                elif tn5_map[mutant] != valid_seq:
                    # Ambiguous (mapped to two different valid BCs) -> remove
                    tn5_map[mutant] = None 

    return tenx_wl, tn5_map

def correct_barcodes(
    raw_bc: str, 
    tenx_wl: Set[str], 
    tn5_map: Dict[str, str]
) -> Tuple[Optional[str], int]:
    """
    Parses 'seq_first_second' from raw read name.
    Returns (Corrected_Full_BC, status_code) or (None, fail_code).
    Status: 0=Exact, 1=Corrected, 2=Fail
    """
    # Raw format often: ..._SEQ_FIRST_SECOND (based on legacy script 1_3)
    # Legacy script logic[cite: 157]: 
    #   full = substr(5, len); seq = substr(5, 16) [RC]; first = substr(21, 5); second = substr(26, 5)
    #   This implies the string is roughly 31 chars long? 
    #   Let's assume the string passed here is the suffix: SEQFIRSTSECOND (26 chars)
    
    if len(raw_bc) < 26:
        return None, 2

    # Parse segments (using offsets from legacy script 1_3 logic)
    # The legacy script splits complex strings, but assuming we isolate the BC string:
    # 10x part is usually 16bp
    # Tn5 parts are 5bp each
    
    # Note: Legacy 1_3 performs Reverse Complement on the 10x part [cite: 158]
    raw_10x = raw_bc[0:16]
    raw_tn5_a = raw_bc[16:21]
    raw_tn5_b = raw_bc[21:26]
    
    seq_10x = reverse_complement(raw_10x)

    # 1. Check 10x (Exact match only for speed on 737k list)
    if seq_10x not in tenx_wl:
        return None, 2 # Fail 10x

    # 2. Check/Correct Tn5 A
    corr_tn5_a = tn5_map.get(raw_tn5_a)
    if not corr_tn5_a:
        return None, 2 # Fail Tn5 A

    # 3. Check/Correct Tn5 B
    corr_tn5_b = tn5_map.get(raw_tn5_b)
    if not corr_tn5_b:
        return None, 2 # Fail Tn5 B

    # Construct final corrected BC
    final_bc = seq_10x + corr_tn5_a + corr_tn5_b
    
    # Check if any correction happened
    status = 1 if (corr_tn5_a != raw_tn5_a or corr_tn5_b != raw_tn5_b) else 0
    
    return final_bc, status

def main():
    parser = argparse.ArgumentParser(description="Scifi-ATAC Pre-Dedup Cleaning")
    parser.add_argument("--input-bam", required=True)
    parser.add_argument("--output-bam", required=True)
    parser.add_argument("--whitelist-10x", required=True)
    parser.add_argument("--whitelist-tn5", required=True)
    parser.add_argument("--min-mapq", type=int, default=20)
    parser.add_argument("--threads", type=int, default=4)
    
    args = parser.parse_args()

    # Load resources
    tenx_wl, tn5_map = load_whitelists(Path(args.whitelist_10x), Path(args.whitelist_tn5))

    # I/O
    # "rb" = read bam, "wb" = write bam
    infile = pysam.AlignmentFile(args.input_bam, "rb", threads=args.threads)
    outfile = pysam.AlignmentFile(args.output_bam, "wb", template=infile, threads=args.threads)

    count_total = 0
    count_pass_mq = 0
    count_pass_bc = 0

    print("[INFO] Processing BAM...", file=sys.stderr)

    for read in infile:
        count_total += 1
        
        # 1. Filter: Unmapped, Low MapQ, Not Proper Pair
        # Legacy script 1_1/Step2 filters: -q {mapq} -f 3 (proper pair) [cite: 343]
        if read.is_unmapped or read.mapping_quality < args.min_mapq or not read.is_proper_pair:
            continue
            
        count_pass_mq += 1

        # 2. Extract Barcode from Name
        # Format expected: @READNAME_SEQFIRSTSECOND (underscores split)
        # Legacy script 1_1 splits by "_" and takes parts [cite: 150]
        qname_parts = read.query_name.split("_")
        
        # Guard against malformed names
        if len(qname_parts) < 2:
            continue
            
        # Assuming the BC is the LAST part of the read name (common in demuxers)
        # Or based on legacy script 1_3: it parses specific substrings.
        # We assume the barcode sequence is appended to the name.
        raw_bc_seq = qname_parts[-1] 

        # 3. Correct Barcodes
        corrected_bc, status = correct_barcodes(raw_bc_seq, tenx_wl, tn5_map)

        if corrected_bc:
            # Update Read Tags
            # BC = Corrected Barcode
            read.set_tag("BC", corrected_bc, value_type="Z")
            
            # Optional: Add status tag if you want to track corrections
            # read.set_tag("BS", status, value_type="i") 
            
            # Clean up query name (remove the barcode suffix to save space?)
            # Legacy script 1_1 puts just the readid[cite: 150]. 
            # read.query_name = "_".join(qname_parts[:-1]) 

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
