import argparse
import sys
import pysam
from collections import defaultdict
from typing import Dict, Counter

def process_and_count(
    input_bam: str,
    output_bam: str,
    counts_file: str,
    library_tag: str,
    threads: int = 4,
    tissue_label: str = "leaf"
):
    # Counters
    # Structure: { barcode: { 'total': 0, 'pt': 0, 'mt': 0 } }
    stats: Dict[str, Counter] = defaultdict(Counter)

    # Open BAMs
    # "rb" = read BAM, "wb" = write BAM
    infile = pysam.AlignmentFile(input_bam, "rb", threads=threads)
    outfile = pysam.AlignmentFile(output_bam, "wb", template=infile, threads=threads)

    count_total_read = 0
    count_written = 0

    print(f"[INFO] Post-processing BAM for library '{library_tag}'...", file=sys.stderr)

    for read in infile:
        count_total_read += 1
        
        # ---------------------------------------------------------
        # 1. Filter: Multi-mapping check (XA tag logic)
        # ---------------------------------------------------------
        # Perl logic: if (XA exists AND mapq < 30):
        #   check diff between XA NM and current NM. 
        #   if diff < 3: near++
        #   if near > 1: skip read
        if read.mapping_quality < 30 and read.has_tag("XA"):
            try:
                # NM:i: tag (Edit distance)
                current_nm = read.get_tag("NM")
                
                # XA:Z: tag (Alternative hits)
                # Format: chr,pos,CIGAR,NM;next_hit;...
                xa_str = read.get_tag("XA")
                
                near_hits = 0
                
                # Split by ';' to get alignments, filter empty strings
                alignments = [x for x in xa_str.split(';') if x]
                
                for aln in alignments:
                    parts = aln.split(',')
                    # The NM value is the last element in the XA comma-list
                    if len(parts) > 0:
                        alt_nm = int(parts[-1])
                        diff = alt_nm - current_nm
                        
                        # "Too close" check
                        if diff < 3:
                            near_hits += 1
                
                # Strict Perl translation: if ($near > 1) { next; }
                if near_hits > 1:
                    continue

            except (KeyError, ValueError, IndexError):
                # If tags are missing or malformed, rely on default behavior (keep read)
                pass

        # ---------------------------------------------------------
        # 2. Filter: CBK tag check
        # ---------------------------------------------------------
        if read.has_tag("CBK"):
            if read.get_tag("CBK") == "F":
                continue

        # ---------------------------------------------------------
        # 3. Barcode Update & Counting
        # ---------------------------------------------------------
        if read.has_tag("BC"):
            old_bc = read.get_tag("BC")
            
            # Append library tag
            new_bc = f"{old_bc}-{library_tag}"
            
            # Update the read
            read.set_tag("BC", new_bc, value_type="Z")
            
            # Count logic
            stats[new_bc]['total'] += 1
            
            # Check reference name for Plastid/Mitochondria
            # Perl used regex: =~ /Pt/ and =~ /Mt/
            rname = read.reference_name
            if "Pt" in rname:
                stats[new_bc]['pt'] += 1
            elif "Mt" in rname:
                stats[new_bc]['mt'] += 1
            
            # Write to output
            outfile.write(read)
            count_written += 1

    infile.close()
    outfile.close()

    # ---------------------------------------------------------
    # 4. Write Counts File
    # ---------------------------------------------------------
    print(f"[INFO] Writing counts to {counts_file}...", file=sys.stderr)
    
    with open(counts_file, 'w') as f:
        # Header matches Perl script
        f.write("cellID\ttotal\tnuclear\tPt\tMt\tlibrary\ttissue\n")
        
        # Sort by total count descending (perl: sort { $bcs{$b} <=> $bcs{$a} })
        sorted_bcs = sorted(stats.keys(), key=lambda k: stats[k]['total'], reverse=True)
        
        for bc in sorted_bcs:
            total = stats[bc]['total']
            pt = stats[bc]['pt']
            mt = stats[bc]['mt']
            nuclear = total - pt - mt
            
            f.write(f"{bc}\t{total}\t{nuclear}\t{pt}\t{mt}\t{library_tag}\t{tissue_label}\n")

    print(f"[INFO] Finished. Processed {count_total_read} reads, wrote {count_written}.", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(description="Post-MarkDuplicates cleanup (XA filter + BC Tagging)")
    
    # Matching the Perl script arguments structure for ease of replacement
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--input-bam", required=True)
    parser.add_argument("--output-bam", required=True)
    parser.add_argument("--counts-file", required=True)
    parser.add_argument("--tag", required=True, help="Library tag to append to barcodes")
    parser.add_argument("--tissue", default="leaf", help="Tissue label for stats file")

    args = parser.parse_args()

    process_and_count(
        input_bam=args.input_bam,
        output_bam=args.output_bam,
        counts_file=args.counts_file,
        library_tag=args.tag,
        threads=args.threads,
        tissue_label=args.tissue
    )

if __name__ == "__main__":
    main()
