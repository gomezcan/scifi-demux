#!/usr/bin/env python3
import argparse
import sys
import pysam

ap = argparse.ArgumentParser(
    description="Emit Tn5 cut sites from a BAM as 1bp BED records (chrom, cut, cut+1, BC, strand).",
)
ap.add_argument("bam", help="Path to BAM file (indexed if --region is used).")
ap.add_argument(
    "--region",
    default=None,
    help="Optional contig name (e.g., chr1) to restrict iteration. "
         "Default: iterate all reads.",
)
args = ap.parse_args()

bamfile = pysam.AlignmentFile(args.bam, "rb")
counts = 0

it = bamfile.fetch(contig=args.region) if args.region else bamfile.fetch()

for read in it:
    counts += 1
    if counts % 1_000_000 == 0:
        print(f" - iterated over {counts} reads …", file=sys.stderr)

    # skip unmapped reads
    if read.is_unmapped:
        continue

    # get BC tag (or NA)
    tags = dict(read.get_tags())
    bc = tags.get('BC', 'NA')

    # compute Tn5 cut site (+4 / -5)
    if read.is_reverse:
        cut = read.reference_end - 5
        strand = '-'
    else:
        cut = read.reference_start + 4
        strand = '+'

    # emit 1 bp BED record
    print(f"{read.reference_name}\t{cut}\t{cut+1}\t{bc}\t{strand}")

bamfile.close()
