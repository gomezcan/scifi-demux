#!/usr/bin/env python3
import sys
import pysam

bamfile = pysam.AlignmentFile(sys.argv[1], "rb")
counts = 0

for read in bamfile.fetch():
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
