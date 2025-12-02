# src/scifi_demux/utils/scifi_fixBC.py

import argparse
import sys
from collections import defaultdict, Counter
from typing import Dict

import pysam


def process_and_count(
    input_bam: str,
    output_bam: str,
    counts_file: str,
    library_tag: str,
    threads: int = 4,
    tissue_label: str = "leaf",
) -> None:
    """
    Post-MarkDuplicates cleanup:

      - XA-based multi-mapping filter:
          if MAPQ < 30 and XA present, check alt NM vs current NM;
          if more than 1 'near' hit (alt_nm - current_nm < 3), drop the read.
      - Drop reads where CBK == 'F'.
      - Append '-{library_tag}' suffix to BC tag.
      - Count per-BC total, Pt, Mt, and write stats to counts_file.

    This is designed to mirror the behavior of 1_5_scifi_fixBC.pl.
    """

    # Structure: { barcode: Counter({'total': ..., 'pt': ..., 'mt': ...}) }
    stats: Dict[str, Counter] = defaultdict(Counter)

    # Open BAMs
    infile = pysam.AlignmentFile(input_bam, "rb", threads=threads)
    outfile = pysam.AlignmentFile(output_bam, "wb", template=infile, threads=threads)

    count_total_read = 0
    count_written = 0

    print(f"[INFO] Post-processing BAM for library '{library_tag}'...", file=sys.stderr)

    for read in infile:
        count_total_read += 1

        # ---------------------------------------------------------
        # 1. Multi-mapping filter (XA + NM based)
        # ---------------------------------------------------------
        if read.mapping_quality < 30 and read.has_tag("XA"):
            try:
                current_nm = read.get_tag("NM")
                xa_str = read.get_tag("XA")

                near_hits = 0
                alignments = [x for x in xa_str.split(";") if x]

                for aln in alignments:
                    parts = aln.split(",")
                    if len(parts) > 0:
                        alt_nm = int(parts[-1])
                        diff = alt_nm - current_nm
                        if diff < 3:
                            near_hits += 1

                # strict: if near_hits > 1, drop read
                if near_hits > 1:
                    continue

            except (KeyError, ValueError, IndexError):
                # if malformed tags, keep the read but don't crash
                pass

        # ---------------------------------------------------------
        # 2. CBK tag filter
        # ---------------------------------------------------------
        if read.has_tag("CBK"):
            if read.get_tag("CBK") == "F":
                continue

        # ---------------------------------------------------------
        # 3. Barcode update & counting
        # ---------------------------------------------------------
        if read.has_tag("BC"):
            old_bc = read.get_tag("BC")
            new_bc = f"{old_bc}-{library_tag}"

            read.set_tag("BC", new_bc, value_type="Z")

            stats[new_bc]["total"] += 1

            rname = read.reference_name or ""
            if "Pt" in rname:
                stats[new_bc]["pt"] += 1
            elif "Mt" in rname:
                stats[new_bc]["mt"] += 1

            outfile.write(read)
            count_written += 1

    infile.close()
    outfile.close()

    # ---------------------------------------------------------
    # 4. Write counts file
    # ---------------------------------------------------------
    print(f"[INFO] Writing counts to {counts_file}...", file=sys.stderr)

    with open(counts_file, "w") as f:
        f.write("cellID\ttotal\tnuclear\tPt\tMt\tlibrary\ttissue\n")

        # sort by total descending
        sorted_bcs = sorted(stats.keys(), key=lambda k: stats[k]["total"], reverse=True)

        for bc in sorted_bcs:
            total = stats[bc]["total"]
            pt = stats[bc]["pt"]
            mt = stats[bc]["mt"]
            nuclear = total - pt - mt

            f.write(
                f"{bc}\t{total}\t{nuclear}\t{pt}\t{mt}\t{library_tag}\t{tissue_label}\n"
            )

    print(
        f"[INFO] Finished. Processed {count_total_read} reads, wrote {count_written}.",
        file=sys.stderr,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Post-MarkDuplicates cleanup (XA filter + BC Tagging)"
    )

    # roughly parallel to the Perl script’s arguments
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
        tissue_label=args.tissue,
    )


if __name__ == "__main__":
    main()
