# src/scifi_demux/demux_core_cli.py
import argparse
from pathlib import Path
from .demux_core import demux_split_barcodes

def main():
    p = argparse.ArgumentParser(description="Demultiplex FASTQ by split Tn5 barcodes (5+5 bp).")
    p.add_argument("layout_file")
    p.add_argument("input_fastq")
    p.add_argument("sample_well_map")
    p.add_argument("--output", default=None)
    p.add_argument("--max-mismatch-per-half", type=int, default=1)
    args = p.parse_args()
    demux_split_barcodes(
        Path(args.layout_file),
        Path(args.input_fastq),
        Path(args.sample_well_map),
        output_dir=Path(args.output) if args.output else None,
        max_mismatch_per_half=args.max_mismatch_per_half,
    )

if __name__ == "__main__":
    main()
