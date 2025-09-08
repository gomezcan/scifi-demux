from __future__ import annotations
from pathlib import Path

def write_step1_library_tsv(out: Path, library: str, raw: int, bc1: int, bc1bc2: int) -> None:
    """
    Write Step1 library summary statistics to a TSV file.
    
    This function creates a summary file for a single library's processing statistics,
    showing the progression of read pairs through different barcode filtering stages.
    
    Args:
        out: Output file path for the TSV
        library: Library identifier/name
        raw: Number of raw read pairs before any processing
        bc1: Number of read pairs after BC1 (first barcode) filtering
        bc1bc2: Number of read pairs after both BC1 and BC2 filtering
    
    Output format:
        library    raw_pairs    bc1_pairs    bc1bc2_pairs
        lib001     1000000      850000       800000
    """
    # Ensure output directory exists
    out.parent.mkdir(parents=True, exist_ok=True)
    
    # Write TSV with header and data row
    out.write_text(
        "library	raw_pairs	bc1_pairs	bc1bc2_pairs\n"
        f"{library}	{raw}	{bc1}	{bc1bc2}\n"
    )


def write_step1_groups_tsv(out: Path, rows: list[tuple[str, str, int, int, float]]) -> None:
    """
    Write Step1 group assignment statistics to a TSV file.
    
    This function creates a summary file showing how read pairs from each library
    were assigned to different sample groups, including assignment efficiency metrics.
    
    Args:
        out: Output file path for the TSV
        rows: List of tuples containing (library, group, assigned_pairs, 
               unassigned_pairs, fraction_assigned)
    
    Output format:
        library    group    assigned_pairs    unassigned_pairs    frac_assigned
        lib001     group1   500000            100000              0.833333
        lib001     group2   200000            100000              0.666667
    """
    # Ensure output directory exists
    out.parent.mkdir(parents=True, exist_ok=True)
    
    # Create header and format each data row
    header = "library	group	assigned_pairs	unassigned_pairs	frac_assigned\n"
    lines = [header] + [
        f"{lib}	{grp}	{ass}	{unas}	{frac:.6f}\n" 
        for lib, grp, ass, unas, frac in rows
    ]
    
    # Write all lines to file
    out.write_text("".join(lines))


def write_step2_task_tsv(out: Path, group: str, genome: str, mapped: int, proper: int, 
                        dup_frac: float, final_pairs: int, tn5_sites: int, final_frac: float) -> None:
    """
    Write Step2 mapping and quality statistics to a TSV file.
    
    This function creates a summary file for a single mapping task, showing
    mapping efficiency, duplicate rates, and final output statistics.
    
    Args:
        out: Output file path for the TSV
        group: Sample group identifier
        genome: Reference genome used for mapping
        mapped: Number of read pairs that mapped to the reference
        proper: Number of properly paired reads (insert size, orientation)
        dup_frac: Fraction of reads that are PCR duplicates (0.0-1.0)
        final_pairs: Final number of read pairs after deduplication
        tn5_sites: Number of unique Tn5 insertion sites detected
        final_frac: Fraction of input reads remaining in final output
    
    Output format:
        group    genome    mapped_pairs    proper_pairs    dup_fraction    final_pairs    tn5_sites    final_fraction_of_input
        group1   hg38      800000          750000          0.125000        700000         350000       0.700000
    """
    # Ensure output directory exists
    out.parent.mkdir(parents=True, exist_ok=True)
    
    # Write TSV with header and formatted data row
    header = "group	genome	mapped_pairs	proper_pairs	dup_fraction	final_pairs	tn5_sites	final_fraction_of_input\n"
    out.write_text(
        header + 
        f"{group}	{genome}	{mapped}	{proper}	{dup_frac:.6f}	{final_pairs}	{tn5_sites}	{final_frac:.6f}\n"
    )
