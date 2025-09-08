from pathlib import Path
from .config import Design
from .io_utils import find_fastqs
from .strategies import demux_by_wells, demux_by_samples


def run_demux(strategy: str, fastq_dir: Path, design_path: Path, out: Path, mode: str = "link", dry_run: bool = False) -> None:
    """
    Main demultiplexing runner function that coordinates the demultiplexing process.
    
    This function serves as an entry point for demultiplexing operations, handling
    configuration loading, file discovery, and routing to the appropriate
    demultiplexing strategy.
    
    Args:
        strategy: Demultiplexing strategy to use. Valid options are:
                 - "wells-by-plate": Organize by well positions
                 - "sample-design": Organize by sample names
        fastq_dir: Directory containing FASTQ files to be processed
        design_path: Path to YAML design file containing sample/well mapping
        out: Output directory where demultiplexed files will be placed
        mode: File operation mode - "link" (symlinks) or "copy" (file copies)
        dry_run: If True, only log actions without performing file operations
    
    Returns:
        None
    
    Raises:
        ValueError: If an unknown strategy is specified
        FileNotFoundError: If design file or FASTQ directory doesn't exist
        YAMLError: If design file has invalid YAML format
    """
    # Load sample/well mapping design from YAML configuration file
    design = Design.from_yaml(design_path)
    
    # Discover all FASTQ files in the specified directory
    # Typically looks for files with .fastq, .fq, .fastq.gz, .fq.gz extensions
    fastqs = find_fastqs(fastq_dir)
    
    # Route to the appropriate demultiplexing strategy based on user selection
    if strategy == "wells-by-plate":
        # Organize files by well positions (e.g., A:1, B:2, etc.)
        demux_by_wells(fastqs, design, out, mode=mode, dry_run=dry_run)
    elif strategy == "sample-design":
        # Organize files by sample names mapped from well positions
        demux_by_samples(fastqs, design, out, mode=mode, dry_run=dry_run)
    else:
        # Handle invalid strategy selection with clear error message
        raise ValueError(f"Unknown strategy: {strategy}")
