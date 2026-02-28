# test/test_fixes.py
"""Tests for all bug fixes applied to scifi-demux."""
from __future__ import annotations

import gzip
import logging
import warnings
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Fix 3 + 12: logging produces exactly one handler
# ---------------------------------------------------------------------------
def test_logging_single_handler():
    from scifi_demux.utils.logging import setup_logging

    # Seed root logger with extra handlers
    root = logging.getLogger()
    for _ in range(3):
        root.addHandler(logging.StreamHandler())

    setup_logging(1)
    assert len(root.handlers) == 1, f"Expected 1 handler, got {len(root.handlers)}"

    # Clean up
    setup_logging(0)


# ---------------------------------------------------------------------------
# Fix 1: run_demux dispatches to the correct strategy
# ---------------------------------------------------------------------------
def test_run_demux_dispatch_wells_by_plate(tmp_path: Path):
    from scifi_demux.cli.main import run_demux

    # Create minimal YAML and a dummy FASTQ
    yaml_path = tmp_path / "design.yaml"
    yaml_path.write_text("plates: []\nsamples: []\n")
    fq_dir = tmp_path / "fastqs"
    fq_dir.mkdir()
    (fq_dir / "sample_R1.fastq.gz").write_bytes(b"")

    with patch("scifi_demux.cli.main.demux_by_wells") as mock_wells:
        run_demux("wells-by-plate", fq_dir, yaml_path, tmp_path / "out")
        mock_wells.assert_called_once()


def test_run_demux_dispatch_sample_design(tmp_path: Path):
    from scifi_demux.cli.main import run_demux

    yaml_path = tmp_path / "design.yaml"
    yaml_path.write_text("plates: []\nsamples: []\n")
    fq_dir = tmp_path / "fastqs"
    fq_dir.mkdir()
    (fq_dir / "sample_R1.fastq.gz").write_bytes(b"")

    with patch("scifi_demux.cli.main.demux_by_samples") as mock_samples:
        run_demux("sample-design", fq_dir, yaml_path, tmp_path / "out")
        mock_samples.assert_called_once()


# ---------------------------------------------------------------------------
# Fix 4: demux_by_split_bc returns non-empty counts
# ---------------------------------------------------------------------------
def test_demux_by_split_bc_returns_counts(tmp_path: Path):
    """Create a tiny synthetic FASTQ and verify demux returns actual counts."""
    from scifi_demux.demux_core import load_barcode_layout, load_sample_to_wells

    # Build a minimal barcode layout (1 well)
    layout = tmp_path / "layout.tsv"
    layout.write_text("\t1\nA\tAAAAA_TTTTT\n")

    # Build a minimal sample-well map
    sample_map = tmp_path / "map.txt"
    sample_map.write_text("Pool1\tA1\n")

    # Build a FASTQ whose read names end with _AAAAA_TTTTT (exact match)
    fq_path = tmp_path / "part_001_R1.bc1.bc2.fastq.gz"
    with gzip.open(fq_path, "wt") as fh:
        for i in range(5):
            fh.write(f"@read{i}_AAAAA_TTTTT\n")
            fh.write("ACGTACGTAC\n")
            fh.write("+\n")
            fh.write("IIIIIIIIII\n")

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    from scifi_demux.steps.primitives import demux_by_split_bc

    result = demux_by_split_bc(
        layout_file=layout,
        sample_well_map=sample_map,
        input_fastq_gz=fq_path,
        out_dir=out_dir,
    )

    assert "Pool1" in result, f"Expected 'Pool1' in result, got {result}"
    assert result["Pool1"]["r1_reads"] == 5, f"Expected 5 R1 reads, got {result['Pool1']}"
    assert result["Pool1"]["r3_reads"] == 0, "R3 should be 0 for R1 input"


# ---------------------------------------------------------------------------
# Fix 2: legacy_script_path resolves bundled scripts
# ---------------------------------------------------------------------------
def test_legacy_script_path():
    from scifi_demux.io_utils import legacy_script_path

    p = legacy_script_path("1_6_scifi_makeTn5bed.py")
    assert p.exists(), f"Script not found at {p}"


def test_legacy_script_path_missing():
    from scifi_demux.io_utils import legacy_script_path

    with pytest.raises(FileNotFoundError):
        legacy_script_path("nonexistent_script.py")


# ---------------------------------------------------------------------------
# Fix 5: version.py matches pyproject.toml
# ---------------------------------------------------------------------------
def test_version_matches_pyproject():
    from scifi_demux import __version__

    toml_path = Path(__file__).resolve().parent.parent / "pyproject.toml"
    if not toml_path.exists():
        pytest.skip("pyproject.toml not found (installed from wheel?)")

    for line in toml_path.read_text().splitlines():
        if line.strip().startswith("version"):
            # version = "0.1.2"  # optional comment
            raw = line.split("=", 1)[1].strip()
            # Strip inline comment, then surrounding quotes
            if "#" in raw:
                raw = raw[: raw.index("#")].strip()
            toml_version = raw.strip('"').strip("'")
            break
    else:
        pytest.fail("Could not find version in pyproject.toml")

    assert __version__ == toml_version, f"version.py={__version__} != pyproject.toml={toml_version}"


# ---------------------------------------------------------------------------
# Fix 9: example design.yaml parses successfully
# ---------------------------------------------------------------------------
def test_example_design_yaml_parses():
    from scifi_demux.config import Design

    yaml_path = Path(__file__).resolve().parent.parent / "example_configs" / "design.yaml"
    if not yaml_path.exists():
        pytest.skip("example_configs/design.yaml not found")

    d = Design.from_yaml(yaml_path)
    assert len(d.plates) == 2
    assert len(d.samples) == 2
    assert d.plates[0].name == "PlateA"


# ---------------------------------------------------------------------------
# Fix 8: config validator warns on unknown well references
# ---------------------------------------------------------------------------
def test_config_warns_on_bad_well_ref():
    from scifi_demux.config import Design

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        Design(
            plates=[],
            samples=[{"name": "S1", "wells": ["PlateX:Z99"]}],
        )
    assert any("PlateX:Z99" in str(x.message) for x in w), f"Expected warning about PlateX:Z99, got {w}"


def test_config_no_warn_on_valid_ref():
    from scifi_demux.config import Design, Plate

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        Design(
            plates=[Plate(name="P1", wells={"A01": "ACGT"})],
            samples=[{"name": "S1", "wells": ["P1:A01"]}],
        )
    plate_warnings = [x for x in w if "not defined in any plate" in str(x.message)]
    assert len(plate_warnings) == 0, f"Unexpected warnings: {plate_warnings}"
