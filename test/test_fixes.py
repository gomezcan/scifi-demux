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


# ---------------------------------------------------------------------------
# Step 2 HPC mode helpers
# ---------------------------------------------------------------------------
def test_load_plan_row(tmp_path: Path):
    from scifi_demux.cli.main import _load_plan_row, _count_plan_rows

    tsv = tmp_path / "plan.tsv"
    tsv.write_text("# header\nPool1\tB73\t/ref/B73\nPool2\tMo17\t/ref/Mo17\t/samples/P2\n")

    assert _count_plan_rows(tsv) == 2

    row1 = _load_plan_row(tsv, 1)
    assert row1 == {"group": "Pool1", "genome": "B73", "ref_path": "/ref/B73"}

    row2 = _load_plan_row(tsv, 2)
    assert row2 == {"group": "Pool2", "genome": "Mo17", "ref_path": "/ref/Mo17", "sample_path": "/samples/P2"}

    with pytest.raises(IndexError):
        _load_plan_row(tsv, 0)
    with pytest.raises(IndexError):
        _load_plan_row(tsv, 3)


def test_resolve_threads_hpc():
    from scifi_demux.cli.main import _resolve_threads_hpc

    # Local mode always returns CLI value
    assert _resolve_threads_hpc("local", 24) == 24

    # HPC mode without env returns CLI value
    with patch.dict("os.environ", {}, clear=True):
        assert _resolve_threads_hpc("hpc", 24) == 24

    # HPC mode with SLURM env returns env value
    with patch.dict("os.environ", {"SLURM_CPUS_PER_TASK": "16"}):
        assert _resolve_threads_hpc("hpc", 24) == 16

    # HPC mode with NSLOTS (SGE) returns env value
    with patch.dict("os.environ", {"NSLOTS": "8"}, clear=True):
        assert _resolve_threads_hpc("hpc", 24) == 8


def _make_step2_fixtures(tmp_path):
    """Create plan TSV, state file, and dummy FASTQs for step2 HPC tests."""
    plan = tmp_path / "run_plan.map.tsv"
    plan.write_text("Pool1\tB73\t/ref/B73\nPool2\tMo17\t/ref/Mo17\n")

    state_path = tmp_path / "state.json"

    # Dummy FASTQs
    fq_dir = tmp_path / "combined"
    fq_dir.mkdir()
    (fq_dir / "Pool1_R1.bc1.bc2.fastq.gz").write_bytes(b"")
    (fq_dir / "Pool1_R3.bc1.bc2.fastq.gz").write_bytes(b"")
    (fq_dir / "Pool2_R1.bc1.bc2.fastq.gz").write_bytes(b"")
    (fq_dir / "Pool2_R3.bc1.bc2.fastq.gz").write_bytes(b"")

    return plan, state_path, fq_dir


def test_step2_hpc_single_dispatch(tmp_path: Path):
    """HPC mode with SLURM_ARRAY_TASK_ID=2 should process only row 2."""
    from scifi_demux.cli.main import _run_step2_single_task, _load_plan_row
    from scifi_demux.utils.state import ensure_state, add_or_get_task, save_state, iter_tasks

    plan, state_path, fq_dir = _make_step2_fixtures(tmp_path)

    # Seed state with both tasks
    s = ensure_state(state_path)
    for row_idx in (1, 2):
        row = _load_plan_row(plan, row_idx)
        tid = f"step2:group:{row['group']}:genome:{row['genome']}"
        t = add_or_get_task(s, tid, kind="step2", group=row["group"], genome=row["genome"])
        for k in ["index", "map", "clean_1"]:
            t.setdefault("steps", {}).setdefault(k, {"status": "pending"})
    save_state(s, state_path)

    r1_fqs = sorted(fq_dir.glob("*_R1*"))
    r3_fqs = sorted(fq_dir.glob("*_R3*"))

    with patch("scifi_demux.cli.main.run_step2_for_sample_genome") as mock_run:
        _run_step2_single_task(
            plan=plan, row_idx=2,
            r1_fqs=r1_fqs, r3_fqs=r3_fqs,
            outdir=tmp_path / "out",
            whitelist_10x=Path("/fake/wl_10x"),
            whitelist_tn5=Path("/fake/wl_tn5"),
            threads=8, mapq_min=20,
            state_obj=s, state_path=state_path,
            resume=False, dry_run=False,
        )
        mock_run.assert_called_once()
        call_kw = mock_run.call_args
        assert call_kw.kwargs["sample_id"] == "Pool2"
        assert call_kw.kwargs["genome_target"] == "Mo17"


def test_step2_hpc_fallback_serial(tmp_path: Path):
    """Without array env, HPC mode runs all rows serially."""
    from scifi_demux.cli.main import _run_step2_single_task, _load_plan_row
    from scifi_demux.utils.state import ensure_state, add_or_get_task, save_state

    plan, state_path, fq_dir = _make_step2_fixtures(tmp_path)

    s = ensure_state(state_path)
    for row_idx in (1, 2):
        row = _load_plan_row(plan, row_idx)
        tid = f"step2:group:{row['group']}:genome:{row['genome']}"
        t = add_or_get_task(s, tid, kind="step2", group=row["group"], genome=row["genome"])
        for k in ["index", "map"]:
            t.setdefault("steps", {}).setdefault(k, {"status": "pending"})
    save_state(s, state_path)

    r1_fqs = sorted(fq_dir.glob("*_R1*"))
    r3_fqs = sorted(fq_dir.glob("*_R3*"))

    call_log = []
    with patch("scifi_demux.cli.main.run_step2_for_sample_genome") as mock_run:
        mock_run.side_effect = lambda **kw: call_log.append(kw["sample_id"])
        for row_idx in range(1, 3):
            _run_step2_single_task(
                plan=plan, row_idx=row_idx,
                r1_fqs=r1_fqs, r3_fqs=r3_fqs,
                outdir=tmp_path / "out",
                whitelist_10x=Path("/fake/wl_10x"),
                whitelist_tn5=Path("/fake/wl_tn5"),
                threads=8, mapq_min=20,
                state_obj=s, state_path=state_path,
                resume=False, dry_run=False,
            )
    assert call_log == ["Pool1", "Pool2"]


def test_step2_resume_skips_done(tmp_path: Path):
    """Resume mode skips tasks already marked done in state."""
    from scifi_demux.cli.main import _run_step2_single_task, _load_plan_row
    from scifi_demux.utils.state import ensure_state, add_or_get_task, save_state

    plan, state_path, fq_dir = _make_step2_fixtures(tmp_path)

    # Seed state: Pool1 is done, Pool2 is pending
    s = ensure_state(state_path)
    row1 = _load_plan_row(plan, 1)
    t1 = add_or_get_task(
        s, f"step2:group:{row1['group']}:genome:{row1['genome']}",
        kind="step2", group=row1["group"], genome=row1["genome"],
    )
    for k in ["index", "map"]:
        t1.setdefault("steps", {})[k] = {"status": "done"}

    row2 = _load_plan_row(plan, 2)
    t2 = add_or_get_task(
        s, f"step2:group:{row2['group']}:genome:{row2['genome']}",
        kind="step2", group=row2["group"], genome=row2["genome"],
    )
    for k in ["index", "map"]:
        t2.setdefault("steps", {})[k] = {"status": "pending"}
    save_state(s, state_path)

    r1_fqs = sorted(fq_dir.glob("*_R1*"))
    r3_fqs = sorted(fq_dir.glob("*_R3*"))

    call_log = []
    with patch("scifi_demux.cli.main.run_step2_for_sample_genome") as mock_run:
        mock_run.side_effect = lambda **kw: call_log.append(kw["sample_id"])
        for row_idx in range(1, 3):
            _run_step2_single_task(
                plan=plan, row_idx=row_idx,
                r1_fqs=r1_fqs, r3_fqs=r3_fqs,
                outdir=tmp_path / "out",
                whitelist_10x=Path("/fake/wl_10x"),
                whitelist_tn5=Path("/fake/wl_tn5"),
                threads=8, mapq_min=20,
                state_obj=s, state_path=state_path,
                resume=True, dry_run=False,
            )
    # Only Pool2 should have run (Pool1 was already done)
    assert call_log == ["Pool2"]
