from pathlib import Path
from scifi_demux.demux_core import (
    mismatch_count_with_N, best_barcode_match_split,
    load_sample_to_wells, build_well_to_sample, build_barcode_to_sample,
)

def test_mismatch_with_N():
    assert mismatch_count_with_N("AAAAA", "AAAAA") == 0
    assert mismatch_count_with_N("AANAA", "AAAAT") == 2  # N counts as mismatch

def test_best_split():
    bc_set = ["AAAAA_TTTTT", "GGGGG_CCCCC"]
    best, mis = best_barcode_match_split("AAAAT_TTTTT", bc_set, max_mismatch=1)
    assert best == "AAAAA_TTTTT" and mis == 1

def test_load_wells(tmp_path: Path):
    m = tmp_path / "map.txt"
    m.write_text("Pool1\tA1-3,B2-2\n")
    s2w = load_sample_to_wells(m)
    w2s = build_well_to_sample(s2w)
    assert w2s["A1"] == "Pool1" and w2s["B2"] == "Pool1"

def test_barcode_to_sample():
    w2b = {"A1":"AAAAA_TTTTT","A2":"GGGGG_CCCCC"}
    w2s = {"A1":"PoolX","A2":"PoolY"}
    b2s = build_barcode_to_sample(w2b, w2s)
    assert b2s["AAAAA_TTTTT"] == "PoolX"
