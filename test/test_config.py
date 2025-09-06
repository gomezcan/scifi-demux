from pathlib import Path
from scifi_demux.config import Design

def test_design_loads(tmp_path: Path):
    y = tmp_path / "d.yaml"
    y.write_text("""
plates:
  - name: P
    wells: {A01: AAAA}
samples: []
""")
    d = Design.from_yaml(y)
    assert d.plates[0].name == "P"
