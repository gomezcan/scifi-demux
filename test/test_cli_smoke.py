from typer.testing import CliRunner
from scifi_demux.cli import app

def test_help():
    r = CliRunner().invoke(app, ["--help"]) 
    assert r.exit_code == 0
