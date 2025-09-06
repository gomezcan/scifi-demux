from __future__ import annotations
from subprocess import Popen, PIPE
from typing import Sequence


def run_cmd(cmd: Sequence[str]) -> int:
    """Run a command, streaming stdout/stderr to terminal. Returns exit code."""
    p = Popen(cmd)
    return p.wait()
