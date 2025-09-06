from __future__ import annotations
import logging
from rich.logging import RichHandler

_DEF_LEVEL = logging.WARNING


def setup_logging(verbosity: int = 0) -> None:
    level = _DEF_LEVEL
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    root = logging.getLogger()
    root.setLevel(level)
    # clear existing
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = RichHandler(rich_tracebacks=True, show_time=False)
    root.addHandler(handler)
