from logging import getLogger, StreamHandler, Formatter, INFO
from rich.logging import RichHandler
import logging

_DEF_FMT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

def setup_logging(verbosity: int = 0) -> None:
  level = logging.WARNING if verbosity <= 0 else logging.INFO if verbosity == 1 else logging.DEBUG
  root = getLogger()
  root.setLevel(level)
  # Clear existing handlers in case of re‑init (pytest, notebooks)
  for h in list(root.handlers):
    root.removeHandler(h)
    rich = RichHandler(rich_tracebacks=True, show_time=False)
    root.addHandler(rich)
    
def get_logger(name: str):
  return getLogger(name)
