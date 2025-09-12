# io_utils.py
from pathlib import Path
from typing import Optional, Union

def data_path(name: str) -> Path:
    """
    Locate packaged data robustly:
      1) Prefer filesystem path relative to this module (works in editable + wheel installs).
      2) Fall back to importlib.resources using the *parent* package (avoids importing __data__).
    """
    # 1) direct filesystem path
    pkg_root = Path(__file__).resolve().parent  # .../scifi_demux
    fs = pkg_root / "__data__" / name
    if fs.exists():
        return fs

    # 2) resource fallback (Python 3.9+)
    try:
        from importlib.resources import files as _files
        return Path(_files("scifi_demux") / "__data__" / name)
    except Exception as e:
        raise FileNotFoundError(f"Could not locate data file '{name}' "
                                f"(looked at {fs}) and resource fallback failed: {e}")

def resolve_tn5_bcs(user_path: Optional[str]) -> Path:
    if user_path and user_path.lower() != "builtin":
        return Path(user_path).resolve()
    return data_path("tn5_bcs.txt")

def resolve_whitelist(user_path: Optional[str]) -> Path:
    if user_path and user_path.lower() != "builtin":
        return Path(user_path).resolve()
    return data_path("737K-cratac-v1.txt")

def resolve_layout_path(layout: Optional[Union[str, Path]]) -> Path:
    """
    None or 'builtin' -> packaged 96-well layout; else use the given path.
    """
    if layout is None or str(layout).lower() == "builtin":
        return data_path("96well_Tn5_bc_layout.txt")
    return Path(layout)
