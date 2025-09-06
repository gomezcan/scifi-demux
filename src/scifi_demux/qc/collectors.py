from __future__ import annotations
from pathlib import Path

def write_step1_library_tsv(out: Path, library: str, raw: int, bc1: int, bc1bc2: int) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("library	raw_pairs	bc1_pairs	bc1bc2_pairs
" f"{library}	{raw}	{bc1}	{bc1bc2}
")


def write_step1_groups_tsv(out: Path, rows: list[tuple[str, str, int, int, float]]) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    header = "library	group	assigned_pairs	unassigned_pairs	frac_assigned
"
    lines = [header] + [f"{lib}	{grp}	{ass}	{unas}	{frac:.6f}
" for lib, grp, ass, unas, frac in rows]
    out.write_text("".join(lines))


def write_step2_task_tsv(out: Path, group: str, genome: str, mapped: int, proper: int, dup_frac: float, final_pairs: int, tn5_sites: int, final_frac: float) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    header = "group	genome	mapped_pairs	proper_pairs	dup_fraction	final_pairs	tn5_sites	final_fraction_of_input
"
    out.write_text(header + f"{group}	{genome}	{mapped}	{proper}	{dup_frac:.6f}	{final_pairs}	{tn5_sites}	{final_frac:.6f}
")
