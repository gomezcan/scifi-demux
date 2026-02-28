# src/scifi_demux/config.py
from __future__ import annotations
import warnings
from pydantic import BaseModel, Field, model_validator
from pathlib import Path
from typing import Dict, List, Optional
import yaml


class Plate(BaseModel):
    name: str
    index: Optional[str] = Field(None, description="Plate-level index/tag if any")
    wells: Dict[str, str] = Field(default_factory=dict, description="Well -> barcode sequence or tag")


class Sample(BaseModel):
    name: str
    wells: List[str] = Field(default_factory=list, description="List of well IDs included in this sample")
    plate: Optional[str] = Field(None, description="Optional plate constraint")


class Design(BaseModel):
    plates: List[Plate] = Field(default_factory=list)
    samples: List[Sample] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check(self):
        # Build set of known plate:well references
        plate_wells: set[str] = set()
        for plate in self.plates:
            for well_id in plate.wells:
                plate_wells.add(f"{plate.name}:{well_id}")

        # Warn about sample well references that don't match any plate well
        for sample in self.samples:
            for well_ref in sample.wells:
                if ":" in well_ref and well_ref not in plate_wells:
                    warnings.warn(
                        f"Sample '{sample.name}' references well '{well_ref}' "
                        f"which is not defined in any plate",
                        stacklevel=2,
                    )
        return self

    @classmethod
    def from_yaml(cls, path: Path) -> "Design":
        with open(path, "r") as fh:
            data = yaml.safe_load(fh) or {}
        return cls(**data)
