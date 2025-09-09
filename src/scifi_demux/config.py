# src/scifi_demux/config.py
from __future__ import annotations
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
        # TODO: add validations if you want (e.g., unique wells, etc.)
        return self

    @classmethod
    def from_yaml(cls, path: Path) -> "Design":
        with open(path, "r") as fh:
            data = yaml.safe_load(fh) or {}
        return cls(**data)
