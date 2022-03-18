__all__ = ["Visit"]

from dataclasses import dataclass


@dataclass(frozen=True)
class Visit:
    instrument: str
    detector: int
    group: str
    snaps: int
    filter: str
    ra: float
    dec: float
    rot: float
    kind: str
