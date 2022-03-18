__all__ = ["VisitMessage"]

from dataclasses import dataclass


@dataclass(frozen=True)
class VisitMessage:
    instrument: str
    detector: int
    group: str
    snaps: int
    filter: str
    ra: float
    dec: float
    rot: float
    kind: str
