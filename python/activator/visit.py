__all__ = ["Visit"]

from dataclasses import dataclass


@dataclass(frozen=True)
class Visit:
    # elements must be hashable and JSON-persistable; built-in types recommended
    instrument: str
    detector: int
    group: str
    snaps: int
    filter: str
    # all angles are in degrees
    ra: float
    dec: float
    rot: float
    kind: str
