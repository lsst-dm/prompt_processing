__all__ = ["Visit"]

from dataclasses import dataclass


@dataclass(frozen=True)
class Visit:
    # elements should use built-in types that are hashable and JSON-persistable
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
