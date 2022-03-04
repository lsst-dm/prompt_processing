from dataclasses import dataclass


@dataclass
class Visit:
    instrument: str
    detector: int
    group: str
    snaps: int
    filter: str
    ra: float
    dec: float
    kind: str
