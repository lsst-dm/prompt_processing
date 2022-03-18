__all__ = ["Visit"]

from dataclasses import dataclass
import lsst.geom


@dataclass(frozen=True)
class Visit:
    instrument: str
    detector: int
    group: str
    snaps: int
    filter: str
    boresight_center: lsst.geom.SpherePoint
    orientation: lsst.geom.Angle
    kind: str
