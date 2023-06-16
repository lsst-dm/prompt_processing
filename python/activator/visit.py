__all__ = ["FannedOutVisit", "SummitVisit", "BareVisit"]

from dataclasses import dataclass, field, asdict
import enum


@dataclass(frozen=True, kw_only=True)
class BareVisit:
    # Elements must be hashable and JSON-persistable; built-in types
    # recommended. list is not hashable, but gets special treatment because
    # neither Kafka nor JSON deserialize sequences as tuples.

    # Inherited from SAL next_visit schema; keep in sync with
    # https://ts-xml.lsst.io/sal_interfaces/ScriptQueue.html#nextvisit
    class CoordSys(enum.IntEnum):
        # This is a redeclaration of lsst.ts.idl.enums.Script.MetadataCoordSys,
        # but we need BareVisit to work in code that can't import lsst.ts.
        NONE = 1
        ICRS = 2
        OBSERVED = 3
        MOUNT = 4

    class RotSys(enum.IntEnum):
        # Redeclaration of lsst.ts.idl.enums.Script.MetadataRotSys.
        NONE = 1
        SKY = 2
        HORIZON = 3
        MOUNT = 4

    class Dome(enum.IntEnum):
        # Redeclaration of lsst.ts.idl.enums.Script.MetadataDome.
        CLOSED = 1
        OPEN = 2
        EITHER = 3

    salIndex: int               # this maps to an instrument
    scriptSalIndex: int
    groupId: str                # observatory-specific ID; not the same as visit number
    coordinateSystem: CoordSys  # coordinate system of position
    # (ra, dec) or (az, alt) in degrees. Use compare=False to exclude from hash.
    position: list[float] = field(compare=False)
    rotationSystem: RotSys      # coordinate system of cameraAngle
    cameraAngle: float          # in degrees
    # physical filter(s) name as used in Middleware. It is a combination of filter and
    # grating joined by a "~". For example, "SDSSi_65mm~empty".
    filters: str
    dome: Dome
    duration: float             # script execution, not exposure
    nimages: int                # number of snaps expected, 0 if unknown
    survey: str                 # survey name
    totalCheckpoints: int

    def __str__(self):
        """Return a short string that represents the visit but does not
        include complete metadata.
        """
        return f"(groupId={self.groupId}, survey={self.survey}, salIndex={self.salIndex})"


@dataclass(frozen=True, kw_only=True)
class FannedOutVisit(BareVisit):
    # Extra information is added by the fan-out service at USDF.
    instrument: str             # short name
    detector: int

    def __str__(self):
        """Return a short string that disambiguates the visit but does not
        include "metadata" fields.
        """
        return f"(instrument={self.instrument}, groupId={self.groupId}, survey={self.survey} " \
               f"detector={self.detector})"

    def get_bare_visit(self):
        """Return visit-level info as a dict"""
        info = asdict(self)
        info.pop("instrument")
        info.pop("detector")
        return info


@dataclass(frozen=True, kw_only=True)
class SummitVisit(BareVisit):
    # Extra fields are in the NextVisit messages from the summit
    private_efdStamp: float = 0.0
    private_kafkaStamp: float = 0.0
    private_identity: str = "ScriptQueue"
    private_revCode: str = "c9aab3df"
    private_origin: int = 0
    private_seqNum: int = 0
    private_rcvStamp: float = 0.0
    private_sndStamp: float = 0.0
