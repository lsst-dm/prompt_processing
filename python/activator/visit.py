__all__ = ["Visit"]

from dataclasses import dataclass, field


@dataclass(frozen=True, kw_only=True)
class Visit:
    # Elements must be hashable and JSON-persistable; built-in types
    # recommended. list is not hashable, but gets special treatment because
    # neither Kafka nor JSON deserialize sequences as tuples.

    # Inherited from SAL next_visit schema; keep in sync with
    # https://ts-xml.lsst.io/sal_interfaces/ScriptQueue.html#nextvisit
    groupId: str        # observatory-specific ID; not the same as visit number
    # (ra, dec) in degrees. Use compare=False to exclude from hash.
    position: list[float] = field(compare=False)
    cameraAngle: float  # in degrees
    filters: str        # physical filter(s)
    nimages: int        # number of snaps expected, 0 if unknown
    survey: str         # survey name

    # Added by the Kafka consumer at USDF.
    instrument: str     # short name
    detector: int

    def __str__(self):
        """Return a short string that disambiguates the visit but does not
        include "metadata" fields.
        """
        return f"(instrument={self.instrument}, groupId={self.groupId}, detector={self.detector})"
