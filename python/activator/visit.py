__all__ = ["Visit"]

from dataclasses import dataclass


@dataclass(frozen=True, kw_only=True)
class Visit:
    # elements must be hashable and JSON-persistable; built-in types recommended

    # Inherited from SAL next_visit schema; keep in sync with
    # https://ts-xml.lsst.io/sal_interfaces/ScriptQueue.html#nextvisit
    groupId: str       # observatory-specific ID; not the same as visit number

    instrument: str    # short name
    detector: int
    snaps: int         # number of snaps expected
    filter: str        # physical filter
    # all angles are in degrees
    ra: float
    dec: float
    rot: float
    kind: str

    def __str__(self):
        """Return a short string that disambiguates the visit but does not
        include "metadata" fields.
        """
        return f"(instrument={self.instrument}, groupId={self.groupId}, detector={self.detector})"
