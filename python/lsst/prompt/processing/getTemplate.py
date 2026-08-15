import dataclasses

from lsst.ip.diffim.getTemplate import (
    GetTemplateConfig,
    GetTemplateConnections,
    GetTemplateTask,
)

__all__ = [
    "GetTemplateConstrainedTask",
    "GetTemplateConstrainedConfig",
]


class GetTemplateConstrainedConnections(GetTemplateConnections):
    """GetTemplate connections with coaddExposures as an initial query constraint.

    Identical to `GetTemplateConnections` except ``coaddExposures`` has
    ``deferGraphConstraint=False``, so template coadd existence anchors the
    initial butler query during QuantumGraph building. An exposure with no
    overlapping templates produces an empty graph rather than quanta that fail
    at ``adjustQuantum``.
    """

    def __init__(self, *, config=None):
        self.coaddExposures = dataclasses.replace(
            self.coaddExposures,
            deferGraphConstraint=False,
        )


class GetTemplateConstrainedConfig(
    GetTemplateConfig,
    pipelineConnections=GetTemplateConstrainedConnections,
):
    pass


class GetTemplateConstrainedTask(GetTemplateTask):
    """GetTemplate with template coadds constraining the initial QGraph query.

    A testing workaround equivalent to setting
    ``GetTemplateConfig.constrainTemplateQuery=True`` in ip_diffim, for use
    when that config field is not yet available in the installed stack.
    """

    ConfigClass = GetTemplateConstrainedConfig
    _DefaultName = "buildTemplate"
