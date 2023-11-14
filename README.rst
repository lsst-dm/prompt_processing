#################
prompt_processing
#################

``prompt_processing`` is a package in the `LSST Science Pipelines <https://pipelines.lsst.io>`_.

``prompt_processing`` contains code used for the Rubin Observatory Prompt Processing framework.
The design for the framework is described in `DMTN-219`_ and `DMTN-260`_.

.. _DMTN-219: https://dmtn-219.lsst.io/

.. _DMTN-260: https://dmtn-260.lsst.io/

At present, the package does not conform exactly to the layout or conventions of LSST stack packages.
In particular, the Python code does not use the ``lsst`` namespace, and the docs do not support Sphinx builds (even local ones).
