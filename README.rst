#################
prompt_processing
#################

.. image:: https://github.com/lsst-dm/prompt_processing/actions/workflows/build-service.yml/badge.svg?branch=main
   :target: https://github.com/lsst-dm/prompt_processing/actions/workflows/build-service.yml
   :alt: Dev Build Status

.. Selecting tag, not release, to catch any accidental runs of Release CI

.. image:: https://img.shields.io/github/v/tag/lsst-dm/prompt_processing?sort=date&label=Release
   :target: https://github.com/lsst-dm/prompt_processing/releases/latest
   :alt: Latest Release

.. image:: https://github.com/lsst-dm/prompt_processing/actions/workflows/ci-release.yaml/badge.svg?event=push
   :target: https://github.com/lsst-dm/prompt_processing/actions/workflows/ci-release.yaml
   :alt: Release CI Build Status

``prompt_processing`` is a package in the `LSST Science Pipelines <https://pipelines.lsst.io>`_.

``prompt_processing`` contains code used for the Rubin Observatory Prompt Processing framework.
The design for the framework is described in `DMTN-219`_ and `DMTN-260`_.

.. _DMTN-219: https://dmtn-219.lsst.io/

.. _DMTN-260: https://dmtn-260.lsst.io/

At present, the package does not conform exactly to the layout or conventions of LSST stack packages.
In particular, the Python code does not use the ``lsst`` namespace, and the docs do not support Sphinx builds (even local ones).
