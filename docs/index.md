Prompt Processing developer guide
=================================

The `prompt_processing` repository is the home of three tightly interconnected applications:

* the Prompt Processing service itself,
* the initializer service that does shared setup before the start of the night, and
* the tester for simulating live observations on our development cluster.

While the repository is as compliant as practical with the [DM package guidelines](https://developer.lsst.io/stack/adding-a-new-package.html), it is not distributed or documented as part of Science Pipelines and uses its own release process.

This guide is for new contributors to the Prompt Processing repository, and covers code organization, quirks, and other details that directly affect development.
It assumes the reader is already familiar with the [DM Developer Guide](https://developer.lsst.io/), particularly the DM Development Workflow and the Python Style Guide, and with the [Butler/Middleware framework](https://pipelines.lsst.io/v/daily/modules/lsst.daf.butler/).
For specific procedures associated with Prompt Processing or related systems (especially the development service), see the [Playbook](https://github.com/lsst-dm/prompt_processing/blob/main/docs/playbook.rst).
For procedures for the on-sky production service, see the [Rubin Data Facilities docs](https://df-ops.lsst.io/usdf-applications/ap/prompt-processing/).
For a comprehensive look at the architecture and design, see [our ADASS preprint](https://arxiv.org/abs/2603.19541) or the upcoming as-built design technote.

<!-- TODO: add DMTN link once it exists and remove ADASS paper -->

Table of Contents
-----------------

* [Core concepts](concepts.md)
