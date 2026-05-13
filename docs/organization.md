Prompt Processing package organization
======================================

This guide describes how the `prompt_processing` repository is organized at the directory and module level.
It assumes familiarity with the [Core Concepts](concepts.md).

For developing in parallel with other repositories, see [Coordinating development](coordinate.md).
For how the module organization complicates testing, see [Testing Prompt Processing](tests.md).
<!-- TODO: add cross-reference to Middleware refactor once it exists -->

Overview
--------

The Prompt Processing repository largely follows [Science Pipelines conventions](https://github.com/lsst/templates/tree/main/project_templates/stack_package/example_pythononly).
It uses [EUPS](https://developer.lsst.io/stack/eups-tutorial.html) to manage Science Pipelines dependencies (the name is `prompt_processing` rather than `prompt-processing` for EUPS compatibility), making it easy to set up on different systems, and uses `scons` for build and test management.
Issues and outstanding are work are tracked in Jira, using the `prompt_processing` component.

However, there are some differences from a typical Science Pipelines package:
* `prompt_processing` is not a member of `lsst_apps`, `lsst_distrib`, or any other metapackage.
This is because it is an autonomous service application(s), and is not designed for use as a dependency or for direct execution by users.
* The Python namespace is not `lsst.prompt.processing`; instead, the three applications use the `activator`, `initializer`, or `tester` namespaces respectively.
Again, this is because it is not a library and its components are not meant to be imported from external code.
* We do not build Sphinx documentation with pipelines.lsst.io, partly because the target audience is different and partly from technical limitations.
Our documentation directory is `docs/` rather than the usual `doc/` to support GitHub Pages instead.
* The `.github/`, `base/`, `init-output-run/`, and `Dockerfile` entries support our Docker builds and standalone release process.
* The `config/` directory contains application-level configuration files.
* The `etc/` directory contains input files for the tester and for maintaining the development Butler repo.
* The `maps/` directory contains softcoded density maps to use with our pipeline selector.
This is for lack of a better way to get them into the Docker container, so alternatives will be welcome.
* The `pipelines/` directory contains small tweaks needed to adapt [standard AP pipelines](https://github.com/lsst/ap_pipe/) to the Prompt Processing runtime environment.

Directory and module organization
---------------------------------

Prompt Processing's Python code is divided into four namespaces (none of which are actually packages):
* `activator` holds the Prompt Processing service itself.
* `initializer` holds a small Kubernetes job that prepares Butler collections and datasets that would be shared by all Prompt Processing pods (see [Integration with Middleware](middleware.md#the-initializer-job) for details).
* `tester` contains command-line scripts for simulating observations during integration testing.
* `shared` contains utilities and definitions that are used by two or more of the above, especially code needed to coordinate different applications.

Prompt Processing's `activator` modules have evolved organically from what used to be a simple prototype, and are the focus of ongoing refactoring work to separate responsibilities and dependencies (see the `Prompt-Processing-refactor` label in Jira).
<!--- TODO: add cross-reference to MWI refactoring once it exists -->
However, they can be roughly divided into primary modules that form the backbone of the service and utility modules that provide a specific class or related set of components.

### Core modules

The core modules are:
* `activator.py` contains the main application logic of Prompt Processing, responsible for processing nextVisit messages (from receipt to final upload), handling errors, and managing long-lived objects.
It is, in part, a workaround for [Middleware's](https://pipelines.lsst.io/v/daily/modules/lsst.daf.butler/) inability to organize processing before all pipeline inputs are available (in particular, the need to identify and preload calibs, refcats, and templates before raws are available).
* `driver_keda.py` and `driver_gunicorn.py` are the entry points to the worker.
They are the only modules that need to know anything about how Prompt Processing is managed at the pod level, and depend on networking packages that are otherwise unavailable (see [Testing](tests.md) for the implications).
    - `driver_keda.py` is our current implementation.
    When KEDA starts a new pod, the pod's container (as defined in the main `Dockerfile`) executes `driver_keda.__main__`.
    The driver code itself is a message-reading loop that polls our Redis Streams queue for nextVisit messages, logs Prometheus metrics, and handles any retry/abort logic.
    - `driver_gunicorn.py` was our previous approach, functioning as a Gunicorn/Flask web server that handled nextVisit messages as HTTP requests.
    The entry point was `driver_gunicorn.create_app`, which was called by a local Gunicorn server.

        Flask handled many details that `driver_keda` requires explicit code for, making it valuable as an alernative implementation and clarifying what must be in the drivers instead of in `activator.py`.
    Although it's dead code, we try to keep `driver_gunicorn.py` up-to-date to enforce this boundary (in particular, to discourage code from migrating from `activator.py` to `driver_keda.py`).
* `middleware_interface.py` was originally a catch-all class that grouped everything that depended on Butler Middleware (in particular, keeping Middleware dependencies out of `activator.py`).
We are in the process of breaking up this module into smaller, more manageable pieces.
As of May 2026, it's still responsible for finding and loading pipeline inputs, ingesting raw images, pipeline selection and execution both before and after raws arrive, and sending (possibly partial) outputs to the central repository.
<!--- TODO: add cross-reference to MWI refactoring once it exists -->
* `local_repo.py` defines a class that represents and manages the worker's individual repo in local storage, including initialization, content management, and teardown.
`activator.py` is responsible for the object's lifetime, but most use is by `middleware_interface.py`.

One of the key priorities in this architecture is separation of dependencies.
Only `driver_keda.py` and `driver_gunicorn.py` depend on packages like `redis` or `flask`, while only `middleware_interface.py` and `local_repo.py` depend on Middleware.
`activator.py` should deal with external dependencies only through the abstractions provided by other modules (an ideal that has not yet been met).

### Important utilities

Prompt Processing has many smaller classes, of which the most important are:
* `shared/visit.py` holds several classes for representing nextVisit messages at different stages of processing.
The most important of these is `FannedOutVisit`, which represents the modified nextVisit sent by the Fan-Out Service and is an input to most code that runs before raws arrive.
* `shared/config.py` provides a class that parses specifications of which pipelines should run in what conditions, and provides a simple interface to query the results.
The activator and MiddlewareInterface hold two instanecs, one for preparatory pipelines and one for the main processing.
* `kafka_butler_writer.py` provides a virtual Butler that accepts datasets but defers actual registry updates to the [Butler Writer Service](concepts.md#butler-writer-service).
* `startstop.py` provides a dependency inversion interface to let the driver modules initialize (or test the initialization of) objects managed by the activator without needing to depend on them.
* `caching.py` and `evictingSet.py` maintain a fixed-size cache of datasets to be kept between processing runs.
While we're not actively developing caching because of difficulties in coordinating workers in the KEDA framework, if improvements to caching algorithms are desired, they should be implemented as subclasses of `EvictingSet`.

GitHub Scripts
--------------

Because Prompt Processing is deployed through Docker containers and is responsible for its own releases, it has an unusually large number of custom GitHub actions and workflows.
Reusable components may be implemented as actions or workflows, depending on that the component needs (in particular, actions let the enclosing workflow define environment variables for them).

For running the workflows, see [the Playbook](https://github.com/lsst-dm/prompt_processing/blob/main/docs/playbook.rst).
