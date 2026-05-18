Prompt Processing integration with Middleware
=============================================

Prompt Processing uses the [Butler Middleware](https://pipelines.lsst.io/v/daily/modules/lsst.daf.butler/) very differently from batch or command-line processing, and some of Middleware's design assumptions do not apply.
This guide describes how Prompt Processing uses Middleware and the code invariants that follow from that design.

This guide assumes familiarity with the [high-level Prompt Processing architecture](concepts.md#workers-and-pods) and the Middleware [pipelines framework](https://pipelines.lsst.io/v/daily/modules/lsst.pipe.base/).
For details on how our Middleware-dependent code is structured, see [Package Organization](organization.md).

Local and central repositories
------------------------------

Pipeline inputs, raw telescope images, and all processed datasets belong in the `embargo` Butler repository at USDF, in line with [our embargo policy](https://dmtn-199.lsst.io/).
However, the Butler was not designed for high levels of parallelism, and would become a bottleneck if hundreds of workers were reading and writing to it simultaneously.
Therefore, each Prompt Processing worker maintains a small local repository in the ephemeral storage allocated to its container by Kubernetes.
Data transfers to or from `embargo` (or alternative long-lived repository, hereafter the "central repo") are kept as few and as small as possible.

<!-- TODO: move this paragraph to the design DMTN (and cite), once it exists -->
While Middleware provides some tools for distributed execution, such as quantum-backed Butler and client/server Butler, these tools assume that all inputs are available when they are started.
To meet its strict performance requirements, Prompt Processing needs to start data transfers while waiting for the image from the telescope, so it manages the local/central distinction itself.

When a Prompt Processing worker starts, it connects to the central repo, then creates a local repo with the same parameters (in particular, the dimension universe version must match exactly to allow two-way transfers).
The local repo lasts for the lifetime of the worker, across multiple processing runs.
Before each run, the worker loads any pipeline inputs from the central repo to the local repo (some inputs are cached, but effectively reusing inputs that depend on the detector number requires more inter-worker coordination than we can easily manage with KEDA).
Raws are likewise copied and ingested into the local repo as soon as they arrive at USDF.
All pipeline processing is done exclusively on the local repo.
At the end of the run, a selected set of pipeline outputs (partial outputs, in the case of a failure) are transferred to the central repo, then deleted (along with raws) from local storage before processing of the next image begins.

The only operations performed against the central repo are the initial queries to identify pipeline inputs, and the two dataset transfers before and after pipeline execution.
In practice, these are further optimized by making the queries and preload against a separate "read replica" Butler registry so that read and write requests don't interfere with each other.
Under no circumstances does Prompt Processing need to write and re-read intermediate datasets from the central repo between each task in the pipeline (and the analogous operations on the local repos are perfectly parallelizable).

Butler object management
------------------------

To avoid the overhead of new connections, Prompt Processing maintains two long-lived [`Butler`](https://pipelines.lsst.io/v/daily/api/lsst.daf.butler.Butler.html) objects for the central repo, one for the primary repo and one for the read replica (if a read replica is not configured, the two Butlers may be the same object).
This provides efficient access to the central repo during sustained observing.
If the worker is idle for long periods of time, the connection may grow stale, but this is handled by a retry internal to the `Butler` class.

The local repo is represented and managed by a `LocalRepo` object, which (as of May 2026) exposes a `Butler` object for access.
Almost all local repo operations are done through this shared object, ensuring the worker sees a self-consistent and up-to-date view of its repo at all times.
The exception is pipeline execution, where for compatibility reasons details such as the input and output collections are delegated to a temporary `Butler` object.
When pipeline execution ends (successfully or not), the shared local `Butler` object is explicitly updated to capture any changes made by the temporary one.

The deployment ID
-----------------

<!-- TODO: move some of this section to the design DMTN (and cite), once it exists -->
To ensure consistent tracking of dataset provenance, Middleware requires that each run collection be created and populated using a single version of Science Pipelines and its dependencies, and a single configuration for each pipeline task.
However, to keep the alerts flowing, Prompt Processing may need to apply hotfixes or roll back to a stable version during the night.
Done carelessly, a newly deployed version would be blocked from transferring its outputs to the central repo by previously written datasets.

To solve this problem, Prompt Processing defines a deployment ID (implemented by `shared.run_utils.get_deployment`) that is guaranteed to be consistent for the duration of any Prompt Processing deployment, but guaranteed to change if any version or task configuration parameter changes.
By incorporating this ID into all our run collection names (see [DMTN-167](https://dmtn-167.lsst.io/#ap-style-processing)), Prompt Processing ensures that incompatible datasets are always written to separate runs.

Previously, the deployment ID was a running counter.
The current implementation can't track its own deployments, and uses a hash of all Science Pipelines packages (including the pipeline definitions), the Prompt Processing code, and any service config variables that could alter the pipelines (currently, only the APDB location).
Note that this deployment ID is not strictly unique -- it may change even if there is no incompatibility, and may stay the same if a redeployment didn't change anything relevant.

The initializer job
-------------------

<!-- TODO: move this entire section to the design DMTN (and cite), once it exists -->
Middleware pipeline execution has an initialization step that records package versions, task configurations, table schemas, and other datasets that are independent of the data.
In command-line execution, this is done immediately before running the pipeline, but it would be difficult (and, in the context of massively parallel processing, potentially expensive) to merge these datasets if every worker had its own copy.

To work around this, Prompt Processing has a separate Kubernetes job that runs pipeline initialization once, directly in the central repo.
These datasets are then copied to the workers as part of their initial preparation.
The initializer runs initialization for each pipeline configured in Prompt Processing, possibly creating run collections that have the initialization outputs but no processed data.

Because the initialization outputs must be placed in the same runs that will later be used for pipeline execution, the initializer job must share a deployment ID (and all code and configuration used to calculate it) with the main service, and must be re-run whenever the run names change (each redeployment and each day).
Rerunning is handled automatically through an Argo CD PostSync hook (for manual deployments) and a Kubernetes CronJob (for daily updates).
Our Helm charts in [Phalanx](https://github.com/lsst-sqre/phalanx) are designed to share configuration between the initializer and the main service.
Argo CD, our deployment manager, is configured not to redeploy Prompt Processing until the initializer succeeds, and the initializer itself is allowed a generous number of retries to keep temporary network or cluster problems from holding up Prompt Processing.
