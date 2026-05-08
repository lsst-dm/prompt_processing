Prompt Processing core concepts
===============================

This guide provides a very broad overview of Prompt Processing's architecture.
It establishes context for the other pages in these docs.

For a more detailed look at Prompt Processing's architecture, see the [Rubin Data Facilities docs](https://df-ops.lsst.io/usdf-applications/ap/prompt-processing/info.html), [the original design proposal](https://dmtn-219.lsst.io/) (now somewhat out of date), [our ADASS preprint](https://arxiv.org/abs/2603.19541), or the upcoming as-built design technote.

<!-- TODO: add DMTN link once it exists and remove ADASS paper -->

Docker
------

Unlike Science Pipelines packages, Prompt Processing is not designed for command-line execution.
Instead, both the main service and the initializer are built as [Docker containers](https://www.docker.com/resources/what-container/), based on the standard Science Pipelines releases.
These containers are deployed through a scalable orchestration framework (currently [Kubernetes](https://kubernetes.io/)), which ensures the service is always ready to process data.
The files `Dockerfile` and `init-output-run/Dockerfile` contain the actual execution commands for the main service and the initializer, respectively.
Because Kubernetes (or similar frameworks) doesn't provide natural hooks for command-line arguments, most Prompt Processing configuration is done through environment variables from [Phalanx](#Phalanx).

Docker containers are built automatically by GitHub pull requests and updates to `main`, and manually as part of the release process.
See the [Playbook](https://github.com/lsst-dm/prompt_processing/blob/main/docs/playbook.rst) for details.

Because the initializer and the main service must use the same code base, it's important to always deploy matching versions and configurations.
<!-- TODO: add link to Middleware topic once it exists -->
To make this easier, all Docker build actions build and label both containers together, and our deployment system (Phalanx, Helm, and Argo CD) is designed to always apply the same settings (including container label) to both applications.

Workers and pods
----------------

The deployed Prompt Processing service has many layers.
At the highest level are hundreds of **pods**, which are created and destroyed by Kubernetes (specifically, [Kubernetes Event-Driven Autoscaling](https://keda.sh/)) based on the current workload.
Within each pod are **containers** based on our Docker image, each a semi-isolated environment with its own CPU, memory, and storage allocation.
Within each container are **workers**, the actual Python instances whose source code makes up the bulk of the `prompt_processing` repo.

Our current architecture has one container per pod (not counting a temporary initContainer that sets up credentials) and one worker per container.
We've experimented with multiple workers per pod in the past, but they never offered an advantage over simply having more pods.
However, some of the code in `driver_*.py` and `activator.py` still guards against the possibility that the worker may need to share its file system with other workers.

The initializer works essentially the same way, but because it has little to do (and must not run in parallel) it always consists of a single pod.

Related repositories
--------------------

The `prompt_processing` repository is part of a larger ecosystem.
These are the repositories with which it has dependencies.

<!-- TODO: add link to syncing topic once it exists -->

### Phalanx

[Phalanx](https://github.com/lsst-sqre/phalanx) is the Rubin Observatory's in-house configuration and deployment system.
It defines the Prompt Processing development and production environments, and the instrument-specific services in each.
Our Kubernetes configurations are stored here as [Helm](https://helm.sh/docs/) charts based on the Go template language.
All configuration parameters for Prompt Processing are declared in Helm values files and then substituted into the chart, usually as environment variables.
The Helm chart is also where we link passwords or other secure credentials from the SLAC Vault.

If you are adding a new feature to Prompt Processing, you will likely need to add corresponding configuration hooks to Phalanx.
See [the Playbook](https://github.com/lsst-dm/prompt_processing/blob/main/docs/playbook.rst#development-service) for more details on working with Phalanx and the [Argo CD](https://argo-cd.readthedocs.io/en/stable/) deployment manager.

### Next Visit Fan Out

[next_visit_fan_out](https://github.com/lsst-dm/next_visit_fan_out) (NVFO) is a secondary service that receives [nextVisit messages](https://ts-xml.lsst.io/sal_interfaces/ScriptQueue.html#nextvisit) from the observatory and creates a copy for each detector.
These visit-detector combinations form the basic unit of work that is then processed by the pool of Prompt Processing workers.
Like Prompt Processing itself, NVFO is an "always online" service, and its configuration and deployment are managed through Phalanx.

The main API between NVFO and Prompt Processing proper is a nextVisit structure and its serialized forms; these are implemented as the `shared.visit.FannedOutVisit` class on the Prompt Processing side and `shared.visit.NextVisitModelKeda` on the NVFO side.
These class definitions must be synchronized to ensure the deployed Prompt Processing can understand the deployed Next Visit Fan Out.
In practice, we usually don't bother with compatibility code and simply consider changes to the data model to be a breaking change for both applications.

<!-- TODO: add link to syncing topic once it exists -->

### Raw Microservice

The [raw microservice](https://github.com/lsst-dm/embargo-butler/blob/main/src/presence.py) records the ingestion of raw images into the `embargo` Butler repository.
Prompt Processing queries the service as a backup for our primary, message-based raw image discovery (the microservice method is slower but more reliable).
The microservice is always online, although its configuration and deployment are managed through SLACLab ([development](https://github.com/slaclab/usdf-embargo-deploy/blob/main/kubernetes/overlays/pp-dev/presence-deploy.yaml) and [production](https://github.com/slaclab/usdf-embargo-deploy/blob/main/kubernetes/overlays/summit-new/presence-deploy.yaml) instances) rather than Phalanx.

Prompt Processing interacts with the raw microservice through a simple web API.
The microservice is not under active development, so we have no established procedure for coordinating changes.
If future changes were made, we would likely need to add explicit versioning to the URI path (e.g., `/v2/presence`).

### Butler Writer Service

Prompt Processing processes each visit-detector in parallel in its pods, then merges the results into the shared `embargo` Butler repository.
The [Butler Writer Service](https://github.com/lsst-dm/prompt_processing_butler_writer) is a secondary service that gathers Butler registry updates from the Prompt Processing workers and applies them to the `embargo` repo serially, preventing contention or even deadlocks from hundreds of concurrent requests.
Like Prompt Processing, the writer is an "always online" service, and its configuration and deployment are managed through Phalanx.
Unlike Prompt Processing or NVFO, ownership of the Writer Service is shared with the Middleware team.

Like NVFO, Prompt Processing and the Writer communicate through a shared API.
It's implemented as the `activator.kafka_butler_writer.KafkaButlerWriter` class in Prompt Processing and the `queued_butler_writer.messages.PromptProcessingOutputEvent` class in the Butler Writer.
As with NVFO and nextVisit, changes to the message format must be done with care and may involve a major version increment on one or both sides.
