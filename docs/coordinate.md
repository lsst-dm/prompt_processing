Coordinating development between Prompt Processing and other repos
==================================================================

This guide describes how to coordinate changes to `prompt_processing` and one of [its related repos](concepts.md#related-repositories): `phalanx`, `next_visit_fan_out`, or `prompt_processing_butler_writer`.
In general, changes to inter-application messages are considered breaking, while changes to configuration options are not.

This guide assumes familiarity with our use of [Docker containers](concepts.md#docker) and with deployment management as described in [the Playbook](https://github.com/lsst-dm/prompt_processing/blob/main/docs/playbook.rst#development-service).

Phalanx
-------

[Phalanx](https://github.com/lsst-sqre/phalanx) defines the Kubernetes configuration for all components of Prompt Processing.
These configurations must be compatible with the Docker image that's being deployed.
Fortunately, it's usually easy to provide a configuration that works.

Some general guidelines:
* Since we can change the deployed Phalanx and/or Argo CD configuration at will, and these settings are not user-facing, configuration options are not considered part of the API for Prompt Processing or Fan Out, and do not trigger a major version release.
The writer service is versioned under Middleware-specific rules, and configuration changes have incremented the major version on occasion.
* We generally don't try to "hold back" Phalanx PRs once the Jira work item has been closed, even for changes to the production service.
Instead, Phalanx `main` should match Prompt Processing `main`, and if necessary we delay deploying the latest changes in Argo CD per the guidelines below.
* When reading environment variables in code, provide a default value wherever possible.
This decouples the code version from the configuration version and avoids needing to worry about compatibility at all.

The most common scenario is a new feature in `prompt_processing` and a flag or other environment variable added in `phalanx`.
Since an unsupported environment variable is simply ignored, it's safe to merge the Phalanx changes first, or even deploy them with older containers.

In the opposite case, where a variable is removed, you should be careful not to sync the configuration until the relevant Prompt Processing container (`latest` in development, or a new release in production) no longer tries to read that variable.
You *can* merge the Phalanx changes to `main` so long as everyone is aware of the potential incompatibility and agrees not to sync Argo CD until a matching container is available, or to sync to a branch that has the old-style config.
As a last resort, you can manually edit the live manifest's YAML in Argo CD to add or remove definitions as needed, but this must be done with care -- it's essential that Prompt Processing and its initializer [always have the same settings](middleware.md#the-deployment-id)!

For more complex changes, there may not be a compatible option at all.
If so, you would have to merge the two PRs simultaneously, then manually sync the configuration once `latest` has been rebuilt with the merged changes.

Next Visit Fan Out / Butler Writer Service
------------------------------------------

Prompt Processing receives messages from [Next Visit Fan Out](https://github.com/lsst-dm/next_visit_fan_out) (NVFO) and sends them to the [Butler Writer Service](https://github.com/lsst-dm/prompt_processing_butler_writer).
Because these two services and Prompt Processing are deployed and configured independently of each other, we must keep track of compatibility in the message format.

The main API between NVFO and Prompt Processing proper is a nextVisit structure and its serialized forms; these are implemented as the `shared.visit.FannedOutVisit` class on the PP side and `shared.visit.NextVisitModelKeda` on the NVFO side.
The current I/O code does not do any compatibility checking or translation, so adding, removing, or renaming fields will cause an error.
Prompt Processing must increment its major version if it can't understand older messages from NVFO, while NVFO must increment its major version if it produces messages that older versions of PP can't understand.
In practice, changes to both classes need to be synchronized, and the new builds of both services must be deployed together.

The main API between Prompt Processing and the Writer Service is a Pydantic-encoded Kafka message, implemented as the `activator.kafka_butler_writer.KafkaButlerWriter` class in PP and the `queued_butler_writer.messages.PromptProcessingOutputEvent` class in the Writer.
It's almost impossible to add compatibility code to Pydantic models, so nearly all changes require major version increments and synchronized deployments.
<!-- TODO: find the exact compatibility rules. It looks like BWS can have extra fields as long as they have a default -->

Science Pipelines
-----------------

Prompt Processing normally treats the pipelines it executes as black boxes.
However, sometimes changes to the pipeline definitions (e.g., a renamed task) require corresponding changes to our pipeline overrides, or there are breaking changes to the Middleware APIs.

Because it's hard to build Prompt Processing against Science Pipelines branches (though it can be done with the [quick-stack pipeline](https://github.com/lsst-dm/prompt_processing/blob/main/docs/playbook.rst#stable-base-containers)), we normally merge such work in two steps.
First, the Science Pipelines changes are tested, and all changes (including in Prompt Processing) are reviewed [as usual](https://developer.lsst.io/work/flow.html#workflow-code-review).
Only the Science Pipelines changes are merged, and we wait for the next day's Science Pipelines container to be built through the normal channels.
Once the merged changes are in Docker, we [create a new base container](https://github.com/lsst-dm/prompt_processing/blob/main/docs/playbook.rst#containers) and use it to unit and [integration test](https://github.com/lsst-dm/prompt_processing/blob/main/docs/playbook.rst#testers) the Prompt Processing changes, which can then be merged to restore compatibility.

This procedure is simple and doesn't deviate too much from DM standards, but it means that changes to Science Pipelines can't be deployed in Prompt Processing until the day after they merge to `main` (longer, if final testing uncovers a problem).
Plan accordingly!
