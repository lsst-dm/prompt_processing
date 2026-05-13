Prompt Processing workflows
===========================

Prompt Processing is a Rubin Data Management project, and follows the [DM Development Workflow](https://developer.lsst.io/work/flow.html).
This guide summarizes team-specific details needed to support our independent release process or deployment management.
It assumes familiarity with the application structure described in [Core Concepts](concepts.md).

Jira
----

On Jira, we mark Prompt Processing related work items in several ways:

* Per the usual DM convention, the component indicates the GitHub package(s) that will be affected by the proposed work.
This can include `prompt_processing`, `phalanx`, `next_visit_fan_out`, `prompt_processing_butler_writer`, or sometimes Science Pipelines packages like `ap_pipe`.
* All work items relevant to Prompt Processing have the `Prompt-processing` label.
This includes third-party work such as Middleware or summit ScriptQueue improvements.
A list of all open Prompt Processing issues is available through the [open issues](https://rubinobs.atlassian.net/issues/?filter=11051) Jira filter, sharable within Jira and bookmarked from the `#dm-prompt-processing-dev` channel.
* A few other labels:
    - `Prompt-processing-Knative` refers to issues related to trying to fix our old Knative implementation.
    It is obsolete (see `driver_gunicorn.py` in [Package Organization](organization.md#directory-and-module-organization)).
    - `Prompt-processing-support` refers to work providing live monitoring and fixes during observatory commissioning.
    It's not quite obsolete, but we're unlikely to use it again in the future.
    - `Prompt-processing-refactor` refers to work to reduce our significant technical debt.
    This includes both our major MiddlewareInterface refactoring project as well as smaller improvements.
    <!-- TODO: add link to refactoring page when it's available -->

GitHub pull requests
--------------------

### Review and Testing

Prompt Processing pull requests should be reviewed per [the usual procedure](https://developer.lsst.io/work/flow.html#workflow-code-review).
Because Prompt Processing is not part of Science Pipelines and is not a library for any other package, it need not be tested with the `stack-os-matrix` Jenkins job (though it can be tested there by explicitly listing the package).

Instead, you should run integration tests of the service as described in [the Playbook](https://github.com/lsst-dm/prompt_processing/blob/main/docs/playbook.rst#testers).
GitHub actions automatically build service and initializer images whenever the pull request is updated.
Even if you're a long way from having something fit for review, it can be useful to create a draft pull request to get some testable images.

### ignore-for-release

Our [release process](https://github.com/lsst-dm/prompt_processing/blob/main/docs/playbook.rst#release-management) uses GitHub's release tools, which automatically populate release notes with all PRs that have merged since a baseline version.
If your PR will *not* affect the built containers in any way (for example, it only modifies documentation or unit tests), you should mark it with the `ignore-for-release` GitHub label to filter it out of the release notes.
This can be done at any time before the release process, but it's simplest to do it when the PR is created.

Internal code changes such as refactoring still change what ends up in the Docker builds, and do *not* qualify for `ignore-for-release`.
