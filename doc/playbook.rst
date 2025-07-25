######################################################
Developers' Playbook for the Prompt Processing Service
######################################################

.. _DMTN-219: https://dmtn-219.lsst.io/

Table of Contents
=================

* `Containers`_
* `Release Management`_
* `Buckets`_
* `Central Repo`_
* `Development Service`_
* `Testers`_
* `next_visit Events`_
* `Databases`_
* `Redis Streams`_


Containers
==========

The service consists of two containers.
The first is a **base container** with the Science Pipelines "stack" code and networking utilities.
The second is a **service container** made from the base that has the Prompt Processing service code.
All containers are managed by `GitHub Container Registry <https://github.com/orgs/lsst-dm/packages?repo_name=prompt_processing>`_ and are built using GitHub Actions.

To build the base container:

* If there are changes to the container, push them to a branch, then open a PR.
  This will build the container automatically.
* If there are no changes (typically because you want to use an updated Science Pipelines container), go to the repository's `Actions tab <https://github.com/lsst-dm/prompt_processing/actions/workflows/build-base.yml>`_ and select "**Run workflow**". From the dropdown menu, select:

  #. The branch whose container definition will be used
  #. The label of the Science Pipelines container.
  #. If using a quick-stack build, the Science Pipelines Container should be set to `ghcr.io/lsst/quick-stack <https://ghcr.io/lsst/quick-stack>`_.

  New containers built from ``main`` are tagged with the corresponding Science Pipelines release (plus ``w_latest`` or ``d_latest`` if the release was requested by that name).
  For automatic ``main`` builds, or if the corresponding box in the manual build is checked, the new container also has the ``latest`` label.
  Containers built from a branch use the same scheme, but prefixed by the ticket number or, for user branches, the branch topic.

.. note::

   If a PR automatically builds both the base and the service container, the service build will *not* use the new base container unless you specifically override it (see below).
   Even then, the service build will not wait for the base build to finish.
   You may need to manually rerun the service container build to get it to use the newly built base.

To build the service container:

* If there are changes to the service, push them to a branch, then open a PR.
  This will build the container automatically using the ``latest`` base container.
* To force a rebuild manually, go to the repository's `Actions tab <https://github.com/lsst-dm/prompt_processing/actions/workflows/build-service.yml>`_ and select "**Run workflow**".
  From the dropdown, select the branch whose code should be built.
  The container will be built using the ``latest`` base container, even if there is a branch build of the base.
* To use a base other than ``latest``, edit ``.github/workflows/_matrix-gen.yaml`` on the branch and override the ``BASE_TAG_LIST`` variable.
  Be careful not to merge the temporary override to ``main``!
* New service containers built from ``main`` have the tags of their base container.
  Containers built from a branch are prefixed by the ticket number or, for user branches, the branch topic.

.. note::

   The ``PYTHONUNBUFFERED`` environment variable defined in the Dockerfiles for the containers ensures that container logs are emitted in real-time.

Stable Base Containers
----------------------

In general, the ``latest`` base container is built from a weekly or other stable Science Pipelines release.
However, it may happen that the ``latest`` base is used for development while production runs should use an older build.
If this comes up, edit ``.github/workflows/_matrix-gen.yaml`` and append the desired base build to the ``BASE_TAG_LIST`` variable.
Any subsequent builds of the service container will build against both bases.

This is the only situation in which a change to ``BASE_TAG_LIST`` should be committed to ``main``.

It will sometimes be necessary to compile a container with the LSST Science Pipelines manually (called a Quick-Stack build).
Generally, this only occurs if the intended daily or weekly stack does not compile.
In these cases, the Science Pipelines themselves must be built ahead of the base container.
Instructions for building the Science Pipelines are `here <https://github.com/lsst/gha_build/blob/main/README.md>`_.
For Prompt Processing, we only need to build ``lsst_distrib``.

Release Management
==================

Releases are largely automated through GitHub Actions (see the `ci-release.yaml <https://github.com/lsst-dm/prompt_processing/actions/workflows/ci-release.yaml>`_  workflow file for details).
When a semantic version tag is pushed to GitHub, Prompt Processing Docker images are published on GitHub and Docker Hub with that version.

Regular releases happen from the ``main`` branch after changes have been merged.
From the ``main`` branch you can release a new major version (``X.0.0``), a new minor version of the current major version (``X.Y.0``), or a new patch of the current major-minor version (``X.Y.Z``).
Release tags are semantic version identifiers following the `pep 440 <https://peps.python.org/pep-0440/>`_ specification.
Please note that the tag does not include a ``v`` at the beginning.

#. Choosing the Version Number

   The first step in making a release is choosing an appropriate version number to describe it. This subsection offers guidance on this choice, as well as on differentiating between major and minor releases.

   **To see which changes have occurred since the last release:**

   On GitHub.com, navigate to the main page of the repository.
   To the right of the list of files, click the latest release.
   At the top of the page, click **## commits to main since this release**.
   (If there's no such link or it doesn't mention ``main``, the release is probably based off a branch; go up to Releases and try older versions until you find one.)
   This is the list of internal changes that will be included in the next release.

   If you are planning to update the Science Pipelines tag, you should also check the `Science Pipelines changelog <https://lsst-dm.github.io/lsst_git_changelog/weekly/>`_.
   In practice, almost any Science Pipelines update is at least a minor version, because new features are added constantly.
   You may need to do a "Quick-Stack" build, or build the science pipelines manually. This would justify a patch version of Prompt Processing.

   **The following changes will trigger a major release:**

   For the ``prompt_processing`` service:

   * Incompatibility with old fanned-out ``nextVisit`` messages (almost any change to ``Visit`` qualifies)
   * Incompatibility with an old `APDB schema`_, `ApdbSql`_, `ApdbCassandra`_, or `ApdbCassandraReplica`_ version (see `DMTN-269`_ for the distinction)
   * Incompatibility with an old `Butler dimensions-config`_ version
   * A new major version of the `Alerts schema`_ (see `DMTN-093`_ for details)

   For the `next_visit_fan_out`_ service:

   * Incompatibility with old Summit ``nextVisit`` messages
   * Breaking changes in the fanned-out ``nextVisit`` messages (almost any change to ``NextVisitModel`` qualifies)

#. Create a Release

   On GitHub.com, navigate to the main page of the repository.
   To the right of the list of files, click **Releases**.
   At the top of the page, click **Draft a new release**.
   Type a tag using semantic versioning described in the previous section.
   The Target should be the main branch.

   .. note::

      The `auto` option is unreliable if there are no changes in Prompt Processing itself.
      In these cases, if there weren't any eligible Pull Requests since the last tag, it may choose an older version than the latest.
   Select **Generate Release Notes**.
   This will generate a list of commit summaries and of submitters.
   Add text as follows:

   * Any specific motivation for the release (for example, including a specific feature, preparing for a specific observing run)
   * Science Pipelines version and rubin-env version
   * Supported `APDB schema`_ and `ApdbSql`_/ `ApdbCassandra`_/ `ApdbCassandraReplica`_ versions (see `DMTN-269`_ for rationale).
     A stack quoting a given minor version is compatible with *older* APDBs of that major version but not necessarily newer ones; for example, a release whose baseline is APDB schema 1.4.0 can access a schema 1.0.0 or 1.4.1 database, but not schema 1.5.
   * Supported `Butler dimensions-config`_ versions
   * The `Alerts schema`_ version used for output (see `DMTN-093`_ for details)

   Select **Publish Release**.

   The `Release CI <https://github.com/lsst-dm/prompt_processing/actions/workflows/ci-release.yaml>`_ GitHub Actions workflow uploads the new release to GitHub packages.
   This may take a few minutes, and the release is not usable until it succeeds.

.. _DMTN-093: https://dmtn-093.lsst.io/#alertmanagement

.. _DMTN-269: https://dmtn-269.lsst.io/

.. _Butler dimensions-config: https://pipelines.lsst.io/v/daily/modules/lsst.daf.butler/dimensions.html#dimension-universe-change-history

.. _APDB schema: https://github.com/lsst/sdm_schemas/blob/main/python/lsst/sdm/schemas/apdb.yaml#L4

.. _ApdbSql: https://github.com/lsst/dax_apdb/blob/main/python/lsst/dax/apdb/sql/apdbSql.py#L71-L75

.. _ApdbCassandra: https://github.com/lsst/dax_apdb/blob/main/python/lsst/dax/apdb/cassandra/apdbCassandra.py#L87-L91

.. _ApdbCassandraReplica: https://github.com/lsst/dax_apdb/blob/main/python/lsst/dax/apdb/cassandra/apdbCassandraReplica.py#L52-L56

.. _Alerts schema: https://github.com/lsst/alert_packet/blob/main/python/lsst/alert/packet/schema/latest.txt


Patch Releases and Release Branches
-----------------------------------

During commissioning and operations, it may be necessary to quickly deploy a bug fix without making any other changes that might potentially introduce new breakages.
This can be done using a patch version (``X.Y.Z``).
If there have been unrelated changes committed since the last release, you will need to isolate the bug fixes on a release branch.

If the repo does not already have a release branch, create one anchored at the corresponding minor version tag:

.. code-block:: sh

   git checkout -b releases/X.Y X.Y.0
   git push -u origin releases/X.Y

If you have a branch for your bug fix, you can make a copy for the release branch:

.. code-block:: sh

   git checkout -b tickets/DM-XXXXX-X.Y tickets/DM-XXXXX
   git rebase --onto releases/X.Y <last main commit before your branch>

Otherwise, you'll have to cherry-pick from ``main``:

.. code-block:: sh

   git checkout -b tickets/DM-XXXXX-X.Y releases/X.Y
   git cherry-pick <last commit before your changes>..<last non-merge commit>

Either way, the ``tickets/DM-XXXXX-X.Y`` branch should consist of ``releases/X.Y``, plus the changes you are trying to backport.

.. note::

   If you are trying to backport multiple tickets' changes at once, you may open a new Jira ticket for the backports, and create one branch for just that ticket.
   You must list all the tickets you are backporting on the omnibus ticket so that the information isn't lost.

Check that the ``latest`` base container is the same as was used for the ``X.Y.0`` release.
Rebuild ``latest`` to match if it's not.

Create a PR for the ``tickets/DM-XXXXX-X.Y`` branch to merge it into ``releases/X.Y`` (**not** ``main``!), and test the resulting build in the dev environment.
Make sure the PR title is as descriptive as the original, because it will appear in the patch release notes.
You do not need to review before merging.

Then, follow the usual procedure for making a release, except that the target on the New Release page should be the release branch, not ``main``.
Check again that the ``latest`` base container matches ``X.Y.0`` before publishing the release.


Buckets
=======

`This document <https://rubinobs.atlassian.net/wiki/spaces/LSSTOps/pages/45636680/USDF+S3+Bucket+Organization>`_ describes the overall organization of S3 buckets and access at USDF.

For development purposes, Prompt Processing has its own buckets, including ``rubin-pp-dev``, ``rubin-pp-dev-users``, ``rubin:rubin-pp``, and ``rubin:rubin-pp-users``.

Current Buckets
---------------

Currently the buckets ``rubin-pp-dev`` and ``rubin-pp-dev-users`` are used with the testers (see `Testers`_).
They are owned by the Ceph user ``prompt-processing-dev``.

The bucket ``rubin-pp-dev`` holds incoming raw images.

The bucket ``rubin-pp-dev-users`` holds:

* ``rubin-pp-dev-users/central_repo_2/`` contains the central repository described in `DMTN-219`_.
  This repository currently contains HSC, LATISS, and LSSTCam data, uploaded with ``make_export.py``.

* ``rubin-pp-dev-users/unobserved/`` contains raw files that the upload scripts can draw from to create incoming raws.

* ``rubin-pp-dev-users/apdb_config/`` contains the canonical configs identifying the development APDBs.

* ``rubin-pp-dev-users/iers-cache.zip`` contains an IERS data cache for direct download by Prompt Processing.
  It can be updated manually by running ``export_iers.py``.

``rubin-pp-dev`` has notifications configured for new file arrival; these publish to the Kafka topic ``prompt-processing-dev``.
The notifications can be viewed at `Kafdrop <https://k8s.slac.stanford.edu/usdf-prompt-processing-dev/kafdrop>`_.

Legacy Buckets
--------------

The buckets ``rubin:rubin-pp`` and ``rubin:rubin-pp-users`` are also for Prompt Processing development and previously used by the testers.
``rubin:rubin-pp-users`` contains an older version of the development central repository.
``rubin:rubin-pp`` has notifications configured to publish to the Kafka topic ``rubin-prompt-processing``.

These buckets are owned by the Ceph user ``rubin-prompt-processing``.
We are in the process of deprecating the ``rubin-prompt-processing`` user as it has more restrictive permissions than ``prompt-processing-dev``.

Bucket Access and Credentials
-----------------------------

The default Rubin users' setup on ``rubin-devl`` includes an AWS credential file at the environment variable ``AWS_SHARED_CREDENTIALS_FILE`` and a default profile without read permission to the prompt processing buckets.
A separate credential for prompt processing developers as the Ceph user ``prompt-processing-dev`` (version 6 or newer) or ``rubin-prompt-processing`` (version 5 or older) is at  `Vault <https://vault.slac.stanford.edu/ui/vault/secrets/secret/show/rubin/usdf-prompt-processing-dev/s3-buckets>`_.
The credential can be set up as another credential profile for Butler or command line tools such as AWS Command Line Interface and MinIO Client.
One way to set up this profile is with the AWS CLI:

.. code-block:: sh

   singularity exec /sdf/sw/s3/aws-cli_latest.sif aws configure --profile prompt-processing-dev

and follow the prompts.
To use the new credentials with the Butler, set the environment variable ``AWS_PROFILE=prompt-processing-dev``.

The AWS CLI can be used to inspect non-tenant buckets:

.. code-block:: sh

   alias s3="singularity exec /sdf/sw/s3/aws-cli_latest.sif aws --endpoint-url https://s3dfrgw.slac.stanford.edu s3"
   s3 --profile prompt-processing-dev [ls|cp|rm] s3://rubin-summit/<path>

.. note::

   You must pass the ``--endpoint-url`` argument even if you have ``S3_ENDPOINT_URL`` defined.

Those buckets starting with ``rubin:`` are Ceph tenant buckets with the tenant prefix.
The bucket name with the tenant prefix violates the standard and is not supported by AWS CLI.
The MinIO Client ``mc`` tool may be used.
One version can be accessed at ``/sdf/group/rubin/sw/bin/mc`` at USDF.
To inspect buckets with the MinIO Client ``mc`` tool, first set up an alias (e.g. ``prompt-processing-dev``) and then can use commands:

.. code-block:: sh

    mc alias set prompt-processing-dev https://s3dfrgw.slac.stanford.edu ACCESS_KEY SECRET_KEY
    mc ls prompt-processing-dev/rubin:rubin-pp


For Butler not to complain about the bucket names, set the environment variable ``LSST_DISABLE_BUCKET_VALIDATION=1``.

Central Repo
============

The central repo for development use is located at ``s3://rubin-pp-dev-users/central_repo_2/``.
You need developer credentials to access it, as described under `Buckets`_.
To run ``butler`` commands, which access the registry, you also need to set ``PGUSER=pp``.

Butler Dimensions Schema Versions
---------------------------------

In general, Prompt Processing can support a range of schema versions: the lower limit is set by assumptions in Prompt Processing code, while the upper limit is set by the underlying Science Pipelines version.
To confirm that we're compatible with the full range, the unit test repo in ``tests/data/central_repo`` should be set to the *lowest* version we offer support for, while the dev central repo should be set to the *highest*.

We should try to support the most recent version that we can, to avoid holding up upgrades of shared repos.
In particular, we should migrate the dev repo to a version, and confirm that we support it, before the Middleware team migrates the production repo (currently ``/repo/embargo``) to that version.

Migrating the Repo
------------------

To perform a schema migration, download the ``migrate`` extension to ``butler``:

.. code-block:: sh

   git clone https://github.com/lsst-dm/daf_butler_migrate/
   cd daf_butler_migrate
   setup -r .
   scons -j 6

This activates ``butler migrate``.
Next, follow the instructions in the `daf.butler_migrate documentation <https://github.com/lsst-dm/daf_butler_migrate/blob/main/doc/lsst.daf.butler_migrate/typical-tasks.rst>`_.
In our case, we want to migrate to the versions that ``/repo/embargo`` is using, which are not necessarily the latest; you can check the desired version by running ``butler migrate show-current`` on ``/repo/embargo``.

.. note::

   Because our local repos both import from and export to the central repo, they must have exactly the same version of ``dimensions-config`` as the central repo.
   This is automatically taken care of on pod start.
   However, when using ``butler migrate`` to update ``dimensions-config``, you should delete all existing pods to ensure that their replacements have the correct version.
   This can be done using ``kubectl delete pod`` or from Argo CD (see `Development Service`_).

Updating Table Permissions
--------------------------

Some ``dimensions-config`` migrations add new tables to the Butler registry schema.
When this happens, our service accounts need to be explicitly given permission to work with those new tables.

To update permissions, use ``psql`` to log in to the registry database as the owner (``pp`` for our dev repo).
See `Databases`_ for more information on using ``psql`` in general.
See ``butler.yaml`` for the address and namespace of the registry.

To inspect table permissions:

.. code-block:: psql

   set search_path to <namespace>;
   \dp

Most tables should grant the SELECT (r) and UPDATE (w) `PostgreSQL privileges`_ to all service users (currently ``latiss_prompt``, ``hsc_prompt``, and ``lsstcam_prompt``).
Some tables also need INSERT (a) and DELETE (d).

We need SELECT (r) and USAGE (U) permissions for the sequence ``collection_seq_collection_id``, but *not* for ``dataset_calibs_*_seq_id``, ``dataset_type_seq_id``, or ``dimension_graph_key_seq_id``.
We expect that most future sequences will only be touched by repository maintenance and not by pipeline runs or data transfers.

If any tables are missing permissions, run:

.. code-block:: psql

   GRANT insert, select, update ON TABLE "<table1>", "<table2>" TO hsc_prompt, latiss_prompt, lsstcam_prompt;

See the `GRANT command`_ for other options.

.. _PostgreSQL privileges: https://www.postgresql.org/docs/current/ddl-priv.html

.. _GRANT command: https://www.postgresql.org/docs/current/sql-grant.html

Adding New Dataset Types
------------------------

When pipelines change, sometimes it is necessary to register the new dataset types in the central repo so to avoid ``MissingDatasetTypeError`` at prompt service export time.
One raw was ingested, visit-defined, and kept in the development central repo, so a ``pipetask`` like the following can be run:

.. code-block:: sh

   apdb-cli create-sql "sqlite:///apdb.db" apdb_config.yaml
   pipetask run -b s3://rubin-pp-dev-users/central_repo_2 -i LSSTCam/raw/all,LSSTCam/defaults,LSSTCam/templates -o u/${USER}/add-dataset-types -d "instrument='LSSTCam' and exposure=2025050100367 and detector=30" -p $AP_PIPE_DIR/pipelines/LSSTCam/ApPipe.yaml -c parameters:apdb_config=apdb_config.yaml -c associateApdb:doPackageAlerts=False --register-dataset-types --init-only
   pipetask run -b s3://rubin-pp-dev-users/central_repo_2 -i LSSTCam/raw/all,LSSTCam/defaults,LSSTCam/templates -o u/${USER}/add-dataset-types -d "instrument='LSSTCam' and exposure=2025050100367 and detector=30" -p $AP_PIPE_DIR/pipelines/LSSTCam/SingleFrame.yaml -c parameters:apdb_config=apdb_config.yaml --register-dataset-types --init-only

.. note::

   The use of ``$AP_PIPE_DIR`` is not a typo.
   The Prompt Processing pipelines run subsets that only work in the context of Prompt Processing; running the baseline version of the pipeline ensures that *all* dataset types are registered.


Development Service
===================

The service can be controlled with ``kubectl`` from ``rubin-devl``.
You must first `get credentials for the development cluster <https://k8s.slac.stanford.edu/usdf-prompt-processing-dev>`_ on the web; ignore the installation instructions and copy the commands from the second box.
Credentials must be renewed if you get a "cannot fetch token: 400 Bad Request" error when running ``kubectl``.

The service container deployment is managed using `Argo CD and Phalanx <https://usdfdev-prompt-processing.slac.stanford.edu/argo-cd>`_.
See the `Phalanx`_ docs for information on working with Phalanx in general (including special developer environment setup).

There are two different ways to deploy a development release of the service:

**Argo patching**:
If you will not be making permanent changes to the Phalanx config, go to the Argo UI, select the specific ``prompt-keda-<instrument>`` service, then select "Details" from the top bar.
Open the "Parameters" tab, click "edit", then update the ``prompt-keda.image.tag`` key to point to the new service container (likely a ticket branch instead of ``latest``).
The changes will not take effect until you click "SYNC" on the main screen.
Changes made in the Parameters tab last indefinitely, so be sure to undo them by clicking "edit" followed by the "Remove override" link.

.. important::

   Never remove the overrides for ``global.host``, ``global.baseUrl``, or ``global.vaultSecretsPath``.

.. note::

   Changes to parameters do not show up in any Diff and aren't overwritten by syncing to a branch.
   The only way to see if a parameter has been changed is to go to the Parameters tab; all overridden parameters are grouped at the top of the list.

**Phalanx branch**:
If you will be making permanent changes, clone the `lsst-sqre/phalanx`_ repo and navigate to the ``applications/prompt-keda-<instrument>`` directory.
Edit ``values-usdfdev-prompt-processing.yaml`` to point to the new service container (likely a ticket branch instead of ``latest``) and push the branch.
You do not need to create a PR.
Then, in the Argo UI, follow the instructions in `the Phalanx docs <https://phalanx.lsst.io/developers/deploy-from-a-branch.html#switching-the-argo-cd-application-to-sync-the-branch>`_.
To force a container update without a corresponding ``phalanx`` update, you need to edit ``template.metadata.annotations.revision`` as described above -- `restarting a deployment <https://phalanx.lsst.io/developers/deploy-from-a-branch.html#restarting-a-deployment>`_ that's part of a service does not check for a newer container, even with Always pull policy.

.. note::

   We used to be able to make changes in Argo by directly patching the Kubernetes config for specific ScaledJob nodes.
   This is no longer safe, because there are multiple components that need to share certain configs.
   Always do your updates either in Argo's Parameters view or in ``values*.yaml`` files.

.. _Phalanx: https://phalanx.lsst.io/developers/
.. _lsst-sqre/phalanx: https://github.com/lsst-sqre/phalanx/

The service configuration is in each instrument's ``values.yaml`` (for settings shared between development and production) and ``values-usdfdev-prompt-processing.yaml`` (for development-only settings).
``values.yaml`` and ``README.md`` provide documentation for all settings.
The actual Kubernetes configs (and the implementation of new config settings or secrets) are in ``charts/prompt-keda/templates/``.
These files fully support the Go template syntax.

A few useful commands for managing the service:

* ``kubectl config set-context usdf-prompt-processing-dev --namespace=prompt-keda-<instrument>`` sets the default namespace for the following ``kubectl`` commands to ``prompt-keda-<instrument>``.
* ``kubectl get serving`` summarizes the state of the service, including which revision(s) are currently handling messages.
  A revision with 0 replicas is inactive.
* ``kubectl get pods`` lists the Kubernetes pods that are currently running, how long they have been active, and how recently they crashed.
* ``kubectl logs <pod>`` outputs the entire log associated with a particular pod.
  This can be a long file, so consider piping to ``less`` or ``grep``.
  ``kubectl logs`` also offers the ``-f`` flag for streaming output.

Troubleshooting
---------------

Printing Timing Logs
^^^^^^^^^^^^^^^^^^^^

The code is filled with timing blocks, but by default their logs are not emitted.
To see timer results, set ``SERVICE_LOG_LEVELS`` to include ``timer.lsst.activator=DEBUG`` in the Prompt Processing config.

Deleting Keda Scaled Job
^^^^^^^^^^^^^^^^^^^^^^^^

In an emergency Keda Scaled Jobs can be deleted to terminate currently running pods and prevent new pods from spawning.  Within ArgoCD select the Application.  On the ``scaledjob`` panel select the three dots in the top right then ``Delete``.  A ``SYNC`` can be perfomed later to restore the Scaled Job.  Scaled Jobs can also be deleted with ``kubectl``.  An example for HSC is below.

.. code-block:: sh

   kubectl delete scaledjob prompt-keda-hsc -n prompt-keda-hsc

Deleting Old Services
^^^^^^^^^^^^^^^^^^^^^

Normally, old revisions of a service are automatically removed when a new revision is deployed.
However, sometimes an old revision will stick around; this seems to be related to Python errors from bad code.
Such revisions usually manifest as a "CrashLoopBackOff" pod in ``kubectl get pods``.

To delete such services manually:

.. code-block:: sh

   kubectl get revision  # Find the name of the broken revision
   kubectl delete revision <revision name>

.. note::

   There's no point to deleting the pod itself, because the service will just recreate it.

Identifying a Pod's Codebase
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To identify which version of Prompt Processing a pod is running, run

.. code-block:: sh

   kubectl describe pod <pod name> | grep "prompt-service@"

This gives the hash of the service container running on that pod.
Actually mapping the hash to a branch version may require a bit of detective work; `the GitHub container registry <https://github.com/lsst-dm/prompt_processing/pkgs/container/prompt-service>`_ (which calls hashes "Digests") is a good starting point.

To find the version of Science Pipelines used, find the container's page in the GitHub registry, then search for ``EUPS_TAG``.

Inspecting a Pod
^^^^^^^^^^^^^^^^

To inspect the state of a pod (e.g., the local repo):

.. code-block:: sh

   kubectl exec -it <pod name> -- bash

Then in the pod:

.. code-block:: sh

   source /opt/lsst/software/stack/loadLSST.bash

The local repo is a directory of the form ``/tmp-butler/butler-????????``.
There should be only one local repo per ``MiddlewareInterface`` object, though each ready worker may have its own repo.
If in doubt, check the logs first.


Testers
=======

``python/tester/upload.py`` and ``python/tester/upload_from_repo.py`` are scripts that simulate the CCS image writer.
It can be run from ``rubin-devl``.

You must have a profile set up for the ``rubin-pp-dev`` bucket (see `Buckets`_, above).

Install the Prompt Processing code, and set it up before use:

.. code-block:: sh

    git clone https://github.com/lsst-dm/prompt_processing
    setup -r prompt_processing

The tester scripts send ``next_visit`` events for each detector via Kafka on the ``next-visit-topic`` topic.
They then upload a batch of files representing the snaps of the visit to the ``rubin-pp-dev`` S3 bucket, simulating incoming raw images.

``python/tester/upload.py``: Command line arguments are the instrument name (currently HSC, LATISS, and LSSTCam), and the number of groups of images to send.

Sample command line:

.. code-block:: sh

   python upload.py LATISS 3
   python upload.py LSSTCam 5

This script draws images stored in the ``rubin-pp-dev-users`` bucket.

* For HSC, 4 groups, in total 10 raw files, are curated.
  They are the COSMOS data as curated in `ap_verify_ci_cosmos_pdr2 <https://github.com/lsst/ap_verify_ci_cosmos_pdr2>`_.
* For LATISS, 3 groups, in total 3 raw fits files and their corresponding json metadata files, are curated.
  One of the files, the unobserved group `2024-09-04T05:59:29.342`, has no templates and is known to fail `calibrateImage` in determining PSF.
  This visit can test pipeline fallback features.
* For LSSTCam, 5 groups, in total 10 raw fits files and their corresponding json metadata files, are curated.

``python/tester/upload_from_repo.py``: Command line arguments are a configuration file, and the number of groups of images to send.

Sample command line:

.. code-block:: sh

   python upload_from_repo.py $PROMPT_PROCESSING_DIR/etc/tester/HSC.yaml 3
   python upload_from_repo.py $PROMPT_PROCESSING_DIR/etc/tester/LATISS.yaml 2 --ordered

This scripts draws images from a butler repository as defined in the input configuration file.
A butler query constrains the data selection.
By default, visits are randomly selected and uploaded as one new group for each visit.
With the optional ``--ordered`` command line argument, images are uploaded following the order of the original exposure IDs.
Currently the upload script does not follow the actual relative timing of the input exposures.
Images can be uploaded in parallel processes.


next_visit Events
=================

The schema of the ``next_visit`` events from the summit can be found at `ScriptQueue documentation <https://ts-xml.lsst.io/sal_interfaces/ScriptQueue.html#nextvisit>`_.

To implement schema changes in the development environment:

* Update the ``*Visit`` classes in ``python/activator/visit.py`` accordingly.
* Update the upload tester scripts ``python/tester/upload.py`` and ``python/tester/upload_from_repo.py`` where simulated ``next_visit`` events originate.
* Update relevant unit tests.
* Register the new schema to the Sasquatch's schema registry for the ``test.next-visit-job`` topic.
  The `Sasquatch documentation <https://sasquatch.lsst.io/user-guide/avro.html>`_ describes the schema evolution.
  The script ``test-msg-dev.sh`` in the `next_visit_fan_out`_ repo can be run on ``rubin-devl`` to send a test event with the new schema; the `Sasquatch REST Proxy <https://sasquatch.lsst.io/user-guide/restproxy.html>`_ will register the new schema and the new schema id will be sent back as ``value_schema_id`` in the HTTP response.
  Use the new schema id in the ``send_next_visit`` utility function used in the testers.
  The test events can be viewed on `Kafdrop <https://usdf-rsp-dev.slac.stanford.edu/kafdrop/topic/test.next-visit-job>`_.
* Update the schema used in the `next_visit_fan_out`_ service.
* Re-deploy and test services.

.. _next_visit_fan_out: https://github.com/lsst-dm/next_visit_fan_out

Databases
=========

A database server is running at ``postgresql:://usdf-prompt-processing-dev.slac.stanford.edu``.
The server runs two databases: ``ppcentralbutler`` (for the Butler registry) and ``lsst-devl`` (for the APDB).

The ``psql`` client is available from ``rubin-env-developer`` 5.0 and later.
The server is visible from ``rubin-devl``, and can be accessed through, e.g.,

.. code-block:: sh

   psql -h usdf-prompt-processing-dev.slac.stanford.edu lsst-devl rubin

Credentials
-----------

Postgres
^^^^^^^^

For passwordless login, create a ``~/.pgpass`` file with contents:

.. code-block::

   # Dev APDBs
   usdf-prompt-processing-dev.slac.stanford.edu:5432:lsst-devl:rubin:PASSWORD
   # Dev central repo, can also go in db-auth (see below)
   usdf-prompt-processing-dev.slac.stanford.edu:5432:ppcentralbutler:pp:PASSWORD

and execute ``chmod 0600 ~/.pgpass``.

Cassandra
^^^^^^^^^

We have a Cassandra cluster at the USDF on dedicated hardware, that is currently deployed in parallel across 12 nodes.
Of those, 6 are reserved for Andy Salnikov's development and testing, and 6 are available for Prompt Processing.
The nodes available for Prompt Processing are ``sdfk8sk001`` through ``sdfk8sk006``.

To access the Cassandra cluster, you must add credentials to your ``~/.lsst/db-auth.yaml``.
The appropriate credentials are stored in the `SLAC Vault <https://vault.slac.stanford.edu/ui/vault/secrets/secret/show/rubin/usdf-apdb-dev/cassandra>`_.
Add the following to your ``db-auth.yaml``, replacing ``PORT`` and ``PASSWORD`` from the Vault:

.. code-block:: sh

   # Cassandra dev APDBs
   - url: cassandra://sdfk8sk001.sdf.slac.stanford.edu:PORT/pp_apdb_*_dev
     username: apdb
     password: PASSWORD
   # Dev central repo, can also go in .pgpass (see above)
   - url: postgresql://usdf-prompt-processing-dev.slac.stanford.edu/ppcentralbutler
     username: pp
     password: PASSWORD
   # Workaround for list-cassandra not having keyspace-agnostic credentials, MUST go after all other entries
   - url: cassandra://sdfk8sk001.sdf.slac.stanford.edu:PORT/*
     username: ANY_CASSANDRA_ACCOUNT
     password: PASSWORD

and execute ``chmod 0600 ~/.lsst/db-auth.yaml``.

Creating an APDB
----------------

Postgres
^^^^^^^^

From ``rubin-devl``, new APDB schemas can be created in the usual way:

.. code-block:: sh

   apdb-cli create-sql --namespace="pp_apdb_latiss" \
       "postgresql://rubin@usdf-prompt-processing-dev.slac.stanford.edu/lsst-devl" apdb_config_latiss.yaml
   apdb-cli metadata set apdb_config_latiss.yaml instrument LATISS

Cassandra
^^^^^^^^^

To set up a new keyspace and connection, use:

.. code-block:: sh

   apdb-cli create-cassandra sdfk8sk001.sdf.slac.stanford.edu sdfk8sk004.sdf.slac.stanford.edu \
       pp_apdb_latiss_dev pp_apdb_latiss-dev.yaml --user apdb --replication-factor=3 --enable-replica
   apdb-cli metadata set pp_apdb_latiss-dev.yaml instrument LATISS

Here ``sdfk8sk001.sdf.slac.stanford.edu`` and ``sdfk8sk004.sdf.slac.stanford.edu`` are two nodes within the Prompt Processing allocation, which are the ``contact_points`` used for the initial connection.
All of the available nodes will be used.
In the above example, ``pp_apdb_latiss`` is the Cassandra keyspace (similar to schema for Postgres), and ``pp_apdb_latiss-dev.yaml`` is the usual APDB config.

The APDB Index
--------------

Standard APDBs, including those used by Prompt Processing, are registered in the file pointed to by ``$DAX_APDB_INDEX_URI``.
This file is **not** visible from Prompt Processing pods, but can be used to operate on existing DBs from ``sdfrome``.
For example, the ``dev`` LATISS APDB is registered under ``pp-dev:latiss``, and ``Apdb`` calls and ``apdb-cli`` commands can substitute ``label:pp-dev:latiss`` for the config URI everywhere except database creation.

In most cases, there is no need to edit the registry.
If you are creating a genuinely new APDB (for example, for a new instrument), add its entry(ies) to the file.
All Prompt Processing APDBs store their config file on S3, so that the file is visible to the pods.

Resetting the APDB
------------------

To restore the APDB to a clean state, add the ``--drop`` option to  ``apdb-cli create-sql`` or ``apdb-cli create-cassandra`` which will recreate all tables:

.. code-block:: sh

   apdb-cli create-sql --drop --namespace="pp_apdb_latiss" \
       "postgresql://rubin@usdf-prompt-processing-dev.slac.stanford.edu/lsst-devl" apdb_config_latiss.yaml
   apdb-cli metadata set apdb_config_latiss.yaml instrument LATISS

Checking the APDB Version
-------------------------

If you have credentials for `rubin-pp-dev-users` configured (see `Buckets`_), you can identify an APDB's schema and ApdbSql/ApdbCassandra versions with ``apdb-cli``.
For example:

.. code-block:: sh

   apdb-cli metadata show label:pp-dev:latiss:sql

See ``apdb-cli list-index`` for a list of valid labels.

For a PostgreSQL APDB, you can do the check without bucket access by running, e.g.:

.. code-block:: sh

   psql -h usdf-prompt-processing-dev.slac.stanford.edu lsst-devl rubin \
       -c 'select * from pp_apdb_latiss.metadata;'

Upgrading the APDB Schema
-------------------------

To perform an APDB schema upgrade, download the ``apdb_migrate`` extension to ``dax``:

.. code-block:: sh

   git clone https://github.com/lsst-dm/dax_apdb_migrate/
   cd dax_apdb_migrate
   setup -r .
   scons -j 6

This activates ``apdb-migrate-sql``.
Next, follow the instructions in the `lsst.dax.apdb_migrate documentation <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/doc/lsst.dax.apdb_migrate/typical-tasks.rst>`_.
In our case, we want to upgrade when we update ``latest`` to a version that has changes to the APDB schema.

.. note::

   Currently this script only works for Postgres APDB databases and cannot be used for Cassandra APDB databases.

REDIS STREAMS
=============

The Next Visit Fan Out Service sends fanned out events to Redis Streams.  Within Redis Streams a stream is configured for each instrument along with a corresponding consumer group and Prompt Processing is configured with a consumer group to read pending messages.   The naming for the streams is ``instrument:<instrument_name>`` so HSC for example is ``instrument:hsc``


Redis CLI
---------

Redis has a CLI to view and manage Redis Streams.  From the appropriate dev or production Prompt Processing vCluster access the CLI with ``kubectl exec -it prompt-redis-0 -n prompt-redis -- redis-cli``.  Below displays a successful connection validated with a ``ping``.

.. code-block:: sh

   kubectl exec -it prompt-redis-0 -n prompt-redis -- redis-cli
   127.0.0.1:6379> ping
   PONG



Creating Redis Streams
----------------------

Redis Streams are created using the `Redis CLI`_.  The ``XGROUP CREATE`` command is used to create the Redis Stream and Consumer Group.  If the Redis Cluster is destroyed the streams need to be recreated with the below commands.  For Dev:

.. code-block:: sh

   XGROUP CREATE instrument:hsc hsc_consumer_group $ mkstream
   XGROUP CREATE instrument:latiss latiss_consumer_group $ mkstream
   XGROUP CREATE instrument:lsstcam lsstcam_consumer_group $ mkstream

For Prod:

.. code-block:: sh

   XGROUP CREATE instrument:lsstcam lsstcam_consumer_group $ mkstream


Viewing Messages Statistics
---------------------------

Prompt Processing is configured to ignore messages that have already been read by another consumer. To view messages statistics for a consumer group enter ``XINFO GROUPS <consumer_group_name>`` with the `Redis CLI`_.  An example below with the LSSTCam consumer group.  The ``lag`` is ``9`` so 9 messages have not been acknowledged.  To manually clear these messages see `Clear Redis Stream`_

.. code-block:: sh

   127.0.0.1:6379> XINFO GROUPS instrument:lsstcam
   1)  1) "name"
      2) "lsstcam_consumer_group"
      3) "consumers"
      4) (integer) 704
      5) "pending"
      6) (integer) 0
      7) "last-delivered-id"
      8) "1740691320501-1"
      9) "entries-read"
      10) (integer) 72
      11) "lag"
      12) (integer) 9

In the above example there are 72 ``entries-read`` which is 72 messages acknowledged.    ``pending`` is how many messages are being actively processed by consumers, but not acknowledged yet.

Clear Redis Stream
------------------
To delete all the events in a Redis stream the ``DEL`` command can be used.  Below is an example with LSSTCam to delete and recreate the Redis Stream.  Please note there will be errors generated by Keda any Scaled Jobs connected to the stream when the stream is deleted.

.. code-block:: sh

   127.0.0.1:6379> DEL instrument:lsstcam
   
   (integer) 1
   127.0.0.1:6379> XGROUP CREATE instrument:lsstcam lsstcam_consumer_group $ mkstream
   
   OK
