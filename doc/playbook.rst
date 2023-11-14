######################################################
Developers' Playbook for the Prompt Processing Service
######################################################

.. _DMTN-219: https://dmtn-219.lsst.io/

Table of Contents
=================

* `Containers`_
* `Buckets`_
* `Development Service`_
* `tester`_
* `Databases`_


Containers
==========

The service consists of two containers.
The first is a base container with the Science Pipelines "stack" code and networking utilities.
The second is a service container made from the base that has the Prompt Processing service code.
All containers are managed by `GitHub Container Registry <https://github.com/orgs/lsst-dm/packages?repo_name=prompt_processing>`_ and are built using GitHub Actions.

To build the base container:

* If there are changes to the container, push them to a branch, then open a PR.
  The container should be built automatically.
* If there are no changes (typically because you want to use an updated Science Pipelines container), go to the repository's `Actions tab <https://github.com/lsst-dm/prompt_processing/actions/workflows/build-base.yml>`_ and select "Run workflow".
  From the dropdown, select the branch whose container definition will be used, and the label of the Science Pipelines container.
* New containers built from ``main`` are tagged with the corresponding Science Pipelines release (plus ``w_latest`` or ``d_latest`` if the release was requested by that name).
  For automatic ``main`` builds, or if the corresponding box in the manual build is checked, the new container also has the ``latest`` label.
  Containers built from a branch use the same scheme, but prefixed by the ticket number or, for user branches, the branch topic.

.. note::

   If a PR automatically builds both the base and the service container, the service build will *not* use the new base container unless you specifically override it (see below).
   Even then, the service build will not wait for the base build to finish.
   You may need to manually rerun the service container build to get it to use the newly built base.

To build the service container:

* If there are changes to the service, push them to a branch, then open a PR.
  The container should be built automatically using the ``latest`` base container.
* To force a rebuild manually, go to the repository's `Actions tab <https://github.com/lsst-dm/prompt_processing/actions/workflows/build-service.yml>`_ and select "Run workflow".
  From the dropdown, select the branch whose code should be built.
  The container will be built using the ``latest`` base container, even if there is a branch build of the base.
* To use a base other than ``latest``, edit ``.github/workflows/build-service.yml`` on the branch and override the ``BASE_TAG_LIST`` variable.
  Be careful not to merge the temporary override to ``main``!
* New service containers built from ``main`` have the tags of their base container.
  Containers built from a branch are prefixed by the ticket number or, for user branches, the branch topic.

.. note::

   The ``PYTHONUNBUFFERED`` environment variable defined in the Dockerfiles for the containers ensures that container logs are emitted in real-time.

Stable Base Containers
----------------------

In general, the ``latest`` base container is built from a weekly or other stable Science Pipelines release.
However, it may happen that the ``latest`` base is used for development while production runs should use an older build.
If this comes up, edit ``.github/workflows/build-service.yml`` and append the desired base build to the ``BASE_TAG_LIST`` variable.
Any subsequent builds of the service container will build against both bases.

This is the only situation in which a change to ``BASE_TAG_LIST`` should be committed to ``main``.


Buckets
=======

The bucket ``rubin:rubin-pp`` holds incoming raw images.

The bucket ``rubin:rubin-pp-users`` holds:

* ``rubin:rubin-pp-users/central_repo/`` contains the central repository described in `DMTN-219`_.
  This repository currently contains a copy of HSC RC2 data, uploaded with ``make_hsc_rc2_export.py`` and ``make_template_export``.

* ``rubin:rubin-pp-users/unobserved/`` contains raw files that the upload script(s) can draw from to create incoming raws.

``rubin:rubin-pp`` has had notifications configured for it; these publish to a Kafka topic.

The default Rubin users' setup on ``rubin-devl`` includes an AWS credential file at the environment variable ``AWS_SHARED_CREDENTIALS_FILE`` and a default profile without read permission to the prompt processing buckets.
A separate credential for prompt processing developers is at  `vault <https://vault.slac.stanford.edu/ui/vault/secrets/secret/show/rubin/usdf-prompt-processing-dev/s3-buckets>`_ and can be set up as another credential profile for Butler or AWS Command Line Interface.
One way to set up this profile is with the AWS CLI:

.. code-block:: sh

   singularity exec /sdf/sw/s3/aws-cli_latest.sif aws configure --profile rubin-prompt-processing

and follow the prompts.
To use the new credentials with the Butler, set the environment variable ``AWS_PROFILE=rubin-prompt-processing``.

The AWS CLI can be used to inspect non-tenenat buckets:

.. code-block:: sh

   alias s3="singularity exec /sdf/sw/s3/aws-cli_latest.sif aws --endpoint-url https://s3dfrgw.slac.stanford.edu s3"
   s3 --profile rubin-prompt-processing [ls|cp|rm] s3://rubin-summit/<path>

.. note::

   You must pass the ``--endpoint-url`` argument even if you have ``S3_ENDPOINT_URL`` defined.

The prompt processing buckets are Ceph tenant buckets and require a tenant prefix.
To inspect them with the MinIO Client ``mc`` tool, first set up an alias (e.g. ``usdf-pp``) and then can use commands:

.. code-block:: sh

    mc alias set usdf-pp https://s3dfrgw.slac.stanford.edu ACCESS_KEY SECRET_KEY
    mc ls usdf-pp/rubin:rubin-pp


For Butler not to complain about the bucket names, set the environment variable ``LSST_DISABLE_BUCKET_VALIDATION=1``.

Development Service
===================

The service can be controlled with ``kubectl`` from ``rubin-devl``.
You must first `get credentials for the development cluster <https://k8s.slac.stanford.edu/usdf-prompt-processing-dev>`_ on the web; ignore the installation instructions and copy the commands from the second box.
Credentials must be renewed if you get a "cannot fetch token: 400 Bad Request" error when running ``kubectl``.

The service container deployment is managed using `Argo CD and Phalanx <https://k8s.slac.stanford.edu/usdf-prompt-processing-dev/argo-cd>`_.
See the `Phalanx`_ docs for information on working with Phalanx in general (including special developer environment setup).

There are two different ways to deploy a development release of the service:

* If you will not be making permanent changes to the Phalanx config, go to the Argo UI, select the specific ``prompt-proto-service-<instrument>`` service, then select the first "svc" node.
  Scroll down to the live manifest, click "edit", then update the ``template.spec.containers.image`` key to point to the new service container (likely a ticket branch instead of ``latest``).
  The service will immediately redeploy with the new image.
  To force an update of the container, edit ``template.metadata.annotations.revision``.
  *Do not* click "SYNC" on the main screen, as that will undo all your edits.
* If you will be making permanent changes of any kind, the above procedure would force you to re-enter your changes with each update of the ``phalanx`` branch.
  Instead, clone the `lsst-sqre/phalanx`_ repo and navigate to the ``applications/prompt-proto-service-<instrument>`` directory.
  Edit ``values-usdfdev-prompt-processing.yaml`` to point to the new service container (likely a ticket branch instead of ``latest``) and push the branch.
  You do not need to create a PR.
  Then, in the Argo UI, follow the instructions in `the Phalanx docs <https://phalanx.lsst.io/developers/deploy-from-a-branch.html#switching-the-argo-cd-application-to-sync-the-branch>`_.
  To force a container update without a corresponding ``phalanx`` update, you need to edit ``template.metadata.annotations.revision`` as described above -- `restarting a deployment <https://phalanx.lsst.io/developers/deploy-from-a-branch.html#restarting-a-deployment>`_ that's part of a service does not check for a newer container, even with Always pull policy.

.. _Phalanx: https://phalanx.lsst.io/developers/
.. _lsst-sqre/phalanx: https://github.com/lsst-sqre/phalanx/

The service configuration is in each instrument's ``values.yaml`` (for settings shared between development and production) and ``values-usdfdev-prompt-processing.yaml`` (for development-only settings).
``values.yaml`` and ``README.md`` provide documentation for all settings.
The actual Kubernetes config (and the implementation of new config settings or secrets) is in ``charts/prompt-proto-service/templates/prompt-proto-service.yaml``.
This file fully supports the Go template syntax.

A few useful commands for managing the service:

* ``kubectl config set-context usdf-prompt-processing-dev --namespace=prompt-proto-service`` sets the default namespace for the following ``kubectl`` commands to ``prompt-proto-service``.
  Note that many of the workflows in `slaclab/rubin-usdf-prompt-processing`_ run in the ``knative-serving`` or ``knative-eventing`` namespaces; to examine the resources of these workflows, add e.g. ``-n knative-eventing`` to the examples below.
* ``kubectl get serving`` summarizes the state of the service, including which revision(s) are currently handling messages.
  A revision with 0 replicas is inactive.
* ``kubectl get pods`` lists the Kubernetes pods that are currently running, how long they have been active, and how recently they crashed.
* ``kubectl logs <pod>`` outputs the entire log associated with a particular pod.
  This can be a long file, so consider piping to ``less`` or ``grep``.
  ``kubectl logs`` also offers the ``-f`` flag for streaming output.

Troubleshooting
---------------

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

   kubectl describe pod <pod name> | grep "prompt-proto-service@"

This gives the hash of the service container running on that pod.
Actually mapping the hash to a branch version may require a bit of detective work; `the GitHub container registry <https://github.com/lsst-dm/prompt_processing/pkgs/container/prompt-proto-service>`_ (which calls hashes "Digests") is a good starting point.

To find the version of Science Pipelines used, find the container's page in the GitHub registry, then search for ``EUPS_TAG``.

Inspecting a Pod
^^^^^^^^^^^^^^^^

To inspect the state of a pod (e.g., the local repo):

.. code-block:: sh

   kubectl exec -it <pod name> -- bash

Then in the pod:

.. code-block:: sh

   source /opt/lsst/software/stack/loadLSST.bash

The local repo is a directory of the form ``/tmp/butler-????????``.
There should be only one local repo per ``MiddlewareInterface`` object, and at the time of writing there should be only one such object per pod.
If in doubt, check the logs first.


tester
======

``python/tester/upload.py`` and ``python/tester/upload_hsc_rc2.py`` are scripts that simulate the CCS image writer.
It can be run from ``rubin-devl``, but requires the user to install the ``confluent_kafka`` package in their environment.

You must have a profile set up for the ``rubin:rubin-pp`` bucket (see `Buckets`_, above).

Install the Prompt Processing code, and set it up before use:

.. code-block:: sh

    git clone https://github.com/lsst-dm/prompt_processing
    setup -r prompt_processing

The tester scripts send ``next_visit`` events for each detector via Kafka on the ``next-visit-topic`` topic.
They then upload a batch of files representing the snaps of the visit to the ``rubin:rubin-pp`` S3 bucket, simulating incoming raw images.

Eventually a set of parallel processes running on multiple nodes will be needed to upload the images sufficiently rapidly.

``python/tester/upload.py``: Command line arguments are the instrument name (currently HSC or LATISS) and the number of groups of images to send.

Sample command line:

.. code-block:: sh

   python upload.py HSC 3
   python upload.py LATISS 2

This script draws images stored in the ``rubin:rubin-pp-users`` bucket.
For HSC, 4 groups, in total 10 raw files, are curated.
For LATISS, 2 groups, in total 2 raw fits files and their corresponding json metadata files, are curated.

``python/tester/upload_hsc_rc2.py``: Command line argument is the number of groups of images to send.

Sample command line:

.. code-block:: sh

   python upload_hsc_rc2.py 3

This scripts draws images from the curated ``HSC/RC2/defaults`` collection at USDF's ``/repo/main`` butler repository.
The source collection includes 432 visits, each with 103 detector images.
The visits are randomly selected and uploaded as one new group for each visit.


.. note::

   Both of the tester scripts use data from a limited pool of raws every time it is run, while the APDB assumes that every visit has unique timestamps.
   This causes collisions in the APDB that crash the pipeline.
   To prevent this, follow the reset instructions under `Databases`_ before calling ``upload.py`` or ``upload_hsc_rc2.py`` again.


Databases
=========

A database server is running at ``postgresql:://usdf-prompt-processing-dev.slac.stanford.edu``.
The server runs two databases: ``ppcentralbutler`` (for the Butler registry) and ``lsst-devl`` (for the APDB).

The ``psql`` client is available from ``rubin-env-developer`` 5.0 and later.
The server is visible from ``rubin-devl``, and can be accessed through, e.g.,

.. code-block:: sh

   psql -h usdf-prompt-processing-dev.slac.stanford.edu lsst-devl rubin

.. TODO: remove this block after DM-36604.
.. note::

   If you are using an environment older than 5.0, you will have to install psql yourself.
   This can be done on ``rubin-devl`` with ``conda create -n psql postgresql``; thereafter type ``conda activate psql`` to enable it.

For passwordless login, create a ``~/.pgpass`` file with contents:

.. code-block::

   usdf-prompt-processing-dev.slac.stanford.edu:5432:lsst-devl:rubin:PASSWORD
   usdf-prompt-processing-dev.slac.stanford.edu:5432:ppcentralbutler:pp:PASSWORD

and execute ``chmod 0600 ~/.pgpass``.

From ``rubin-devl``, a new APDB schema can be created in the usual way:

.. code-block:: sh

   make_apdb.py -c namespace="pp_apdb" \
       -c db_url="postgresql://rubin@usdf-prompt-processing-dev.slac.stanford.edu/lsst-devl"

Resetting the APDB
------------------

To restore the APDB to a clean state, run the following:

.. code-block:: sh

   psql -h usdf-prompt-processing-dev.slac.stanford.edu lsst-devl rubin -c 'drop schema "pp_apdb" cascade;'
   make_apdb.py -c namespace="pp_apdb" \
       -c db_url="postgresql://rubin@usdf-prompt-processing-dev.slac.stanford.edu/lsst-devl"
