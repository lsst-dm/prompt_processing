#########################################################
Playbook for the Prompt Processing Proposal and Prototype
#########################################################

.. _DMTN-219: https://dmtn-219.lsst.io/

Table of Contents
=================

* `Containers`_
* `Pub/Sub Topics`_
* `Buckets`_
* `Prototype Service`_
* `tester`_
* `Databases`_
* `Middleware Worker VM`_


Containers
==========

The prototype consists of two containers.
The first is a base container with the Science Pipelines "stack" code and Google Cloud Platform utilities.
The second is a service container made from the base that has the Prompt Processing prototype service code.

To build the base container using Google Cloud Build:

.. code-block:: sh

   cd base
   gcloud builds submit --tag us-central1-docker.pkg.dev/prompt-proto/prompt/prompt-proto-base

To build the service container:

.. code-block:: sh

   cd activator
   gcloud builds submit --tag us-central1-docker.pkg.dev/prompt-proto/prompt/prompt-proto-service

These commands publish to Google Artifact Registry.

You will need to authenticate to Google Cloud first using :command:`gcloud auth login`.

.. note::

   The ``PYTHONUNBUFFERED`` environment variable defined in the Dockerfiles for the containers ensures that container logs are emitted in real-time.


Pub/Sub Topics
==============

One Google Pub/Sub topic is used for ``nextVisit`` events.
Additional topics are used for images from each instrument, where the instrument is one of ``LSSTCam``, ``LSSTComCam``, ``LATISS``, ``DECam``, or ``HSC``.

To create the topic, in the Google Cloud Console for the ``prompt-proto`` project:

* Choose "Pub/Sub"
* Choose "Create Topic"
* Set "Topic ID" to ``nextVisit`` or ``{instrument}-image``, replacing ``{instrument}`` with the name from the list above.

The single ``nextVisit`` topic is used for multiple messages per visit, one per detector.
It is also expected that it can be used with multiple instruments, with filtering on the subscription distinguishing between them.
Using a single topic this way could simplify the translator from SAL/DDS.
On the other hand, using multiple topics is also simple to do.


Buckets
=======

Google Cloud
------------

A single bucket named ``rubin-prompt-proto-main`` has been created to hold the central repository described in `DMTN-219`_, as well as incoming raw images.

The bucket ``rubin-prompt-proto-support-data-template`` contains a pristine copy of the calibration datasets and templates.
This bucket is not intended for direct use by the prototype, but can be used to restore the central repository to its state at the start of an observing run.

The bucket ``rubin-prompt-proto-unobserved`` contains raw files that the upload script(s) can draw from to create incoming raws for ``rubin-prompt-proto-main``.

The ``-main`` bucket has had notifications configured for it; these publish to a Google Pub/Sub topic as mentioned in the previous section.
To configure these notifications, in a shell:

.. code-block:: sh

   gsutil notification create \
       -t project/prompt-proto/topics/{instrument}-image \
       -f json \
       -e OBJECT_FINALIZE \
       -p {instrument}/ \
       gs://rubin-prompt-proto-main

This creates a notification on the given topic using JSON format when an object has been finalized (transfer of it has completed).
Notifications are only sent on this topic for objects with the instrument name as a prefix.

USDF
----

The bucket ``rubin-pp`` holds incoming raw images.

The bucket ``rubin-pp-users`` holds the central repository described in `DMTN-219`_.
The central repository currently contains:

* HSC: a copy of `ap_verify_ci_cosmos_pdr2/preloaded@u/kfindeisen/DM-35052-expansion <https://github.com/lsst/ap_verify_ci_cosmos_pdr2/tree/u/kfindeisen/DM-35052-expansion/preloaded>`_.

``rubin-pp`` has had notifications configured for it; these publish to a Kafka topic.

The default Rubin users' setup changes how S3 credentials are handled on ``rubin-devl``.
The best way to set up credentials for either Butler or S3 commands is to run:

.. code-block:: sh

   singularity exec /sdf/sw/s3/aws-cli_latest.sif aws configure --profile rubin-prompt-processing

and follow the prompts.
To use the new credentials with the Butler, set the environment variable ``AWS_PROFILE=rubin-prompt-processing``.

The buckets can be inspected by calling the following from ``rubin-devl``:

.. code-block:: sh

   alias s3="singularity exec /sdf/sw/s3/aws-cli_latest.sif aws --endpoint-url https://s3dfrgw.slac.stanford.edu s3"
   s3 --profile rubin-prompt-processing [ls|cp|rm] s3://rubin-pp-users/<path>

.. note::

   You must pass the ``--endpoint-url`` argument even if you have ``S3_ENDPOINT_URL`` defined.

Prototype Service
=================

The service can be controlled by Google Cloud Run, which will automatically trigger instances based on ``nextVisit`` messages and can autoscale the number of them depending on load.
Each time the service container is updated, a new revision of the service should be edited and deployed.
(Continuous deployment has not yet been set up.)

To create or edit the Cloud Run service in the Google Cloud Console:

* Choose "Create Service" or "Edit & Deploy New Revision"
* Select the container image URL from "Artifact Registry > prompt-proto-service"
* In the Variables & Secrets tab, set the following required parameters:

  * RUBIN_INSTRUMENT: the "short" instrument name
  * PUBSUB_VERIFICATION_TOKEN: choose an arbitrary string matching the Pub/Sub endpoint URL below
  * IMAGE_BUCKET: bucket containing raw images (``rubin-prompt-proto-main``)
  * CALIB_REPO: URI to repo containing calibrations (and templates)
  * IP_APDB: IP address or hostname and port of the APDB (see `Databases`_, below)
  * IP_REGISTRY: IP address or hostname and port of the registry database (see `Databases`_)
  * DB_APDB: PostgreSQL database name for the APDB
  * DB_REGISTRY: PostgreSQL database name for the registry database

* There are also five optional parameters:

  * IMAGE_TIMEOUT: timeout in seconds to wait for raw image, default 50 sec.
  * LOCAL_REPOS: absolute path (in the container) where local repos are created, default ``/tmp``.
  * USER_APDB: database user for the APDB, default "postgres"
  * USER_REGISTRY: database user for the registry database, default "postgres"
  * NAMESPACE_APDB: the database namespace for the APDB, defaults to the DB's default namespace

* One variable is set by Cloud Run and should not be overridden:

  * PORT

* Also in the Variables & Secrets tab, reference the following secrets:

  * ``butler-registry-db-pass``, as the environment variable ``PSQL_REGISTRY_PASS``
  * ``apdb-db-pass``, as the environment variable ``PSQL_APDB_PASS``

* In the Connections tab, select the ``db-connector`` VPC connector. Do *not* create anything under "Cloud SQL connections"
* Set the "Request timeout" to 600 seconds (if a worker has not responded by then, it will be killed).
* Set the "Maximum requests per container" to 1.
* Under "Autoscaling", the minimum number should be set to 0 to save money while debugging, but it would be a multiple of the number of detectors in production.
  The maximum number depends on how many simultaneous visits could be in process.

The Cloud Run service URL is given at the top of the service details page.
Copy it for use in the Pub/Sub subscription.

One subscription needs to be created (once) for the ``nextVisit`` topic.
It accepts messages and gateways them to Cloud Run.

* Choose "Pub/Sub"
* Choose "Subscriptions"
* Choose "Create Subscription"
* Set "Subscription ID" to "nextVisit-sub"
* Select the ``projects/prompt-proto/topics/nextVisit`` topic
* Set "Delivery type" to "Push"
* Set the "Endpoint URL" to the service URL from Cloud Run, with ``?token={PUBSUB_VERIFICATION_TOKEN}`` appended to it.
  As mentioned, the string ``{PUBSUB_VERIFICATION_TOKEN}`` should be replaced by an arbitrary string matching the variable set above.
* Enable authentication using a service account that has Artifact Registry Reader, Cloud Run Invoker, Pub/Sub Editor, Pub/Sub Subscriber, and Storage Object Viewer roles
* Set "Message retention duration" to 10 minutes
* Do not "Retain acknowledged messages", and do not expire the subscription
* Set the acknowledgement deadline to 600 seconds
* Set the "Retry policy" to "Retry immediately"


tester
======

``python/tester/upload.py`` is a script that simulates the CCS image writer.
On a local machine, it requires a JSON token in ``./prompt-proto-upload.json``.
To obtain a token, see the GCP documentation on `service account keys`_; the relevant service account is ``prompt-image-upload@prompt-proto.iam.gserviceaccount.com``.

.. _service account keys: https://cloud.google.com/iam/docs/creating-managing-service-account-keys

Run the tester either on a local machine, or in Cloud Shell.  
In Cloud Shell, install the prototype code and the GCP PubSub client:

.. code-block:: sh

    gcloud config set project prompt-proto
    git clone https://github.com/lsst-dm/prompt_prototype.git
    pip3 install google-cloud-pubsub

Command line arguments are the instrument name (currently HSC only; other values will upload dummy raws that the pipeline can't process) and the number of groups of images to send.

Sample command line:

.. code-block:: sh

   python upload.py HSC 3

It sends ``next_visit`` events for each detector via Google Pub/Sub on the ``nextVisit`` topic.
It then uploads a batch of files representing the snaps of the visit to the ``rubin-prompt-proto-main`` GCS bucket.

Eventually a set of parallel processes running on multiple nodes will be needed to upload the images sufficiently rapidly.

.. note::

   ``upload.py`` uploads from the same small pool of raws every time it is run, while the AP pipeline assumes that every visit has unique visit IDs.
   This causes collisions in the APDB that crash the pipeline.
   To prevent this, follow the reset instructions under `Databases`_ before calling ``upload.py`` again.


Databases
=========

Google Cloud
------------

Two PostgreSQL databases have been created on Cloud SQL: ``butler-registry`` and ``apdb``.

To access these for manual operations, start by creating a virtual machine in Google Compute Engine.

* In the Cloud Console, go to "Compute Engine > VM instances".
* Select "Create Instance" at the top.
* Enter an instance name (e.g. ``ktl-db-client``).
* Under "Identity and API access / Access scopes", select "Set access for each API".
* Select "Enabled" for "Cloud SQL".
  If desired, change "Storage" to "Read Write" and "Cloud Pub/Sub" to "Enabled".
* Expand "Networking, Disks, Security, Management, Sole-Tenancy".
* Under "Networking", add the tag ``ssh``.
  This enables the firewall rule to allow connections from the Google Identity-Aware Proxy to the ssh port on the machine.
* You can leave all the rest at their defaults unless you think you need more CPU or memory.
  If you do (e.g. if you wanted to run Pipelines code on the VM), it's probably better to switch to an N2 series machine.
* With the project owner role, you should have appropriate permissions to connect to the machine and also to ``sudo`` to ``root`` on it, allowing installation of software.
* When the green check shows up in the status column, click "SSH" under "Connect" to start an in-browser shell to the machine.
* Then execute the following to install client software, set up proxy forwarding, and connect to the database:

.. code-block:: sh

   sudo apt-get update
   sudo apt-get install postgresql-client-11 cloudsql-proxy
   cloud_sql_proxy -instances=prompt-proto:us-central1:butler-registry=tcp:5432 &
   psql -h localhost -U postgres

A separate ``cloud_sql_proxy`` using a different port will be needed to communicate with the ``apdb`` database.

For passwordless login, create a ``~/.pgpass`` file with contents ``localhost:5432:postgres:postgres:PASSWORD`` and execute ``chmod 0600 ~/.pgpass``.
   
On a VM with the Science Pipelines installed, a new APDB schema can be created in the usual way: 

.. code-block:: sh

    make_apdb.py -c db_url="postgresql://postgres@localhost:<PORT>/postgres"

Resetting the APDB
^^^^^^^^^^^^^^^^^^

To restore the APDB to a clean state, run the following (replacing 5433 with the appropriate port on your machine):

.. code-block:: sh

   psql -h localhost -U postgres -p 5433 -c 'drop table "DiaForcedSource", "DiaObject", "DiaObject_To_Object_Match", "DiaSource", "SSObject" cascade;'
   make_apdb.py -c db_url="postgresql://postgres@localhost:5433/postgres"


USDF
----

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
^^^^^^^^^^^^^^^^^^

To restore the APDB to a clean state, run the following:

.. code-block:: sh

   psql -h usdf-prompt-processing-dev.slac.stanford.edu lsst-devl rubin -c 'drop schema "pp_apdb" cascade;'
   make_apdb.py -c namespace="pp_apdb" \
       -c db_url="postgresql://rubin@usdf-prompt-processing-dev.slac.stanford.edu/lsst-devl"


Middleware Worker VM
====================

The ``rubin-utility-middleware`` VM on Google Compute Engine is intended as a general-purpose environment for working with Butler repositories.
It can work with both local repositories and ones based on Google Storage.
However, it has limited computing power, and is not suited for things like pipeline runs.

Built-in support:

* a complete install of the Science Pipelines in ``/software/lsst_stack/``
* a running instance of ``cloud_sql_proxy`` mapping the ``butler-registry`` database to port 5432
* global configuration pointing Butler ``s3://`` URIs to Google Storage buckets (though ``gs://`` URIs now work as well)

The user is responsible for:

* running ``source /software/lsst_stack/loadLSST.sh`` on login
* database authentication (see `Databases`_, above)
* `Google Storage Authentication`_


Google Storage Authentication
-----------------------------

To access `Google Storage-Backed Repositories`_, you must first set up Boto authentication.
If you don't have one, `create an HMAC key`_ (this is *not* the same as the token for running the `tester`_); the relevant service account is ``service-620570835826@gs-project-accounts.iam.gserviceaccount.com``.
Then create a ``~/.aws/credentials`` file with the contents::

    [default]
    aws_access_key_id=<access key>
    aws_secret_access_key=<secret key>

and execute ``chmod go-rwx ~/.aws/credentials``.

.. _create an HMAC key: https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create

PostgreSQL-Backed Repositories
------------------------------

By default, ``butler create`` creates a repository whose registry is stored in SQLite.
To instead store the registry in the ``butler-registry`` database, create a seed config YAML such as:

.. code-block:: yaml

   registry:
     db: postgresql://postgres@localhost:5432/
     namespace: <unique namespace>

Then run ``butler create --seed-config seedconfig.yaml <repo location>`` to create the repository.

Each repository needs its own ``namespace`` value, corresponding to a PostgreSQL schema.
Schemas can be listed from within ``psql`` using the ``\dn`` command, and corrupted or outdated registries can be deleted using the ``DROP SCHEMA`` command.

.. warning::

   Be sure to always provide a unique namespace.
   Otherwise, the registry will be created in the database's ``public`` schema, making it very difficult to clean up later.


Google Storage-Backed Repositories
----------------------------------

All Google Storage repositories must also be `PostgreSQL-Backed Repositories`_.
Otherwise, no special configuration is needed to create one.

To create or access a Google Storage repository, give the repository location as a URI, e.g.::

    butler query-collections gs://<bucket name>/<repo location in bucket>
