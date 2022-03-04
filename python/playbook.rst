#########################################################
Playbook for the Prompt Processing Proposal and Prototype
#########################################################

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
Additional topics are used for images from each instrument, where the instrument is one of ``LSSTCam``, ``LSSTComCam``, ``LATISS``, or ``DECam``.

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

A single bucket named ``rubin-prompt-proto-main`` has been created to hold incoming raw images.

An additional bucket will be needed eventually to hold a Butler repo containing calibration datasets and templates.

The raw image bucket has had notifications configured for it; these publish to a Google Pub/Sub topic as mentioned in the previous section.
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

Prototype Service
=================

The service can be controlled by Google Cloud Run, which will automatically trigger instances based on ``nextVisit`` messages and can autoscale the number of them depending on load.
Each time the service container is updated, a new revision of the service should be edited and deployed.
(Continuous deployment has not yet been set up.)

To create or edit the Cloud Run service in the Google Cloud Console:

* Choose "Create Service" or "Edit & Deploy New Revision"
* Select the container image URL from "Artifact Registry > prompt-proto-service"
* In the Variables & Secrets tab, set the following required parameters:

  * RUBIN_INSTRUMENT: name of instrument
  * PUBSUB_VERIFICATION_TOKEN: choose an arbitrary string matching the Pub/Sub endpoint URL below
  * IMAGE_BUCKET: bucket containing raw images (``rubin-prompt-proto-main``)
  * CALIB_REPO: repo containing calibrations (and templates); only used for log messages at present

* There is also one optional parameter:

  * IMAGE_TIMEOUT: timeout in seconds to wait for raw image, default 50 sec.

* One variable is set by Cloud Run and should not be overridden:

  * PORT

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

``tester/upload.py`` is a script that simulates the CCS image writer.
It takes a JSON token in ``./prompt-proto-upload.json``.
Command line arguments are the instrument name (LSSTCam, LSSTComCam, LATISS, DECam) and the number of groups of images to send.

Sample command line:

.. code-block:: sh

   python upload.py LSSTComCam 3

It sends ``next_visit`` events for each detector via Google Pub/Sub on the ``nextVisit`` topic.
It then uploads a batch of files representing the snaps of the visit to the ``rubin-prompt-proto-main`` GCS bucket.
LSSTCam and LSSTComCam are currently hard-coded at two snaps per group.
The current data uploaded is just a tiny string.

Eventually a set of parallel processes running on multiple nodes will be needed to upload the images sufficiently rapidly.
