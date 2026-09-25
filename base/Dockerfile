ARG PIPE_CONTAINER=ghcr.io/lsst/scipipe
ARG STACK_TAG=d_latest
FROM ${PIPE_CONTAINER}:${STACK_TAG}
ENV PYTHONUNBUFFERED=True
RUN <<EOT
  set -ex
  source /opt/lsst/software/stack/loadLSST.bash
  # Needed only for Knative support
  conda install -y flask gdb
  pip install --no-input cloudevents
  pip install --no-input memray
  pip install --no-input prometheus-client
  pip install --no-input redis
EOT
