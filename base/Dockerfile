ARG PIPE_CONTAINER=lsstsqre/centos
ARG STACK_TAG=d_latest
FROM ${PIPE_CONTAINER}:${STACK_TAG}
ENV PYTHONUNBUFFERED True
ARG KAFKA=python-confluent-kafka
RUN <<EOT
  set -ex
  source /opt/lsst/software/stack/loadLSST.bash
  mamba install -y flask gunicorn
  mamba list ${KAFKA} | grep ${KAFKA} || mamba install -y "${KAFKA}=2.4.0"
  pip install --no-input cloudevents
EOT
