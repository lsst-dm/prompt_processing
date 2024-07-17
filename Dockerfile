ARG BASE_TAG=latest
FROM ghcr.io/lsst-dm/prompt-base:${BASE_TAG}
ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
ENV PROMPT_PROCESSING_DIR $APP_HOME
# Normally defined in the Kubernetes config.
ENV WORKER_RESTART_FREQ ${WORKER_RESTART_FREQ:-0}
ENV WORKER_TIMEOUT ${WORKER_TIMEOUT:-0}
ENV WORKER_GRACE_PERIOD ${WORKER_GRACE_PERIOD:-30}
ARG PORT
WORKDIR $APP_HOME
COPY python/activator activator/
COPY pipelines pipelines/
CMD source /opt/lsst/software/stack/loadLSST.bash \
    && setup lsst_distrib \
    && exec gunicorn --workers 1 --threads 1 --timeout $WORKER_TIMEOUT --max-requests $WORKER_RESTART_FREQ \
        --graceful-timeout $WORKER_GRACE_PERIOD \
        --bind :$PORT activator.activator:app
