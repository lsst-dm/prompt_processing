ARG BASE_TAG=latest
FROM ghcr.io/lsst-dm/prompt-base:${BASE_TAG}
ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
ENV PROMPT_PROCESSING_DIR $APP_HOME
ARG PORT
WORKDIR $APP_HOME
COPY python/activator activator/
COPY pipelines pipelines/
CMD source /opt/lsst/software/stack/loadLSST.bash \
    && setup lsst_distrib \
    && exec gunicorn --bind :$PORT --workers 1 --threads 1 --timeout 0 activator.activator:app
