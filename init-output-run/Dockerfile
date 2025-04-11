ARG BASE_TAG=latest
FROM ghcr.io/lsst-dm/prompt-base:${BASE_TAG}
ENV PYTHONUNBUFFERED=True
ENV APP_HOME=/app
ENV PROMPT_PROCESSING_DIR=$APP_HOME
ENV PYTHONPATH=$APP_HOME
ENV XDG_CONFIG_HOME=$APP_HOME
ARG PORT
WORKDIR $APP_HOME
# Activator is not run by this container, but its code is needed to generate consistent version hashes.
COPY ../python/activator activator/
COPY ../python/initializer initializer/
COPY ../python/shared shared/
COPY ../pipelines pipelines/
COPY config/astropy.cfg $XDG_CONFIG_HOME/astropy/
# Maps aren't used by this container, but they're needed to generate consistent version hashes.
COPY maps maps/
# #! editor doesn't run in the build workflow, but we don't need it
COPY ../bin.src/write_init_outputs.py bin/
CMD source /opt/lsst/software/stack/loadLSST.bash \
    && setup lsst_distrib \
    && exec python bin/write_init_outputs.py
