FROM us-central1-docker.pkg.dev/prompt-proto/prompt/prompt-proto-base:latest
ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
ARG RUBIN_INSTRUMENT
ARG PUBSUB_VERIFICATION_TOKEN
ARG PORT
ARG CALIB_REPO
ARG IMAGE_BUCKET
ARG IMAGE_TIMEOUT
WORKDIR $APP_HOME
COPY . ./
CMD source /opt/lsst/software/stack/loadLSST.bash \
    && setup lsst_distrib \
    && exec gunicorn --bind :$PORT --workers 1 --threads 1 --timeout 0 activator:app
