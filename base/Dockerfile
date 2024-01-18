ARG STACK_TAG=d_latest
FROM lsstsqre/centos:${STACK_TAG}
ENV PYTHONUNBUFFERED True
RUN source /opt/lsst/software/stack/loadLSST.bash \
    && mamba install -y \
        flask \
        gunicorn \
        python-confluent-kafka \
    && pip install --no-input \
        cloudevents
