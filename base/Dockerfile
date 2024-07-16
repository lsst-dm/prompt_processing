ARG STACK_TAG=d_latest
FROM lsstsqre/centos:${STACK_TAG}
ENV PYTHONUNBUFFERED True
ARG KAFKA=python-confluent-kafka
RUN source /opt/lsst/software/stack/loadLSST.bash \
    && mamba install -y \
        flask \
        gunicorn \
    && (mamba list ${KAFKA} | grep ${KAFKA} || \
        mamba install -y "${KAFKA}=2.4.0" ) \
    && pip install --no-input \
        cloudevents
