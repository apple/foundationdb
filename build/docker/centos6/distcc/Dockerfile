ARG REPOSITORY=foundationdb/build
ARG VERSION=centos6-latest
FROM ${REPOSITORY}:${VERSION}

RUN useradd distcc && \
    source /opt/rh/devtoolset-8/enable && \
    source /opt/rh/rh-python36/enable && \
    update-distcc-symlinks

EXPOSE 3632
EXPOSE 3633
USER distcc
ENV ALLOW 0.0.0.0/0

ENTRYPOINT distccd \
           --daemon \
           --enable-tcp-insecure \
           --no-detach \
           --port 3632 \
           --log-stderr \
           --log-level info \
           --listen 0.0.0.0 \
           --allow ${ALLOW} \
           --jobs `nproc`