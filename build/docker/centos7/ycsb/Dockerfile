ARG REPOSITORY=foundationdb/build
ARG VERSION=centos7-latest
FROM ${REPOSITORY}:${VERSION}

ENV YCSB_VERSION=ycsb-foundationdb-binding-0.17.0 \
    PATH=${PATH}:/usr/bin

RUN cd /opt \
    && eval curl "-Ls https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-foundationdb-binding-0.17.0.tar.gz" \
    | tar -xzvf -

RUN rm -Rf /opt/${YCSB_VERSION}/lib/fdb-java-5.2.5.jar

# COPY The Appropriate fdb-java-.jar Aaron from packages
# COPY binary RPM for foundationd-db
# Install Binary

WORKDIR "/opt/${YCSB_VERSION}"

ENTRYPOINT ["bin/ycsb.sh"]