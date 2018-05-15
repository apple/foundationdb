FROM ubuntu:18.04

ARG LISTEN_ADDR="0.0.0.0"

ARG FDB_VERSION="5.1.7"
ARG FDB_DEB_REVISION="1"

ARG FDB_PKG_URL="https://www.foundationdb.org/downloads/${FDB_VERSION}/ubuntu/installers"
ARG FDB_CLIENTS_PKG="foundationdb-clients_${FDB_VERSION}-${FDB_DEB_REVISION}_amd64.deb"
ARG FDB_SERVER_PKG="foundationdb-server_${FDB_VERSION}-${FDB_DEB_REVISION}_amd64.deb"

ADD ${FDB_PKG_URL}/${FDB_CLIENTS_PKG} /tmp/${FDB_CLIENTS_PKG}
ADD ${FDB_PKG_URL}/${FDB_SERVER_PKG} /tmp/${FDB_SERVER_PKG}

RUN apt update && \
    apt install -y python

RUN dpkg -i /tmp/${FDB_CLIENTS_PKG} /tmp/${FDB_SERVER_PKG} && \
    rm /tmp/${FDB_CLIENTS_PKG} /tmp/${FDB_SERVER_PKG}

# Note: Remove this instruction if deb postinst is changed to not auto-start foundationdb.
RUN systemctl stop foundationdb.service && \
    rm -rf /var/lib/foundationdb/data/*

# Keeps default configuration from being overlaid by bind mounts.
RUN mkdir /etc/foundationdb.default && \
    mv /etc/foundationdb/* /etc/foundationdb.default && \
    chown -R foundationdb:foundationdb /etc/foundationdb.default

USER foundationdb:foundationdb

RUN sed -i "s/^listen_address.*/listen_address = ${LISTEN_ADDR}:4500/" /etc/foundationdb.default/foundationdb.conf

EXPOSE 4500

# Writes default configuration if not present and starts fdbmonitor.
CMD cp -r --no-clobber /etc/foundationdb.default/* /etc/foundationdb && \
    /usr/lib/foundationdb/fdbmonitor
