FROM ubuntu:18.04

ENV FDB_UID=1000
ENV FDB_GID=1000

ARG FDB_LISTEN_ADDR="0.0.0.0"

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
RUN service foundationdb stop && \
    rm -rf /var/lib/foundationdb/data/*

# Replace the default listen_address with 0.0.0.0 or an argument specified at build time.
RUN sed -i "s/^listen_address.*/listen_address = ${FDB_LISTEN_ADDR}:4500/" /etc/foundationdb/foundationdb.conf

# Keeps the default configuration from being overlaid by bind mounts.
RUN cp -r /etc/foundationdb /etc/foundationdb.default

# Writes the default configuration to bind mounts if not present.
CMD cp -r --no-clobber /etc/foundationdb.default/* /etc/foundationdb && \
    # Syncs host and container file ownership for privilege separation.
    groupmod -g ${FDB_GID} foundationdb && \
    usermod -g ${FDB_GID} -u ${FDB_UID} --non-unique foundationdb && \
    chown -R foundationdb:foundationdb \
      /etc/foundationdb \
      /var/log/foundationdb \
      /var/lib/foundationdb && \
    # Starts foundationdb
    /usr/lib/foundationdb/fdbmonitor
