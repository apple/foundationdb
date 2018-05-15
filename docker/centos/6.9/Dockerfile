FROM centos:6.9

ARG FDB_VERSION="5.1.7"
ARG FDB_RPM_RELEASE="1"

ARG FDB_PKG_URL="https://www.foundationdb.org/downloads/${FDB_VERSION}/rhel6/installers"
ARG FDB_CLIENTS_PKG="foundationdb-clients-${FDB_VERSION}-${FDB_RPM_RELEASE}.el6.x86_64.rpm"
ARG FDB_SERVER_PKG="foundationdb-server-${FDB_VERSION}-${FDB_RPM_RELEASE}.el6.x86_64.rpm"

ARG FDB_LISTEN_ADDR="0.0.0.0"

ENV FDB_UID="1000"
ENV FDB_GID="1000"

ADD ${FDB_PKG_URL}/${FDB_CLIENTS_PKG} /tmp/${FDB_CLIENTS_PKG}
ADD ${FDB_PKG_URL}/${FDB_SERVER_PKG} /tmp/${FDB_SERVER_PKG}

RUN yum install -y python initscripts
RUN rpm -Uvh /tmp/${FDB_CLIENTS_PKG} /tmp/${FDB_SERVER_PKG} && \
    rm /tmp/${FDB_CLIENTS_PKG} /tmp/${FDB_SERVER_PKG}

RUN sed -i "s/^listen_address.*/listen_address = ${FDB_LISTEN_ADDR}:4500/" /etc/foundationdb/foundationdb.conf && \
    cp -r /etc/foundationdb /etc/foundationdb.default

CMD cp -rn /etc/foundationdb.default/* /etc/foundationdb && \
    groupmod -g ${FDB_GID} --non-unique foundationdb && \
    usermod -g ${FDB_GID} -u ${FDB_UID} --non-unique foundationdb && \
    chown -R foundationdb:foundationdb /etc/foundationdb /var/log/foundationdb /var/lib/foundationdb && \
    /usr/lib/foundationdb/fdbmonitor
