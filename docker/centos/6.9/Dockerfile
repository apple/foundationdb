FROM centos:6.9

ARG FDB_VERSION="5.1.7"
ARG FDB_REVISION="-1"

ARG FDB_PKG_URL="https://www.foundationdb.org/downloads/${FDB_VERSION}/rhel6/installers"
ARG FDB_CLIENTS_PKG="foundationdb-clients-${FDB_VERSION}${FDB_REVISION}.el6.x86_64.rpm"
ARG FDB_SERVER_PKG="foundationdb-server-${FDB_VERSION}${FDB_REVISION}.el6.x86_64.rpm"

ARG FDB_LISTEN_ADDR="0.0.0.0"

ADD ${FDB_PKG_URL}/${FDB_CLIENTS_PKG} /tmp/${FDB_CLIENTS_PKG}
ADD ${FDB_PKG_URL}/${FDB_SERVER_PKG} /tmp/${FDB_SERVER_PKG}

RUN yum install -y python initscripts
RUN rpm -Uvh /tmp/${FDB_CLIENTS_PKG} /tmp/${FDB_SERVER_PKG} &&\
    rm /tmp/${FDB_CLIENTS_PKG} /tmp/${FDB_SERVER_PKG}

RUN sed -i "s/^listen_address.*/listen_address = ${FDB_LISTEN_ADDR}:4500/" /etc/foundationdb/foundationdb.conf &&\
    cp -r /etc/foundationdb /etc/foundationdb.default

CMD cp -rn /etc/foundationdb.default/* /etc/foundationdb &&\
    /usr/lib/foundationdb/fdbmonitor
