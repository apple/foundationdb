FROM ubuntu:16.04

ARG FDB_VERSION="5.1.7"
ARG FDB_REVISION="-1"

ARG FDB_PKG_URL="https://www.foundationdb.org/downloads/${FDB_VERSION}/ubuntu/installers"
ARG FDB_CLIENTS_PKG="foundationdb-clients_${FDB_VERSION}${FDB_REVISION}_amd64.deb"
ARG FDB_SERVER_PKG="foundationdb-server_${FDB_VERSION}${FDB_REVISION}_amd64.deb"

ARG FDB_LISTEN_ADDR="0.0.0.0"

ADD ${FDB_PKG_URL}/${FDB_CLIENTS_PKG} /tmp/${FDB_CLIENTS_PKG}
ADD ${FDB_PKG_URL}/${FDB_SERVER_PKG} /tmp/${FDB_SERVER_PKG}

RUN apt update &&\
    apt install -y python
RUN dpkg -i /tmp/${FDB_CLIENTS_PKG} /tmp/${FDB_SERVER_PKG} &&\
    rm /tmp/${FDB_CLIENTS_PKG} /tmp/${FDB_SERVER_PKG}

RUN sed -i "s/^listen_address.*/listen_address = ${FDB_LISTEN_ADDR}:4500/" /etc/foundationdb/foundationdb.conf &&\
    cp -r /etc/foundationdb /etc/foundationdb.default

CMD cp -rn /etc/foundationdb.default/* /etc/foundationdb &&\
    /usr/lib/foundationdb/fdbmonitor
