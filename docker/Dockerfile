FROM ubuntu:18.04

ARG FDB_VERSION="5.1.7"
ARG FDB_REVISION="1"
ARG LISTEN_ADDR="0.0.0.0"

ADD https://www.foundationdb.org/downloads/"$FDB_VERSION"/ubuntu/installers/foundationdb-clients_"$FDB_VERSION"-"$FDB_REVISION"_amd64.deb /tmp/foundationdb-clients_"$FDB_VERSION"-"$FDB_REVISION"_amd64.deb
ADD https://www.foundationdb.org/downloads/"$FDB_VERSION"/ubuntu/installers/foundationdb-server_"$FDB_VERSION"-"$FDB_REVISION"_amd64.deb /tmp/foundationdb-server_"$FDB_VERSION"-"$FDB_REVISION"_amd64.deb

RUN apt update &&\
    apt install -y python
RUN dpkg -i /tmp/foundationdb-clients_*_amd64.deb &&\
    dpkg -i /tmp/foundationdb-server_*_amd64.deb &&\
    service foundationdb stop &&\
    rm -rf /tmp/foundationdb-*_"$FDB_VERSION"-"$FDB_REVISION"_amd64.deb /var/lib/foundationdb/data/*
RUN sed -i "s/^listen_address.*/listen_address = $LISTEN_ADDR:4500/" /etc/foundationdb/foundationdb.conf

CMD chown foundationdb:foundationdb /etc/foundationdb/* &&\
    /usr/lib/foundationdb/fdbmonitor

EXPOSE 4500
