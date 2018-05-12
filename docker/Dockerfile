FROM ubuntu:18.04

ADD https://www.foundationdb.org/downloads/5.1.7/ubuntu/installers/foundationdb-clients_5.1.7-1_amd64.deb /tmp/foundationdb-clients_5.1.7-1_amd64.deb
ADD https://www.foundationdb.org/downloads/5.1.7/ubuntu/installers/foundationdb-server_5.1.7-1_amd64.deb /tmp/foundationdb-server_5.1.7-1_amd64.deb

RUN apt update &&\
    apt install -y python
RUN dpkg -i /tmp/foundationdb-clients_*_amd64.deb &&\
    dpkg -i /tmp/foundationdb-server_*_amd64.deb &&\
    rm -f /tmp/foundationdb-*_amd64.deb

RUN sed -i '25s/.*/listen_address = 0.0.0.0:4500/' /etc/foundationdb/foundationdb.conf

CMD /usr/lib/foundationdb/fdbmonitor

EXPOSE 4500
