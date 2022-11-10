# Dockerfile
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM centos:7.9.2009 as base

RUN yum install -y \
    epel-release-7-11 \
    centos-release-scl-2-3.el7.centos && \
    yum install -y \
    bind-utils-9.11.4-26.P2.el7_9.7 \
    binutils-2.27-44.base.el7_9.1 \
    curl-7.29.0-59.el7_9.1 \
    gdb-7.6.1-120.el7 \
    hostname-3.13-3.el7_7.1 \
    jq-1.6-2.el7  \
    less-458-9.el7 \
    libubsan-7.3.1-5.16.el7 \
    lsof-4.87-6.el7 \
    net-tools-2.0-0.25.20131004git.el7 \
    nmap-ncat-6.40-19.el7 \
    perf-3.10.0-1160.45.1.el7 \
    perl-5.16.3-299.el7_9 \
    procps-ng-3.3.10-28.el7 \
    strace-4.24-6.el7 \
    sysstat-10.1.5-19.el7 \
    tar-1.26-35.el7 \
    tcpdump-4.9.2-4.el7_7.1 \
    telnet-0.17-66.el7 \
    traceroute-2.0.22-2.el7 \
    unzip-6.0-22.el7_9 \
    openssl-1.0.2k-24.el7_9.x86_64 \
    vim-enhanced-7.4.629-8.el7_9 && \
    yum clean all && \
    rm -rf /var/cache/yum

WORKDIR /tmp

RUN curl -Ls https://github.com/krallin/tini/releases/download/v0.19.0/tini-amd64 -o tini  && \
    echo "93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c  tini" > tini-sha.txt && \
    sha256sum --quiet -c tini-sha.txt && \
    chmod +x tini && \
    mv tini /usr/bin/ && \
    rm -rf /tmp/*

WORKDIR /

FROM golang:1.16.7-bullseye AS go-build

COPY fdbkubernetesmonitor/ /fdbkubernetesmonitor
WORKDIR /fdbkubernetesmonitor
RUN go build -o /fdb-kubernetes-monitor *.go

FROM base as foundationdb-base

WORKDIR /tmp
ARG FDB_VERSION=6.3.22
ARG FDB_LIBRARY_VERSIONS="${FDB_VERSION}"
ARG FDB_WEBSITE=https://github.com/apple/foundationdb/releases/download

RUN mkdir -p /var/fdb/{logs,tmp,lib} && \
    mkdir -p /usr/lib/fdb/multiversion && \
    echo ${FDB_VERSION} > /var/fdb/version

# Set up a non-root user
RUN groupadd --gid 4059 \
             fdb && \
    useradd --gid 4059 \
            --uid 4059 \
            --no-create-home \
            --shell /bin/bash \
            fdb && \
    chown -R fdb:fdb /var/fdb

COPY website /tmp/website/

# Install FoundationDB Binaries
RUN for file in fdbserver fdbbackup fdbcli fdbmonitor; do \
        curl --fail -Ls ${FDB_WEBSITE}/${FDB_VERSION}/$file.x86_64 -o $file; \
        chmod +x $file; \
        mv $file /usr/bin; \
    done

# Setup all symlinks for the other binaries that are a copy of fdbbackup
RUN for file in fdbdr fdbrestore backup_agent dr_agent fastrestore_tool; do \
        ln -s /usr/bin/fdbbackup /usr/bin/$file; \
    done

# Install additional FoundationDB Client Libraries
RUN for version in $FDB_LIBRARY_VERSIONS; do \
    curl --fail -Ls ${FDB_WEBSITE}/${version}/libfdb_c.x86_64.so -o /usr/lib/fdb/multiversion/libfdb_c_${version%.*}.so; \
    done

# Install additional FoundationDB Client Libraries (for sidecar)
RUN mkdir -p /var/fdb/lib && \
    for version in $FDB_LIBRARY_VERSIONS; do \
    curl --fail -Ls ${FDB_WEBSITE}/${version}/libfdb_c.x86_64.so -o /var/fdb/lib/libfdb_c_${version%.*}.so; \
    done

RUN curl -Ls $FDB_WEBSITE/$FDB_VERSION/libfdb_c.x86_64.so -o /usr/lib/libfdb_c.so

RUN rm -rf /tmp/*
WORKDIR /

FROM foundationdb-base as fdb-kubernetes-monitor

# Install the kubernetes monitor binary
COPY --from=go-build /fdb-kubernetes-monitor /usr/bin/

# Runtime Configuration Options
USER fdb
WORKDIR /var/fdb
VOLUME /var/fdb/data
ENTRYPOINT ["/usr/bin/fdb-kubernetes-monitor"]

FROM foundationdb-base as foundationdb-kubernetes-sidecar

RUN yum -y install \
    rh-python38-2.0-4.el7 \
    yum clean all && \
    rm -rf /var/cache/yum && \
    source /opt/rh/rh-python38/enable && \
    pip3 install watchdog==0.9.0

WORKDIR /
ADD entrypoint.bash sidecar.py /
RUN chmod a+x /entrypoint.bash /sidecar.py
USER fdb
VOLUME /var/input-files
VOLUME /var/output-files
ENV LISTEN_PORT 8080
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/entrypoint.bash"]

FROM foundationdb-base as foundationdb

WORKDIR /tmp
RUN curl -LsO https://raw.githubusercontent.com/brendangregg/FlameGraph/90533539b75400297092f973163b8a7b067c66d3/stackcollapse-perf.pl && \
    curl -LsO https://raw.githubusercontent.com/brendangregg/FlameGraph/90533539b75400297092f973163b8a7b067c66d3/flamegraph.pl && \
    echo "a682ac46497d6fdbf9904d1e405d3aea3ad255fcb156f6b2b1a541324628dfc0  flamegraph.pl" > flamegraph-sha.txt && \
    echo "5bcfb73ff2c2ab7bf2ad2b851125064780b58c51cc602335ec0001bec92679a5  stackcollapse-perf.pl" >> flamegraph-sha.txt && \
    sha256sum --quiet -c flamegraph-sha.txt && \
    chmod +x stackcollapse-perf.pl flamegraph.pl && \
    mv stackcollapse-perf.pl flamegraph.pl /usr/bin && \
    rm -rf /tmp/*
WORKDIR /
# Set Up Runtime Scripts and Directories
ADD fdb.bash /var/fdb/scripts/
RUN chmod a+x /var/fdb/scripts/fdb.bash
VOLUME /var/fdb/data
ENV FDB_PORT 4500
ENV FDB_CLUSTER_FILE /var/fdb/fdb.cluster
ENV FDB_NETWORKING_MODE container
ENV FDB_COORDINATOR ""
ENV FDB_COORDINATOR_PORT 4500
ENV FDB_CLUSTER_FILE_CONTENTS ""
ENV FDB_PROCESS_CLASS unset
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/var/fdb/scripts/fdb.bash"]

FROM base as ycsb

RUN yum -y install \
    java-11-openjdk-11.0.13.0.8-1.el7_9 && \
    yum clean all && \
    rm -rf /var/cache/yum

WORKDIR /tmp
RUN NO_PROXY="" no_proxy="" curl -Ls https://s3.us-west-2.amazonaws.com/amazon-eks/1.22.6/2022-03-09/bin/linux/amd64/kubectl -o kubectl && \
    echo "860c3d37a5979491895767e7332404d28dc0d7797c7673c33df30ca80e215a07  kubectl" > kubectl.txt && \
    sha256sum --quiet -c kubectl.txt && \
    mv kubectl /usr/local/bin/kubectl && \
    chmod 755 /usr/local/bin/kubectl && \
    curl -Ls https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.7.34.zip -o "awscliv2.zip" && \
    echo "daf9253f0071b5cfee9532bc5220bedd7a5d29d4e0f92b42b9e3e4c496341e88  awscliv2.zip" > awscliv2.txt && \
    sha256sum --quiet -c awscliv2.txt && \
    unzip -qq awscliv2.zip && \
    ./aws/install && \
    rm -rf /tmp/*

# TODO: Log4J complains that it's eating the HTracer logs.  Even without it, we get per-operation
# time series graphs of throughput, median, 90, 99, 99.9 and 99.99 (in usec).
ADD run_ycsb.sh /usr/local/bin/run_ycsb.sh
RUN mkdir -p /var/log/fdb-trace-logs && \
    chmod +x /usr/local/bin/run_ycsb.sh

ADD YCSB /YCSB
WORKDIR /YCSB
ENV FDB_NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY=/var/dynamic-conf/lib/multiversion/
ENV FDB_NETWORK_OPTION_TRACE_ENABLE=/var/log/fdb-trace-logs
ENV LD_LIBRARY_PATH=/var/dynamic-conf/lib/
ENV BUCKET=""
CMD ["run_ycsb.sh"]
