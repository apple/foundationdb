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

FROM ubuntu:18.04

RUN apt-get update && \
    apt-get install -y \
        curl \
        dnsutils \
        jq \
        less \
        linux-tools-generic \
        lsof \
        net-tools \
        netcat \
        openssl \
        perl \
        strace \
        tcpdump \
        tcptraceroute \
        telnet \
        vim && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /tmp
# Adding tini as PID 1 https://github.com/krallin/tini
RUN curl -Ls https://github.com/krallin/tini/releases/download/v0.19.0/tini-amd64 -o tini  && \
    echo "93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c  tini" > tini-amd64.sha256sum && \
    sha256sum -c tini-amd64.sha256sum && \
    chmod +x tini && \
    mv tini /usr/bin/ && \
    rm -rf /tmp/*

# Install flamegraph
RUN curl -LsO https://raw.githubusercontent.com/brendangregg/FlameGraph/90533539b75400297092f973163b8a7b067c66d3/stackcollapse-perf.pl && \
    curl -LsO https://raw.githubusercontent.com/brendangregg/FlameGraph/90533539b75400297092f973163b8a7b067c66d3/flamegraph.pl && \
    echo "a682ac46497d6fdbf9904d1e405d3aea3ad255fcb156f6b2b1a541324628dfc0  flamegraph.pl" > flamegraph.sha256sum && \
    echo "5bcfb73ff2c2ab7bf2ad2b851125064780b58c51cc602335ec0001bec92679a5  stackcollapse-perf.pl" >> flamegraph.sha256sum && \
    sha256sum -c flamegraph.sha256sum && \
    chmod +x stackcollapse-perf.pl flamegraph.pl && \
    mv stackcollapse-perf.pl flamegraph.pl /usr/bin

ARG FDB_VERSION
ARG FDB_ADDITIONAL_VERSIONS="5.1.7"
ARG FDB_WEBSITE=https://www.foundationdb.org

COPY website /mnt/website/

RUN curl $FDB_WEBSITE/downloads/$FDB_VERSION/linux/libfdb_c_$FDB_VERSION.so -o /usr/lib/libfdb_c.so

# Install FoundationDB Binaries
RUN curl $FDB_WEBSITE/downloads/$FDB_VERSION/linux/fdb_$FDB_VERSION.tar.gz | tar zxf - --strip-components=1 && \
    chmod u+x fdbbackup fdbcli fdbdr fdbmonitor fdbrestore fdbserver backup_agent dr_agent && \
    mv fdbbackup fdbcli fdbdr fdbmonitor fdbrestore fdbserver backup_agent dr_agent /usr/bin && \
    rm -rf /tmp/*

# Install additional FoundationDB Client Libraries
RUN mkdir -p /usr/lib/fdb/multiversion && \
    for version in $FDB_ADDITIONAL_VERSIONS; do \
        curl $FDB_WEBSITE/downloads/$version/linux/libfdb_c_$version.so -o /usr/lib/fdb/multiversion/libfdb_c_$version.so; \
    done && \
    rm -rf /mnt/website

WORKDIR /

# Set Up Runtime Scripts and Directories
ADD release/fdb.bash /var/fdb/scripts/
RUN chmod a+x /var/fdb/scripts/fdb.bash

RUN mkdir -p /var/fdb/logs

VOLUME /var/fdb/data

# Runtime Configuration Options

ENV FDB_PORT 4500
ENV FDB_CLUSTER_FILE /var/fdb/fdb.cluster
ENV FDB_NETWORKING_MODE container
ENV FDB_COORDINATOR ""
ENV FDB_COORDINATOR_PORT 4500
ENV FDB_CLUSTER_FILE_CONTENTS ""
ENV FDB_PROCESS_CLASS unset

ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/var/fdb/scripts/fdb.bash"]
