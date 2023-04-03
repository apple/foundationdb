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

# Build foundationdb-base, foundationdb-kubernetes-sidecar and foundationdb
# requiements:
# 1. --build-arg FDB_VERSION=... must be a full version name, ex. 7.1.25-5.ow.1
# 2. /mnt/distr must be mounted to a directory with foundationdb built artifacts
#   foundationdb-bins-${FDB_VERSION}.x86_64.tgz
#   and foundationdb-libs-${FDB_VERSION}.x86_64.tgz must be there.

ARG BASE_IMAGE=oraclelinux:9
FROM ${BASE_IMAGE} as base

RUN OS_SPECIFIC_PACKAGES="" \
  && source /etc/os-release \
  && case "$ID" in \
    "ol") \
       OS_SPECIFIC_PACKAGES=oracle-epel-release-`echo $PLATFORM_ID | cut -d: -f2` \
       ;; \
  esac \
  && dnf install -y $OS_SPECIFIC_PACKAGES \
  && dnf install -y \
    bind-utils \
    binutils \
    curl \
    gdb \
    hostname \
    jq \
    less \
    libatomic \
    libubsan \
    lsof \
    net-tools \
    nmap-ncat \
    openssl \
    perf \
    perl \
    procps \
    strace \
    sysstat \
    tar \
    tcpdump \
    telnet \
    traceroute \
    unzip \
    vim-enhanced \
    && dnf clean all \
    && rm -rf /var/cache/dnf

WORKDIR /tmp

RUN curl -Ls https://github.com/krallin/tini/releases/download/v0.19.0/tini-amd64 -o tini  && \
    echo "93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c  tini" > tini-sha.txt && \
    sha256sum --quiet -c tini-sha.txt && \
    chmod +x tini && \
    mv tini /usr/bin/ && \
    rm -rf /tmp/*

WORKDIR /

FROM base as foundationdb-base

WORKDIR /tmp
ARG FDB_VERSION=6.3.25-5.ow.1

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

RUN tar -xvf /mnt/distr/foundationdb-bins-${FDB_VERSION}.x86_64.tgz -C /usr/bin \
  && tar -xvf /mnt/distr/foundationdb-libs-${FDB_VERSION}.x86_64.tgz -C /var/fdb/lib \
  && for F in /var/fdb/lib/*.so; do cp $F /usr/lib/fdb/multiversion/$(basename ${F%.so}_${FDB_VERSION}.so); done \
  && cp /usr/lib/fdb/multiversion/*.so /var/fdb/lib/ \
  && rm -rf /tmp/*

WORKDIR /

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

FROM foundationdb-base as foundationdb-kubernetes-sidecar

RUN dnf -y install python3 python3-pip \
    && dnf clean all \
    && rm -rf /var/cache/dnf \
    && pip3 install watchdog

WORKDIR /
ADD entrypoint.bash sidecar.py /
RUN chmod a+x /entrypoint.bash /sidecar.py
USER fdb
VOLUME /var/input-files
VOLUME /var/output-files
ENV LISTEN_PORT 8080
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/entrypoint.bash"]
