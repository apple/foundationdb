# Dockerfile
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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
FROM rockylinux/rockylinux:9.6-minimal AS base

# Disable the faulty mirror list for RockyLinux for now. Since all the other images use the base image
# as base we don't have to repeat this step.
RUN sed -i.bak 's/^#baseurl=/baseurl=/; s/^mirrorlist=/#mirrorlist=/' /etc/yum.repos.d/rocky.repo && \ 
    microdnf install -y \
    bind-utils \
    binutils \
    curl \
    gdb \
    hostname \
    jq \
    less \
    libubsan \
    lsof \
    net-tools \
    nmap-ncat \
    perf \
    perl \
    procps-ng \
    strace \
    sysstat \
    tar \
    tcpdump \
    telnet \
    traceroute \
    unzip \
    openssl \
    vim-enhanced && \
    microdnf clean all

WORKDIR /tmp

ARG TARGETARCH
RUN curl -Ls "https://github.com/krallin/tini/releases/download/v0.19.0/tini-$TARGETARCH" -o "tini-$TARGETARCH"  && \
    echo "93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c  tini-amd64" > "tini-amd64-sha.txt" && \
    echo "07952557df20bfd2a95f9bef198b445e006171969499a1d361bd9e6f8e5e0e81  tini-arm64" > "tini-arm64-sha.txt" && \
    sha256sum --quiet -c "tini-${TARGETARCH}-sha.txt" && \
    chmod +x "tini-$TARGETARCH" && \
    mv "tini-$TARGETARCH" /usr/bin/tini && \
    rm -rf /tmp/*

WORKDIR /

FROM golang:1.24.9-bookworm AS go-build
COPY fdbkubernetesmonitor/ /fdbkubernetesmonitor
WORKDIR /fdbkubernetesmonitor
RUN go build -o /fdb-kubernetes-monitor *.go

# Build the fdb-aws-s3-credentials-fetcher in a dedicated build
FROM go-build AS go-credentials-fetcher-build
COPY fdb-aws-s3-credentials-fetcher/ /fdb-aws-s3-credentials-fetcher
WORKDIR /fdb-aws-s3-credentials-fetcher
RUN go build -o /fdb-aws-s3-credentials-fetcher *.go

# For now use 'base'. Later, could use a more stripped down image
# since this script needs little.
FROM base AS fdb-aws-s3-credentials-fetcher-sidecar
RUN groupadd --gid 4059 fdb && \
    useradd --gid 4059 \
            --uid 4059 \
            --no-create-home \
            --shell /bin/bash fdb
USER fdb
COPY --from=go-credentials-fetcher-build /fdb-aws-s3-credentials-fetcher /usr/bin/
ENTRYPOINT ["/usr/bin/fdb-aws-s3-credentials-fetcher", "-dir", "/var/fdb"]

FROM base AS foundationdb-base
ARG FDB_VERSION=7.3.63
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

WORKDIR /tmp
COPY website /tmp/website/

# Install FoundationDB Binaries and additional FoundationDB Client Libraries
RUN if [ "$TARGETARCH" = "amd64" ]; then \
        FDB_ARCH=x86_64; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
        FDB_ARCH=aarch64; \
    else \
        echo "ERROR: unsupported architecture $TARGETARCH" 1>&2; \
        exit 1; \
    fi; \
    CURL="curl --fail -Ls"; \
    for file in fdbserver fdbbackup fdbcli fdbmonitor; do \
        URL="${FDB_WEBSITE}/${FDB_VERSION}/$file.$FDB_ARCH"; \
        echo "Downloading $URL"; \
        $CURL "$URL" -o "$file" && \
        chmod +x "$file" && \
        mv "$file" /usr/bin/ || exit $?; \
    done && \
    mkdir -p /var/fdb/lib && \
    for version in $FDB_LIBRARY_VERSIONS; do \
        URL="${FDB_WEBSITE}/${version}/libfdb_c.${FDB_ARCH}.so"; \
        echo "Downloading $URL"; \
        TARGET="/usr/lib/fdb/multiversion/libfdb_c_${version%.*}.so"; \
        $CURL "$URL" -o "$TARGET" && \
        cp "$TARGET" /var/fdb/lib/ || exit $?; \
    done && \
    $CURL $FDB_WEBSITE/$FDB_VERSION/libfdb_c.${FDB_ARCH}.so -o /usr/lib/libfdb_c.so

# Setup all symlinks for the other binaries that are a copy of fdbbackup
RUN for file in fdbdr fdbrestore backup_agent dr_agent fastrestore_tool; do \
        ln -s /usr/bin/fdbbackup "/usr/bin/$file"; \
    done && \
    cd / && \
    rm -rf /tmp/*

WORKDIR /

FROM foundationdb-base AS fdb-kubernetes-monitor

# Install the kubernetes monitor binary
COPY --from=go-build /fdb-kubernetes-monitor /usr/bin/

# Runtime Configuration Options
USER fdb
WORKDIR /var/fdb
VOLUME /var/fdb/data
ENTRYPOINT ["/usr/bin/fdb-kubernetes-monitor"]

FROM foundationdb-base AS foundationdb-kubernetes-sidecar

# We use python3.9 here as the tests with 3.12 resulted in the following error:
# error:0A000126:SSL routines::unexpected eof while reading. This could be an issue
# with the installed openssl version. Since the sidecar support will be deprecated in the
# future it's fine to use python 3.9 here.
# EOL for python 3.9 is 2025-10: https://devguide.python.org/versions/
RUN microdnf -y install \
    python3.9 \
    python3.9-pip && \
    microdnf clean all && \
    update-alternatives --install /usr/bin/python python /usr/bin/python3.9 20 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 20 && \
    python3 -m pip install watchdog==4.0.1

WORKDIR /
COPY entrypoint.bash sidecar.py /
RUN chmod a+x /entrypoint.bash /sidecar.py
USER fdb
VOLUME /var/input-files
VOLUME /var/output-files
ENV LISTEN_PORT=8080
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/entrypoint.bash"]

FROM foundationdb-base AS foundationdb

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
COPY fdb.bash fdb_single.bash /var/fdb/scripts/
RUN chmod a+x /var/fdb/scripts/fdb.bash /var/fdb/scripts/fdb_single.bash
VOLUME /var/fdb/data
ENV FDB_PORT=4500
ENV FDB_CLUSTER_FILE=/var/fdb/fdb.cluster
ENV FDB_NETWORKING_MODE=container
ENV FDB_COORDINATOR=""
ENV FDB_COORDINATOR_PORT=4500
ENV FDB_CLUSTER_FILE_CONTENTS=""
ENV FDB_PROCESS_CLASS=unset
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/var/fdb/scripts/fdb.bash"]

FROM foundationdb-base AS ycsb

RUN microdnf -y install \
    java-11-openjdk java-11-openjdk-devel && \
    microdnf clean all && \
    rm -rf /var/cache/yum

WORKDIR /tmp
RUN if [ "$TARGETARCH" = "amd64" ]; then \
        AWS_ARCH=x86_64; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
        AWS_ARCH=aarch64; \
    else \
        echo "ERROR: unsupported architecture $TARGETARCH" 1>&2; \
        exit 1; \
    fi; \
    NO_PROXY="" no_proxy="" curl -Ls https://s3.us-west-2.amazonaws.com/amazon-eks/1.22.6/2022-03-09/bin/linux/$TARGETARCH/kubectl -o kubectl && \
    echo "860c3d37a5979491895767e7332404d28dc0d7797c7673c33df30ca80e215a07  kubectl" > kubectl.txt && \
    sha256sum --quiet -c kubectl.txt && \
    mv kubectl /usr/local/bin/kubectl && \
    chmod 755 /usr/local/bin/kubectl && \
    curl -Ls https://awscli.amazonaws.com/awscli-exe-linux-${AWS_ARCH}-2.7.34.zip -o "awscliv2-${AWS_ARCH}.zip" && \
    echo "daf9253f0071b5cfee9532bc5220bedd7a5d29d4e0f92b42b9e3e4c496341e88  awscliv2-x86_64.zip" > awscliv2-x86_64.txt && \
    echo "bcb195622f0956c8569736b95bff9cff7f5684e400e694a33661f2d498fbc799  awscliv2-aarch64.zip" > awscliv2-aarch64.txt && \
    sha256sum --quiet -c awscliv2-${AWS_ARCH}.txt && \
    unzip -qq awscliv2-${AWS_ARCH}.zip && \
    ./aws/install && \
    rm -rf /tmp/*

# TODO: Log4J complains that it's eating the HTracer logs.  Even without it, we get per-operation
# time series graphs of throughput, median, 90, 99, 99.9 and 99.99 (in usec).
COPY run_ycsb.sh run_ycsb_standalone.sh /usr/local/bin/
RUN mkdir -p /var/log/fdb-trace-logs && \
    chmod +x /usr/local/bin/run_ycsb.sh && \
    chmod +x /usr/local/bin/run_ycsb_standalone.sh


COPY YCSB /YCSB
WORKDIR /YCSB
ENV FDB_NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY=/var/dynamic-conf/lib/multiversion/
ENV FDB_NETWORK_OPTION_TRACE_ENABLE=/var/log/fdb-trace-logs
ENV LD_LIBRARY_PATH=/var/dynamic-conf/lib/
ENV BUCKET=""
ENTRYPOINT ["run_ycsb.sh"]
