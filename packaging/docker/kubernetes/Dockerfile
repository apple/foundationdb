# Dockerfile
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2021 Apple Inc. and the FoundationDB project authors
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

# This docker image assumes that the context for the docker build is pointed
# at the root of the foundationdb repository.

# Build the Kubernetes monitor

FROM golang:1.16.7-bullseye AS go-build

COPY fdbkubernetesmonitor/ /fdbkubernetesmonitor
WORKDIR /fdbkubernetesmonitor
RUN go build -o /fdb-kubernetes-monitor *.go

# Build the main image

FROM centos:7.9.2009

RUN yum install -y \
	binutils-2.27-44.base.el7 \
	bind-utils-9.11.4-26.P2.el7_9.7 \
	curl-7.29.0-59.el7_9.1 \
	less-458-9.el7 \
	lsof-4.87-6.el7 \
	nano-2.3.1-10.el7 \
	nmap-ncat-6.40-19.el7 \
	net-tools-2.0-0.25.20131004git.el7 \
	strace-4.24-6.el7 \
	tar-1.26-35.el7 \
	telnet-0.17-66.el7 \
	traceroute-2.0.22-2.el7 \
	tcpdump-4.9.2-4.el7_7.1 \
	vim-enhanced-7.4.629-8.el7_9 \
	&& yum clean all

ARG FDB_VERSION
ARG FDB_LIBRARY_VERSIONS="${FDB_VERSION}"
ARG FDB_WEBSITE=https://www.foundationdb.org

COPY packaging/docker/website /mnt/website/

# Install FoundationDB Binaries
RUN mkdir -p /var/fdb/logs && mkdir -p /var/fdb/tmp && \
	curl $FDB_WEBSITE/downloads/$FDB_VERSION/linux/fdb_$FDB_VERSION.tar.gz | tar zxf - --strip-components=1 && \
	chmod u+x fdbbackup fdbcli fdbdr fdbmonitor fdbrestore fdbserver backup_agent dr_agent && \
	mv fdbbackup fdbcli fdbdr fdbmonitor fdbrestore fdbserver backup_agent dr_agent /usr/bin && \
	echo ${FDB_VERSION} > /var/fdb/version

# Install additional FoundationDB Client Libraries
RUN mkdir -p /usr/lib/fdb/multiversion && \
    for version in $FDB_LIBRARY_VERSIONS; do curl $FDB_WEBSITE/downloads/$version/linux/libfdb_c_$version.so -o /usr/lib/fdb/multiversion/libfdb_c_${version%.*}.so; done

# Clean up temporary directories
RUN	rm -rf /mnt/website && rm -r /var/fdb/tmp

# Install the kubernetes monitor binary
COPY --from=go-build /fdb-kubernetes-monitor /usr/bin/

# Set up a non-root user

RUN groupadd --gid 4059 fdb && \
	useradd --gid 4059 --uid 4059 --no-create-home --shell /bin/bash fdb && \
	chown -R fdb:fdb /var/fdb

# Runtime Configuration Options

USER fdb
WORKDIR /var/fdb
ENTRYPOINT ["/usr/bin/fdb-kubernetes-monitor"]
VOLUME /var/fdb/data
