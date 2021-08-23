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
RUN go build -o /fdb-kubernetes-monitor ./...

# Build the main image

FROM ubuntu:18.04

RUN apt-get update && \
	apt-get install -y curl>=7.58.0-2ubuntu3.6 \
		dnsutils>=1:9.11.3+dfsg-1ubuntu1.7 \
		lsof>=4.89+dfsg-0.1 \
		tcptraceroute>=1.5beta7+debian-4build1 \
		telnet>=0.17-41 \
		netcat>=1.10-41.1 \
		strace>=4.21-1ubuntu1 \
		tcpdump>=4.9.3-0ubuntu0.18.04.1 \
		less>=487-0.1 \
		vim>=2:8.0.1453-1ubuntu1.4 \
		net-tools>=1.60+git20161116.90da8a0-1ubuntu1 \
		jq>=1.5+dfsg-2 && \
	rm -rf /var/lib/apt/lists/*

ARG FDB_VERSION
ARG FDB_LIBRARY_VERSIONS="${FDB_VERSION}"
ARG FDB_WEBSITE=https://www.foundationdb.org

COPY packaging/docker/website /mnt/website/

# Install FoundationDB Binaries
RUN mkdir -p /var/fdb/logs && mkdir -p /var/fdb/tmp && \
	curl $FDB_WEBSITE/downloads/$FDB_VERSION/linux/fdb_$FDB_VERSION.tar.gz | tar zxf - --strip-components=1 && \
	chmod u+x fdbbackup fdbcli fdbdr fdbmonitor fdbrestore fdbserver backup_agent dr_agent && \
	mv fdbbackup fdbcli fdbdr fdbmonitor fdbrestore fdbserver backup_agent dr_agent /usr/bin

# Install additional FoundationDB Client Libraries
ADD packaging/docker/release/download_multiversion_libraries.bash /var/fdb/tmp
RUN	bash /var/fdb/tmp/download_multiversion_libraries.bash $FDB_WEBSITE $FDB_LIBRARY_VERSIONS

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
