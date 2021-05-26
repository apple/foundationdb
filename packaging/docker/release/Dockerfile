# Dockerfile
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

COPY misc/tini-amd64.sha256sum /tmp/
# Adding tini as PID 1 https://github.com/krallin/tini
ARG TINI_VERSION=v0.19.0
RUN curl -sLO https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-amd64 && \
  sha256sum -c /tmp/tini-amd64.sha256sum && \
  chmod +x tini-amd64 && \
  mv tini-amd64 /usr/bin/tini

ARG FDB_VERSION
ARG FDB_ADDITIONAL_VERSIONS="5.1.7"
ARG FDB_WEBSITE=https://www.foundationdb.org

WORKDIR /var/fdb/tmp
COPY website /mnt/website/

# Install FoundationDB Binaries
RUN curl $FDB_WEBSITE/downloads/$FDB_VERSION/linux/fdb_$FDB_VERSION.tar.gz | tar zxf - --strip-components=1 && \
	chmod u+x fdbbackup fdbcli fdbdr fdbmonitor fdbrestore fdbserver backup_agent dr_agent && \
	mv fdbbackup fdbcli fdbdr fdbmonitor fdbrestore fdbserver backup_agent dr_agent /usr/bin && \
	rm -r /var/fdb/tmp

WORKDIR /

## TODO: Can unify everything above this line
## TODO: we can almost unify the additional client library download,
##       but sidecar.py expects them in a different location,
##        with a different naming convention.

RUN curl $FDB_WEBSITE/downloads/$FDB_VERSION/linux/libfdb_c_$FDB_VERSION.so -o /usr/lib/libfdb_c.so

# Set Up Runtime Scripts and Directories
ADD release/*.bash /var/fdb/scripts/
RUN chmod a+x /var/fdb/scripts/*.bash

# Install additional FoundationDB Client Libraries
RUN	/var/fdb/scripts/download_multiversion_libraries.bash $FDB_WEBSITE $FDB_ADDITIONAL_VERSIONS

RUN	rm -rf /mnt/website

RUN	mkdir -p logs

VOLUME /var/fdb/data

# Runtime Configuration Options

ENV FDB_PORT 4500
ENV FDB_CLUSTER_FILE /var/fdb/fdb.cluster
ENV FDB_NETWORKING_MODE container
ENV FDB_COORDINATOR ""
ENV FDB_COORDINATOR_PORT 4500
ENV FDB_CLUSTER_FILE_CONTENTS ""
ENV FDB_PROCESS_CLASS unset

ENTRYPOINT ["/usr/bin/tini", "-g", "--"]
CMD /var/fdb/scripts/fdb.bash
