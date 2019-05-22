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

# Install dependencies

RUN apt-get update && \
	apt-get install -y curl>=7.58.0-2ubuntu3.6 \
	dnsutils>=1:9.11.3+dfsg-1ubuntu1.7 && \
	rm -r /var/lib/apt/lists/*

# Install FoundationDB Binaries

ARG FDB_VERSION
ARG FDB_WEBSITE=https://www.foundationdb.org

WORKDIR /var/fdb/tmp
ADD website /mnt/website
RUN ls -l /mnt/website
RUN curl $FDB_WEBSITE/downloads/$FDB_VERSION/linux/fdb_$FDB_VERSION.tar.gz -o fdb_$FDB_VERSION.tar.gz && \
	tar -xzf fdb_$FDB_VERSION.tar.gz --strip-components=1 && \
	rm fdb_$FDB_VERSION.tar.gz && \
	chmod u+x fdbbackup fdbcli fdbdr fdbmonitor fdbrestore fdbserver backup_agent dr_agent && \
	mv fdbbackup fdbcli fdbdr fdbmonitor fdbrestore fdbserver backup_agent dr_agent /usr/bin && \
	rm -r /var/fdb/tmp

WORKDIR /var/fdb

# Install FoundationDB Client Libraries

ARG FDB_ADDITIONAL_VERSIONS="5.1.7"

COPY download_multiversion_libraries.bash scripts/

RUN curl $FDB_WEBSITE/downloads/$FDB_VERSION/linux/libfdb_c_$FDB_VERSION.so -o /usr/lib/libfdb_c.so && \
	bash scripts/download_multiversion_libraries.bash $FDB_WEBSITE $FDB_ADDITIONAL_VERSIONS && \
	rm -rf /mnt/website

# Set Up Runtime Scripts and Directories

COPY fdb.bash scripts/
COPY create_server_environment.bash scripts/
COPY create_cluster_file.bash scripts/
RUN chmod u+x scripts/*.bash && \
	mkdir -p logs
VOLUME /var/fdb/data

CMD /var/fdb/scripts/fdb.bash

# Runtime Configuration Options

ENV FDB_PORT 4500
ENV FDB_CLUSTER_FILE /var/fdb/fdb.cluster
ENV FDB_NETWORKING_MODE container
ENV FDB_COORDINATOR ""
ENV FDB_CLUSTER_FILE_CONTENTS ""
ENV FDB_PROCESS_CLASS unset
