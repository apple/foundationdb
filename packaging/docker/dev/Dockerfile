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

ARG REPOSITORY=foundationdb/build
ARG VERSION=centos7-latest
FROM $REPOSITORY:$VERSION

# Install FoundationDB Binaries

WORKDIR /var/fdb/tmp

COPY docker/scripts scripts/

RUN chmod u+x scripts/*.bash && \
	mkdir -p logs

COPY . /var/fdb/tmp/packages

WORKDIR /var/fdb/tmp/packages/bin

RUN chmod u+x fdbbackup fdbcli fdbdr fdbmonitor fdbrestore fdbserver backup_agent dr_agent && \
	mv fdbbackup fdbcli fdbdr fdbmonitor fdbrestore fdbserver backup_agent dr_agent /usr/bin

WORKDIR /var/fdb/tmp/packages/lib

RUN mv libfdb_c.so /usr/lib/libfdb_c.so && \
	mv libfdb_java.so /usr/lib/libfdb_java.so

# Set Up Runtime Scripts and Directories

VOLUME /var/fdb/data

CMD /var/fdb/scripts/fdb.bash

# Runtime Configuration Options

ENV FDB_PORT 4500
ENV FDB_CLUSTER_FILE /var/fdb/fdb.cluster
ENV FDB_NETWORKING_MODE container
ENV FDB_COORDINATOR ""
ENV FDB_COORDINATOR_PORT 4500
ENV FDB_CLUSTER_FILE_CONTENTS ""
ENV FDB_PROCESS_CLASS unset

# Adding tini as PID 1 https://github.com/krallin/tini
ENTRYPOINT ["/usr/bin/tini", "-g", "--"]
