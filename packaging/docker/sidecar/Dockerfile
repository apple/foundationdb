# Dockerfile
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2018-2019 Apple Inc. and the FoundationDB project authors
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

FROM python:3.9-slim

WORKDIR /var/fdb/tmp
ARG FDB_VERSION=6.2.30
ARG FDB_LIBRARY_VERSIONS="6.2.30 6.1.13"
ARG FDB_WEBSITE=https://www.foundationdb.org
# Adding tini as PID 1 https://github.com/krallin/tini
ARG TINI_VERSION=v0.19.0

COPY website /mnt/website
RUN apt-get update && \
	apt-get install -y --no-install-recommends curl && \
	curl --fail $FDB_WEBSITE/downloads/$FDB_VERSION/linux/fdb_$FDB_VERSION.tar.gz -o fdb_$FDB_VERSION.tar.gz && \
	tar -xzf fdb_$FDB_VERSION.tar.gz --strip-components=1 && \
	rm fdb_$FDB_VERSION.tar.gz && \
	chmod u+x fdbbackup fdbcli fdbdr fdbmonitor fdbrestore fdbserver backup_agent dr_agent && \
	mv fdbbackup fdbcli fdbdr fdbmonitor fdbrestore fdbserver backup_agent dr_agent /usr/bin && \
	echo ${FDB_VERSION} > /var/fdb/version && mkdir -p /var/fdb/lib && \
	for version in $FDB_LIBRARY_VERSIONS; do curl --fail $FDB_WEBSITE/downloads/$version/linux/libfdb_c_$version.so -o /var/fdb/lib/libfdb_c_${version%.*}.so; done && \
	curl -LO https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-amd64 && \
    curl -LO https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-amd64.sha256sum && \
	sha256sum -c tini-amd64.sha256sum && \
	rm -f tini-amd64.sha256sum && \
    chmod +x tini-amd64 && \
	mv tini-amd64 /usr/bin/tini && \
	rm -r /var/fdb/tmp && \
	groupadd --gid 4059 fdb && \
	useradd --gid 4059 --uid 4059 --no-create-home --shell /bin/bash fdb && \
	apt-get remove -y curl && \
	rm -rf /var/lib/apt/lists/*

WORKDIR /

COPY sidecar/entrypoint.bash /
COPY sidecar/requirements.txt /
COPY sidecar/sidecar.py /

RUN pip install -r /requirements.txt && rm /requirements.txt && chmod a+x /entrypoint.bash

VOLUME /var/input-files
VOLUME /var/output-files

USER fdb

ENV LISTEN_PORT 8080

ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/entrypoint.bash"]
