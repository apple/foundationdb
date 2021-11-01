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

FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl && \
    pip install watchdog==0.9.0 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /tmp
# Adding tini as PID 1 https://github.com/krallin/tini
RUN curl -Ls https://github.com/krallin/tini/releases/download/v0.19.0/tini-amd64 -o tini  && \
    echo "93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c  tini" > tini-amd64.sha256sum && \
    sha256sum -c tini-amd64.sha256sum && \
    chmod +x tini && \
    mv tini /usr/bin/ && \
    rm -rf /tmp/*

ARG FDB_VERSION
ARG FDB_ADDITIONAL_VERSIONS="6.2.30 6.1.13"
ARG FDB_WEBSITE=https://www.foundationdb.org

COPY website /mnt/website/

# Install FoundationDB Binaries
RUN curl $FDB_WEBSITE/downloads/$FDB_VERSION/linux/fdb_$FDB_VERSION.tar.gz | tar zxf - --strip-components=1 && \
    chmod u+x fdbbackup fdbcli fdbdr fdbmonitor fdbrestore fdbserver backup_agent dr_agent && \
    mv fdbbackup fdbcli fdbdr fdbmonitor fdbrestore fdbserver backup_agent dr_agent /usr/bin && \
    rm -rf /tmp/*

# Install additional FoundationDB Client Libraries
RUN mkdir -p /var/fdb/lib && \
    for version in $FDB_ADDITIONAL_VERSIONS; do \
    curl $FDB_WEBSITE/downloads/$version/linux/libfdb_c_$version.so -o /var/fdb/lib/libfdb_c_${version%.*}.so; \
    done && \
    rm -rf /mnt/website

WORKDIR /

# Set Up Runtime Scripts and Directories
ADD sidecar/entrypoint.bash sidecar/sidecar.py /
RUN chmod a+x /entrypoint.bash /sidecar.py

RUN echo ${FDB_VERSION} > /var/fdb/version && \
    mkdir -p /var/fdb/lib && \
    groupadd --gid 4059 fdb && \
    useradd --gid 4059 --uid 4059 --no-create-home --shell /bin/bash fdb


VOLUME /var/input-files
VOLUME /var/output-files

USER fdb

# Runtime Configuration Options

ENV LISTEN_PORT 8080

ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/entrypoint.bash"]
