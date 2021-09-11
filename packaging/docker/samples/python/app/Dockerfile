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

ARG FDB_VERSION

FROM --platform=linux/amd64 foundationdb/foundationdb:${FDB_VERSION} as fdb
FROM --platform=linux/amd64 python:3.8-slim 

RUN apt-get update; apt-get install -y dnsutils curl && rm -rf /var/lib/apt/lists/*

COPY --from=fdb /usr/lib/libfdb_c.so /usr/lib
COPY --from=fdb /usr/bin/fdbcli /usr/bin/
COPY --from=fdb /var/fdb/scripts/create_cluster_file.bash /

ARG FDB_WEBSITE=https://www.foundationdb.org
ARG FDB_ADDITIONAL_VERSIONS
ENV FDB_NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY=/usr/lib/fdb-multiversion
RUN mkdir /usr/lib/fdb-multiversion; \
	for version in ${FDB_ADDITIONAL_VERSIONS}; do \
	curl ${FDB_WEBSITE}/downloads/$version/linux/libfdb_c_$version.so -o /usr/lib/fdb-multiversion/libfdb_c_$version.so; \
	done

WORKDIR /app

COPY /app /app
RUN pip install -r requirements.txt

ENV FLASK_APP=server.py
ENV FLASK_ENV=development

CMD /app/start.bash
