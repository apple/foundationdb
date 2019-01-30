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

FROM python:3.6

RUN apt-get update; apt-get install -y dnsutils

RUN mkdir -p /app
WORKDIR /app

COPY --from=foundationdb:5.2.5 /usr/lib/libfdb_c.so /usr/lib
COPY --from=foundationdb:5.2.5 /usr/bin/fdbcli /usr/bin/
COPY --from=foundationdb:5.2.5 /var/fdb/scripts/create_cluster_file.bash /app

COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY start.bash /app
COPY server.py /app
RUN chmod u+x /app/start.bash

CMD /app/start.bash

ENV FLASK_APP=server.py
ENV FLASK_ENV=development