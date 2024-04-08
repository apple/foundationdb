# Dockerfile
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

# ARG needs to be defined for both FROM instructions,
# see https://github.com/moby/moby/issues/34129
ARG FDB_VERSION
FROM foundationdb/foundationdb:${FDB_VERSION} as fdb
FROM golang:1.21
ARG FDB_VERSION

WORKDIR /tmp

RUN apt-get update
# dnsutils is needed to have dig installed to create cluster file
RUN apt-get install -y --no-install-recommends ca-certificates dnsutils

RUN wget "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_amd64.deb"
RUN dpkg -i foundationdb-clients_${FDB_VERSION}-1_amd64.deb

WORKDIR /go/src/app
COPY . .

COPY start.bash /start.bash

RUN go get -d -v ./...
RUN go install -v ./...

CMD ["/start.bash"]
