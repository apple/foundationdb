#! /bin/bash

#
# start.bash
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

set -eu

FDB_CLUSTER_FILE="${FDB_CLUSTER_FILE:-docker.cluster}"
FDB_PORT="${FDB_PORT:-4550}"

FDB_PORT=$FDB_PORT docker-compose up -d fdb
echo "docker:docker@127.0.0.1:$FDB_PORT" > $FDB_CLUSTER_FILE

# Attempt to connect. Configure the database if necessary.
if ! fdbcli -C $FDB_CLUSTER_FILE --exec status --timeout 1 ; then
    if ! fdbcli -C $FDB_CLUSTER_FILE --exec "configure new single memory ; status" --timeout 10 ; then 
        echo "Unable to configure new FDB cluster."
        exit 1
    fi
fi

echo "Can now connect to docker-based FDB cluster using $FDB_CLUSTER_FILE."
