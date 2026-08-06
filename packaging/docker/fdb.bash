#!/bin/bash

#
# fdb.bash
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

function create_cluster_file() {
    FDB_CLUSTER_FILE=${FDB_CLUSTER_FILE:-/etc/foundationdb/fdb.cluster}
    mkdir -p "$(dirname $FDB_CLUSTER_FILE)"

    if [[ -n "$FDB_CLUSTER_FILE_CONTENTS" ]]; then
        echo "$FDB_CLUSTER_FILE_CONTENTS" > "$FDB_CLUSTER_FILE"
        if [[ $? != 0 ]]; then
            echo "FDB_CLUSTER_FILE_CONTENTS is defined, but cannot write to FDB_CLUSTER_FILE ($FDB_CLUSTER_FILE). Is something mounted read-only there?"
            exit 1
        fi
    elif [[ -n $FDB_COORDINATOR ]]; then
        if (( $public_ip_stack == 4 )); then
            coordinator_ip=$(getent ahostsv4 "$FDB_COORDINATOR" | awk 'END{ print $1 }')
        else
            coordinator_ip='['$(getent ahostsv6 "$FDB_COORDINATOR" | awk 'END{ print $1 }')']'
        fi

        if (( $? != 0 )); then
            echo "Failed to look up coordinator address for $FDB_COORDINATOR" 1>&2
            exit 1
        fi
        echo "docker:docker@$coordinator_ip:$FDB_COORDINATOR_PORT" > "$FDB_CLUSTER_FILE"
    elif [[ ! -r "$FDB_CLUSTER_FILE" ]]; then
        echo "Neither FDB_CLUSTER_FILE_CONTENTS nor FDB_COORDINATOR are set, but no readable cluster file is at FDB_CLUSTER_FILE ($FDB_CLUSTER_FILE)."
        exit 1
    else
        echo "Using existing FDB_CLUSTER_FILE at $FDB_CLUSTER_FILE"
    fi
}

function create_server_environment() {
    if [[ "$FDB_NETWORKING_MODE" == "host" ]]; then
        public_ip=127.0.0.1
    elif [[ "$FDB_NETWORKING_MODE" == "container" ]]; then
        public_ip=$(hostname -i | awk '{print $1}')
        public_ip_stack=4
        listen_addr=0.0.0.0
        # IPv6 addresses need to be enclosed in brackets
        if [[ $public_ip == *":"* ]]; then
            listen_addr='[::]'
            public_ip_stack=6
            public_ip="[$public_ip]"
        fi

        echo "The server is currently running on an IPv$public_ip_stack stack!"
    else
        echo "Unknown FDB Networking mode \"$FDB_NETWORKING_MODE\"" 1>&2
        exit 1
    fi

    export PUBLIC_IP=$public_ip
    export LISTEN_ADDR=$listen_addr
    # Set default cluster file contents only if no configuration exists already.
    if [[ (! -s "$FDB_CLUSTER_FILE") && -z $FDB_COORDINATOR && -z "$FDB_CLUSTER_FILE_CONTENTS" ]]; then
        FDB_CLUSTER_FILE_CONTENTS="docker:docker@$public_ip:$FDB_PORT"
    fi

    create_cluster_file
}

create_server_environment
echo "Starting FDB server on $PUBLIC_IP:$FDB_PORT, listening on $LISTEN_ADDR:$FDB_PORT"
fdbserver --listen-address $LISTEN_ADDR:"$FDB_PORT" --public-address "$PUBLIC_IP:$FDB_PORT" \
    --datadir /var/fdb/data --logdir /var/fdb/logs \
    --locality-zoneid="$(hostname)" --locality-machineid="$(hostname)" --class "$FDB_PROCESS_CLASS" --knob_disable_posix_kernel_aio=1 \
    $@
