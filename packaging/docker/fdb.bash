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


    if [[ -n "$FDB_CLUSTER_FILE_CONTENTS" || -n "$FDB_COORDINATOR" ]]; then
        if [[ ! -w  "$FDB_CLUSTER_FILE" ]]; then
            echo "Clusterfile to be overwritten at \"$FDB_CLUSTER_FILE\" already exists but is read-only."
            exit 1
        elif [[ -r "$FDB_CLUSTER_FILE" ]]; then
            echo "Overwriting existing clusterfile."
        fi
    fi

    if [[ -n "$FDB_CLUSTER_FILE_CONTENTS" ]]; then
        echo "$FDB_CLUSTER_FILE_CONTENTS" > "$FDB_CLUSTER_FILE"
    elif [[ -n $FDB_COORDINATOR ]]; then
        coordinator_ip=$(dig +short "$FDB_COORDINATOR")
        if [[ -z "$coordinator_ip" ]]; then
            echo "Failed to look up coordinator address for $FDB_COORDINATOR" 1>&2
            exit 1
        fi
        coordinator_port=${FDB_COORDINATOR_PORT:-4500}
        echo "docker:docker@$coordinator_ip:$coordinator_port" > "$FDB_CLUSTER_FILE"
    elif [[ ! -r "$FDB_CLUSTER_FILE" ]]; then
        echo "FDB_CLUSTER_FILE_CONTENTS and FDB_COORDINATOR are not set, and no clusterfile at \"$FDB_CLUSTER_FILE\"." 1>&2
        exit 1
    else
        echo "Using existing clusterfile at \"$FDB_CLUSTER_FILE\"."
    fi

    if (( $? != 0 )); then
        echo "An unexpected error occurred while setting configuration." 1>&2
        exit 1
    fi
}

function create_server_environment() {
    env_file=/var/fdb/.fdbenv

    if [[ "$FDB_NETWORKING_MODE" == "host" ]]; then
        public_ip=127.0.0.1
    elif [[ "$FDB_NETWORKING_MODE" == "container" ]]; then
        public_ip=$(hostname -i | awk '{print $1}')
    else
        echo "Unknown FDB Networking mode \"$FDB_NETWORKING_MODE\"" 1>&2
        exit 1
    fi

    echo "export PUBLIC_IP=$public_ip" > $env_file
    # Set default cluster file contents only if no other configuration is specified.
    if [[ (! -s "$FDB_CLUSTER_FILE") && -z "$FDB_CLUSTER_FILE_CONTENTS" && -z "$FDB_COORDINATOR" ]]; then
        echo "Warning: No configuration available, falling back to self-coordinated."
        FDB_CLUSTER_FILE_CONTENTS="docker:docker@$public_ip:$FDB_PORT"
    fi

    create_cluster_file
}

create_server_environment
source /var/fdb/.fdbenv
echo "Starting FDB server on $PUBLIC_IP:$FDB_PORT"
fdbserver --listen-address 0.0.0.0:"$FDB_PORT" --public-address "$PUBLIC_IP:$FDB_PORT" \
    --datadir /var/fdb/data --logdir /var/fdb/logs \
    --locality-zoneid="$(hostname)" --locality-machineid="$(hostname)" --class "$FDB_PROCESS_CLASS" --knob_disable_posix_kernel_aio=1
