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

    if [[ -n $FDB_COORDINATOR ]]; then
        if [[ "$FDB_IP_VERSION" == '4' ]]; then
            coordinator_ip="$(getent ahostsv4 $FDB_COORDINATOR | awk 'END{ print $1 }')"
        elif [[ "$FDB_IP_VERSION" == '6' ]]; then
            coordinator_ip="[$(getent ahostsv6 $FDB_COORDINATOR | awk 'END{ print $1 }')]"
        fi
        
        if [[ -z "$coordinator_ip" ]]; then
            echo "Failed to look up coordinator address for $FDB_COORDINATOR" 1>&2
            exit 1
        fi
        coordinator_port=${FDB_COORDINATOR_PORT:-4500}
        FDB_CLUSTER_FILE_CONTENTS="docker:docker@$coordinator_ip:$coordinator_port"
    fi

    if [[ -n "$FDB_CLUSTER_FILE_CONTENTS" ]]; then
        if [[ -w "$FDB_CLUSTER_FILE" ]]; then
            echo "Overwriting existing clusterfile." 1>&2
        fi
        echo "$FDB_CLUSTER_FILE_CONTENTS" > "$FDB_CLUSTER_FILE"
    elif [[ ! -w "$FDB_CLUSTER_FILE" ]]; then
        # fdbserver requires write permissions to clusterfile, or it may *eventually* fail due to cluster migrations.
        # https://apple.github.io/foundationdb/administration.html#required-permissions
        echo "Clusterfile at \"$FDB_CLUSTER_FILE\" is not writable." 1>&2
        exit 1
    else
        echo "Using existing clusterfile at \"$FDB_CLUSTER_FILE\"." 1>&2
    fi

    if (( $? != 0 )); then
        echo "Unable to write to FDB_CLUSTER_FILE." 1>&2
        exit 1
    fi
}

function first_hostname_with_str() {
    for addr in $(hostname -I); do
        if [[ $addr == *"$1"* ]]; then
            echo "$addr"
            return 0
        fi
    done
    return 1
}

function create_server_environment() {
    FDB_IP_VERSION=${FDB_IP_VERSION:-v4}

    if [[ "$FDB_IP_VERSION" == '4' ]]; then
        export FDB_LISTEN_IP=${FDB_LISTEN_IP:-'0.0.0.0'}
        public_ip=${FDB_PUBLIC_IP:-"$(first_hostname_with_str '.')"}
    elif [[ "$FDB_IP_VERSION" == '6' ]]; then
        export FDB_LISTEN_IP=${FDB_LISTEN_IP:-'[::]'}
        public_ip=${FDB_PUBLIC_IP:-"[$(first_hostname_with_str ':')]"}
    else
        echo "Unknown FDB IP version \"$FDB_IP_VERSION\"" 1>&2
        exit 1
    fi

    if (( $? > 0 )); then
        echo "No valid IP for IP version \"$FDB_IP_VERSION\"" 1>&2
        exit 1
    fi

    export FDB_PUBLIC_IP="$public_ip"
    

    # Set default cluster file contents only if no other configuration is specified.
    if [[ (! -s "$FDB_CLUSTER_FILE") && -z "$FDB_CLUSTER_FILE_CONTENTS" && -z "$FDB_COORDINATOR" ]]; then
        echo "Warning: No configuration available, falling back to self-coordinated." 1>&2
        FDB_CLUSTER_FILE_CONTENTS="docker:docker@$FDB_PUBLIC_IP:$FDB_PORT"
    fi

    create_cluster_file
}

create_server_environment
echo "Starting FDB server on $FDB_PUBLIC_IP:$FDB_PORT, listening on $FDB_LISTEN_IP:$FDB_PORT"
fdbserver --listen-address "$FDB_LISTEN_IP:$FDB_PORT" --public-address "$FDB_PUBLIC_IP:$FDB_PORT" \
    --datadir /var/fdb/data --logdir /var/fdb/logs \
    --locality-zoneid="$(hostname)" --locality-machineid="$(hostname)" --class "$FDB_PROCESS_CLASS" --knob_disable_posix_kernel_aio=1 \
    "$@"
