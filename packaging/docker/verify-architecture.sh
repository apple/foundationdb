#!/bin/sh
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2026 Apple Inc. and the FoundationDB project authors
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

set -eu

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <amd64|arm64> <binary> [<binary> ...]" >&2
    exit 1
fi

target_arch=$1
shift
case "$target_arch" in
    amd64) expected_machine=3e00 ;;
    arm64) expected_machine=b700 ;;
    *) echo "Unsupported target architecture: $target_arch" >&2; exit 1 ;;
esac

for binary do
    # ELF identification specifies the class and byte order; e_machine starts
    # at byte 18. Both supported container architectures use ELF64 little-endian.
    identification=$(od -An -tx1 -N6 "$binary" | tr -d '[:space:]')
    machine=$(od -An -tx1 -j18 -N2 "$binary" | tr -d '[:space:]')
    if [ "$identification" != 7f454c460201 ] || [ "$machine" != "$expected_machine" ]; then
        echo "Architecture mismatch: $binary must be an ELF64 little-endian $target_arch binary (identification=$identification, e_machine=$machine)" >&2
        exit 1
    fi
done
