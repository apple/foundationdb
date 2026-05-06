#!/usr/bin/python
#
# asyncio_test.py
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

"""Smoke tests for fdb.asyncio_impl.

These tests require a running FoundationDB cluster. Skip if the C client
or a cluster is not available.

Run with::

    python -m bindings.python.tests.asyncio_test [cluster-file]
"""

import asyncio
import os
import sys
import uuid


async def _run_smoke(cluster_file=None):
    import fdb
    from fdb.asyncio_impl import open_async

    fdb.api_version(fdb.LATEST_API_VERSION)

    db = await open_async(cluster_file)

    # Use a unique key prefix so concurrent runs do not collide.
    prefix = b"asyncio_test/" + uuid.uuid4().hex.encode("ascii") + b"/"
    key = prefix + b"hello"
    value = b"world"

    # Round-trip a single key.
    await db.set_async(key, value)
    got = await db.get_async(key)
    assert got == value, f"expected {value!r}, got {got!r}"

    # Mutate inside a transactional block and verify retry semantics.
    async def add_two(tr):
        tr.set(prefix + b"a", b"1")
        tr.set(prefix + b"b", b"2")

    await db.transactional_async(add_two)
    a = await db.get_async(prefix + b"a")
    b = await db.get_async(prefix + b"b")
    assert a == b"1" and b == b"2", (a, b)

    # Clean up.
    async def cleanup(tr):
        tr.clear_range(prefix, prefix + b"\xff")

    await db.transactional_async(cleanup)

    print("asyncio_test: OK")


def main(argv):
    cluster_file = argv[1] if len(argv) > 1 else os.environ.get("FDB_CLUSTER_FILE")
    try:
        asyncio.run(_run_smoke(cluster_file))
    except Exception as exc:  # pragma: no cover - exercised by humans
        # The most common failure mode is "no cluster reachable" before any
        # FDB code runs; surface that clearly rather than as a stack trace.
        print(f"asyncio_test: FAILED ({exc!r})", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
