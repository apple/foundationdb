#
# asyncio_impl.py
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

"""asyncio-friendly wrapper for FoundationDB Python bindings.

This module provides a thin shim that lets callers ``await`` FoundationDB
futures and run transactional functions inside an asyncio event loop. The
implementation is intentionally a wrapper rather than a native integration:

* The blocking ``Future.wait()`` calls supplied by ``fdb.impl`` are dispatched
  to a thread executor via ``loop.run_in_executor``. This isolates the asyncio
  event loop from the underlying C-extension blocking semantics without
  requiring changes to the FDB network thread.
* ``transactional_async`` mirrors ``fdb.transactional`` but awaits each step
  through that executor and retries via ``Transaction.on_error``.

A native asyncio integration would attach the FDB callback machinery directly
to the event loop's ``call_soon_threadsafe`` queue and skip the executor; the
work is sketched in ``ASYNCIO_DESIGN.md``. This shim is meant as a
forward-compatible scaffold so users can write ``await`` code today while the
native path lands.

Typical usage::

    import asyncio
    import fdb
    from fdb.asyncio_impl import open_async

    fdb.api_version(fdb.LATEST_API_VERSION)

    async def main():
        db = await open_async()
        await db.set_async(b"hello", b"world")
        value = await db.get_async(b"hello")
        print(value)

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import functools
from typing import Any, Awaitable, Callable, Optional


# Resolved lazily so that ``import fdb.asyncio_impl`` does not require the
# caller to have already invoked ``fdb.api_version``.
def _impl():
    import fdb.impl

    return fdb.impl


async def _await_future(future: Any) -> Any:
    """Await an ``fdb.impl.Future`` by resolving it on the default executor.

    The underlying ``Future.wait()`` is a blocking call backed by the C
    client's network thread. Running it in an executor lets the event loop
    keep servicing other coroutines while the FDB future resolves. This is
    not zero-copy asyncio integration, but it is correct: the thread blocks,
    the event loop does not.
    """
    if future.is_ready():
        # Fast path: avoid scheduling work onto the executor when the future
        # has already completed.
        return future.wait()

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, future.wait)


class AsyncTransaction:
    """Async wrapper around ``fdb.impl.Transaction``.

    Every read returns a coroutine that awaits the underlying FDB future via
    the executor shim above. Writes are synchronous in the C client so they
    are forwarded directly without await semantics.
    """

    def __init__(self, tr: Any) -> None:
        self._tr = tr

    # ----- reads ---------------------------------------------------------
    async def get(self, key: bytes) -> Optional[bytes]:
        return await _await_future(self._tr.get(key))

    async def get_key(self, key_selector: Any) -> bytes:
        return await _await_future(self._tr.get_key(key_selector))

    async def get_range(self, begin: Any, end: Any, limit: int = 0) -> list:
        # ``get_range`` returns an FDBRange container whose ``to_list`` is
        # itself driven off futures. We materialise eagerly here for
        # simplicity; callers who need streaming should use the lower level
        # ``Transaction`` directly.
        future = self._tr.get_range(begin, end, limit)._future
        await _await_future(future)
        return list(self._tr.get_range(begin, end, limit))

    # ----- writes --------------------------------------------------------
    def set(self, key: bytes, value: bytes) -> None:
        self._tr.set(key, value)

    def clear(self, key: bytes) -> None:
        self._tr.clear(key)

    def clear_range(self, begin: bytes, end: bytes) -> None:
        self._tr.clear_range(begin, end)

    # ----- lifecycle -----------------------------------------------------
    async def commit(self) -> None:
        await _await_future(self._tr.commit())

    async def on_error(self, code: int) -> None:
        await _await_future(self._tr.on_error(code))

    def reset(self) -> None:
        self._tr.reset()

    @property
    def raw(self) -> Any:
        """Escape hatch: the underlying ``fdb.impl.Transaction``."""
        return self._tr


class AsyncDatabase:
    """Async wrapper around ``fdb.impl.Database``."""

    def __init__(self, db: Any) -> None:
        self._db = db

    def create_transaction(self) -> AsyncTransaction:
        return AsyncTransaction(self._db.create_transaction())

    # Convenience single-shot helpers. They open a transaction, perform the
    # operation, commit, and retry on retryable errors -- mirroring the
    # behaviour of ``Database.get`` / ``Database.set`` in the sync bindings.
    async def get_async(self, key: bytes) -> Optional[bytes]:
        async def op(tr: AsyncTransaction) -> Optional[bytes]:
            return await tr.get(key)

        return await self.transactional_async(op)

    async def set_async(self, key: bytes, value: bytes) -> None:
        async def op(tr: AsyncTransaction) -> None:
            tr.set(key, value)

        await self.transactional_async(op)

    async def clear_async(self, key: bytes) -> None:
        async def op(tr: AsyncTransaction) -> None:
            tr.clear(key)

        await self.transactional_async(op)

    async def transactional_async(
        self,
        func: Callable[[AsyncTransaction], Awaitable[Any]],
    ) -> Any:
        """Run ``func`` inside a retry loop driven by ``Transaction.on_error``.

        The semantics match ``fdb.transactional``: the callable is invoked
        with an ``AsyncTransaction``, its result awaited, the transaction
        committed, and any retryable ``FDBError`` retried. Non-retryable
        errors propagate.
        """
        impl = _impl()
        FDBError = impl.FDBError

        tr = self.create_transaction()
        while True:
            try:
                result = await func(tr)
                await tr.commit()
                return result
            except FDBError as exc:
                # ``on_error`` raises if the error is non-retryable, in
                # which case we propagate. Otherwise the transaction is
                # reset and we loop.
                await tr.on_error(exc.code)

    @property
    def raw(self) -> Any:
        """Escape hatch: the underlying ``fdb.impl.Database``."""
        return self._db


async def open_async(cluster_file: Optional[str] = None) -> AsyncDatabase:
    """Open a database and return an :class:`AsyncDatabase` wrapper.

    ``fdb.api_version`` must have been called before this. The underlying
    ``fdb.open`` is itself synchronous; we run it in the default executor so
    the event loop is not blocked while the network thread is brought up.
    """
    import fdb

    loop = asyncio.get_running_loop()
    db = await loop.run_in_executor(
        None, functools.partial(fdb.open, cluster_file)
    )
    return AsyncDatabase(db)


def wrap_database(db: Any) -> AsyncDatabase:
    """Wrap an already-opened ``fdb.impl.Database`` in an :class:`AsyncDatabase`.

    Useful when an application has existing sync code paths that hold a
    Database handle and wants to add an async surface alongside.
    """
    return AsyncDatabase(db)


__all__ = [
    "AsyncDatabase",
    "AsyncTransaction",
    "open_async",
    "wrap_database",
]
