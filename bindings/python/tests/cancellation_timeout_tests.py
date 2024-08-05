#
# cancellation_timeout_tests.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

import time
import fdb

TestError = Exception


def retry_with_timeout(seconds):
    def decorator(f):
        def wrapper(db):
            timeout_tr = db.create_transaction()

            tr = db.create_transaction()
            while True:
                try:
                    timeout_tr.options.set_timeout(seconds * 1000)
                    f(tr)
                    return
                except fdb.FDBError as e:
                    timeout_tr.on_error(e).wait()
                    tr = db.create_transaction()

        return wrapper

    return decorator


default_timeout = 60


def test_cancellation(db):
    # (1) Basic cancellation
    @retry_with_timeout(default_timeout)
    def txn1(tr):
        tr.cancel()
        try:
            tr.commit().wait()  # should throw
            raise TestError("Basic cancellation unit test failed.")
        except fdb.FDBError as e:
            if e.code != 1025:
                raise

    txn1(db)

    # (2) Cancellation does not survive reset
    @retry_with_timeout(default_timeout)
    def txn2(tr):
        tr.cancel()
        tr.reset()
        try:
            tr.commit().wait()  # should not throw
        except fdb.FDBError as e:
            if e.code == 1025:
                raise TestError("Cancellation survived reset.")
            else:
                raise

    txn2(db)

    # (3) Cancellation does survive on_error()
    @retry_with_timeout(default_timeout)
    def txn3(tr):
        tr.cancel()
        try:
            tr.on_error(fdb.FDBError(1007)).wait()  # should throw
            raise TestError("on_error() did not notice cancellation.")
        except fdb.FDBError as e:
            if e.code != 1025:
                raise
        try:
            tr.commit().wait()  # should throw
            raise TestError("Cancellation did not survive on_error().")
        except fdb.FDBError as e:
            if e.code != 1025:
                raise

    txn3(db)

    # (4) Cancellation works with weird operations
    @retry_with_timeout(default_timeout)
    def txn4(tr):
        tr[b"foo"]
        tr.cancel()
        try:
            tr.get_read_version().wait()  # should throw
            raise TestError("Cancellation didn't throw on weird operation.")
        except fdb.FDBError as e:
            if e.code != 1025:
                raise

    txn4(db)

    return


def test_retry_limits(db):
    err = fdb.FDBError(1007)

    # (1) Basic retry limits
    @retry_with_timeout(default_timeout)
    def txn1(tr):
        tr.options.set_retry_limit(1)
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw
        tr[b"foo"] = b"bar"
        tr.options.set_retry_limit(1)
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(1) Retry limit was ignored.")
        except fdb.FDBError as e:
            if e.code != err.code:
                raise
        tr[b"foo"] = b"bar"
        tr.options.set_retry_limit(1)
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(1) Transaction not cancelled after error.")
        except fdb.FDBError as e:
            if e.code != 1025:
                raise

    txn1(db)

    # (2) Retry limits don't survive resets
    @retry_with_timeout(default_timeout)
    def txn2(tr):
        tr.options.set_retry_limit(1)
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw
        tr.options.set_retry_limit(1)
        tr[b"foo"] = b"bar"
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(2) Retry limit was ignored.")
        except fdb.FDBError as e:
            if e.code != err.code:
                raise
        tr.reset()
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw

    txn2(db)

    # (3) Number of retries does not survive resets
    @retry_with_timeout(default_timeout)
    def txn3(tr):
        tr.options.set_retry_limit(0)
        tr[b"foo"] = b"bar"
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(3) Retry limit was ignored.")
        except fdb.FDBError as e:
            if e.code != err.code:
                raise
        tr.reset()
        tr.options.set_retry_limit(1)
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw

    txn3(db)

    # (4) Retries accumulate when limits are turned off, and are respected retroactively
    @retry_with_timeout(default_timeout)
    def txn4(tr):
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw
        tr.options.set_retry_limit(1)
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(4) Retry limit was ignored.")
        except fdb.FDBError as e:
            if e.code != err.code:
                raise
        tr.reset()
        tr[b"foo"] = b"bar"
        tr.options.set_retry_limit(1)
        tr.on_error(err).wait()  # should not throw
        tr[b"foo"] = b"bar"
        tr.options.set_retry_limit(2)
        tr.on_error(err).wait()  # should not throw
        tr[b"foo"] = b"bar"
        tr.options.set_retry_limit(3)
        tr.on_error(err).wait()  # should not throw
        tr[b"foo"] = b"bar"
        tr.options.set_retry_limit(4)
        tr.on_error(err).wait()  # should not throw
        tr.options.set_retry_limit(4)
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(4) Retry limit was ignored.")
        except fdb.FDBError as e:
            if e.code != err.code:
                raise
        tr.options.set_retry_limit(4)
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(4) Transaction not cancelled after error.")
        except fdb.FDBError as e:
            if e.code != 1025:
                raise

    txn4(db)

    # (5) Retry limits survive on_error()
    @retry_with_timeout(default_timeout)
    def txn5(tr):
        tr.options.set_retry_limit(2)
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(5) Retry limit was ignored.")
        except fdb.FDBError as e:
            if e.code != err.code:
                raise
        try:
            tr[b"foo"] = b"bar"
            tr.on_error(err).wait()  # should throw
            raise TestError("(5) Transaction not cancelled after error.")
        except fdb.FDBError as e:
            if e.code != 1025:
                raise

    txn5(db)


def test_db_retry_limits(db):
    err = fdb.FDBError(1007)
    db.options.set_transaction_retry_limit(1)

    # (1) Basic retry limit
    @retry_with_timeout(default_timeout)
    def txn1(tr):
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw
        tr[b"foo"] = b"bar"
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(1) Retry limit from database was ignored.")
        except fdb.FDBError as e:
            if e.code != err.code:
                raise
        tr[b"foo"] = b"bar"
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(1) Transaction not cancelled after error.")
        except fdb.FDBError as e:
            if e.code != 1025:
                raise

    txn1(db)

    # (2) More retries in transaction than database
    @retry_with_timeout(default_timeout)
    def txn2(tr):
        tr.options.set_retry_limit(2)
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw
        tr[b"foo"] = b"bar"
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(2) Retry limit from transaction was ignored.")
        except fdb.FDBError as e:
            if e.code != err.code:
                raise
        tr[b"foo"] = b"bar"
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(2) Transaction not cancelled after error.")
        except fdb.FDBError as e:
            if e.code != 1025:
                raise

    txn2(db)

    # (3) More retries in database than transaction
    db.options.set_transaction_retry_limit(2)

    @retry_with_timeout(default_timeout)
    def txn3(tr):
        tr.options.set_retry_limit(1)
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw
        tr[b"foo"] = b"bar"
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(3) Retry limit from transaction was ignored.")
        except fdb.FDBError as e:
            if e.code != err.code:
                raise
        tr[b"foo"] = b"bar"
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(3) Transaction not cancelled after error.")
        except fdb.FDBError as e:
            if e.code != 1025:
                raise

    txn3(db)

    # (4) Reset resets to database default
    @retry_with_timeout(default_timeout)
    def txn4(tr):
        tr.options.set_retry_limit(1)
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw
        tr[b"foo"] = b"bar"
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(4) Retry limit from transaction was ignored.")
        except fdb.FDBError as e:
            if e.code != err.code:
                raise
        tr.reset()
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw
        tr[b"foo"] = b"bar"
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(4) Retry limit from database was ignored.")
        except fdb.FDBError as e:
            if e.code != err.code:
                raise
        try:
            tr.on_error(err).wait()  # should throw
            raise TestError("(4) Transaction not cancelled after error.")
        except fdb.FDBError as e:
            if e.code != 1025:
                raise

    txn4(db)

    db.options.set_transaction_retry_limit(-1)  # reset to default (infinite retries)


def test_timeouts(db):
    # (1) Basic timeouts
    @retry_with_timeout(default_timeout)
    def txn1(tr):
        tr.options.set_timeout(10)
        time.sleep(1)
        try:
            tr.commit().wait()  # should throw
            raise TestError("Timeout didn't fire.")
        except fdb.FDBError as e:
            if e.code != 1031:
                raise

    txn1(db)

    # (2) Timeout survives on_error()
    @retry_with_timeout(default_timeout)
    def txn2(tr):
        tr.options.set_timeout(100)
        tr.on_error(fdb.FDBError(1007)).wait()  # should not throw
        time.sleep(1)
        try:
            tr.commit().wait()  # should throw
        except fdb.FDBError as e:
            if e.code != 1031:
                raise

    txn2(db)

    # (3) Timeout having fired survives on_error()
    @retry_with_timeout(default_timeout)
    def txn3(tr):
        tr.options.set_timeout(100)
        time.sleep(1)
        try:
            tr.on_error(fdb.FDBError(1007)).wait()  # should throw
            raise TestError("Timeout didn't fire.")
        except fdb.FDBError as e:
            if e.code != 1031:
                raise
        try:
            tr.commit().wait()  # should throw
            raise TestError("Timeout didn't fire.")
        except fdb.FDBError as e:
            if e.code != 1031:
                raise

    txn3(db)

    # (4) Timeout does not survive reset
    @retry_with_timeout(default_timeout)
    def txn4(tr):
        tr.options.set_timeout(100)
        tr.reset()
        time.sleep(1)
        tr.commit().wait()  # should not throw

    txn4(db)

    # (5) Timeout having fired does not survive reset
    @retry_with_timeout(default_timeout)
    def txn5(tr):
        tr.options.set_timeout(100)
        time.sleep(1)
        try:
            tr.commit().wait()  # should throw
        except fdb.FDBError as e:
            if e.code != 1031:
                raise
        tr.reset()
        tr.commit().wait()  # should not throw

    txn5(db)

    # (6) Timeout will fire "retroactively"
    @retry_with_timeout(default_timeout)
    def txn6(tr):
        tr[b"foo"] = b"bar"
        time.sleep(1)
        tr.options.set_timeout(10)
        try:
            tr.commit().wait()  # should throw
            raise TestError("Timeout didn't fire.")
        except fdb.FDBError as e:
            if e.code != 1031:
                raise

    txn6(db)

    # (7) Transaction reset also resets time from which timeout is measured
    @retry_with_timeout(default_timeout)
    def txn7(tr):
        tr[b"foo"] = b"bar"
        time.sleep(1)
        start = time.time()
        tr.reset()
        tr[b"foo"] = b"bar"
        tr.options.set_timeout(500)
        try:
            tr.commit().wait()  # should not throw, but could if commit were slow:
        except fdb.FDBError as e:
            if e.code != 1031:
                raise
            if time.time() - start < 0.49:
                raise

    txn7(db)

    # (8) on_error() does not reset time from which timeout is measured
    @retry_with_timeout(default_timeout)
    def txn8(tr):
        tr[b"foo"] = b"bar"
        time.sleep(1)
        tr.on_error(fdb.FDBError(1007)).wait()  # should not throw
        tr[b"foo"] = b"bar"
        tr.options.set_timeout(100)
        try:
            tr.commit().wait()  # should throw
            raise TestError("Timeout didn't fire.")
        except fdb.FDBError as e:
            if e.code != 1031:
                raise

    txn8(db)

    # (9) Timeouts can be unset
    @retry_with_timeout(default_timeout)
    def txn9(tr):
        tr[b"foo"] = b"bar"
        tr.options.set_timeout(100)
        tr[b"foo"] = b"bar"
        tr.options.set_timeout(0)
        time.sleep(1)
        tr.commit().wait()  # should not throw

    txn9(db)

    # (10) Unsetting a timeout after it has fired doesn't help
    @retry_with_timeout(default_timeout)
    def txn10(tr):
        tr[b"foo"] = b"bar"
        tr.options.set_timeout(100)
        time.sleep(1)
        tr.options.set_timeout(0)
        try:
            tr.commit().wait()  # should throw
            raise TestError("Timeout didn't fire.")
        except fdb.FDBError as e:
            if e.code != 1031:
                raise

    txn10(db)

    # (11) An error thrown in commit does not reset time from which timeout is measured
    @retry_with_timeout(default_timeout)
    def txn11(tr):
        for i in range(2):
            tr.options.set_timeout(1500)
            tr.set_read_version(0x7FFFFFFFFFFFFFF0)
            _ = tr[b"foo"]
            try:
                tr.commit().wait()
                tr.reset()
            except fdb.FDBError as e:
                if i == 0:
                    if e.code != 1009:  # future_version
                        raise fdb.FDBError(
                            1007
                        )  # Something weird happened; raise a retryable error so we run this transaction again
                    else:
                        tr.on_error(e).wait()
                elif i == 1 and e.code != 1031:
                    raise

    txn11(db)


def test_db_timeouts(db):
    err = fdb.FDBError(1007)
    db.options.set_transaction_timeout(500)

    # (1) Basic timeout
    @retry_with_timeout(default_timeout)
    def txn1(tr):
        time.sleep(1)
        try:
            tr.commit().wait()  # should throw
            raise TestError("(1) Timeout didn't fire.")
        except fdb.FDBError as e:
            if e.code != 1031:
                raise

    txn1(db)

    # (2) Timeout after on_error
    @retry_with_timeout(default_timeout)
    def txn2(tr):
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw
        time.sleep(1)
        _ = tr[b"foo"]
        try:
            tr.commit().wait()  # should throw
            raise TestError("(2) Timeout didn't fire.")
        except fdb.FDBError as e:
            if e.code != 1031:
                raise

    txn2(db)

    # (3) Longer timeout than database timeout
    @retry_with_timeout(default_timeout)
    def txn3(tr):
        tr.options.set_timeout(1000)
        time.sleep(0.75)
        tr[b"foo"] = b"bar"
        tr.on_error(err).wait()  # should not throw
        _ = tr[b"foo"]
        time.sleep(0.75)
        try:
            tr.commit().wait()  # should throw
            raise TestError("(3) Timeout didn't fire.")
        except fdb.FDBError as e:
            if e.code != 1031:
                raise

    txn3(db)

    # (4) Shorter timeout than database timeout
    @retry_with_timeout(default_timeout)
    def txn4(tr):
        tr.options.set_timeout(100)
        tr[b"foo"] = b"bar"
        time.sleep(0.2)
        try:
            tr.commit().wait()  # should throw
            raise TestError("(4) Timeout didn't fire.")
        except fdb.FDBError as e:
            if e.code != 1031:
                raise

    txn4(db)

    # (5) Reset resets timeout to database timeout
    @retry_with_timeout(default_timeout)
    def txn5(tr):
        tr.options.set_timeout(100)
        tr[b"foo"] = b"bar"
        time.sleep(0.2)
        try:
            tr.commit().wait()  # should throw
            raise TestError("(5) Timeout didn't fire.")
        except fdb.FDBError as e:
            if e.code != 1031:
                raise
        tr.reset()
        tr[b"foo"] = b"bar"
        time.sleep(0.2)
        tr.on_error(err).wait()  # should not throw
        tr[b"foo"] = b"bar"
        time.sleep(0.8)
        try:
            tr.commit().wait()  # should throw
            raise TestError("(5) Timeout didn't fire.")
        except fdb.FDBError as e:
            if e.code != 1031:
                raise

    txn5(db)

    db.options.set_transaction_timeout(0)  # reset to default (no timeout)


def test_combinations(db):
    # (1) Cancellation does survive on_error() even when retry limit is hit
    @retry_with_timeout(default_timeout)
    def txn1(tr):
        tr.options.set_retry_limit(0)
        tr.cancel()
        try:
            tr.on_error(fdb.FDBError(1007)).wait()  # should throw
            raise TestError("on_error() did not notice cancellation.")
        except fdb.FDBError as e:
            if e.code != 1025:
                raise
        try:
            tr.commit().wait()  # should throw
            raise TestError("Cancellation did not survive on_error().")
        except fdb.FDBError as e:
            if e.code != 1025:
                raise

    txn1(db)
