#!/usr/bin/env python3
#
# transaction_profiling_analyzer_audit_tests.py
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

import json
import os
import re
import tempfile
import unittest
from types import SimpleNamespace as Obj
from unittest import mock

from transaction_profiling_analyzer import (
    AUDIT_LOG_FILE_NAME,
    AuditLog,
    KeyRange,
    Mutation,
    audit_log_path,
    cluster_description,
    local_ip,
    printable,
)


TRACE, AUDIT = "FDB_NETWORK_OPTION_TRACE_ENABLE", "FDB_TRANSACTION_PROFILER_AUDIT_LOG_ENABLE"


def read_log(path):
    with open(path) as f:
        return [json.loads(line) for line in f]


class AuditLogTest(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(tempfile.mkdtemp(), "audit.json")

    def test_printable(self):
        self.assertEqual(printable(b"user_a"), "user_a")
        self.assertEqual(printable(b"\xff\x02/a\\b\x00"), "\\xff\\x02/a\\\\b\\x00")

    def test_audit_log_path(self):
        # --audit-log, --audit-log-dir and FDB_TRANSACTION_PROFILER_AUDIT_LOG_ENABLE=<dir>, like fdbcli. The directory
        # is the env var value if it's set, else --audit-log-dir, and if that's empty, the trace directory, else the
        # current directory.
        trace, audit, audit2 = (tempfile.mkdtemp() for _ in range(3))
        cases = [
            # --audit-log, --audit-log-dir, env, directory (None for off)
            (False, None, {}, None),
            (False, None, {TRACE: trace}, None),
            (True, None, {}, "."),
            (True, None, {TRACE: ""}, "."),
            (True, None, {TRACE: trace}, trace),
            (False, None, {AUDIT: ""}, "."),
            (False, None, {AUDIT: "", TRACE: trace}, trace),
            (False, None, {AUDIT: audit}, audit),
            (False, None, {AUDIT: audit, TRACE: trace}, audit),
            (True, None, {AUDIT: audit}, audit),
            (True, audit, {}, audit),
            (True, audit, {TRACE: trace}, audit),
            # the env var wins over --audit-log-dir, even when empty, like FDB_NETWORK_OPTION_TRACE_ENABLE over
            # fdbcli's --log-dir
            (True, audit, {AUDIT: audit2}, audit2),
            (False, audit, {AUDIT: audit2}, audit2),
            (False, audit, {AUDIT: ""}, "."),
            (False, audit, {AUDIT: "", TRACE: trace}, trace),
        ]
        for audit_log, audit_log_dir, env, expected in cases:
            with mock.patch.dict(os.environ, env, clear=True):
                path = audit_log_path(audit_log, audit_log_dir)
            case = (audit_log, audit_log_dir, env)
            self.assertEqual(path, expected and os.path.join(expected, AUDIT_LOG_FILE_NAME), case)

    def test_audit_log_path_errors(self):
        log_dir, missing = os.path.dirname(self.path), os.path.join(os.path.dirname(self.path), "missing")
        open(self.path, "w").close()
        # unlike --log-dir without --log in fdbcli, so reads don't go unaudited
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(SystemExit, "--audit-log-dir requires audit logging to be enabled"):
                audit_log_path(False, log_dir)
        bad = [
            (True, missing, {}, missing),
            (True, self.path, {}, self.path),  # not a directory
            (False, None, {AUDIT: missing}, missing),
            (False, None, {AUDIT: "1"}, "1"),  # the value is a directory, not a flag
            (True, None, {TRACE: missing}, missing),  # the trace directory it falls back to is checked too
        ]
        if os.geteuid() != 0:  # root can write to any directory
            read_only = tempfile.mkdtemp()
            os.chmod(read_only, 0o555)
            bad.append((True, read_only, {}, read_only))
        for audit_log, audit_log_dir, env, d in bad:
            with mock.patch.dict(os.environ, env, clear=True):
                message = "audit log directory `%s' does not exist or is not writable" % d
                with self.assertRaisesRegex(SystemExit, re.escape(message)):
                    audit_log_path(audit_log, audit_log_dir)

    def test_cluster_description(self):
        with open(self.path, "w") as f:
            f.write("# DO NOT EDIT!\nmycluster:abc@127.0.0.1:4500\n")
        self.assertEqual(cluster_description(self.path), "mycluster")

    def test_local_ip(self):
        for coordinators, expected in [
            ("127.0.0.1:4500", {"127.0.0.1"}),
            ("127.0.0.1:4500:tls", {"127.0.0.1"}),
            ("[::1]:4500", {"::1"}),
            ("localhost:4500", {"127.0.0.1", "::1"}),
            ("bad,127.0.0.1:4500", {"127.0.0.1"}),  # unusable coordinators are skipped
        ]:
            with open(self.path, "w") as f:
                f.write("c:id@%s\n" % coordinators)
            self.assertIn(local_ip(self.path), expected, coordinators)

    def test_log_appends_one_json_line_per_entry(self):
        AuditLog(self.path, "c1", "10.0.0.1").log("r1", "top_writes", {"top_requests": 1}, key=b"a", count=3)
        AuditLog(self.path, "c1").log("r2", "top_writes", {"top_requests": 1}, key=b"b", count=1)
        records = read_log(self.path)
        self.assertEqual([r["key"] for r in records], ["a", "b"])
        self.assertEqual(
            sorted(records[0]),
            ["cluster", "count", "input", "ip_address", "key", "level", "msg", "operation", "reqid", "ts"],
        )
        self.assertEqual(records[0]["cluster"], "c1")
        self.assertEqual(records[0]["ip_address"], "10.0.0.1")
        self.assertRegex(records[0]["ts"], r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$")

    def test_log_transaction(self):
        get = Obj(start_timestamp=1.0, latency=0.1, value_size=1, key=b"a", tenant=b"t")
        get_range = Obj(start_timestamp=1.0, latency=0.1, range_size=2, key_range=KeyRange(b"a", b"c"))
        commit = Obj(
            start_timestamp=1.0,
            num_mutations=2,
            mutations=[Mutation(0, b"a", b"1"), Mutation(1, b"b", b"c")],
            read_conflict_range=[KeyRange(b"a", b"a\x00")],
            write_conflict_range=[KeyRange(b"a", b"a\x00"), KeyRange(b"b", b"c")],
        )
        info = Obj(gets=[get], get_ranges=[get_range], commit=commit, error_gets=[], error_get_ranges=[], error_commits=[])
        AuditLog(self.path, "c1").log_transaction(info)
        records = read_log(self.path)

        summary = [(r["operation"], r["key"], r.get("end_key"), r.get("value")) for r in records]
        self.assertEqual(
            summary,
            [
                ("get", "a", None, None),
                ("get_range", "a", "c", None),
                ("commit", "a", None, "1"),
                ("commit", "b", None, "c"),
                ("commit", "a", "a\\x00", None),
                ("commit", "a", "a\\x00", None),
                ("commit", "b", "c", None),
            ],
        )
        # one input op -> one reqid for all of its outputs
        self.assertEqual(len({r["reqid"] for r in records}), 3)
        self.assertEqual(len({r["reqid"] for r in records[2:]}), 1)
        self.assertEqual(records[0]["input"], {"start_timestamp": 1.0, "latency": 0.1, "value_size": 1})
        self.assertEqual(records[3]["mutation_type"], "CLEAR_RANGE")
        self.assertEqual([r.get("conflict_range") for r in records[4:]],
                         ["read_conflict_range", "write_conflict_range", "write_conflict_range"])


if __name__ == "__main__":
    unittest.main()
