#!/usr/bin/python
#
# authz_test.py
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
import admin_server
import argparse
import authlib
import fdb
import os
import pytest
import random
import sys
import time
from multiprocessing import Process, Pipe
from typing import Union
from util import alg_from_kty, public_keyset_from_keys, random_alphanum_str, random_alphanum_bytes, to_str, to_bytes, KeyFileReverter, token_claim_1h, wait_until_tenant_tr_succeeds, wait_until_tenant_tr_fails

special_key_ranges = [
    ("transaction description", b"/description", b"/description\x00"),
    ("global knobs", b"/globalKnobs", b"/globalKnobs\x00"),
    ("knobs", b"/knobs0", b"/knobs0\x00"),
    ("conflicting keys", b"/transaction/conflicting_keys/", b"/transaction/conflicting_keys/\xff\xff"),
    ("read conflict range", b"/transaction/read_conflict_range/", b"/transaction/read_conflict_range/\xff\xff"),
    ("conflicting keys", b"/transaction/write_conflict_range/", b"/transaction/write_conflict_range/\xff\xff"),
    ("data distribution stats", b"/metrics/data_distribution_stats/", b"/metrics/data_distribution_stats/\xff\xff"),
    ("kill storage", b"/globals/killStorage", b"/globals/killStorage\x00"),
]

def test_simple_tenant_access(private_key, token_gen, default_tenant, tenant_tr_gen):
    token = token_gen(private_key, token_claim_1h(default_tenant))
    tr = tenant_tr_gen(default_tenant)
    tr.options.set_authorization_token(token)
    tr[b"abc"] = b"def"
    tr.commit().wait()
    tr = tenant_tr_gen(default_tenant)
    tr.options.set_authorization_token(token)
    assert tr[b"abc"] == b"def", "tenant write transaction not visible"

def test_cross_tenant_access_disallowed(private_key, default_tenant, token_gen, tenant_gen, tenant_tr_gen):
    # use default tenant token with second tenant transaction and see it fail
    second_tenant = random_alphanum_bytes(12)
    tenant_gen(second_tenant)
    token_second = token_gen(private_key, token_claim_1h(second_tenant))
    tr_second = tenant_tr_gen(second_tenant)
    tr_second.options.set_authorization_token(token_second)
    tr_second[b"abc"] = b"def"
    tr_second.commit().wait()
    token_default = token_gen(private_key, token_claim_1h(default_tenant))
    tr_second = tenant_tr_gen(second_tenant)
    tr_second.options.set_authorization_token(token_default)
    # test that read transaction fails
    try:
        value = tr_second[b"abc"].value
        assert False, f"expected permission denied, but read transaction went through, value: {value}"
    except fdb.FDBError as e:
        assert e.code == 6000, f"expected permission_denied, got {e} instead"
    # test that write transaction fails
    tr_second = tenant_tr_gen(second_tenant)
    tr_second.options.set_authorization_token(token_default)
    try:
        tr_second[b"def"] = b"ghi"
        tr_second.commit().wait()
        assert False, "expected permission denied, but write transaction went through"
    except fdb.FDBError as e:
        assert e.code == 6000, f"expected permission_denied, got {e} instead"

def test_system_and_special_key_range_disallowed(db, tenant_tr_gen, token_gen):
    second_tenant = random_alphanum_bytes(12)
    try:
        fdb.tenant_management.create_tenant(db, second_tenant)
        assert False, "disallowed create_tenant has succeeded"
    except fdb.FDBError as e:
        assert e.code == 6000, f"expected permission_denied, got {e} instead"

    try:
        tr = db.create_transaction()
        tr.options.set_access_system_keys()
        kvs = tr.get_range(b"\xff", b"\xff\xff", limit=1).to_list()
        assert False, f"disallowed system keyspace read has succeeded. found item: {kvs}"
    except fdb.FDBError as e:
        assert e.code == 6000, f"expected permission_denied, got {e} instead"

    for range_name, special_range_begin, special_range_end in special_key_ranges:
        tr = db.create_transaction()
        tr.options.set_access_system_keys()
        tr.options.set_special_key_space_relaxed()
        try:
            kvs = tr.get_range(special_range_begin, special_range_end, limit=1).to_list()
            assert False, f"disallowed special keyspace read for range {range_name} has succeeded. found item {kvs}"
        except fdb.FDBError as e:
            assert e.code == 6000, f"expected permission_denied from attempted read to range {range_name}, got {e} instead"

    try:
        tr = db.create_transaction()
        tr.options.set_access_system_keys()
        del tr[b"\xff":b"\xff\xff"]
        tr.commit().wait()
        assert False, f"disallowed system keyspace write has succeeded"
    except fdb.FDBError as e:
        assert e.code == 6000, f"expected permission_denied, got {e} instead"

    for range_name, special_range_begin, special_range_end in special_key_ranges:
        tr = db.create_transaction()
        tr.options.set_access_system_keys()
        tr.options.set_special_key_space_relaxed()
        try:
            del tr[special_range_begin:special_range_end]
            tr.commit().wait()
            assert False, f"write to disallowed special keyspace range {range_name} has succeeded"
        except fdb.FDBError as e:
            assert e.code == 6000, f"expected permission_denied from attempted write to range {range_name}, got {e} instead"

    try:
        tr = db.create_transaction()
        tr.options.set_access_system_keys()
        kvs = tr.get_range(b"", b"\xff", limit=1).to_list()
        assert False, f"disallowed normal keyspace read has succeeded. found item {kvs}"
    except fdb.FDBError as e:
        assert e.code == 6000, f"expected permission_denied, got {e} instead"

def test_public_key_set_rollover(
        kty, private_key_gen, private_key, public_key_refresh_interval,
        cluster, default_tenant, token_gen, tenant_gen, tenant_tr_gen):
    new_kid = random_alphanum_str(12)
    new_kty = "EC" if kty == "RSA" else "RSA"
    new_key = private_key_gen(kty=new_kty, kid=new_kid)
    token_default = token_gen(private_key, token_claim_1h(default_tenant))

    second_tenant = random_alphanum_bytes(12)
    tenant_gen(second_tenant)
    token_second = token_gen(new_key, token_claim_1h(second_tenant))

    interim_set = public_keyset_from_keys([new_key, private_key])
    max_repeat = 10

    print(f"interim keyset: {interim_set}")
    old_key_json = None
    with open(cluster.public_key_json_file, "r") as keyfile:
        old_key_json = keyfile.read()

    delay = public_key_refresh_interval

    with KeyFileReverter(cluster.public_key_json_file, old_key_json, delay):
        with open(cluster.public_key_json_file, "w") as keyfile:
            keyfile.write(interim_set)
        wait_until_tenant_tr_succeeds(second_tenant, new_key, tenant_tr_gen, token_gen, max_repeat, delay)
        print("interim key set activated")
        final_set = public_keyset_from_keys([new_key])
        print(f"final keyset: {final_set}")
        with open(cluster.public_key_json_file, "w") as keyfile:
            keyfile.write(final_set)
        wait_until_tenant_tr_fails(default_tenant, private_key, tenant_tr_gen, token_gen, max_repeat, delay)

def test_public_key_set_broken_file_tolerance(
        private_key, public_key_refresh_interval,
        cluster, public_key_jwks_str, default_tenant, token_gen, tenant_tr_gen):
    delay = public_key_refresh_interval
    # retry limit in waiting for keyset file update to propagate to FDB server's internal keyset
    max_repeat = 10

    with KeyFileReverter(cluster.public_key_json_file, public_key_jwks_str, delay):
        # key file update should take effect even after witnessing broken key file
        with open(cluster.public_key_json_file, "w") as keyfile:
            keyfile.write(public_key_jwks_str.strip()[:10]) # make the file partial, injecting parse error
        time.sleep(delay * 2)
        # should still work; internal key set only clears with a valid, empty key set file
        tr_default = tenant_tr_gen(default_tenant)
        tr_default.options.set_authorization_token(token_gen(private_key, token_claim_1h(default_tenant)))
        tr_default[b"abc"] = b"def"
        tr_default.commit().wait()
        with open(cluster.public_key_json_file, "w") as keyfile:
            keyfile.write('{"keys":[]}')
        # eventually internal key set will become empty and won't accept any new tokens
        wait_until_tenant_tr_fails(default_tenant, private_key, tenant_tr_gen, token_gen, max_repeat, delay)

def test_public_key_set_deletion_tolerance(
        private_key, public_key_refresh_interval,
        cluster, public_key_jwks_str, default_tenant, token_gen, tenant_tr_gen):
    delay = public_key_refresh_interval
    # retry limit in waiting for keyset file update to propagate to FDB server's internal keyset
    max_repeat = 10

    with KeyFileReverter(cluster.public_key_json_file, public_key_jwks_str, delay):
        # key file update should take effect even after witnessing deletion of key file
        with open(cluster.public_key_json_file, "w") as keyfile:
            keyfile.write('{"keys":[]}')
        time.sleep(delay)
        wait_until_tenant_tr_fails(default_tenant, private_key, tenant_tr_gen, token_gen, max_repeat, delay)
        os.remove(cluster.public_key_json_file)
        time.sleep(delay * 2)
        with open(cluster.public_key_json_file, "w") as keyfile:
            keyfile.write(public_key_jwks_str)
        # eventually updated key set should take effect and transaction should be accepted
        wait_until_tenant_tr_succeeds(default_tenant, private_key, tenant_tr_gen, token_gen, max_repeat, delay)

def test_public_key_set_empty_file_tolerance(
        private_key, public_key_refresh_interval,
        cluster, public_key_jwks_str, default_tenant, token_gen, tenant_tr_gen):
    delay = public_key_refresh_interval
    # retry limit in waiting for keyset file update to propagate to FDB server's internal keyset
    max_repeat = 10

    with KeyFileReverter(cluster.public_key_json_file, public_key_jwks_str, delay):
        # key file update should take effect even after witnessing an empty file
        with open(cluster.public_key_json_file, "w") as keyfile:
            keyfile.write('{"keys":[]}')
        # eventually internal key set will become empty and won't accept any new tokens
        wait_until_tenant_tr_fails(default_tenant, private_key, tenant_tr_gen, token_gen, max_repeat, delay)
        # empty the key file
        with open(cluster.public_key_json_file, "w") as keyfile:
            pass
        time.sleep(delay * 2)
        with open(cluster.public_key_json_file, "w") as keyfile:
            keyfile.write(public_key_jwks_str)
        # eventually key file should update and transactions should go through
        wait_until_tenant_tr_succeeds(default_tenant, private_key, tenant_tr_gen, token_gen, max_repeat, delay)

def test_bad_token(private_key, token_gen, default_tenant, tenant_tr_gen):
    def del_attr(d, attr):
        del d[attr]
        return d

    def set_attr(d, attr, value):
        d[attr] = value
        return d

    claim_mutations = [
        ("no nbf", lambda claim: del_attr(claim, "nbf")),
        ("no exp", lambda claim: del_attr(claim, "exp")),
        ("no iat", lambda claim: del_attr(claim, "iat")),
        ("too early", lambda claim: set_attr(claim, "nbf", time.time() + 30)),
        ("too late", lambda claim: set_attr(claim, "exp", time.time() - 10)),
        ("no tenants", lambda claim: del_attr(claim, "tenants")),
        ("empty tenants", lambda claim: set_attr(claim, "tenants", [])),
    ]
    for case_name, mutation in claim_mutations:
        tr = tenant_tr_gen(default_tenant)
        tr.options.set_authorization_token(token_gen(private_key, mutation(token_claim_1h(default_tenant))))
        try:
            value = tr[b"abc"].value
            assert False, f"expected permission_denied for case {case_name}, but read transaction went through"
        except fdb.FDBError as e:
            assert e.code == 6000, f"expected permission_denied for case {case_name}, got {e} instead"
        tr = tenant_tr_gen(default_tenant)
        tr.options.set_authorization_token(token_gen(private_key, mutation(token_claim_1h(default_tenant))))
        tr[b"abc"] = b"def"
        try:
            tr.commit().wait()
            assert False, f"expected permission_denied for case {case_name}, but write transaction went through"
        except fdb.FDBError as e:
            assert e.code == 6000, f"expected permission_denied for case {case_name}, got {e} instead"

    # unknown key case: override "kid" field in header
    # first, update only the kid field of key with export-update-import
    key_dict = private_key.as_dict(is_private=True)
    key_dict["kid"] = random_alphanum_str(10)
    renamed_key = authlib.jose.JsonWebKey.import_key(key_dict)
    unknown_key_token = token_gen(
                renamed_key,
                token_claim_1h(default_tenant),
                headers={
                    "typ": "JWT",
                    "kty": renamed_key.kty,
                    "alg": alg_from_kty(renamed_key.kty),
                    "kid": renamed_key.kid,
                })
    tr = tenant_tr_gen(default_tenant)
    tr.options.set_authorization_token(unknown_key_token)
    try:
        value = tr[b"abc"].value
        assert False, f"expected permission_denied for 'unknown key' case, but read transaction went through"
    except fdb.FDBError as e:
        assert e.code == 6000, f"expected permission_denied for 'unknown key' case, got {e} instead"
    tr = tenant_tr_gen(default_tenant)
    tr.options.set_authorization_token(unknown_key_token)
    tr[b"abc"] = b"def"
    try:
        tr.commit().wait()
        assert False, f"expected permission_denied for 'unknown key' case, but write transaction went through"
    except fdb.FDBError as e:
        assert e.code == 6000, f"expected permission_denied for 'unknown key' case, got {e} instead"
