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
import pytest
import sys
import time
from multiprocessing import Process, Pipe
from typing import Union
from util import public_keyset_from_keys, random_alphanum_str, random_alphanum_bytes, to_str, to_bytes

def token_claim_1h(tenant_name):
    now = time.time()
    return {
        "iss": "fdb-authz-tester",
        "sub": "authz-test",
        "aud": ["tmp-cluster"],
        "iat": now,
        "nbf": now - 1,
        "exp": now + 60 * 60, 
        "jti": random_alphanum_str(10),
        "tenants": [to_str(tenant_name)],
    }

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
        assert False, "expected permission denied, but read transaction went through, value: {}".format(value)
    except fdb.FDBError as e:
        assert e.code == 6000, "expected permission_denied, got {} instead".format(e)
    # test that write transaction fails
    tr_second = tenant_tr_gen(second_tenant)
    tr_second.options.set_authorization_token(token_default)
    try:
        tr_second[b"def"] = b"ghi"
        tr_second.commit().wait()
        assert False, "expected permission denied, but write transaction went through"
    except fdb.FDBError as e:
        assert e.code == 6000, "expected permission_denied, got {} instead".format(e)

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

    print("interim key set: {}".format(interim_set))
    old_key_json = None
    with open(cluster.public_key_json_file, "r") as keyfile:
        old_key_json = keyfile.read()

    delay = public_key_refresh_interval * 3

    try:
        with open(cluster.public_key_json_file, "w") as keyfile:
            keyfile.write(interim_set)
        print("sleeping {} seconds for interim key file to take effect".format(delay))
        time.sleep(delay)

        tr_default = tenant_tr_gen(default_tenant)
        tr_default.options.set_authorization_token(token_default)
        tr_default[b"abc"] = b"def"
        tr_default.commit().wait()

        tr_second = tenant_tr_gen(second_tenant)
        tr_second.options.set_authorization_token(token_second)
        tr_second[b"ghi"] = b"jkl"
        tr_second.commit().wait()

        final_set = public_keyset_from_keys([new_key])
        print("final key set: {}".format(final_set))
        with open(cluster.public_key_json_file, "w") as keyfile:
            keyfile.write(final_set)

        print("sleeping {} seconds for final key file to take effect".format(delay))
        time.sleep(delay)

        # token_default is still cached and with active TTL. transaction should work
        tr_default = tenant_tr_gen(default_tenant)
        tr_default.options.set_authorization_token(token_default)
        value = tr_default[b"abc"].value

        # Generate and use a new token for default tenant
        # As this token is not cached, it should fail
        tr_default = tenant_tr_gen(default_tenant)
        tr_default.options.set_authorization_token(token_gen(private_key, token_claim_1h(default_tenant)))
        try:
            value = tr_default[b"abc"].value
            assert False, "expected permission_denied, but read transaction went through"
        except fdb.FDBError as ferr:
            assert ferr.code == 6000, "expected permission_denied, got {} instead".format(ferr)
        try:
            tr_default[b"abc"] = b"ghi"
            tr_default.commit().wait()
        except fdb.FDBError as ferr:
            assert ferr.code == 6000, "expected permission_denied, got {} instead".format(ferr)

        tr_second = tenant_tr_gen(second_tenant)
        tr_second.options.set_authorization_token(token_second)
        assert tr_second[b"ghi"] == b"jkl", "earlier write transaction for tenant {} not visible".format(second_tenant)
    except Exception as e:
        print("unexpected exception {}, reverting key file to default".format(e))
        raise e
    finally:
        with open(cluster.public_key_json_file, "w") as keyfile:
            keyfile.write(old_key_json)
        print("key file reverted. waiting {} seconds for the update to take effect...".format(delay))
        time.sleep(delay)

# shouldn't introduce admin server subprocess as fixture
# because it would clone pytest framework context initialized up to that point as well
# instead, we fork at the beginning and explicitly pass in test files as cli args
if __name__ == "__main__":
    fdb.api_version(720)
    rc = 0
    with admin_server.Server() as server:
        rc = pytest.main(["conftest.py"] + sys.argv) # argv would include this test file and CLI args
    sys.exit(rc)
