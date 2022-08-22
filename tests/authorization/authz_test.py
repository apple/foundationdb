import authlib
import argparse
import fdb
import pytest
import time
import sys
import fdb
from multiprocessing import Process, Pipe
from typing import Union
from util import random_alphanum_str, random_alphanum_bytes, to_str, to_bytes
from admin_server import AdminServer

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

def test_simple_tenant_access(token_gen, default_tenant, tenant_tr_gen):
    token = token_gen(token_claim_1h(default_tenant))
    tr = tenant_tr_gen(default_tenant)
    tr.options.set_authorization_token(token)
    tr[b"abc"] = b"def"
    tr.commit().wait()
    tr = tenant_tr_gen(default_tenant)
    tr.options.set_authorization_token(token)
    assert tr[b"abc"] == b"def", "tenant write transaction not visible"

def test_cross_tenant_access_disallowed(default_tenant, token_gen, tenant_gen, tenant_tr_gen):
    # use default tenant token with second tenant transaction and see it fail
    second_tenant = random_alphanum_bytes(12)
    tenant_gen(second_tenant)
    token_second = token_gen(token_claim_1h(second_tenant))
    tr_second = tenant_tr_gen(second_tenant)
    tr_second.options.set_authorization_token(token_second)
    tr_second[b"abc"] = b"def"
    tr_second.commit().wait()
    token_default = token_gen(token_claim_1h(default_tenant))
    tr_second = tenant_tr_gen(second_tenant)
    tr_second.options.set_authorization_token(token_default)
    # test that read transaction fails
    try:
        value = tr_second[b"abc"].value()
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

if __name__ == "__main__":
    fdb.api_version(720)
    with AdminServer() as server:
        sys.exit(pytest.main(args=sys.argv[1:]))
