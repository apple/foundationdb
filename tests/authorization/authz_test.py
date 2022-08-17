import authlib
import argparse
import fdb
import pytest
import time
from conftest import random_alphanum_str

fdb.api_version(720)

def token_claim_1h(tenant_name: str):
    now = time.time()
    return {
        "iss": "fdb-authz-tester",
        "sub": "authz-test",
        "aud": ["tmp-cluster"],
        "iat": now,
        "nbf": now - 1,
        "exp": now + 60 * 60, 
        "jti": random_alphanum_str(10),
        "tenants": [tenant_name],
    }

def test_simple_tenant_access(db, token_gen, default_tenant, default_tenant_tr_gen):
    token = token_gen(token_claim_1h(str(default_tenant)))
    tr = default_tenant_tr_gen()
    tr.options.set_authorization_token(token)
    tr[b'abc'] = b'def'
    tr.commit().wait()
    tr = default_tenant_tr_gen()
    tr.options.set_authorization_token(token)
    assert tr[b'abc'] == b'def'
