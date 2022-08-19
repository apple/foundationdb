import pytest
import fdb
import random
import string
import subprocess
from authlib.jose import JsonWebKey, KeySet, jwt
from local_cluster import TLSConfig
from tmp_cluster import TempCluster
from typing import Union

fdb.api_version(720)
cluster_scope = "module"

def to_str(s: Union[str, bytes]):
    if isinstance(s, bytes):
        s = s.decode("utf8")
    return s

def to_bytes(s: Union[str, bytes]):
    if isinstance(s, str):
        s = s.encode("utf8")
    return s

def pytest_addoption(parser):
    parser.addoption(
            "--build-dir", action="store", dest="build_dir", help="FDB build directory", required=True)
    parser.addoption(
            "--kty", action="store", choices=["EC", "RSA"], default="EC", dest="kty", help="Token signature algorithm")
    parser.addoption(
            "--trusted-client",
            action="store_true",
            default=False,
            dest="trusted_client",
            help="Whether client shall be configured trusted, i.e. mTLS-ready")

def random_alphanum_str(k: int):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=k))

def random_alphanum_bytes(k: int):
    return random_alphanum_str(k).encode("ascii")

@pytest.fixture(scope="session")
def build_dir(request):
    return request.config.option.build_dir

@pytest.fixture(scope="session")
def kty(request):
    return request.config.option.kty

@pytest.fixture(scope="session")
def trusted_client(request):
    return request.config.option.trusted_client

@pytest.fixture(scope="session")
def alg(kty):
    if kty == "EC":
        return "ES256"
    else:
        return "RS256"

@pytest.fixture(scope="session")
def kid():
    return random_alphanum_str(12)

@pytest.fixture(scope="session")
def private_key_gen():
    def fn(kty: str, kid: str):
        if kty == "EC":
            return JsonWebKey.generate_key(kty=kty, crv_or_size="P-256", is_private=True, options={"kid": kid})
        else:
            return JsonWebKey.generate_key(kty=kty, crv_or_size=4096, is_private=True, options={"kid": kid})
    return fn

@pytest.fixture(scope="session")
def private_key(kty, kid, private_key_gen):
    return private_key_gen(kty, kid)

@pytest.fixture(scope="session")
def public_key_jwks_str(private_key, kid, alg):
    return KeySet([private_key]).as_json(
            is_private=False,
            alg=alg,
        )

@pytest.fixture(scope="session")
def token_gen(private_key, kid, alg):
    def fn(claims, headers={}):
        if not headers:
            headers = { "typ": "JWT", "kty": private_key.kty, "alg": alg, "kid": kid }
        return jwt.encode(headers, claims, private_key)
    return fn

@pytest.fixture(autouse=True, scope=cluster_scope)
def cluster(build_dir, public_key_jwks_str, trusted_client):
    with TempCluster(
            build_dir=build_dir,
            tls_config=TLSConfig(server_chain_len=3, client_chain_len=2),
            public_key_json_str=public_key_jwks_str,
            remove_at_exit=True,
            custom_config={"code-probes": "all"}) as cluster:
        fdb.options.set_tls_key_path(str(cluster.client_key_file) if trusted_client else "")
        fdb.options.set_tls_cert_path(str(cluster.client_cert_file) if trusted_client else "")
        fdb.options.set_tls_ca_path(str(cluster.server_ca_file))
        fdb.options.set_trace_enable()
        yield cluster

@pytest.fixture(scope=cluster_scope)
def db(cluster):
    db = fdb.open(str(cluster.cluster_file))
    db.options.set_transaction_timeout(2000) # 2 seconds
    db.options.set_transaction_retry_limit(3)
    return db

@pytest.fixture(scope=cluster_scope)
def tenant_gen(cluster):
    def fn(tenant):
        cluster.fdbcli_exec("createtenant {}".format(to_str(tenant)))
    return fn

@pytest.fixture(scope=cluster_scope)
def tenant_del(cluster):
    def fn(tenant):
        tenant = to_str(tenant)
        cluster.fdbcli_exec("writemode on;usetenant {};clearrange \\x00 \\xff;defaulttenant;deletetenant {}".format(tenant, tenant))
    return fn

@pytest.fixture(scope=cluster_scope)
def default_tenant(tenant_gen, tenant_del):
    tenant = random_alphanum_bytes(8)
    tenant_gen(tenant)
    yield tenant
    tenant_del(tenant)

@pytest.fixture(scope=cluster_scope)
def tenant_tr_gen(db):
    def fn(tenant):
        tenant = db.open_tenant(to_bytes(tenant))
        return tenant.create_transaction()
    return fn
