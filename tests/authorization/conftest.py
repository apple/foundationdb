import pytest
import fdb
import random
import string
from authlib.jose import JsonWebKey, KeySet, jwt
from local_cluster import TLSConfig
from tmp_cluster import TempCluster

fdb.api_version(720)

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

def cleanup_tenant(db, tenant_name):
    try:
        tenant = db.open_tenant(tenant_name)
        del tenant[:]
        fdb.tenant_management.delete_tenant(db, tenant_name)
    except fdb.FDBError as e:
        if e.code == 2131: # tenant not found
            pass
        else:
            raise

@pytest.fixture
def build_dir(request):
    return request.config.option.build_dir

@pytest.fixture
def kty(request):
    return request.config.option.kty

@pytest.fixture
def trusted_client(request):
    return request.config.option.trusted_client

@pytest.fixture
def alg(kty):
    if kty == "EC":
        return "ES256"
    else:
        return "RS256"

@pytest.fixture
def kid():
    return random_alphanum_str(12)

@pytest.fixture
def private_key(kty, kid):
    if kty == "EC":
        return JsonWebKey.generate_key(kty=kty, crv_or_size="P-256", is_private=True, options={"kid": kid})
    else:
        return JsonWebKey.generate_key(kty=kty, crv_or_size=4096, is_private=True, options={"kid": kid})

@pytest.fixture
def public_key_jwks_str(private_key, kid, alg):
    return KeySet([private_key]).as_json(
            is_private=False,
            alg=alg,
        )

@pytest.fixture
def token_gen(private_key, kid, alg):
    def fn(claims, headers={}):
        if not headers:
            headers = { "typ": "JWT", "kty": private_key.kty, "alg": alg, "kid": kid }
        return jwt.encode(headers, claims, private_key)
    return fn

@pytest.fixture(autouse=True)
def cluster(build_dir, public_key_jwks_str, trusted_client):
    with TempCluster(
            build_dir=build_dir,
            tls_config=TLSConfig(server_chain_len=3, client_chain_len=2),
            public_key_json_str=public_key_jwks_str,
            remove_at_exit=False,
            custom_config={"code-probes": "all"}) as cluster:
        fdb.options.set_tls_key_path(str(cluster.client_key_file) if trusted_client else "")
        fdb.options.set_tls_cert_path(str(cluster.client_cert_file) if trusted_client else "")
        fdb.options.set_tls_ca_path(str(cluster.server_ca_file))
        fdb.options.set_trace_enable()
        yield cluster

@pytest.fixture
def db(cluster):
    db = fdb.open(str(cluster.cluster_file))
    db.options.set_transaction_timeout(2000) # 2 seconds
    db.options.set_transaction_retry_limit(3)
    yield db
    tenants = fdb.tenant_management.list_tenants(db, b'', b'\xff', 100)
    print("Cleaning up tenants: {}".format(tenants))
    for tenant in tenants:
        cleanup_tenant(db, tenant)

@pytest.fixture
def default_tenant(db):
    tenant = random_alphanum_str(8).encode("ascii")
    yield tenant
    cleanup_tenant(db, tenant)

@pytest.fixture
def default_tenant_tr_gen(db, default_tenant):
    def fn():
        tenant = db.open_tenant(default_tenant)
        return tenant.create_transaction()
    return fn
