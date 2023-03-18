from authlib.jose import JsonWebKey, KeySet, jwt
from typing import List
import json
import time

def private_key_gen(kty: str, kid: str):
    assert kty == "EC" or kty == "RSA"
    if kty == "EC":
        return JsonWebKey.generate_key(kty=kty, crv_or_size="P-256", is_private=True, options={"kid": kid})
    else:
        return JsonWebKey.generate_key(kty=kty, crv_or_size=4096, is_private=True, options={"kid": kid})

def public_keyset_from_keys(keys: List):
    keys = list(map(lambda key: key.as_dict(is_private=False, alg=alg_from_kty(key.kty)), keys))
    return json.dumps({ "keys": keys })

def alg_from_kty(kty: str):
    assert kty == "EC" or kty == "RSA"
    if kty == "EC":
        return "ES256"
    else:
        return "RS256"

def token_gen(private_key, claims, headers={}):
    if not headers:
        headers = {
            "typ": "JWT",
            "kty": private_key.kty,
            "alg": alg_from_kty(private_key.kty),
            "kid": private_key.kid,
        }
    return jwt.encode(headers, claims, private_key)

def token_claim_1h(tenant_id: int):
    # JWT claim that is valid for 1 hour since time of invocation
    now = time.time()
    return {
        "iss": "fdb-authz-tester",
        "sub": "authz-test",
        "aud": ["tmp-cluster"],
        "iat": now,
        "nbf": now - 1,
        "exp": now + 60 * 60,
        "jti": random_alphanum_str(10),
        "tenants": [to_str(base64.b64encode(tenant_id.to_bytes(8, "big")))],
    }
