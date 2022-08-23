import fdb
import json
import random
import string
from typing import Union, List

def to_str(s: Union[str, bytes]):
    if isinstance(s, bytes):
        s = s.decode("utf8")
    return s

def to_bytes(s: Union[str, bytes]):
    if isinstance(s, str):
        s = s.encode("utf8")
    return s

def random_alphanum_str(k: int):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=k))

def random_alphanum_bytes(k: int):
    return random_alphanum_str(k).encode("ascii")

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

def alg_from_kty(kty: str):
	if kty == "EC":
		return "ES256"
	else:
		return "RS256"

def public_keyset_from_keys(keys: List):
	keys = list(map(lambda key: key.as_dict(is_private=False, alg=alg_from_kty(key.kty)), keys))
	return json.dumps({ "keys": keys })
