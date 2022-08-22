import random
import string
import fdb

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


