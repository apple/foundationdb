import base64
import fdb
import random
import string
import time
from typing import Union
from authz_util import token_gen


def to_str(s: Union[str, bytes]):
    if isinstance(s, bytes):
        s = s.decode("utf8")
    return s


def to_bytes(s: Union[str, bytes]):
    if isinstance(s, str):
        s = s.encode("utf8")
    return s


def random_alphanum_str(k: int):
    return "".join(random.choices(string.ascii_letters + string.digits, k=k))


def random_alphanum_bytes(k: int):
    return random_alphanum_str(k).encode("ascii")


def cleanup_tenant(db, tenant_name):
    try:
        tenant = db.open_tenant(tenant_name)
        del tenant[:]
        fdb.tenant_management.delete_tenant(db, tenant_name)
    except fdb.FDBError as e:
        if e.code == 2131:  # tenant not found
            pass
        else:
            raise


class KeyFileReverter(object):
    def __init__(self, filename: str, content: str, refresh_delay: int):
        self.filename = filename
        self.content = content
        self.refresh_delay = refresh_delay

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        with open(self.filename, "w") as keyfile:
            keyfile.write(self.content)
        print(
            f"key file reverted. waiting {self.refresh_delay * 2} seconds for the update to take effect..."
        )
        time.sleep(self.refresh_delay * 2)


# repeat try-wait loop up to max_repeat times until both read and write tr fails for tenant with permission_denied
# important: only use this function if you don't have any data dependencies to key "abc"
def wait_until_tenant_tr_fails(
    tenant, private_key, tenant_tr_gen, max_repeat, delay, token_claim_1h
):
    repeat = 0
    read_blocked = False
    write_blocked = False
    while (not read_blocked or not write_blocked) and repeat < max_repeat:
        time.sleep(delay)
        tr = tenant_tr_gen(tenant)
        # a token needs to be generated at every iteration because once it is accepted/cached,
        # it will pass verification by caching until it expires
        tr.options.set_authorization_token(
            token_gen(private_key, token_claim_1h(tenant))
        )
        try:
            if not read_blocked:
                tr[b"abc"].wait()
        except fdb.FDBError as e:
            assert e.code == 6000, f"expected permission_denied, got {e} instead"
            read_blocked = True
        if not read_blocked:
            repeat += 1
            continue

        try:
            if not write_blocked:
                tr[b"abc"] = b"def"
                tr.commit().wait()
        except fdb.FDBError as e:
            assert e.code == 6000, f"expected permission_denied, got {e} instead"
            write_blocked = True
        if not write_blocked:
            repeat += 1
    assert (
        repeat < max_repeat
    ), f"tenant transaction did not start to fail in {max_repeat * delay} seconds"


# repeat try-wait loop up to max_repeat times until both read and write tr succeeds for tenant
# important: only use this function if you don't have any data dependencies to key "abc"
def wait_until_tenant_tr_succeeds(
    tenant, private_key, tenant_tr_gen, max_repeat, delay, token_claim_1h
):
    repeat = 0
    token = token_gen(private_key, token_claim_1h(tenant))
    while repeat < max_repeat:
        try:
            time.sleep(delay)
            tr = tenant_tr_gen(tenant)
            tr.options.set_authorization_token(token)
            tr[b"abc"].wait()
            tr[b"abc"] = b"qwe"
            tr.commit().wait()
            break
        except fdb.FDBError as e:
            assert e.code == 6000, f"expected permission_denied, got {e} instead"
            repeat += 1
    assert (
        repeat < max_repeat
    ), f"tenant transaction did not start to succeed in {max_repeat * delay} seconds"
