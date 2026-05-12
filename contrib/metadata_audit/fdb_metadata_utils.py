#!/usr/bin/env python3
"""
Shared utilities for FDB metadata manipulation scripts.

Provides:
- MoveKeysLock management (required for writing to serverKeys/keyServers)
- Common key prefixes
- Helper functions
"""

import fdb
import uuid

# Try API versions from newest to oldest
_api_version_set = False
for _api_version in [730, 720, 710, 700]:
    try:
        fdb.api_version(_api_version)
        _api_version_set = True
        break
    except:
        continue

if not _api_version_set:
    raise RuntimeError("Could not set any FDB API version")

# Key prefixes
SERVER_KEYS_PREFIX = b'\xff/serverKeys/'
SERVER_LIST_PREFIX = b'\xff/serverList/'
KEY_SERVERS_PREFIX = b'\xff/keyServers/'

# MoveKeysLock keys - required for writing to serverKeys/keyServers
DD_MODE_KEY = b'\xff/dataDistributionMode'
MOVEKEYS_LOCK_OWNER_KEY = b'\xff/moveKeysLock/Owner'
MOVEKEYS_LOCK_WRITE_KEY = b'\xff/moveKeysLock/Write'

# serverKeys values
SERVER_KEYS_TRUE = b'\x01'
SERVER_KEYS_FALSE = b'\x00'


def strinc(key):
    """Return the first key greater than the given key that doesn't have key as a prefix."""
    if not key:
        return b'\x00'
    for i in range(len(key) - 1, -1, -1):
        if key[i] != 0xff:
            return key[:i] + bytes([key[i] + 1])
    return key + b'\x00'


# Precomputed end keys
SERVER_KEYS_END = strinc(SERVER_KEYS_PREFIX)
SERVER_LIST_END = strinc(SERVER_LIST_PREFIX)
KEY_SERVERS_END = strinc(KEY_SERVERS_PREFIX)


def take_movekeys_lock(db):
    """
    Disable DD and take ownership of MoveKeysLock.

    Required before writing to serverKeys/keyServers. This is exactly what
    FDB's internal code does (see MoveKeys.actor.cpp).

    Returns our owner UID.
    """
    our_owner_uid = uuid.uuid4().bytes

    @fdb.transactional
    def do_take_lock(tr):
        tr.options.set_access_system_keys()
        tr.options.set_lock_aware()
        tr.options.set_timeout(30000)
        tr.options.set_priority_system_immediate()
        # Disable DD
        tr[DD_MODE_KEY] = b'\x00'
        # Take ownership of MoveKeysLock
        tr[MOVEKEYS_LOCK_OWNER_KEY] = our_owner_uid
        # Set initial write key
        tr[MOVEKEYS_LOCK_WRITE_KEY] = uuid.uuid4().bytes

    do_take_lock(db)
    return our_owner_uid


def release_movekeys_lock(db):
    """
    Re-enable DD and release MoveKeysLock.

    Call this after finishing writes to serverKeys/keyServers.
    """
    @fdb.transactional
    def do_release_lock(tr):
        tr.options.set_access_system_keys()
        tr.options.set_lock_aware()
        tr.options.set_timeout(30000)
        tr.options.set_priority_system_immediate()
        # Release lock by setting new random owner (DD will reclaim)
        tr[MOVEKEYS_LOCK_OWNER_KEY] = uuid.uuid4().bytes
        tr[MOVEKEYS_LOCK_WRITE_KEY] = uuid.uuid4().bytes
        # Re-enable DD
        tr[DD_MODE_KEY] = b'\x01'

    do_release_lock(db)


def update_movekeys_lock_write(tr):
    """
    Update the MoveKeysLock write key within a transaction.

    Call this in each transaction that writes to serverKeys/keyServers
    to prevent conflicts with DD.
    """
    tr[MOVEKEYS_LOCK_WRITE_KEY] = uuid.uuid4().bytes


def set_write_transaction_options(tr, timeout_ms=60000):
    """
    Set standard options for a transaction that writes to system keys.
    """
    tr.options.set_access_system_keys()
    tr.options.set_lock_aware()
    tr.options.set_timeout(timeout_ms)
    tr.options.set_priority_system_immediate()


def set_read_transaction_options(tr, timeout_ms=60000):
    """
    Set standard options for a transaction that reads system keys.
    """
    tr.options.set_read_system_keys()
    tr.options.set_lock_aware()
    tr.options.set_timeout(timeout_ms)
