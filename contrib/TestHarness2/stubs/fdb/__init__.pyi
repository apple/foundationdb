from typing import Generic, TypeVar, Callable
from . import fdboptions
from fdboptions import StreamingMode

def api_version(version: int) -> None: ...

def open(cluster_file: str = None, event_model: str = None) -> Database:
    ...

T = TypeVar('T')
options = fdboptions.NetworkOptions
StreamingMode = StreamingMode


class Future(Generic[T]):
    def wait(self) -> T:
        ...

    def is_ready(self) -> bool:
        ...

    def block_until_ready(self) -> None:
        ...

    def on_ready(self, callback: Callable[[Future[T]], None]) -> None:
        ...

    def cancel(self):
        ...

    @staticmethod
    def wait_for_any(*futures: Future):
        ...


class FutureString(Future[bytes]):
    __class__ = property(bytes)
    def as_foundationdb_key(self) -> bytes:
        ...

    def as_foundationdb_value(self) -> bytes:
        ...


class ValueType(FutureString):
    def present(self) -> bool:
        ...

class KeyType(FutureString):
    pass


Key = KeyType | bytes
Value = ValueType | bytes
Version = int

class KVIter:
    def __iter__(self) -> KVIter:
        ...

    def __next__(self) -> KeyValue:
        ...


class KeyValue:
    key: Key
    value: Value

    def __iter__(self) -> KVIter:
        ...


class KeySelector:
    @classmethod
    def last_less_than(cls, key: Key) -> KeySelector:
        ...

    @classmethod
    def last_less_or_equal(cls, key: Key) -> KeySelector:
        ...

    @classmethod
    def first_greater_than(cls, key: Key) -> KeySelector:
        ...

    @classmethod
    def first_greater_or_equal(cls, key: Key) -> KeySelector:
        ...

    def __add__(self, offset: int) -> KeySelector:
        ...

    def __sub__(self, offset: int) -> KeySelector:
        ...


class Error:
    code: int
    description: str


class Tenant:
    def create_transaction(self) -> Transaction:
        ...


class _SnapshotTransaction:
    db: Database
    def get(self, key: Key) -> bytes | None:
        ...

    def get_key(self, key_selector: KeySelector) -> bytes:
        ...

    def __getitem__(self, key: Key | slice) -> bytes:
        ...

    def get_range(self, begin: Key, end: Key, limit: int = 0, reverse: bool = False,
                  streaming_mode: StreamingMode = StreamingMode.exact) -> KeyValue:
        ...

    def get_range_startswith(self, prefix: bytes,
                             limit: int = 0,
                             reverse: bool = False,
                             streaming_mode: StreamingMode = StreamingMode.exact):
        ...


class Transaction(_SnapshotTransaction):
    options: fdboptions.TransactionOptions
    snapshot: _SnapshotTransaction

    def set(self, key: Key, value: Value) -> None:
        ...

    def clear(self, key: Key) -> None:
        ...

    def clear_range(self, begin: Key, end: Key) -> None:
        ...

    def clear_range_startswith(self, prefix: bytes) -> None:
        ...

    def __setitem__(self, key: Key, value: Value) -> None:
        ...

    def __delitem__(self, key: Key) -> None:
        ...

    def add(self, key: Key, param: bytes) -> None:
        ...

    def bit_and(self, key: Key, param: bytes) -> None:
        ...

    def bit_or(self, key: Key, param: bytes) -> None:
        ...

    def bit_xor(self, key: Key, param: bytes) -> None:
        ...

    def max(self, key: Key, param: bytes) -> None:
        ...

    def byte_max(self, key: Key, param: bytes) -> None:
        ...

    def min(self, key: Key, param: bytes) -> None:
        ...

    def byte_min(self, key: Key, param: bytes) -> None:
        ...

    def set_versionstamped_key(self, key: Key, param: bytes) -> None:
        ...

    def set_versionstamped_value(self, key: Key, param: bytes) -> None:
        ...

    def commit(self) -> Future[None]:
        ...

    def on_error(self, err: Error) -> Future[None]:
        ...

    def reset(self) -> None:
        ...

    def cancel(self) -> None:
        ...

    def watch(self, key: Key) -> Future[None]:
        ...

    def add_read_conflict_range(self, begin: Key, end: Key) -> None:
        ...

    def add_read_conflict_key(self, key: Key) -> None:
        ...

    def add_write_conflict_range(self, begin: Key, end: Key) -> None:
        ...

    def add_write_conflict_key(self, key: Key) -> None:
        ...

    def set_read_version(self, version: Version) -> None:
        ...

    def get_read_version(self) -> Version:
        ...

    def get_committed_version(self) -> Version:
        ...

    def get_versionstamp(self) -> FutureString:
        ...


class Database:
    options: fdboptions.DatabaseOptions

    def create_transaction(self) -> Transaction:
        ...

    def open_tenant(self, tenant_name: str) -> Tenant:
        ...

    def get(self, key: Key) -> bytes | None:
        ...

    def get_key(self, key_selector: KeySelector) -> bytes:
        ...

    def clear(self, ):

    def __getitem__(self, key: Key | slice) -> bytes:
        ...

    def __setitem__(self, key: Key, value: Value) -> None:
        ...

    def __delitem__(self, key: Key) -> None:
        ...

    def get_range(self, begin: Key, end: Key, limit: int = 0, reverse: bool = False,
                  streaming_mode: StreamingMode = StreamingMode.exact) -> KeyValue:
        ...


class Subspace:
    def __init__(self, **kwargs):
        ...

    def key(self) -> bytes:
        ...

    def pack(self, tuple: tuple = tuple()) -> bytes:
        ...

    def pack_with_versionstamp(self, tuple: tuple) -> bytes:
        ...

    def unpack(self, key: bytes) -> tuple:
        ...

    def range(self, tuple: tuple = tuple()) -> slice:
        ...

    def contains(self, key: bytes) -> bool:
        ...

    def subspace(self, tuple: tuple) -> Subspace:
        ...

    def __getitem__(self, item) -> Subspace:
        ...


class DirectoryLayer:
    def __init__(self, *kwargs):
        ...

    def create_or_open(self, tr: Transaction, path: tuple | str, layer: bytes = None) -> DirectorySubspace:
        ...

    def open(self, tr: Transaction, path: tuple | str, layer: bytes = None) -> DirectorySubspace:
        ...

    def create(self, tr: Transaction, path: tuple | str, layer: bytes = None) -> DirectorySubspace:
        ...

    def move(self, tr: Transaction, old_path: tuple | str, new_path: tuple | str) -> DirectorySubspace:
        ...

    def remove(self, tr: Transaction, path: tuple | str) -> None:
        ...

    def remove_if_exists(self, tr: Transaction, path: tuple | str) -> None:
        ...

    def list(self, tr: Transaction, path: tuple | str = ()) -> List[str]:
        ...

    def exists(self, tr: Transaction, path: tuple | str) -> bool:
        ...

    def get_layer(self) -> bytes:
        ...

    def get_path(self) -> tuple:
        ...


class DirectorySubspace(DirectoryLayer, Subspace):
    def move_to(self, tr: Transaction, new_path: tuple | str) -> DirectorySubspace:
        ...

directory: DirectoryLayer

def transactional(*tr_args, **tr_kwargs) -> Callable:
    ...