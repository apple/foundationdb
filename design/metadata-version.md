## Motivation

FoundationDB (FDB) is a Key-Value (KV) storage database that supports Atomic, Consistency, Isolation, and Durability (ACID) properties. The ACID is guaranteed by a global versioning mechanism. The database will record the version of the latest committed mutations. When read, the client will first go to the GetReadVersion (GRV) proxy for the recorded version and then forward the version to StorageServers (SS) to retrieve the keys and values at the specific version.

In certain cases, every transaction will read a small amount of cold data. [One example](https://forums.foundationdb.org/t/whats-the-purpose-of-the-directory-layer/677/2) is that the client exploits the directory layer's advantage, and the prefix is configurable via a key in the database. The client must read the prefix before accessing data to construct the path. [Another example](https://forums.foundationdb.org/t/a-new-tool-for-managing-layer-metadata/1191/3) is that the server stores documents while the indices are added/dropped at runtime; the active indices list must be queried before any read transaction.

In the two examples above, the prefix and the indices are usually cold, and accessing them will overload the storage server with the corresponding keys. However, simply caching the data may cause read inconsistency and program corruption. Introducing a global metadata version will solve this problem with a minor performance penalty.

The metadata version key is a special system key defined in `fdbclient/SystemData.h:metadataVersionKey` with a single `Versionstamp` value. When a commit is completed, the Commit Proxy (CP) server reports the correspondent value to the Master server (MS), which keeps the newest value. Whenever the GRV requests the committed version, the Versionstamp is returned with the read version and then forwarded to the client.

The abovementioned caching issues can be solved by: 1. When the cold data is changed, the client will update the metadata version in the same transaction; 2. Whenever the client retrieves the read version, it compares the remote metadata version to its local copy. If the values are different, the local cache is stale.

## Usage

Unlike other system keys, the metadata version can be accessed without ACCESS_SYSTEM_KEYS flags enabled.

### Value

The metadata version value must be a single Versionstamp with no additional information attached. The `Versionstamp.user_version` must be 0.

### Reading

The metadata version can be accessed like ordinary keys. The value is read from the MS rather than the SS, and the reading is guaranteed to be synchronous.

### Writing

The key can only be written atomically in the transaction, with the mutate type `SetVersiondstampedValue`. The value must be `metadataVersionRequired`Value, or the FDB library will raise an `Invalid API Call` error. When the transaction is committed, the commit version will be used to overwrite the value. 

### Tenants

All tenants will share the same global metadata version; there is no isolation between tenants for this key.

## Example

### Python

Define

```python
METADATA_VERSION_KEY = b"\xff/metadataVersion"
METADATA_VERSION_REQUIRED_VALUE = b'\0' * 14
```

#### Read

```python
db = fdb.open()

local_metadata_version = db[METADATA_VERSION_KEY]
```

SS is not involved in the query process as the value is stored in the MS and included in every read version response.

#### Write

```python
@fdb.transactional
def fdb_transaction(tr):
    tr.set_versionstamped_value(metadata_version_key, METADATA_VERSION_REQUIRED_VALUE)
```

Note that the value **must** be a 14-byte zero-filled array. The leading 12 bytes are for the `Versionstamp,` while the tailing 2 bytes are for the offset, which is *always* 0 in this situation.

## Implementation Details

TODO

## References

[1] https://forums.foundationdb.org/t/a-new-tool-for-managing-layer-metadata/1191/3

[2] https://github.com/apple/foundationdb/pull/1213
