# Transaction State Store (txnStateStore)

This document describes the transaction state store (often is referred as `txnStateStore` in the code) in FDB. The transaction state store keeps important metadata about the database to bootstrap the database, to guide the transaction system to persist writes (i.e., help assign storage tags to mutations at commit proxies), and to manage data (i.e., shard) movement metadata. This is a critical piece of information that have to be consistent across many processes and to be persistent for recovery.

Acknowledgment: A lot of contents are taken from [Evan's FDB brownbag talk](https://drive.google.com/file/d/15UvKiNc-jSFfDGygNmLQP_d4b14X3DAS/).

## What information is stored in transaction state store?

The information includes: shard mapping (key range to storage server mapping, i.e.,
`keyServers`), storage server tags (`serverTags`), tagLocalityList, storage server tag
history, database locked flag, metadata version, mustContainSystemMutations, coordinators,
storage server interface (`serverList`), database configurations, TSS mappings and
quarantines, backup apply mutation ranges and log ranges, etc.

The information of transaction state store is kept in the system key space, i.e., using the
`\xff` prefix. Note all data in the system key space are saved on storage servers. The
`txnStateStore` is only a part of the `\xff` key space, and is additionally kept in the
memory of commit proxies as well as disks of the log system (i.e., TLogs). Changes to
the `txnStateStore` are special mutations to the `\xff` key space, and are called
inconsistently in the code base as "metadata mutations" in commit proxies and
"state mutations" in Resolvers.

## Why do we need transaction state store?

When bootstrapping an FDB cluster, the cluster controller (CC) role recruits a
new transaction system and initializes them. In particular, the transaction state store
is first read by the CC from previous generation's log system, and then broadcast to
all commit proxies of the new transaction system. After initializing `txnStateStore`, these
commit proxies know how to assign mutations with storage server tags: `txnStateStore`
contains the shard map from key range to storage servers; commit proxies use the shard
map to find and attach the destination storage tags for each mutation.

## How is transaction state store replicated?

The `txnStateStore` is replicated in all commit proxies' memories. It is very important
that `txnStateStore` data are consistent, otherwise, a shard change issued by one commit
proxy could result in a situation where different proxies think they should send a
mutation to different storage servers, thus causing data corruptions.

FDB solves this problem by state machine replication: all commit proxies start with the
same `txnStateStore` data (from CC broadcast), and apply the same sequence of mutations.
Because commits happen at all proxies, it is difficult to maintain the same order as well
as minimize the communication among them. Fortunately, every transaction has to send a
conflict resolution request to all Resolvers and they process transactions in strict order
of commit versions. Leveraging this mechanism, each commit proxy sends all metadata
(i.e., system key) mutations to all Resolvers. Resolvers keep these mutations in memory
and forward to other commit proxies in separate resolution response. Each commit proxy
receive resolution response, along with metadata mutations happened at other proxies before
its commit version, and apply all these metadata mutations in the commit order.
Finally, this proxy only writes metadata mutations in its own transaction batch to TLogs,
i.e., do not write other proxies' metadata mutations to TLogs to avoid repeated writes.

It's worth calling out that everything in the `txnStateStore` is stored at some storage
servers and a client (e.g., `fdbcli`) can read from these storage servers. During the
commit process, commit proxies parse all mutations in a batch of transactions, and apply
changes (i.e., metadata mutations) to its in-memory copy of `txnStateStore`. Later, the
same changes are applied at storage servers for persistence. Additionally, the process
to store `txnStateStore` at log system is described below.

Notably `applyMetadataMutations()` is the function that commit proxies use to make changes
to `txnStateStore`. The key ranges stored in `txnStateStore` include `[\xff, \xff\x02)` and
`[\xff\x03, \xff\xff)`, but not everything in these ranges. There is no data in the range
of `[\xff\x02, \xff\x03)` belong to `txnStateStore`, e.g., `\xff\x02` prefix is used for
backup data and is *NOT* metadata mutations.

## How is transaction state store persisted at log system?

When a commit proxy writes metadata mutations to the log system, the proxy assigns a
"txs" tag to the mutation. Depending on FDB versions, the "txs" tag can be one special
tag `txsTag{ tagLocalitySpecial, 1 }` for `TLogVersion::V3` (FDB 6.1, obsolete now) or a randomized
"txs" tag for `TLogVersion::V4` (FDB 6.2 and later) and larger. The idea of randomized
"txs" tag is to spread metadata mutations to all TLogs for faster parallel recovery of
`txnStateStore`.

At TLogs, all mutation data are indexed by tags. "txs" tag data is special, since it is
only peeked by the cluster controller (CC) during the transaction system recovery.
See [TLog Spilling doc](tlog-spilling.md.html) for more detailed discussion on the
topic of spilling "txs" data. In short, `txsTag` is spilled by value.
"txs" tag data is indexed and stored in both primary TLogs and satellite TLogs.
Note satellite TLogs only index log router tags and "txs" tags.

## How is transaction state store implemented?

`txnStateStore` is kept in memory at commit proxies using `KeyValueStoreMemory`, which
uses `LogSystemDiskQueueAdapter` to be durable with the log system. As a result, reading
from `txnStateStore` never blocks, which means the futures returned by read calls should
always be ready. Writes to `txnStateStore` are first buffered by the `LogSystemDiskQueueAdapter`
in memory. After a commit proxy pushes transaction data to the log system and the data
becomes durable, the proxy clears the buffered data in `LogSystemDiskQueueAdapter`.

Note `KeyValueStoreMemory` periodically checkpoints its data to disks to speed up its
recovery. For `txnStateStore`, this checkpoint allows versions before it to be popped,
i.e., "txs" tag data on the log system can be reclaimed so that log queue does not grow
unbounded. Thus, the `txsPoppedVersion` is the version right before a checkpoint, and
during recovery, `txnStateStore` is recovered by reading all data from this version.

* CC reads `txnStateStore` from old log system starting at `txsPoppedVersion`: https://github.com/apple/foundationdb/blob/6e31821bf578073409087779c567e26de317acd5/fdbserver/ClusterRecovery.actor.cpp#L1418

* CC broadcasts `txnStateStore` to commit proxies: https://github.com/apple/foundationdb/blob/6e31821bf578073409087779c567e26de317acd5/fdbserver/ClusterRecovery.actor.cpp#L1286-L1313

* Commit proxies receive txnStateStore broadcast and builds the `keyInfo` map: https://github.com/apple/foundationdb/blob/6281e647784e74dccb3a6cb88efb9d8b9cccd376/fdbserver/CommitProxyServer.actor.cpp#L1886-L1927
  * Look up `keyInfo` map for `GetKeyServerLocationsRequest`: https://github.com/apple/foundationdb/blob/6281e647784e74dccb3a6cb88efb9d8b9cccd376/fdbserver/CommitProxyServer.actor.cpp#L1464
  * Look up `keyInfo` map for assign mutations with storage tags: https://github.com/apple/foundationdb/blob/6281e647784e74dccb3a6cb88efb9d8b9cccd376/fdbserver/CommitProxyServer.actor.cpp#L926 and https://github.com/apple/foundationdb/blob/6281e647784e74dccb3a6cb88efb9d8b9cccd376/fdbserver/CommitProxyServer.actor.cpp#L965-L1010

* Commit proxies recover database lock flag and metadata version: https://github.com/apple/foundationdb/blob/6281e647784e74dccb3a6cb88efb9d8b9cccd376/fdbserver/CommitProxyServer.actor.cpp#L1942-L1944

* Commit proxies add metadata mutations to Resolver request: https://github.com/apple/foundationdb/blob/6281e647784e74dccb3a6cb88efb9d8b9cccd376/fdbserver/CommitProxyServer.actor.cpp#L137-L140

* Resolvers keep these mutations in memory: https://github.com/apple/foundationdb/blob/6281e647784e74dccb3a6cb88efb9d8b9cccd376/fdbserver/Resolver.actor.cpp#L220-L230

* Resolvers copy metadata mutations to resolution reply message: https://github.com/apple/foundationdb/blob/6281e647784e74dccb3a6cb88efb9d8b9cccd376/fdbserver/Resolver.actor.cpp#L244-L249

* Commit proxies apply all metadata mutations (including those from other proxies) in the commit order: https://github.com/apple/foundationdb/blob/6281e647784e74dccb3a6cb88efb9d8b9cccd376/fdbserver/CommitProxyServer.actor.cpp#L740-L770

* Commit proxies only write metadata mutations in its own transaction batch to TLogs: https://github.com/apple/foundationdb/blob/6281e647784e74dccb3a6cb88efb9d8b9cccd376/fdbserver/CommitProxyServer.actor.cpp#L772-L774 adds mutations to `storeCommits`. Later in `postResolution()`, https://github.com/apple/foundationdb/blob/6281e647784e74dccb3a6cb88efb9d8b9cccd376/fdbserver/CommitProxyServer.actor.cpp#L1162-L1176, only the last one in `storeCommits` are send to TLogs.

* Commit proxies clear the buffered data in `LogSystemDiskQueueAdapter` after TLog push: https://github.com/apple/foundationdb/blob/6281e647784e74dccb3a6cb88efb9d8b9cccd376/fdbserver/CommitProxyServer.actor.cpp#L1283-L1287
