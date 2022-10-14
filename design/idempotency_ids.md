# Goals

The main goal is to make transactions safer and easier to reason about. New users should get a "just works" experience. One of the main selling points of FoundationDB is that it solves the hard distributed systems problems for you, so that you only need to concern yourself with your business logic. Non-idempotent transactions is probably the biggest "gotcha" that users need to be made aware of -- and they won't discover it organically. In order to achieve this "just works" experience I believe it is necessary to make automatic idempotency have low-enough overhead so that we can enable it by default.

As an intermediate goal, I plan to introduce this feature disabled by default. The long-term plan is to make it the default.

# API

Introduce a new transaction option `IDEMPOTENCY_ID`, which will be validated to be at most 255 bytes.
Add 
```
FDBFuture* fdb_transaction_commit_result(FDBTransaction* tr, uint8_t const* idempotency_id, int idempotency_id_length)
```
, which can be used to determine the result of a commit that failed with `transaction_timed_out`.

Commits for transactions with idempotency ids would not fail with `commit_unknown_result`, but in (extremely) rare cases could fail with a new error that clients are expected to handle by restarting the process.
# Background

- https://forums.foundationdb.org/t/automatically-providing-transaction-idempotency/1873
- https://github.com/apple/foundationdb/issues/1321
- https://docs.google.com/document/d/19LDQuurg4Tt8eUcig3-8g2VOG9ZpQvtWrp_691RqMo8/edit#

# Data model

Commit proxies would combine idempotency IDs for transactions within a batch. The purpose of this is to try to limit the number of distinct database keys that need to be written, and to lessen the number of extra mutation bytes for idempotency IDs.

## Key format
```
\xff\x02/idmp/${commit_version_big_endian (8 bytes)}${high_order_byte_of_batch_index (1 byte)}
```

- `commit_version_big_endian` the commit version stored big-endian so that the cleaner worker can find the oldest idempotency ids easily, and also so that "unknown_committed" transactions can recover their commit version.
- `high_order_byte_of_batch_index` this limits us to 256 idempotency ids per value

## Value format
```
${protocol_version}(${n (1 byte)}${idempotency_id (n bytes)}${low_order_byte_of_batch_index})*
```

The batch index for each idempotency id can be reconstructed from the high order byte and low order bytes stored in the key and value, respectively. This is necessary for an "unknown_committed" transaction to recover their full version stamp. Batch index is a `short int`, i.e. 2 bytes.

# Cleaning up old idempotency ids

After learning the result of an attempt to commit a transaction with an
idempotency id, the client may inform the cluster that it's no longer interested
in that id and the cluster can reclaim the space used to store the idempotency
id. The happy-path reply to a CommitTransactionRequest will say which proxy this
request should be sent to, and all idempotency ids for a database key will be
sent to the same proxy so that it can clear the key once it receives all of
them. The first proxy will also periodically clean up the oldest idempotency ids, based on a policy determined by two knobs. One knob will control the minimum lifetime of an idempotency id (i.e. don't delete anything younger than 1 day), and the other will control the target byte size of the idempotency keys (e.g. keep 100 MB of idempotency keys around).

# Commit protocol

The basic change will be that a commit future will not become ready until the client confirms whether or not the commit succeeded. (`transaction_timed_out` is an unfortunate exception here)

The idempotency id will be automatically added to both the read conflict range and the write conflict range, before makeSelfConflicting is called so that we don't duplicate that work. We can reuse the `\xff/SC/` self-conflicting key space here.

## Did I already commit?

The first version of this scans the keys in the idmp key range to check for the idempotency ids. The plan for the next version is the following:

Storage servers would have a new endpoint that clients can use to ask if the transaction for an idempotency id already committed. Clients would need to check every possible shard that their idempotency id may have ended up in.

Storage servers would maintain a map from idempotency id to versionstamp in memory, and clients would need to contact all storage servers responsible for the `[\xff\x02/idmp/, \xff\x02/idmp0)` keyspace to be sure of their commit status. Assuming an idempotency id + versionstamp is 16 + 10 bytes, and that the lifetime of most idempotency ids is less than 1 second, that corresponds to at least 260 MB of memory on the storage server at 1,000,000 transactions/s, which seems acceptable. Let's double that to account for things like hash table load factor and allocating extra memory to ensure amortized constant time insertion. Still seems acceptable. We probably want to use a hashtable with open addressing to avoid frequent heap allocations. I _think_ [swisstables](https://abseil.io/about/design/swisstables) would work here.

When a transaction learns that it did in fact commit, the commit future succeeds, and the versionstamp gets filled with the original, successful transaction's versionstamp. After the successful commit is reported, it's no longer necessary to store its idempotency ID. The client will send an RPC to the cleaner role indicating that it can remove this idempotency ID.

If a transaction learns that it did in fact _not_ commit, the commit future will fail with an error that indicates that the transaction did not commit. Perhaps `transaction_too_old`.

If a transaction learns that it has been in-flight so long that its idempotency id could have been expired, then it will fail with a new, non-retriable error. It is expected that this will be rare enough that crashing the application is acceptable.

# Considerations

- Additional storage space on the cluster. This can be controlled directly via an idempotency id target bytes knob/config.
- Potential write hot spot.

# Multi-version client

The multi-version client will generate its own idempotency id for a transaction and manage its lifecycle. It will duplicate the logic in NativeApi to achieve the same guarantees. As part of this change we will also ensure that the previous commit attempt is no longer in-flight before allowing the commit future to become ready. This will fix a potential "causal-write-risky" issue if a commit attempt fails with `cluster_version_changed`.

# Experiments

- Initial experiments show that this is about 1% overhead for the worst case workload which is transactions that only update a single key.

```
Single replication redwood cluster with dedicated ebs disks for tlog and storage. All tests saturated the tlog disk's IOPs.

volume_type: gp3
volume_size: 384
iops: 9000
throughput: 250

$ bin/mako --mode run --rows 1000000 -x u1 -p 8 -t 8 --cluster=$HOME/fdb.cluster  --seconds 100 # already warm, but quiesced

Baseline:

19714.67 TPS

"user space" method of writing idempotency id -> versionstamp in every transaction:

13831.00 TPS

"combine idempotency ids in transaction batch" method:

19515.62 TPS
```
