<meta charset="utf-8">

# TLog Spill-By-Reference Design

## Background

(This assumes a basic familiarity with [FoundationDB's architecture](https://www.youtu.be/EMwhsGsxfPU).)

Transaction logs are a distributed Write-Ahead-Log for FoundationDB.  They
receive commits from commit proxies, and are responsible for durably storing 
those commits, and making them available to storage servers for reading.

Clients send *mutations*, the list of their set, clears, atomic operations,
etc., to commit proxies. Commit proxies collect mutations into a *batch*, which
is the list of all changes that need to be applied to the database to bring it
from version `N-1` to `N`. Commit proxies then walk through their in-memory 
mapping of shard boundaries to associate one or more *tags*, a small integer 
uniquely identifying a destination storage server, with each mutation. They 
then send a *commit*, the full list of `(tags, mutation)` for each mutation in
a batch, to the transaction logs.

The transaction log has two responsibilities: it must persist the commits to
disk and notify the proxy when a commit is durably stored, and it must make the
commit available for consumption by the storage server.  Each storage server
*peeks* its own tag, which requests all mutations from the transaction log with
the given tag at a given version or above.  After a storage server durably
applies the mutations to disk, it *pops* the transaction logs with the same tag
and its new durable version, notifying the transaction logs that they may
discard mutations with the given tag and a lesser version.

To persist commits, a transaction log appends commits to a growable on-disk
ring buffer, called a *disk queue*, in version order.  Commit data is *pushed*
onto the disk queue, and when all mutations in the oldest commit persisted are
no longer needed, the disk queue is *popped* to trim its tail.

To make commits available to storage servers efficiently, a transaction log
maintains a copy of the commit in-memory, and maintains one queue per tag that
indexes the location of each mutation in each commit with the specific tag,
sequentially.  This way, responding to a peek from a storage server only
requires sequentially walking through the queue, and copying each mutation
referenced into the response buffer.

Transaction logs internally handle commits via performing two operations
concurrently.  First, they walk through each mutation in the commit, and push
the mutation onto an in-memory queue of mutations destined for that tag.
Second, they include the data in the next batch of pages to durably persist to
disk.  These in-memory queues are popped from when the corresponding storage
server has persisted the data to its own disk.  The disk queue only exists to
allow the in-memory queues to be rebuilt if the transaction log crashes, is
never read from except during a transaction log recovering post-crash, and is
popped when the oldest version it contains is no longer needed in memory.

TLogs will need to hold the last 5-7 seconds of mutations.  In normal
operation, the default 1.5GB of memory is enough such that the last 5-7 seconds
of commits should almost always fit in memory.  However, in the presence of
failures, the transaction log can be required to buffer significantly more
data.  Most notably, when a storage server fails, its tag isn't popped until
data distribution is able to re-replicate all of the shards that storage server
was responsible for to other storage servers.  Before that happens, mutations
will accumulate on the TLog destined for the failed storage server, in case it
comes back and is able to rejoin the cluster.

When this accumulation causes the memory required to hold all the unpopped data
to exceed `TLOG_SPILL_THRESHOLD` bytes, the transaction log offloads the
oldest data to disk.  This writing of data to disk to reduce TLog memory
pressure is referred to as *spilling*.

**************************************************************
*                      Transaction Log                       *
*                                                            *
*                                                            *
*  +------------------+   pushes   +------------+            *
*  | Incoming Commits |----------->| Disk Queue |  +------+  *
*  +------------------+            +------------+  |SQLite|  *
*          |                                ^      +------+  *
*          |                                |        ^       *
*          |                               pops      |       *
*          +------+-------+------+          |       writes   *
*                 |       |      |          |        |       *
*                 v       v      v         +----------+      *
*    in-memory  +---+   +---+  +---+       |Spill Loop|      *
*    queues     | 1 |   | 2 |  | 3 |       +----------+      *
*    per-tag    |   |   |   |  |   |            ^            *
*               |...|   |...|  |...|            |            *
*                 |       |      |              |            *
*                 v       v      v              |            *
*                 +-------+------+--------------+            *
*                    queues spilled on overflow              *
*                                                            *
**************************************************************

## Overview

Previously, spilling would work by writing the data to a SQLite B-tree.  The
key would be `(tag, version)`, and the value would be all the mutations
destined for the given tag at the given version.  Peek requests have a start
version, that is the latest version for which the storage server knows about,
and the TLog responds by range-reading the B-tree from the start version.  Pop
requests allow the TLog to forget all mutations for a tag until a specific
version, and the TLog thus issues a range clear from `(tag, 0)` to
`(tag, pop_version)`.  After spilling, the durably written data in the disk
queue would be trimmed to only include from the spilled version on, as any
required data is now entirely, durably held in the B-tree.  As the entire value
is copied into the B-tree, this method of spilling will be referred to as
*spill-by-value* in the rest of this document.

Unfortunately, it turned out that spilling in this fashion greatly impacts TLog
performance.  A write bandwidth saturation test was run against a cluster, with
a modification to the transaction logs to have them act as if there was one
storage server that was permanently failed; it never sent pop requests to allow
the TLog to remove data from memory.  After 15min, the write bandwidth had
reduced to 30% of its baseline.  After 30min, that became 10%.  After 60min,
that became 5%.  Writing entire values gives an immediate 3x additional write
amplification, and the actual write amplification increases as the B-tree gets
deeper.  (This is an intentional illustration of the worst case, due to the
workload being a saturating write load.)

With the recent multi-DC/multi-region work, a failure of a remote data center
would cause transaction logs to need to buffer all commits, as every commit is
tagged as destined for the remote datacenter.  This would rapidly push
transaction logs into a spilling regime, and thus write bandwidth would begin
to rapidly degrade.  It is unacceptable for a remote datacenter failure to so
drastically affect the primary datacenter's performance in the case of a
failure, so a more performant way of spilling data is required.

Whereas spill-by-value copied the entire mutation into the B-tree and removes
it from the disk queue, spill-by-reference leaves the mutations in the disk
queue and writes a pointer to it into the B-tree. Performance experiments
revealed that the TLog's performance while spilling was dictated more by the
number of writes done to the SQLite B-tree, than by the size of those writes.
Thus, "spill-by-reference" being able to do a significantly better batching
with its writes to the B-tree is more important than that it writes less data
in aggregate.  Spill-by-reference significantly reduces the volume of data
written to the B-tree, and the less data that we write, the more we can batch
versions to be written together.

************************************************************************
*                              DiskQueue                               *
*                                                                      *
*    ------- Index in B-tree -------     ---- Index in memory ----     *
*   /                               \   /                         \    *
* +-----------------------------------+-----------------------------+  *
* |            Spilled Data           |       Most Recent Data      |  *
* +-----------------------------------+-----------------------------+  *
* lowest version                                      highest version  *
*                                                                      *
************************************************************************

Spill-by-reference works by taking a larger range of versions, and building a
single key-value pair per tag that describes where in the disk queue is every
relevant commit for that tag.  Concretely, this takes the form
`(tag, last_version) -> [(version, start, end, mutation_bytes), ...]`, where:

 * `tag` is the small integer representing the storage server this mutation batch is destined for.
 * `last_version` is the last/maximum version contained in the value's batch.
 * `version` is the version of the commit that this index entry points to.
 * `start` is an index into the disk queue of where to find the beginning of the commit.
 * `end` is an index into the disk queue of where the end of the commit is.
 * `mutation_bytes` is the number of bytes in the commit that are relevant for this tag.

And then writing only once per tag spilled into the B-tree for each iteration
through spilling.  This turns the number of writes into the B-Tree from
`O(tags * versions)` to `O(tags)`.

Note that each tuple in the list represents a commit, and not a mutation.  This
means that peeking spilled commits will involve reading all mutations that were
a part of the commit, and then filtering them to only the ones that have the
tag of interest.  Alternatively, one could have each tuple represent a mutation
within a commit, to prevent over-reading when peeking.  There exist
pathological workloads for each strategy.  The purpose of this work is most
importantly to support spilling of log router tags.  These exist on every
mutation, so that it will get copied to other datacenters.  This is the exact
pathological workload for recording each mutation individually, because it only
increases the number of IO operations used to read the same amount of data.
For a wider set of workloads, there's room to establish a heuristic as to when
to record mutation(s) versus the entire commit, but performance testing hasn't
surfaced this as important enough to include in the initial version of this
work.

Peeking spilled data now works by issuing a range read to the B-tree from
`(tag, peek_begin)` to `(tag, infinity)`.  This is why the key contains the
last version of the batch, rather than the beginning, so that a range read from
the peek request's version will always return all relevant batches.  For each
batched tuple, if the version is greater than our peek request's version, then
we read the commit containing that mutation from disk, extract the relevant
mutations, and append them to our response.  There is a target size of the
response, 150KB by default.  As we iterate through the tuples, we sum
`mutation_bytes`, which already informs us how many bytes of relevant mutations
we'll get from a given commit.  This allows us to make sure we won't waste disk
IOs on reads that will end up being discarded as unnecessary.

Popping spilled data works similarly to before, but now requires recovering
information from disk.  Previously, we would maintain a map from version to
location in the disk queue for every version we hadn't yet spilled.  Once
spilling has copied the value into the B-tree, knowing where the commit was in
the disk queue is useless to us, and is removed.  In spill-by-reference, that
information is still needed to know how to map "pop until version 7" to "pop
until byte 87" in the disk queue.  Unfortunately, keeping this information in
memory would result in TLogs slowly consuming more and more
memory[^versionmap-memory] as more data is spilled.  Instead, we issue a range
read of the B-tree from `(tag, pop_version)` to `(tag, infinity)` and look at
the first commit we find with a version greater than our own.  We then use its
starting disk queue location as the limit of what we could pop the disk queue
until for this tag.

[^versionmap-memory]: Pessimistic assumptions would suggest that a TLog spilling 1TB of data would require ~50GB of memory to hold this map, which isn't acceptable.

## Detailed Implementation

The rough outline of concrete changes proposed looks like:

1. Allow a new TLog and old TLog to co-exist and be configurable, upgradeable, and recoverable
1. Modify spilling in new TLogServer
1. Modify peeking in new TLogServer
1. Modify popping in new TLogServer
1. Spill txsTag specially

### Configuring and Upgrading

Modifying how transaction logs spill data is a change to the on-disk files of
transaction logs.  The work for enabling safe upgrades and rollbacks of
persistent state changes to transaction logs was split off into a separate
design document: "Forward Compatibility for Transaction Logs".

That document describes a `log_version` configuration setting that controls the
availability of new transaction log features.  A similar configuration setting
was created, `log_spill`, that at `log_version>=3`, one may `fdbcli>
configure log_spill:=2` to enable spill-by-reference.  Only FDB 6.1 or newer
will be unable to recover transaction log files that were using
spill-by-reference.  FDB 6.2 will use spill-by-reference by default.

| FDB Version | Default | Configurable |
|-------------|---------|--------------|
| 6.0         | No      | No           |
| 6.1         | No      | Yes          |
| 6.2         | Yes     | Yes          |

If running FDB 6.1, the full command to enable spill-by-reference is
`fdbcli> configure log_version:=3 log_spill:=2`.

The TLog implementing spill-by-value was moved to `OldTLogServer_6_0.actor.cpp`
and namespaced similarly.  `tLogFnForOptions` takes a `TLogOptions`, which is
the version and spillType, and returns the correct TLog implementation
according to those settings.  We maintain a map of
`(TLogVersion, StoreType, TLogSpillType)` to TLog instance, so that only
one SharedTLog exists per configuration variant.

### Generations

As a background, each time FoundationDB goes through a recovery, it will
recruit a new generation of transaction logs.  This new generation of
transaction logs will often be recruited on the same worker that hosted the
previous generation's transaction log.  The old generation of transaction logs
will only shut down once all the data that they have has been fully popped.
This means that there can be multiple instances of a transaction log in the
same process.

Naively, this would create resource issues.  Each instance would think that it
is allowed its own 1.5GB buffer of in-memory mutations.  Instead, internally to
the TLog implementation, the transaction log is split into two parts.  A
`SharedTLog` is all the data that should be shared across multiple generations.
A TLog is all the data that is private to one generation.  Most notably, the
1.5GB mutation buffer and the on-disk files are owned by the `SharedTLog`.  The
index for the data added to that buffer is maintained within each TLog.  In the
code, a SharedTLog is called `struct TLogData`, and a TLog is `struct LogData`.
(These names can be confusing at first).

This background is required, because one needs to keep in mind that we might be
committing in one TLog instance, a different one might be spilling, and yet
another might be the one popping data.

*********************************************************
*                   SharedTLog                          *
*                                                       *
* +--------+--------+--------+--------+--------+        *
* | TLog 1 | TLog 2 | TLog 3 | TLog 4 | TLog 5 |        *
* +--------+--------+--------+--------+--------+        *
*   ^ popping         ^spilling         ^committing     * 
*********************************************************

Conceptually, this is because each TLog owns a separate part of the same Disk
Queue file.  The earliest TLog instance needs to be the one that controls when
the earliest part of the file can be discarded.  We spill in version order, and
thus whatever TLog is responsible for the earliest unspilled version needs to
be the one doing the spilling.  We always commit the newest version, so the
newest TLog must be the one writing to the disk queue and inserting new data
into the buffer of mutations.


### Spilling

`updatePersistentData()` is the core of the spilling loop, that takes a new
persistent data version, writes the in-memory index for all commits less than
that version to disk, and then removes them from memory.  By contact, once
spilling commits an updated persistentDataVersion to the B-tree, then those
bytes will not need to be recovered into memory after a crash, nor will the
in-memory bytes be needed to serve a peek response.

Our new method of spilling iterates through each tag, and builds up a
`vector<SpilledData>` for each tag, where `SpilledData` is:

``` CPP
struct SpilledData {
  Version version;
  IDiskQueue::location start;
  uint32_t length;
  uint32_t mutationBytes;
};
```

And then this vector is serialized, and written to the B-tree as
`(logId, tag, max(SpilledData.version))` = `serialized(vector<SpilledData>)`

As we iterate through each commit, we record the number of mutation bytes in
this commit that have our tag of interest.  This is so that later, peeking can
read exactly the number of commits that it needs from disk.

Although the focus of this project is on the topic of spilling, the code
implementing itself saw the least amount of total change.

### Peeking

A `TLogPeekRequest` contains a `Tag` and a `Version`, and is a request for all
commits with the specified tag with a commit version greater than or equal to
the given version.  The goal is to return a 150KB block of mutations.

When servicing a peek request, we will read up to 150KB of mutations from the
in-memory index.  If the peek version is lower than the version that we've
spilled to disk, then we consult the on-disk index for up to 150KB of
mutations.  (If we tried to read from disk first, and then read from memory, we
would then be racing with the spilling loop moving data from memory to disk.)

**************************************************************************
*                                                                        *
* +---------+  Tag    +---------+           Tag    +--------+            *
* |  Peek   |-------->| Spilled | ...------------->| Memory |            *
* | Request | Version |  Index  |          Version | Index  |            *
* +---------+         +---------+                  +--------+            *
*                        |                                 |             *
*      +-----------------+-----------------+               |             *
*     / \  Start=100   _/ \_  Start=500    +  Start=900    +  Ptr=0xF00  *
*    /   \ Length=50  /     \ Length=70   / \ Length=30   / \ Length=30  *
*  +------------------------------------------------+------------------+ *
*  |                   Disk Queue                   |  Also In Memory  | *
*  +------------------------------------------------+------------------+ *
*                                                                        *
**************************************************************************

Spill-by-value and memory storage engine only ever read from the DiskQueue when
recovering, and read the entire file linearly.  Therefore, `IDiskQueue` had no
API for random reads to the DiskQueue.  That ability is now required for
peeking, and thus, `IDiskQueue`'s API has been enhanced correspondingly:

``` CPP
BOOLEAN_PARAM(CheckHashes);

class IDiskQueue {
    // ...
    Future<Standalone<StringRef>> read(location start, location end, CheckHashes ch);
    // ...
};
```

Internally, the DiskQueue adds page headers every 4K, which are stripped out
from the returned data.  Therefore, the length of the result will not be the
same as `end-start`, intentionally.  For this reason, the API is `(start, end)`
and not `(start, length)`.

Spilled data, when using spill-by-value, was resistant to bitrot via data being
checksummed internally within SQLite's B-tree.  Now that reads can be done
directly, the responsibility for verifying data integrity falls upon the
DiskQueue.  `CheckHashes::TRUE` will cause the DiskQueue to use the checksum in
each DiskQueue page to verify data integrity.  If an externally maintained
checksums exists to verify the returned data, then `CheckHashes::FALSE` can be
used to elide the checksumming.  A page failing its checksum will cause the
transaction log to die with an `io_error()`.  

What is read from disk is a `TLogQueueEntry`:

``` CPP
struct TLogQueueEntryRef {
    UID id;
    Version version;
    Version knownCommittedVersion;
    StringRef messages;
}
```

Which provides the commit version and the logId of the TLog generation that
produced this commit, in addition to all of the mutations for that version.
(`knownCommittedVersion` is only used during FDB's recovery process.)

### Popping

As storage servers persist data, they send `pop(tag, version)` requests to the
transaction log to notify it that it is allowed to discard data for `tag` up
through `version`.  Once all the tags have been popped from the oldest commit
in the DiskQueue, the tail of the DiskQueue can be discarded to reclaim space.

If our popped version is in the range of what has been spilled, then we need to
consult our on-disk index to see what is the next location in the disk queue
that has data which is useful to us.  This act would race with the spilling
loop changing what data is spilled, and thus disk queue popping
(`popDiskQueue()`) was made to run serially after spilling completes.

Also due to spilling and popping largely overlapping in state, the disk queue
popping loop does not immediately react to a pop request from a storage server
changing the popped version for a tag.  Spilling saves the popped version for
each tag when the spill loop runs, and if that version changed, then
`popDiskQueue()` refreshes its knowledge of what the minimum location in the
disk queue is required for that tag.  We can pop the disk queue to the minimum
of all minimum tag locations, or to the minimum location needed for an
in-memory mutation if there is no spilled data.

As a post implementation note, this ended up being a "here be dragons"
experience, with a surprising number of edge cases in races between
spilling/popping, various situations of having/not having/having inaccurate
data for tags, or that tags can stop being pushed to when storage servers are
removed but their corresponding `TagData` is never removed.

### Transaction State Store

For FDB to perform a recovery, there is information that it needs to know about
the database, such as the configuration, worker exclusions, backup status, etc.
These values are stored into the database in the `\xff` system keyspace.
However, during a recovery, FDB can't read this data from the storage servers,
because recovery hasn't completed, so it doesn't know who the storage servers
are yet.  Thus, a copy of this data is held in-memory on every proxy in the
*transaction state store*, and durably persisted as a part of commits on the
transaction logs.  Being durably stored on the transaction logs means the list
of transaction logs can be fetched from the coordinators, and then used to load
the rest of the information about the database.

The in-memory storage engine writes an equal amount of mutations and snapshot
data to a queue, an when a full snapshot of the data has been written, deletes
the preceding snapshot and begins writing a new one.  When backing an
in-memory storage engine with the transaction logs, the
`LogSystemDiskQueueAdapter` implements writing to a queue as committing
mutations to the transaction logs with a special tag of `txsTag`, and deleting
the preceding snapshot as popping the transaction logs for the tag of `txsTag`
until the version where the last full snapshot began.

This means that unlike every other commit that is tagged and stored on the
transaction logs, `txsTag` signifies data that is:

1. Committed to infrequently
2. Only peeked on recovery
3. Popped infrequently, and a large portion of the data is popped at once
4. A small total volume of data

The most problematic of these is the infrequent popping.  Unpopped data will be
spilled after some time, and if `txsTag` data is spilled and not popped, it
will prevent the DiskQueue from being popped as well.  This will cause the
DiskQueue to grow continuously.  The infrequent commits and small data volume
means that there benefits of spill-by-reference over spill-by-value don't apply
for this tag.

Thus, even when configured to spill-by-reference, `txsTag` is spilled by value.

### Disk Queue Recovery

If a transaction log dies and restarts, all commits that were in memory at the
time of the crash must be loaded back into memory.  Recovery is blocked on this
process, as there might have been a commit to the transaction state store
immediately before crashing, and that data needs to be fully readable during a
recovery.

In spill-by-value, the DiskQueue only ever contained commits that were also
held in memory, and thus recovery would need to read up to 1.5GB of data.  With
spill-by-reference, the DiskQueue could theoretically contain terabytes of
data. To keep recovery times boundedly low, FDB must still only read the
commits that need to be loaded back into memory.

This is done by persisting the location in the DiskQueue of the last spilled
commit to the SQLite B-Tree.  This is done in the same transaction as the
spilling of that commit.  This provides an always accurate pointer to where
data that needs to be loaded into memory begins.  The pointer is to the
beginning of the last commit rather than the end, to make sure that the pointer
is always contained within the DiskQueue.  This provides extra sanity checking
on the validity of the DiskQueue's contents at recovery, at the cost of
potentially reading 10MB more than what would be required.

## Testing

Correctness bugs in spilling would manifest as data corruption, which is well covered by simulation.
The only special testing code added was to enable changing `log_spill` in `ConfigureTest`.
This covers switching between spilling methods in the presence of faults.

An `ASSERT` was added to simulation that verifies that commits read from the
DiskQueue on recovery are only the commits which have not been spilled.

The rest of the testing is to take a physical cluster and try the extremes that
can only happen at scale:

* Verify that recovery times are not impacted when a large amount of data is spilled
* Verify that long running tests hit a steady state of memory usage (and thus there are likely no leaks).
* Plot how quickly (MB/s) a remote datacenter can catch up in old vs new spilling strategy
* See what happens when there's 1 tlog and more than 100 storage servers.
  * Verify that peek requests get limited
	* See if tlog commits can get starved by excessive peeking

# TLog Spill-By-Reference Operational Guide

## Notable Behavior Changes

TL;DR: Spilling involves less IOPS and is faster.  Peeking involves more IOPS and is slower.  Popping involves >0 IOPS.

### Spilling

The most notable effect of the spilling changes is that the Disk Queue files
will now grow to potentially terrabytes in size.

 1. Spilling will occur in larger batches, which will result in a more
sawtooth-like `BytesInput - BytesDurable` value.  I'm not aware that this will have any meaningful impact.

 * Disk queue files will grow when spilling is happening
   * Alerting based on DQ file size is no longer appropriate

As a curious aside, throughput decreases as spilled volume increases, which
quite possibly worked as accidental backpressure.  As a feature, this no longer
exists, but means write-heavy workloads can drown storage servers faster than
before.

### Peeking

Peeking has seen tremendous changes.  Its involves more IO operations and memory usage.

The expected implication of this are:

1.  A peek of spilled data will involve a burst of IO operations.

		Theoretically, this burst can drown out queued write operations to disk,
		thus and slowing down TLog commits.  This hasn't been observed in testing.

		Low IOPS devices, such as HDD or network attached storage, would struggle
		more here than locally attached SSD.

2.  Generating a peek response of 150KB could require reading 100MB of data, and allocating buffers to hold that 100MB.

		OOMs were observed in early testing.  Code has been added to specifically
		limit how much memory can be allocated for serving a single peek request
		and all concurrent peek requests, with knobs to allow tuning this per
		deployment configuration.

### Popping

Popping will transition from being an only in-memory operation to one that
can involve reads from disk if the popped tag has spilled data.

Due to a strange quirk, TLogs will allocate up to 2GB of memory as a read cache
for SQLite's B-tree.  The expected maximum size of the B-tree has drastically
reduced, so these reads should almost never actually hit disk.  The number of
writes to disk will stay the same, so performance should stay unchanged.

### Disk Queues

This work should have a minimal impact on recovery times, which is why recovery
hasn't been significantly mentioned in this document. However, there are two
minor impacts on recovery times:

1.  Larger disk queue file means more file to zero out in the case of recovery.

    This should be negligible when fallocate `ZERO_RANGE` is available, because then it's only a metadata operation.

2.  A larger file means more bisection iterations to find the first page.

		If we say Disk Queue files are typically ~4GB now, and people are unlikely
		to have more than 4TB drives, then this means in the worst case, another 8
		sequential IOs will need to be done when first recovering a disk queue file
		to find the most recent page with a binary search.

		If this turns out to be an issue, it's trivial to address.  There's no
		reason to do only a binary search when drives support parallel requests.  A
		32-way search could reasonably be done, and would would make a 4TB Disk
		Queue file faster to recover than a 4GB one currently.

3.  Disk queue files can now shrink.

    The particular logic currently used is that:

		If one file is significantly larger than the other file, then it will be
		truncated to the size of the other file.  This resolves situations where a
		particular storage server or remote DC being down causes one DiskQueue file
		to be grown to a massive size, and then the data is rapidly popped.

		Otherwise, If the files are of reasonably similar size, then we'll take
		`pushLocation - popLocation` as the number of "active" bytes, and then
		shrink the file by `TLOG_DISK_QUEUE_SHRINK_BYTES` bytes if the file is
		larger than `active + TLOG_DISK_QUEUE_EXTENSION_BYTES + TLOG_DISK_QUEUE_SHRINK_BYTES`.

## Knobs

`REFERENCE_SPILL_UPDATE_STORAGE_BYTE_LIMIT`
: How many bytes of mutations should be spilled at once in a spill-by-reference TLog.<br>
  Increasing it could increase throughput in spilling regimes.<br>
  Decreasing it will decrease how sawtooth-like TLog memory usage is.<br>

`UPDATE_STORAGE_BYTE_LIMIT`
: How many bytes of mutations should be spilled at once in a spill-by-value TLog.<br>
  This knob is pre-existing, and has only been "changed" to only apply to spill-by-value.<br>

`TLOG_SPILL_REFERENCE_MAX_BATCHES_PER_PEEK`
: How many batches of spilled data index batches should be read from disk to serve one peek request.<br>
  Increasing it will potentially increase the throughput of peek requests.<br>
	Decreasing it will decrease the number of read IOs done per peek request.<br>

`TLOG_SPILL_REFERENCE_MAX_BYTES_PER_BATCH`
: How many bytes a batch of spilled data indexes can be.<br>
  Increasing it will increase TLog throughput while spilling.<br>
	Decreasing it will decrease the latency and increase the throughput of peek requests.<br>

`TLOG_SPILL_REFERENCE_MAX_PEEK_MEMORY_BYTES`
: How many bytes of memory can be allocated to hold the results of reads from disk to respond to peek requests.<br>
  Increasing it will increase the number of parallel peek requests a TLog can handle at once.<br>
	Decreasing it will reduce TLog memory usage.<br>
	If increased, `--memory` should be increased by the same amount.<br>

`TLOG_DISK_QUEUE_EXTENSION_BYTES`
: When a DiskQueue needs to extend a file, by how many bytes should it extend the file.<br>
  Increasing it will reduce metadata operations done to the drive, and likely tail commit latency.<br>
	Decreasing it will reduce allocated but unused space in the DiskQueue files.<br>
  Note that this was previously hardcoded to 20MB, and is only being promoted to a knob.<br>

`TLOG_DISK_QUEUE_SHRINK_BYTES`
: If a DiskQueue file has extra space left when switching to the other file, by how many bytes should it be shrunk.<br>
  Increasing this will cause disk space to be returned to the OS faster.<br>
  Decreasing this will decrease TLog tail latency due to filesystem metadata updates.<br>

## Observability

With the new changes, we must ensure that sufficient information has been exposed such that:

1. If something goes wrong in production, we can understand what and why from trace logs.
2. We can understand if the TLog is performing suboptimally, and if so, which knob we should change and by how much.

The following metrics were added to `TLogMetrics`:

### Spilling

`PersistentDataVersion`
: The version of the last commit that is being spilled to disk. If there's no
  spill in progress, this will be equal to `PersistentDataDurableVersion`.

`PersistentDataDurableVersion`
: The version of the last commit that has been durably spilled to disk.

### Peeking

`PeekMemoryRequestsStalled`
: The number of peek requests that are blocked on acquiring memory for reads.

`PeekMemoryReserved`
: The amount of memory currently reserved for serving peek requests.

### Popping

`QueuePoppedVersion`
: The oldest version that's still useful.

`MinPoppedTagLocality`
: The locality of the tag that's preventing the DiskQueue from being further popped.

`MinPoppedTagId`
: The id of the tag that's preventing the DiskQueue from being further popped.

## Monitoring and Alerting

To answer questions like:

1. What new graphs should exist?
2. What old graphs might exist that would no longer be meaningful?
3. What alerts might exist that need to be changed?
4. What alerts should be created?

Of which I'm aware of:

* Any current alerts on "Disk Queue files more than [constant size] GB" will need to be removed.
* Any alerting or monitoring of `log*.sqlite` as an indication of spilling will no longer be effective.

* A graph of `BytesInput - BytesPopped` will give an idea of the number of "active" bytes in the DiskQueue file.

<!-- Force long-style table of contents -->
<script>window.markdeepOptions={}; window.markdeepOptions.tocStyle="long";</script>
<!-- When printed, top level section headers should force page breaks -->
<style>.md h1, .md .nonumberh1 {page-break-before:always}</style>
<!-- Markdeep: -->
<style class="fallback">body{visibility:hidden;white-space:pre;font-family:monospace}</style><script src="markdeep.min.js" charset="utf-8"></script><script src="https://casual-effects.com/markdeep/latest/markdeep.min.js" charset="utf-8"></script><script>window.alreadyProcessedMarkdeep||(document.body.style.visibility="visible")</script>
