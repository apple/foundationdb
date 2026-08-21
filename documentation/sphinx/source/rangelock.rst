###################################
FoundationDB Range Locks (WiP)
###################################

| Author: Zhe Wang
| Reviewer: Jingyu Zhou
| Audience: FDB developers, SREs and expert users.

Overview
========
Range Lock is a feature that blocks write traffic to a specific key range in FoundationDB (FDB).
The locked range must be within the user key space, aka ``"" ~ \xff``.
If a user grabs a lock on a range, other users can read the range but cannot write to the range. 
A range can have at most one lock by one user. 
Note that the "user" here is not an user of the database, but an application or a feature that uses the range lock.
In this document, we use "user" to represent the application or feature that uses the range lock.

Comparison with general locking concepts
----------------------------------------
The range lock is similar to a "read lock" --- when a user wants to do read, the user grabs a read lock which prevents other users
to write to the locked data while the lock does not block any read operation from other users. 
However, the range lock is different from a "read lock". 
Normally, the read lock is not exclusive. Multiple users can read the same range at the same time. However, in the context of FDB range lock,
the current read lock is exclusive. A range can have at most one lock of a user. 
We will implement the non-exclusive read lock later on demand.

On the other hand, there is a concept of write lock in the context of FDB range lock --- If a user takes a write lock on a range, 
other users cannot do any read nor write. The write lock is exclusive, if a user takes the write lock on a range, the range must have not any other lock.
Currently, we only implemented the read lock. The write lock is currently not implemented. we will implement the write lock later on demand. 

Example use cases
-----------------
Currently, BulkLoad feature is an example of using the range lock. 
BulkLoad is developed to load a large amount of data into a FDB range without going through the transaction system.
As a result, BulkLoad must ensure the correctness during the data injection in the presence of user traffic. 
To achieve this, BulkLoad locks the range to prevent user traffic from writing to the database, and then loads the range data directly into storage servers.
After the data injection is done, BulkLoad releases the lock.
Ideally, we would use the write lock to achieve this; however, we are currently using the exclusive read lock as a temporary solution.

How to use?
-----------
Currently, FDB provides the ManagementAPI for range locking, intended as an interface for FDB feature development.
Before locking a range, a user must first register their identity with the database.
Only registered users are permitted to acquire range locks.
The following API can be used to register an identity and lock a range.

Put an exclusive read lock on a range. The range must be within the user key space, aka ``"" ~ \xff``.
The locking request is rejected with a range_lock_reject error if the range contains any existing lock with a different range, user, or lock type.
Currently, only the ExclusiveReadLock type is supported, but the design allows for future extension.

``ACTOR Future<Void> takeExclusiveReadLockOnRange(Database cx, KeyRange range, RangeLockOwnerName ownerUniqueID);``

Release an exclusive read lock on a range. The range must be within the user key space, aka ``"" ~ \xff``.
The release request is rejected with a range_lock_reject error if the range contains any existing lock with a different range, user, or lock type.

``ACTOR Future<Void> releaseExclusiveReadLockOnRange(Database cx, KeyRange range, RangeLockOwnerName ownerUniqueID);``

Note that takeExclusiveReadLockOnRange and releaseExclusiveReadLockOnRange are transactional. 
If the execution of the API is successful, all ranges are guaranteed to be locked/unlocked at a single version.
If the execution is failed, no range is locked/unlocked.

Get exclusive read locks on the input range

``ACTOR Future<std::vector<std::pair<KeyRange, RangeLockState>>> findExclusiveReadLockOnRange(Database cx, KeyRange range);``

Register a range lock owner to database metadata.

``ACTOR Future<Void> registerRangeLockOwner(Database cx, RangeLockOwnerName ownerUniqueID, std::string description);``

Remove an owner from the database metadata

``ACTOR Future<Void> removeRangeLockOwner(Database cx, RangeLockOwnerName ownerUniqueID);``

Get all registered range lock owners

``ACTOR Future<std::vector<RangeLockOwner>> getAllRangeLockOwners(Database cx);``

Get a range lock owner by uniqueId

``ACTOR Future<Optional<RangeLockOwner>> getRangeLockOwner(Database cx, RangeLockOwnerName ownerUniqueID);``


Using ``fdbcli``
----------------
For ad-hoc operational use — inspecting active locks, releasing a lock left behind by a failed bulkload job, taking a lock for a maintenance window, or driving a perf test — ``fdbcli`` exposes a thin wrapper around the management API:

::

    rangelock register <OWNER_ID> <DESCRIPTION>
    rangelock unregister <OWNER_ID>
    rangelock owners
    rangelock take <BEGIN_KEY> <END_KEY> <OWNER_ID>
    rangelock release <BEGIN_KEY> <END_KEY> <OWNER_ID>
    rangelock release-all <OWNER_ID>
    rangelock list [<BEGIN_KEY> <END_KEY>]

Range locks only take effect when commit proxies are started with ``knob_enable_read_lock_on_range=true``. ``rangelock take`` prints an advisory notice as a reminder, but cannot probe the server-side knob — the take itself succeeds either way; the lock simply has no effect when the knob is off.

The bulkload-specific commands (``bulkload addlockowner`` / ``bulkload clearlock`` / ``bulkload printlockowner``) remain available; they are a constrained subset of the above scoped to the bulkload workflow.


Example usage
-------------
When submitting a bulk load task on a range, we block user write traffic to the range.

``ACTOR Future<Void> setBulkLoadSubmissionTransaction(Transaction* tr, BulkLoadTaskState bulkLoadTask);``

Upon a bulk load task completes on a range, we unblock user write traffic on the range.

``ACTOR Future<Void> setBulkLoadFinalizeTransaction(Transaction* tr, KeyRange range, UID taskId);``

Range Lock Design (Exclusive Read Lock)
=======================================
The range lock information is persisted in ``\xff/rangeLock/`` system key space.
Users specify ranges to lock in ``\xff/rangeLock/`` system key space via a transaction. 
The range lock can be only within user key space, aka ``"" ~ \xff``.
The value within the key space is either empty or a set of locks.
Note that the design is specific to the exclusive read lock, however the metadata is extensible to the non-exclusive read lock and the write lock.
If a range has a lock, the cluster will block any transaction that writes to the range. 
If the range value is empty, the cluster does not reject the traffic as the range is unlocked.

The range lock API issues transactions to update the ``\xff/rangeLock/`` system metadata. 
When the mutation arrives at commit proxies, each commit proxy keeps an in-memory key range map of range locking status,  
and they update the maps when applying the metadata mutation.
When following normal mutation arrives at the commit proxy, the commit proxy checks the range locking status in the in-memory map.
If the mutation is within a locked range, the commit proxy rejects the transaction containing the mutation.
This process happens in the phase of postResolution, where the commit proxy scans all transactions. 
In addition, the lockRange mutation is also persisted to txnStateStore. When recovery, a new commit proxy will rebuild the in-memory lockRange 
map based on the status persisted in txnStateStore.

When a locking mutation arrives at a commit proxy, 
the lock takes effect on all mutations that arrive at any commit proxy after this locking mutation. 
Note that it is possible that this locking mutation is batched with other mutations with the same commit version. 
The locking mutation takes effect on all mutations after the locking mutation in the same batch.
To achieve this, the locking transaction adds a write_conflict_range on the lock range.
As a result, any following transactions in the batch that writes to the locked range will be marked as ``Conflict``.

Steady-state cost when no locks are held
----------------------------------------
The per-mutation lookup described above runs only when at least one exclusive read lock is held cluster-wide. Each commit proxy tracks an ``anyExclusiveLockHeld_`` flag on its ``RangeLock`` struct, refreshed when the lock set changes (during ``consumePendingRequest`` and recovery's ``initKeyPoint``). When the flag is false — the steady state for any cluster running with ``--knob_enable_read_lock_on_range=1`` but no active bulkload — ``rejectMutationsForReadLockOnRange`` short-circuits at the top of the function and the per-mutation work is skipped entirely.

Two ``ProxyMetrics`` counters expose which path the proxy took:

* ``RangeLockFastPath`` increments once per commit batch when the early return fired (no locks held).
* ``RangeLockSlowPath`` increments once per commit batch when the per-mutation check loop ran (at least one lock held).

Operators can use these counters to confirm in production that the optimization is firing, and to detect the silent-degradation case where the flag fails to clear after a release. In normal operation only one counter advances at a time per proxy.

Correctness across proxies
--------------------------
The ``anyExclusiveLockHeld_`` flag is per-proxy and never directly synchronized between proxies. Convergence comes from the ``txnStateStore`` mutation broadcast already used by the in-memory ``coreMap``: every proxy applies the same ``\xff/rangeLock/`` mutation stream in commit-version order, so each proxy's flag value at version V is a deterministic function of the same prefix of the log.

* **Within a single batch.** ``applyMetadataMutations`` (Phase 3a) runs before ``rejectMutationsForReadLockOnRange`` (Phase 3b). If the batch took a lock, ``consumePendingRequest`` has already set the flag by the time the reject loop reads it. Mutations ordered after the lock in the same batch are caught by the existing ``write_conflict_range`` mechanism, not by the flag.

* **Across batches.** A proxy never starts processing batch ``V+1`` until version ``V``\ 's metadata mutations are applied locally. So when batch ``V+1`` reaches Phase 3b, every lock taken at version ``<= V`` is reflected in the flag.

* **During recovery.** ``initKeyPoint`` is monotonic-up — it only sets the flag to true. Combined with ``consumePendingRequest``\ 's post-coalesce full recompute at runtime, the flag can be temporarily stuck at true (harmless: the slow path runs, finds no locks, rejects nothing — extra CPU, no behavior change) but cannot be stuck at false while locks are actually held. Stuck-true is observable through the ``RangeLockFastPath`` counter; stuck-false would manifest as missing ``transaction_rejected_range_locked`` rejections, which existing simulation workloads (``tests/fast/RangeLocking.toml``, ``tests/fast/RangeLockCycle.toml``) already assert against.

The flag is therefore a layer on top of an already-coordinated invariant — the consistency of ``coreMap`` itself across proxies — rather than introducing a new coordination requirement of its own.

Support multiple range lock users
---------------------------------
To support rangeLock for multiple applications, we add ownership concept to rangeLock. 
In the context of the exclusive read lock, if a range is locked by a user using the exclusive read lock, 
the range cannot be locked by a different user until the range is unlocked by the user.
An owner can only unlock its own rangeLock. A mutation will be rejected if it updates keys in a range that has a rangeLock with an owner. 
An owner can lock a range if and only if the owner has registered its identity (including uniqueID string and description string) to the database. 
The identity is persisted to the system metadata (``\xff/rangeLockOwner/``).

Transaction error handling
--------------------------
If a transaction has a mutation accessing to a locked range, the proxy will mark the transaction as rejected and reply client with transaction_rejected_range_locked error. 
Transaction.onError can automatically retry with this error code, similar to other mutation lock/throttling mechanisms.

Compatibility
-------------
* Database lock: RangeLock is transparent to the database lock. When the database lock is on, the rangeLock metadata transaction with LockAware can still update the rangeLock metadata, but rangeLock does not reject any transaction.

* Backup and restore: RangeLock can cause losing mutations when restoring. Restoring should automatically detect the failure due to rangeLock and self-retry from a clean state.

* Version vector: Version vector has a different path of updating metadata at proxies than the default one. Therefore, rangeLock temporarily is not available when the version vector is on.

* Encryption: Currently, RangeLock does not have a clear functionality in the context of encryption, so when encryption is enabled, we disable rangeLock for clarity.
