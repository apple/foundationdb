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

Range locks are a trusted administrative write-exclusion mechanism, not an authorization boundary.
Transactions using ``LOCK_AWARE`` bypass range locks; registering an owner does not grant a separate database identity.
The feature remains experimental. Enabling it on an existing database requires the upgrade and reconciliation procedure below.

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
The release request is rejected with a range_unlock_reject error if the range contains any existing lock with a different range, user, or lock type.

``ACTOR Future<Void> releaseExclusiveReadLockOnRange(Database cx, KeyRange range, RangeLockOwnerName ownerUniqueID);``

Note that takeExclusiveReadLockOnRange and releaseExclusiveReadLockOnRange are transactional. 
If the execution of the API is successful, all ranges are guaranteed to be locked/unlocked at a single version.
If the execution is failed, no range is locked/unlocked.

Get exclusive read locks on the input range

``ACTOR Future<std::vector<std::pair<KeyRange, RangeLockState>>> findExclusiveReadLockOnRange(Database cx, KeyRange range);``

Register a range lock owner to database metadata.

``ACTOR Future<Void> registerRangeLockOwner(Database cx, RangeLockOwnerName ownerUniqueID, std::string description);``

Remove an owner from the database metadata. An owner that still holds a lock cannot be removed.

``ACTOR Future<Void> removeRangeLockOwner(Database cx, RangeLockOwnerName ownerUniqueID);``

Get all registered range lock owners

``ACTOR Future<std::vector<RangeLockOwner>> getAllRangeLockOwners(Database cx);``

Get a range lock owner by uniqueId

``ACTOR Future<Optional<RangeLockOwner>> getRangeLockOwner(Database cx, RangeLockOwnerName ownerUniqueID);``

Fenced ownership
----------------
The name-only take and release APIs are administrative operations. They cannot distinguish an old caller from a new caller that reuses the same owner name and range. Long-running work should use a fenced acquisition instead:

1. Read the registered ``RangeLockOwner`` and retain its ``getGeneration()`` value.
2. Construct a ``RangeLockState`` with a fresh, non-empty acquisition ID that will never be reused.
3. Call ``takeExclusiveReadLockOnRange`` with that state and the expected owner generation.
4. Retain the exact state and pass it to the fenced ``releaseExclusiveReadLockOnRange`` overload.

Retried takes are idempotent only for the same acquisition. A stale release cannot remove a replacement lock, even if its owner name and range are identical. The expected-owner overload of ``removeRangeLockOwner`` similarly protects against owner-name reuse. None of these operations gives a lock an automatic expiration time.

BulkLoad persists the exact acquisition alongside the active job. Data-distributor task submission, cancellation, and finalization check that acquisition before changing the job or releasing its lock. A source dump ID can be loaded more than once, so it is not itself a submission token. Normal administrative release commands refuse to remove an active BulkLoad job's fence. Prefer ``bulkload cancel <JOBID>``. The ``rangelock force-release`` and ``rangelock force-release-all`` commands are emergency operations that can invalidate a running job; use them only after coordinating with the job owner.


Using ``fdbcli``
----------------
For ad-hoc operational use — inspecting active locks, releasing a lock left behind by a failed bulkload job, taking a lock for a maintenance window, or driving a perf test — ``fdbcli`` exposes a thin wrapper around the management API:

::

    rangelock status
    rangelock reconcile [<MIGRATION_ID>]
    rangelock register <OWNER_ID> <DESCRIPTION>
    rangelock unregister <OWNER_ID>
    rangelock owners
    rangelock take <BEGIN_KEY> <END_KEY> <OWNER_ID>
    rangelock release <BEGIN_KEY> <END_KEY> <OWNER_ID>
    rangelock release-all <OWNER_ID>
    rangelock list [<BEGIN_KEY> <END_KEY>]
    rangelock force-release <BEGIN_KEY> <END_KEY> <OWNER_ID>
    rangelock force-release-all <OWNER_ID>

``rangelock status`` reports the durable readiness state and probes every currently advertised commit proxy. A new acquisition requires Ready state and ``knob_enable_read_lock_on_range=true`` on every current commit proxy. Unsupported modes or incomplete readiness return ``range_lock_not_ready`` (1256); a take does not report success merely because metadata was written. Turning the knob off stops new acquisitions but does not disable enforcement or prevent release of an existing lock.

The bulkload-specific commands (``bulkload addlockowner`` / ``bulkload clearlock`` / ``bulkload printlockowner``) remain available; they are a constrained subset of the above scoped to the bulkload workflow.

Activation, reconciliation, and downgrade
----------------------------------------
The system key ``\xff/rangeLockConfiguration`` records one of three states:

* **Unknown:** the marker is absent, as on a database created by an older binary. Its transaction-state lock map may be incomplete or stale. New locks are rejected. With acquisition disabled, the absence of the marker does not itself stop ordinary writes; enabling acquisition before reconciliation makes ordinary writes fail closed.
* **Migrating:** an exact database-lock UID and a durable cursor identify an interrupted or running reconciliation. Ordinary writes and unrelated range-lock changes are blocked.
* **Ready:** the complete lock map has been initialized or reconciled. Existing locks are enforced independently of the acquisition knob.

The durable marker alone is not sufficient if recovery detects malformed legacy lock boundaries. ``rangelock status`` also reports whether every current proxy has valid enforcement state. An invalid recovered map blocks ordinary writes and lock changes even with acquisition disabled, but leaves the guarded reconciliation path available. Reconciliation can repair bad transaction-state-only rows from valid storage metadata; malformed authoritative storage data requires operator investigation and is not silently discarded.

New databases initialize Ready. For an existing database, use this rollout sequence:

1. Stop new range-lock and BulkLoad submissions. Upgrade all commit proxies, resolvers, and cluster controllers to the new implementation. Keep the relevant server knobs consistent. This is a homogeneous transaction-system upgrade; the proxy status probe does not establish the versions of the other roles.
2. Run ``rangelock status``. If the state is Unknown, run ``rangelock reconcile`` and retain the printed migration UID. Reconciliation takes a database-wide lock, rebuilds transaction-state metadata from the authoritative storage-server lock map in bounded transactions, and releases its database lock only after publishing Ready.
3. If interrupted, resume with ``rangelock reconcile <MIGRATION_ID>``. The saved cursor survives recovery. Do not use a generic database unlock to bypass an unfinished migration.
4. Enable ``knob_enable_read_lock_on_range=true`` consistently and check ``rangelock status`` before admitting new users. New acquisitions currently require version vectors and version-vector TLog unicast to be disabled. BulkLoad additionally checks shard-location metadata encoding on the current commit proxies and data distributor; keep that knob consistent on replacement roles as well.

Reconciliation is also available for an already Ready database and with acquisition disabled. It preserves the logical storage-server map, including unlocked intervals, while repairing missing or stale transaction-state boundaries. A failure after migration begins deliberately leaves the database lock held for a same-UID resume. Repeating a completed migration with its original UID is safe. If recovery later finds invalid state in a Ready database, start a new reconciliation with a fresh UID; retrying an already-completed UID does not reset that state. An invalid in-progress migration retains its lock and cursor and requires operator investigation.

This change does not bump the network protocol or make old binaries enforce the new readiness rules. Before downgrading to an older implementation, stop new acquisitions, finish or cancel protected jobs, release every lock, and complete any active reconciliation while all transaction-system roles still run the new code. Do not run active protected work through a mixed old/new transaction system.


Example usage
-------------
When submitting a bulk load task on a range, we block user write traffic to the range.

``ACTOR Future<Void> setBulkLoadSubmissionTransaction(Transaction* tr, BulkLoadTaskState bulkLoadTask);``

Upon a bulk load task completes on a range, we unblock user write traffic on the range.

``ACTOR Future<Void> setBulkLoadFinalizeTransaction(Transaction* tr, KeyRange range, UID taskId);``

Range Lock Design (Exclusive Read Lock)
=======================================
The ``\xff/rangeLock/`` system-key range stores a key-range map over normal keys. Each interval contains an empty value or a set of locks. Lock identity is compared using the decoded owner, type, range, and, for fenced operations, acquisition ID. Display strings are not sufficient to establish ownership.

Every transaction-system context persists range-lock metadata into ``txnStateStore``, including when acquisition is disabled. Commit proxies rebuild their in-memory maps from that durable state during recovery. The maps retain the exact raw KRM boundaries: coalescing equal-valued boundaries would change the effect of a later raw boundary clear.

Ordinary mutation checks intersect the mutation range with normal keys, including a clear whose end is exactly ``\xff``. Transactions containing both transaction-system metadata and ordinary writes need an earlier decision: rejecting them only after applying metadata would violate atomicity. Such a transaction is checked against the proxy's known lock-state version and receives a resolver-0 read-conflict certificate. Every lock-map, readiness, or database-lock change writes the corresponding server-added fence. A stale certificate conflicts before metadata or resolver-private mutations take effect. A certified transaction is not retroactively rejected by a later lock in the same batch.

Lock-management transactions must use ``LOCK_AWARE`` and must not contain ordinary-key mutations or ordinary-key read conflicts. Normal-key write conflicts remain allowed. The server validates readiness transitions and reconciliation pages before resolution; a raw metadata caller cannot skip the replay cursor or publish Ready early. In resolver-private-mutation mode, system-only management reads are decided by resolver 0 so another resolver cannot invalidate an already-applied management effect.

Steady-state cost when no locks are held
----------------------------------------
When readiness permits ordinary writes and no exclusive read lock is held, ``rejectMutationsForReadLockOnRange`` skips its per-mutation loop. Each proxy maintains a count of exclusive-lock boundaries as raw KRM mutations are applied or recovered. A non-ready state that requires write rejection cannot take this fast path.

Two ``ProxyMetrics`` counters expose which path the proxy took:

* ``RangeLockFastPath`` increments once per commit batch when the early return fired (no locks held).
* ``RangeLockSlowPath`` increments once per commit batch when the per-mutation check loop ran (at least one lock held).

Operators can use these counters to check that the fast path resumes after the last release. Each batch increments one of the two counters.

Correctness across proxies
--------------------------
Every proxy applies the same committed transaction-state mutation stream in version order. Resolver certificates protect admission decisions made against an older local prefix of that stream. Recovery restores both the readiness marker and raw lock boundaries before normal commit processing. During reconciliation the database lock and server-side transition checks prevent ordinary writes from observing the temporarily incomplete rebuilt map.

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
An ordinary transaction that mutates a locked range receives ``transaction_rejected_range_locked`` (1242). Its user and transaction-system metadata effects are rejected together. ``Transaction.onError`` can retry this error when ``TRANSACTION_LOCK_REJECTION_RETRIABLE`` is enabled. A caller should not assume the lock will expire automatically.

``range_lock_not_ready`` (1256) means acquisition is disabled, readiness is incomplete, or a lock-management transition is invalid. Resolve the configuration or migration problem before retrying. A new BulkLoad submission reports an unmet readiness, admission, or shard-encoding prerequisite as ``bulkload_invalid_configuration`` (1250).

Compatibility
-------------
* Database lock: ``LOCK_AWARE`` maintenance transactions retain their trusted bypass. Reconciliation additionally requires the exact database-lock UID and prevents unrelated unlocks until the final guarded transition.

* Backup and restore: non-lock-aware restores can encounter ordinary range-lock rejection. Lock-aware internal restore paths bypass the exclusion and must be coordinated with the protected operation. A new BulkLoad restore checks the live prerequisites before queuing work or acquiring the database lock. It accepts success only from matching-acquisition terminal Complete history, not merely from disappearance of the active job.

* Version vectors: new acquisition is disabled while version vectors or version-vector TLog unicast are enabled. Already Ready locks remain enforced and can be released after such a mode change. Resolver-private mutations use the same admission and conflict-fence rules.

* BulkLoad: enabling dispatch requires Ready state and shard-location metadata encoding on the current commit proxies and data distributor; creating a new job also requires new-lock admission. An existing fenced job can be inspected and drained with acquisition disabled. Storage engines without direct SST ingestion can use the existing key/value ingestion path.
