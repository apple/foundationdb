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
If a user/app grabs a lock on a range, other user/app can read the range but cannot write to the range. Essentially, this lock type is
a read lock. The read lock is exclusive --- A range can have at most one lock by one user. 

Note that normally, the read lock is not exclusive. Multiple users can read the same range at the same time. However, in the context of FDB range lock,
the current read lock is exclusive according to our use case.

On the other hand, there is a concept of write lock in the context of FDB range lock --- If a user/app takes a write lock on a range, 
other user/app cannot do any read nor write. The write lock is exclusive, if a user/app takes the write lock on a range, the range must have not any other lock.
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
Currently, FDB only provides ManagementAPI to lock a range. 
Before a user can lock a range, the user must register its identity to the database.
A range can only be locked by a registered owner.
The user can use the following API to register an identity and lock a range.

Put an exclusive read lock on a range. The range must be within the user key space, aka ``"" ~ \xff``.

``ACTOR Future<Void> takeExclusiveReadLockOnRange(Database cx, KeyRange range, RangeLockOwnerName ownerUniqueID);``

Release an exclusive read lock on a range. The range must be within the user key space, aka ``"" ~ \xff``.

``ACTOR Future<Void> releaseExclusiveReadLockOnRange(Database cx, KeyRange range, RangeLockOwnerName ownerUniqueID);``

Note that takeExclusiveReadLockOnRange and releaseExclusiveReadLockOnRange are transactional. 
If the execution of the API is successful, all ranges are guaranteed to be locked/unlocked at a single version.

Get exclusive read locks on the input range

``ACTOR Future<std::vector<KeyRange>> getExclusiveReadLockOnRange(Database cx, KeyRange range);``

Register a range lock owner to database metadata.

``ACTOR Future<Void> registerRangeLockOwner(Database cx, std::string uniqueId, std::string description);``

Remove an owner from the database metadata

``ACTOR Future<Void> removeRangeLockOwner(Database cx, std::string uniqueId);``

Get all registered range lock owners

``ACTOR Future<std::vector<RangeLockOwner>> getAllRangeLockOwners(Database cx);``

Get a range lock owner by uniqueId

``ACTOR Future<Optional<RangeLockOwner>> getRangeLockOwner(Database cx, std::string uniqueId);``


Example usage
-------------
Blocking user write traffic for bulk load based on range lock.

``ACTOR Future<Void> setBulkLoadSubmissionTransaction(Transaction* tr, BulkLoadTaskState bulkLoadTask);``

Unblocking user write traffic for bulk load based on range lock.

``ACTOR Future<Void> setBulkLoadFinalizeTransaction(Transaction* tr, KeyRange range, UID taskId);``

Range Lock Design (Exclusive Read Lock)
=============================
The range lock information is persisted in ``\xff/rangeLock/`` system key space.
Users specify ranges to lock in ``\xff/rangeLock/`` system key space via a transaction. 
The range lock can be only within user key space, aka ``"" ~ \xff``.
The value within the key space is either empty or a set of locks.
Note that the design is specific to the exclusive read lock, however the metadata is extensible to the non-exclusive read lock and the write lock.
If a range has an exclusive read lock, the cluster will block any transaction that writes to the range. 
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

* ChangeFeed: RangeLock can trigger assertion failure. The reason is unclear yet.

* Version vector: Version vector has a different path of updating metadata at proxies than the default one. Therefore, rangeLock temporarily is not available when the version vector is on.

* Encryption and tenant: Currently, RangeLock does not have a clear functionality in the context of encryption and tenant, so when the encryption and tenant are on, we disable rangeLock for the clarity.

