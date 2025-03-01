##############################
FoundationDB Per-Range Lock (WiP)
##############################

| Author: Zhe Wang
| Reviewer: Jingyu Zhou
| Audience: FDB developers, SREs and expert users.


Overview
========
Per-Range Lock is a feature that allows users to shut down write traffic to a specific range of keys in FoundationDB (FDB).
If a user/app grabs a lock on a range, other user/app can read the range but cannot write to the range. Essentially, this lock type is
a readLock. The read lock is not exclusive --- A range can have multiple locks by different users. 

On the other hand, there is a concept of write lock in the context of FDB per-range lock --- If a user/app takes a writeLock on a range, 
other user/app cannot do any read nor write. The write lock is exclusive, if a user/app takes the writeLock on a range, the range must have not any other lock.
Currently, we only implemented the readLock. The writeLock is currently not implemented. we will implement the writeLock later on demand. 

Example use cases
-----------------
Currently, BulkLoad feature is an example of using the per-range lock. 
BulkLoad is developed to load a large amount of data into a FDB range without going through the transaction system.
As a result, BulkLoad must ensure the correctness during the data injection in the presence of user traffic. 
To achieve this, BulkLoad locks the range that it is going to write to, and then it writes the data to the range.
After the data injection is done, BulkLoad releases the lock.

How to use?
-----------
Currently, FDB only provides ManagementAPI to lock a range. 
Before a user can lock a range, the user must register its identity to the database.
A range can only be locked by a registered owner.
The user can use the following API to register an identity and lock a range.

Lock a range

``ACTOR Future<Void> takeReadLockOnRange(Database cx, KeyRange range, std::string ownerUniqueID);``

Unlock a user range

``ACTOR Future<Void> releaseReadLockOnRange(Database cx, KeyRange range, std::string ownerUniqueID);``

Get locked ranges within the input range

``ACTOR Future<std::vector<KeyRange>> getReadLockOnRange(Database cx, KeyRange range);``

Register a rangeLock owner to database metadata.

``ACTOR Future<Void> registerRangeLockOwner(Database cx, std::string uniqueId, std::string description);``

Remove an owner form the database metadata

``ACTOR Future<Void> removeRangeLockOwner(Database cx, std::string uniqueId);``

Get all registered rangeLock owner

``ACTOR Future<std::vector<RangeLockOwner>> getAllRangeLockOwners(Database cx);``

Get the owner of a rangeLock

``ACTOR Future<Optional<RangeLockOwner>> getRangeLockOwner(Database cx, std::string uniqueId);``


Example usage
-------------
Turn off user traffic for bulk load based on range lock

``ACTOR Future<Void> turnOffUserWriteTrafficForBulkLoad(Transaction* tr, KeyRange range);``

Turn on user traffic for bulk load based on range lock

``ACTOR Future<Void> turnOnUserWriteTrafficForBulkLoad(Transaction* tr, KeyRange range);``


Per-Range Lock Design
=====================
The per-range lock information is persisted in ``\xff/rangeLock/`` system key space.
Users specify ranges to lock in ``\xff/rangeLock/`` system key space via a transaction. 
The range lock can be only within user key space, aka ``"" ~ \xff``.
The value of the key space is either empty or setting as a set of locks. 
If a range having a ReadLock, the cluster will reject any mutation within the range. 
If the range value is empty, the cluster does not reject the traffic as the range is unlocked.

The per-range lock API issues transactions to update the ``\xff/rangeLock/`` system metadata. 
When the mutation arrives at commit proxies, the commit proxies keep the in-memory key range map of range locking status, 
and they update the map when applying the metadata mutation. 
When following normal mutation arrives at the commit proxy, the commit proxy checks the range locking status in the in-memory map.
If the mutation is within a locked range, the commit proxy rejects the mutation.
This process happens in the phase of postResolution, where the commit proxy scans all transactions. 
In addition, the lockRange mutation is also persisted to txnStateStore. When recovery, a new commit proxy will rebuild the in-memory lockRange 
map based on the status persisted in txnStateStore.

When a locking mutation arrives at a commit proxy, 
the lock takes effect on all mutations that arrive at any commit proxy after this locking mutation. 
Note that it is possible that this locking mutation is batched with other mutations with the same commit version. 
The locking mutation takes effect on all mutations after the locking mutation in the same batch.
To achieve this, the locking transaction adds a write_conflict_range on the entire user key space (``"" ~ \xff``).
As a result, any following mutation to the locked range will be marked as ``Conflict``. 

Support multiple range lock users
---------------------------------
To support rangeLock for multiple applications, we add ownership concept to rangeLock. Any range can have multiple locks with different owners. 
An owner can only unlock its own rangeLock. A mutation will be reject if it goes to a range that has a rangeLock with an owner. 
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

