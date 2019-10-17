.. _metrics:

######################################################
Metrics
######################################################

.. _metric-introduction:

Introduction
============

FoundationDB's has different metrics.

### ProcessMetrics

ProcessMetrics are responsible for getting information of events listed:

- Current Connections: Data count connection established
- Connection Established: Number of connection count established per elapsed minutes.
- Connection Closed: Number of open connections closed
- Connection Errors: Number of error received in an open connection
- Memory: Memory size
- Unsued Allocated Memory: Size of the unused allocated memory
- Memory Limit: Max memory limit
- Disk Total Bytes: Total number of bytes present in disk
- Disk Free Bytes: Total number of free bytes present in disk
- Cache Hits: Number of cache hits
- Cache Misses: Number of cache misses
- Elapsed: Elapsed time
- Machine IP: Machine address
- ID: Unique ID
- CPU Seconds: CPU Seconds available
- MainThreadCPUSeconds: CPU seconds for main thread of the process
- UptimeSeconds: Time units for up time of a process
- ResidentMemory: Resident memory size is the portion of memory occupied by the process
  held by the random access memory.
- AllocatedMemory [16-4096]: Allocated memory for from range 16 - 4096 bytes size.
- ApproximateUnusedMemory [16-4096]:  Unused memory from range 16 - 4096 bytes size.
- MbpsSent: Mega bytes per second sent.
- MbpsReceived: Mega bytes per second received.
- DiskTotalBytes: Total bytes present in a disk memory.
- DiskFreeBytes: Total free bytes present in a disk memory.
- DiskQueueDepth: The number of outstanding IOs(read/write) requests waiting for accessing
  disk.
- DiskIdleSeconds: Disk idle seconds.
- DiskReads: Number of disk reads.
- DiskWrites: Number of disk writes.
- DiskReadsCount: Number of disk reads count.
- DiskWritesCount:  Number of disk writes count.
- DiskWriteSectors: Number of disk write sectors.
- DiskReadSectors: Number of disk read sectors.
- FileWrites: Number of file writes.
- FileReads: Number of file reads.
- CacheReadBytes: Number of bytes read from a cache.
- CacheFinds: Number of cache finds.
- CacheWritesBlocked: Number of cache writes blocked.
- CacheReadsBlocked: Number of cache reads blocked.
- CachePageReadsMerged: Number of page reads merged.
- CacheWrites: Number of cache writes.

### Machine Metrics

Machine metrics collects the information about statistics for a machine:

- Mbps Sent: Amount of data sent.
- Mbps Received: Number of mega bytes per second received.
- CPU Seconds: CPU seconds alloted.
- Total Memory: Total memory size.
- Committed Memory: Committed memory.
- Available Memory: Available memory.
- Zone ID
- Machine ID

### Proxy Metrics

Proxy metrics collects the information about machine/server proxies listed:

As the name signify most of the fields are having information for the start in and start
out for proxy setting.

- Machine Address: Machine address
- ID
- Elapsed Time
- txnStartIn: Transaction start in for a proxy
- txnStartOut: Transaction start out for a proxy
- txnStartBatch: Batch request for transaction for a proxy
- txnSystemPriorityStartIn: System priority start in for a proxy
- txnSystemPriorityStartOut
- txnBatchPriorityStartIn: Priorities for batch transaction
- txnBatchPriorityStartOut
- txnDefaultPriorityStartIn: Default priorities for generic proxy transaction
- txnDefaultPriorityStartOut
- txnCommitIn: Transaction Commit in time
- txnCommitVersionAssigned: Commit version assigned for a transaction
- txnCommitResolving: Transaction commit resolve status
- txnCommitResolved: Transaction commit resolved
- txnCommitOut: Transaction commit out time
- txnCommitOutSuccess: Transaction commit out statrus
- txnConflicts: Transaction coherent conflicts
- commitBatchIn: Transaction batch input value
- commitBatchOut: Batch transaction output value
- mutationBytes: number of input bytes, without MVCC accounting
- mutations: Mutations are logs changes.
- conflictRanges: Conflict ranges
- lastAssignedCommitVersion: Last assigned commit version for transaction
- version: Proxy version
- committedVersion: Committed version
- logGroup: Log group assigned for a proxy transaction

### TransactionMetrics

Transaction metrics constitutes information about transactions includes

- Machine Address
- ID
- ReadVersions: Set of read versions
- LogicalUncachedReads: Number of logical uncached reads
- PhysicalReadRequests: Number of physical read requests
- CommittedMutations: Number of committed mutations for a transaction
- CommittedMutationBytes: Number of committted mutation data bytes
- CommitStarted: Timestamp of commit start
- CommitCompleted: Timestamp of commit completed
- TooOld: Transaction marked as too old
- FutureVersions: List of future version
- NotCommitted: Transaction not yet in committed state
- MaybeCommitted: Transaction may be in committed state
- MeanLatency: Mean latency value of a set of transactions
- MedianLatency: Medain latency value of a set of transactions
- Latency90: Latency value with percentil 90
- Latency98: Latency value with percentile 98
- MaxLatency: Max Latency valye
- MeanRowReadLatency: Mean row read latency value
- MedianRowReadLatency: Medain valye for row read latency for a set of transactions
- MaxRowReadLatency: Max row read latency for a set of transactions
- MeanGRVLatency: Mean value of GRV latency
- MedianGRVLatency: Medain value of GRV latency
- MaxGRVLatency: MAx value of GRV latency
- MeanCommitLatency: Mean latency commit value for a set of transactions
- MedianCommitLatency: Medain latency commit value for a set of transactions
- MaxCommitLatency: Max commit latency value for a set of transactions
- MeanMutationsPerCommit: Mean mutations value per commit for a set of transactions
- MedianMutationsPerCommit: Medain mutations per commit
- MaxMutationsPerCommit: Max number of mutations per commit
- MeanBytesPerCommit: Mean consumed bytes per commit
- MedianBytesPerCommit: Medain bytes per commit
- MaxBytesPerCommit: Max bytes per commit


### StorageMetrics

Storage metrics collects information about storage related metrics having the listed
fields:

- Machine Address
- ID
- Elapsed: Elapsed time
- QueryQueue: The query queue for staorage
- getKeyQueries: The get key queries for storage
- getValueQueries: The get value queries for storage
- getRangeQueries: The get range queries for storage
- finishedQueries: The finished queries for storage
- rowsQueried: The number of rows queried
- bytesQueried: The number of bytes queried
- bytesInput: The number of bytes input
- bytesDurable: The number of bytes durabled
- bytesFetched: The number of bytes fetched
- mutationBytes: The number of mutation bytes 
- updateBatches: The update batches request
- updateVersions: The update versions request
- loops: The loops value
- lastTLogVersion: The last TLog version for storage transaction/query
- version: Version assigned
- storageVersion: Storage version assigned
- durableVersion: Durablility version assgined
- desiredOldestVersion: Desired oldest version available
- FetchKeysFetchActive: Fetch active keys list
- FetchKeysWaiting: Fetch waiting keys
- QueryQueueMax: The max size of query queue
- bytesStored: Bytes stored
- kvstoreBytesUsed: The number of bytes present in TLogData having persistent data.
- kvstoreBytesFree: The number of free bytes present in TLogData having persistent data.
- kvstoreBytesAvailable: The number of available bytes present in TLogData having persistent data.
- kvstoreBytesTotal: The total number of bytes present in persistent data
- logGroup: Assigned log group value

### DiskMetrics

Disk Metrics collects metrics about disk operations listed:

- Machine Address
- ID
- ReadOps: Number of read operations performed
- WriteOps: Number of write operations performed
- ReadQueue: Read queue write operations performed
- WriteQueue: Write queue operations performed
- GlobalSQLiteMemoryHighWater

### SpringCleaningMetrics

Spring Clean Metrics collects information about memory cleanup having fields listed:

- Machine Address
- ID
- SpringCleaningCount: Total number of memory pages cleanup
- LazyDeletePages: Number of lazy deletes
- VacuumedPages: Number of vaccumed pages
- SpringCleaningTime: The scheduled time for memory pages cleanup
- LazyDeleteTime: The scheduled time for lazy delete
- VacuumTime: The vaccum time for pages cleaning
