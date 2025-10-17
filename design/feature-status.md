
# FDB Feature Status

|Feature |Maturity |Description |Current Owner |Long Term|
|---|---|---|---|---|
|[HA/Fearless](https://github.com/apple/foundationdb/wiki/Multi-Region-Replication)|production|Since FDB 6.3. It's the multi-region configuration feature where primary region has satellites for storing mutation logs. In case of primary DC failure, the database will automatically fail over to the remote region without data loss.|Apple FDB Team|Supported|
|[DR](https://apple.github.io/foundationdb/backups.html?highlight=disaster#backup-vs-dr)|production|Recommend to consider HA instead, or three datahall mode|Apple FDB Team|Favor HA and will no longer work with backup V2.|
|RocksDB|production|RocksDB storage engine is in production in FDB 7.3|Apple FDB Team|Supported|
|[Redwood](https://apple.github.io/foundationdb/redwood.html)|production|Since FDB 7.1 and in production, but not at Apple. Better performance than SQLite storage engine.  Supports per tenant encryption. This B+ tree implementation needs documentation.|None.|Need a maintainer|
|Gray failure|production|Since FDB 7.1. Automatically detect gray network failures and trigger recovery to heal the cluster.|Apple FDB Team|Supported|
|[Testing Storage Server (TSS)](https://apple.github.io/foundationdb/tss.html)|production|Since FDB 7.1. Running a storage server pairs with identical data (but different engines) and compare their performance and correctness.|Apple FDB Team|Supported|
|[Perpetual Storage Wiggle](https://apple.github.io/foundationdb/perpetual-storage-wiggle.html)|production|Replacing storage servers gradually for storage space reclaimation or storage engine migration.|Apple FDB Team|Supported|
|[FDB K8s Operator](https://github.com/FoundationDB/fdb-kubernetes-operator)|production|Since FDB 6.3 and in a separate repo|Apple FDB Team|Supported|
|[mTLS](https://apple.github.io/foundationdb/tls.html)|production|Since FDB 6.1. Mutual TLS between clients and FDB servers.|Apple FDB Team|Supported|
|Parallel Restore|experimental|Restore V2 backup files and is used in simulation with V2 backup. Not fault tolerant for production use.|Apple FDB Team|Deprecate and remove the feature.|
|Encrpted Backup Files|experimental|Use an encryption key to encrypt backup files.|Apple FDB Team|Supported|
|[V2 backup (partitioned mutation logs)](https://github.com/apple/foundationdb/blob/main/design/backup_v2_partitioned_logs.md)|experimental|Mutation logs are no longer stored separately on storage servers. A new backup worker role pulls mutations from tlogs and uploads to S3, reducing half write bandwidth to tlogs. We are targeting 7.4 release for production use.|Apple FDB Team|Will evolve into range-partitioned mutation logs, i.e., V3 backup.|
|Sharded RocksDB Storage Engine|experimental|Use column family for shards so that data movement, backup and restore can use file-based data copies, instead of going through the storage engine, for performance gains.|Apple FDB Team|Supported|
|[Bulk Loading & Dumping](https://apple.github.io/foundationdb/bulkdump.html)|experimental|Since 7.4. Dumping an idle cluster to S3 and loading data from S3 into an empty cluster.|Apple FDB Team|Will evolve into any keyrange dumping and loading with support to work with live clusters.|
|Version Vector|experimental|An attempt to scale the transaction system so that proxy commits data no longer broadcasts to all tlogs.|Apple FDB Team|TBD|
|gRPC|under development|Adding gRPC endpoints for certain client/server communications, e.g., some fdbcli usage, file transfer to and from fdbserver processes.|Apple FDB Team|Supported|
|Multitenancy|experimental|Multi-tenant support, contributed from the community.|None|Development is incomplete and feature is unowned. Scheduled for deletion.|
|Metacluster|experimental|contributed from the community.|None|Development is incomplete and feature is unowned. Scheduled for deletion.|
|Storage Cache|experimental|contributed from the community. Serve as memory cache for storage servers. This is probably never finished and another solution to read hot shard is using large storage teams.|None|Development is incomplete and feature is unowned. Scheduled for deletion.|
|Blob granule|experimental|contributed from the community. Related to backup.|None|Development is incomplete and feature is unowned. NOTE: This feature has been deleted.|
|ChangeFeed|experimental|contributed from the community.|None|Development is incomplete and feature is unowned. NOTE: This feature has been deleted.|
|Configuration Database|experimental|contributed from the community. Have data consistency issues, probably not finished.|None|Development is incomplete and feature is unowned. Scheduled for deletion.|
|OpenTelemetry|experimental|contributed from the community. Probably not finished.|None|Need a new owner.|
|Dynamic knobs|experimental|Allows changing fdbserver process knob values without requiring a process restart|None|Need a new owner or become deprecated.|
|Tag Throttling|experimental|limits the transaction rate for specific transaction tags|None|Need a new owner or become deprecated.|
|Windows Binary|experimental|FoundationDB on Windows Platform|Community (To be filled out by the the exact company/person)|Unclear if needed.|
