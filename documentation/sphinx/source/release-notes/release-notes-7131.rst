#############
Release Notes
#############

71.3.1
======

Features
--------
- Add HoltLinearSmoother class
- EKP and KMS Health Check
- Adding blob granule restarting tests to use tenants, encryption
- Implement IStorageMetricsService interface
- Make stepSize configurable for preloadApplyMutationsKeyVersionMap
- Add networkoption to disable non-TLS connections
- New ConsistencyScan
- Add WatchMapSize and OutstandingWatches to TransactionMetrics 
- Add blob manifest url and mlogs url in status json. 
- Add tenant_id_prefix to metacluster status
- Support blob granule restore in fdbrestore 
- Move lastFlushTs to BlobGranuleBackupConfig 
- Cache change feeds durably on blob workers
- Support restoring a cluster with a tenant in the error state
- EaR RESTSimKMS Vault 
- EaR: Update KMS URL refresh policy and fix bugs 
- EaR: Add test case to validate decryption with invalid key
- Decouple token bucket knobs for different types of throttlers 
- Adding blob ranges to backup keys 
- Allow reading from system keyspace even with default tenant
- Add ExpectStableThroughput simulation test 
- Simplify GlobalTagThrottler limiting TPS calculation 


Performance
-----------
- Re-enabling change feed coalesce knob
- Tune default GlobalTagThrottler knobs
- Improve performance of TransactionTagCounter
- Update knob values for Storage Quota polling intervals
- Removing verbose logs that are not useful for 71.3
- Remove some unnecessary ref-counting in the PTree
- Adding delta file count before resnapshot to minimize impact of small delta files   
- EaR: reduce metrics logging 
- Made blob metadata load lazily from EKP
- Fix retransmits in corruption check
- Increase coverage for slow tests in TestHarness2
- Dump manifest by using multiple transactions 
- EaR: Optimize logging from GetEncryptCipherKey 
- Speed up BackupCorrectnessClean
- Speed up DR upgrade tests


Reliability
-----------
- Fixing stable feeds check by allowing more time post speedup sim
- Check if var is None before comparing with int
- Handle operation_cancelled properly in configuration database
- Reset connection idle time when restarting connection monitor
- BlobGranuleRestore - skip muations applying if restore target version
- Update SimKmsVault unit test assert checking max encryption keys
- Check serverList before update storage metadata 
- Test use of the metacluster after a restore 
- Decrease the number of records in MutationLogReaderCorrectness 
- EaR: Remove usage of ENABLE_CONFIGURABLE_ENCRYPTION knob
- Only starting bg restarting correctness
- Ignore AutomaticIdempotency tests 
- Remove mockkms dir from the build dir

Fixes
-----

* Make network address returned by SimExternalConnection's dns resolution public to fix resolving an fdb process' ip in simulation
* Transaction could block if system priority not set, which would block BM startup
* Make redwood tests terminate after certain amount of time
* Fix poll and notify bug of opsReadSample
* Fixing BlobGranuleRequests to properly bump read version on retry
* Miscellaneous bug fixes and improvements for GlobalTagThrottler 
* Fix get*OperationCost functions for empty mutations/results
* Coordination server crash on file_not_found error
* Fix RangeResult.readThrough misuse 
* Fix queued relocations missing     
* Stop consistency scanner while restore is in progress
* Fix computeRestoreEndVersion bug when outLogs is null 
* Fix issues when start DataDistributor in mock DD test 
* Remove blobGranuleLockKeys after blob granule restore 
* Fix MoveKeysClean.toml failure
* Fix a dynamic knobs bug where fulfilling a promise could cause it to get deleted
* Fix a couple of incorrect snowflake/release-71.3 protocol versions
* Fix flushing empty range
* Disable all parallel restore tests
* Fix check in getExactRange that determines whether we can return early
* Fix issue with inconsistent coordinator disk queue 
* Fix tenant map update race when applying mutations 
* Fix per-scan-loop throttling to properly throttle in the loop 
* Fix worker server handleIOErrors heap-use-after-free
* Fix blob restore stuck issue 
* Fix bug in FDB MultiVersionTransaction.actor.cpp 
* Disable Change-Feed cache for 71.3 restarting tests

Status
------

Bindings
--------

Other Changes
-------------

Change Log
---------------------
* https://github.com/apple/foundationdb/pull/10409 
* https://github.com/apple/foundationdb/pull/10396 
* https://github.com/apple/foundationdb/pull/10414 
* https://github.com/apple/foundationdb/pull/10332 
* https://github.com/apple/foundationdb/pull/10318 
* https://github.com/apple/foundationdb/pull/10309 
* https://github.com/apple/foundationdb/pull/10416 
* https://github.com/apple/foundationdb/pull/10252
* https://github.com/apple/foundationdb/pull/10350 
* https://github.com/apple/foundationdb/pull/10352
* https://github.com/apple/foundationdb/pull/10368
* https://github.com/apple/foundationdb/pull/10390 
* https://github.com/apple/foundationdb/pull/10355 
* https://github.com/apple/foundationdb/pull/10364
* https://github.com/apple/foundationdb/pull/10417
* https://github.com/apple/foundationdb/pull/10339 
* https://github.com/apple/foundationdb/pull/10415
* https://github.com/apple/foundationdb/pull/10429 
* https://github.com/apple/foundationdb/pull/10438
* https://github.com/apple/foundationdb/pull/10419
* https://github.com/apple/foundationdb/pull/10404 
* https://github.com/apple/foundationdb/pull/10426
* https://github.com/apple/foundationdb/pull/10432 
* https://github.com/apple/foundationdb/pull/10434
* https://github.com/apple/foundationdb/pull/10237
* https://github.com/apple/foundationdb/pull/10446
* https://github.com/apple/foundationdb/pull/10294
* https://github.com/apple/foundationdb/pull/10468
* https://github.com/apple/foundationdb/pull/10298  
* https://github.com/apple/foundationdb/pull/10270
* https://github.com/apple/foundationdb/pull/10481      
* https://github.com/apple/foundationdb/pull/10496         
* https://github.com/apple/foundationdb/pull/10464 
* https://github.com/apple/foundationdb/pull/10493 
* https://github.com/apple/foundationdb/pull/10473 
* https://github.com/apple/foundationdb/pull/10494 
* https://github.com/apple/foundationdb/pull/10427 
* https://github.com/apple/foundationdb/pull/10467 
* https://github.com/apple/foundationdb/pull/10489 
* https://github.com/apple/foundationdb/pull/10483 
* https://github.com/apple/foundationdb/pull/10478 
* https://github.com/apple/foundationdb/pull/10471 
* https://github.com/apple/foundationdb/pull/10457 
* https://github.com/apple/foundationdb/pull/10456 
* https://github.com/apple/foundationdb/pull/10444 
* https://github.com/apple/foundationdb/pull/10422 
* https://github.com/apple/foundationdb/pull/10403 
* https://github.com/apple/foundationdb/pull/10421 
* https://github.com/apple/foundationdb/pull/10506 
* https://github.com/apple/foundationdb/pull/10499 
* https://github.com/apple/foundationdb/pull/10517 
* https://github.com/apple/foundationdb/pull/10514 
* https://github.com/apple/foundationdb/pull/10511 
* https://github.com/apple/foundationdb/pull/10512 
* https://github.com/apple/foundationdb/pull/10519 
* https://github.com/apple/foundationdb/pull/10532 
* https://github.com/apple/foundationdb/pull/10531 
* https://github.com/apple/foundationdb/pull/10539 
* https://github.com/apple/foundationdb/pull/10533 
* https://github.com/apple/foundationdb/pull/10530 
* https://github.com/apple/foundationdb/pull/10536 
* https://github.com/apple/foundationdb/pull/10538 
* https://github.com/apple/foundationdb/pull/10537 
* https://github.com/apple/foundationdb/pull/10544 
* https://github.com/apple/foundationdb/pull/10543 
* https://github.com/apple/foundationdb/pull/10545 
* https://github.com/apple/foundationdb/pull/10549 
* https://github.com/apple/foundationdb/pull/10551 
* https://github.com/apple/foundationdb/pull/10558 
* https://github.com/apple/foundationdb/pull/10571 
* https://github.com/apple/foundationdb/pull/10573 
* https://github.com/apple/foundationdb/pull/10509 
* https://github.com/apple/foundationdb/pull/10560 
* https://github.com/apple/foundationdb/pull/10575 
* https://github.com/apple/foundationdb/pull/10199 
* https://github.com/apple/foundationdb/pull/10580 
* https://github.com/apple/foundationdb/pull/10582 
* https://github.com/apple/foundationdb/pull/10564 
* https://github.com/apple/foundationdb/pull/10606 
* https://github.com/apple/foundationdb/pull/10607 
* https://github.com/apple/foundationdb/pull/10547 
* https://github.com/apple/foundationdb/pull/10612 
* https://github.com/apple/foundationdb/pull/10608 
* https://github.com/apple/foundationdb/pull/10587 
* https://github.com/apple/foundationdb/pull/10634 
* https://github.com/apple/foundationdb/pull/10649 