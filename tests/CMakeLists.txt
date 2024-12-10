include(AddFdbTest)

# We need some variables to configure the test setup
set(ENABLE_BUGGIFY ON CACHE BOOL "Enable buggify for tests")
set(ENABLE_SIMULATION_TESTS OFF CACHE BOOL "Enable simulation tests (useful if you can't run Joshua)")
set(RUN_IGNORED_TESTS OFF CACHE BOOL "Run tests that are marked for ignore")
set(TEST_KEEP_LOGS "FAILED" CACHE STRING "Which logs to keep (NONE, FAILED, ALL)")
set(TEST_KEEP_SIMDIR "NONE" CACHE STRING "Which simfdb directories to keep (NONE, FAILED, ALL)")
set(TEST_AGGREGATE_TRACES "NONE" CACHE STRING "Create aggregated trace files (NONE, FAILED, ALL)")
set(TEST_LOG_FORMAT "xml" CACHE STRING "Format for test trace files (xml, json)")
set(TEST_INCLUDE ".*" CACHE STRING "Include only tests that match the given regex")
set(TEST_EXCLUDE ".^" CACHE STRING "Exclude all tests matching the given regex")
set(SANITIZER_OPTIONS "UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/contrib/tsan.suppressions;LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/contrib/lsan.suppressions" CACHE STRING "Environment variables setting sanitizer options")

# for the restart test we optimally want to use the last stable fdbserver
# to test upgrades
if(WITH_PYTHON)
  find_program(OLD_FDBSERVER_BINARY
    fdbserver-7.1.23 fdbserver fdbserver.exe
    HINTS /opt/foundationdb/old/7.1.23/bin/ /opt/foundationdb/old /usr/sbin /usr/libexec /usr/local/sbin /usr/local/libexec)
  if(OLD_FDBSERVER_BINARY)
    message(STATUS "Use old fdb at ${OLD_FDBSERVER_BINARY}")
  else()
    if(WIN32)
      set(fdbserver_location ${CMAKE_BINARY_DIR}/bin/@CTEST_CONFIGURATION_TYPE@/fdbserver.exe)
      set(OLD_FDBSERVER_BINARY ${fdbserver_location} CACHE FILEPATH "Old fdbserver binary" FORCE)
    else()
      set(fdbserver_location ${CMAKE_BINARY_DIR}/bin/fdbserver)
      set(OLD_FDBSERVER_BINARY ${fdbserver_location} CACHE FILEPATH "Old fdbserver binary" FORCE)
    endif()

    message(WARNING "\
  No old fdbserver binary found - using ${fdbserver_location} \
  It is recommended to install the current stable version from https://github.com/apple/foundationdb/releases \
  Or provide a path to another fdbserver")
  endif()

  configure_file(${PROJECT_SOURCE_DIR}/tests/TestRunner/fdb_version.py.cmake ${PROJECT_BINARY_DIR}/tests/TestRunner/fdb_version.py)

  set(TestRunner "${PROJECT_SOURCE_DIR}/tests/TestRunner/TestRunner.py")

  configure_file(${PROJECT_SOURCE_DIR}/tests/CTestCustom.ctest.cmake ${PROJECT_BINARY_DIR}/CTestCustom.ctest @ONLY)

  configure_testing(TEST_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    ERROR_ON_ADDITIONAL_FILES
    IGNORE_PATTERNS ".*/CMakeLists.txt" ".*/requirements.txt")

  add_fdb_test(TEST_FILES AsyncFileCorrectness.txt UNIT IGNORE)
  add_fdb_test(TEST_FILES AsyncFileMix.txt UNIT IGNORE)
  add_fdb_test(TEST_FILES AsyncFileRead.txt UNIT IGNORE)
  add_fdb_test(TEST_FILES AsyncFileReadRandom.txt UNIT IGNORE)
  add_fdb_test(TEST_FILES AsyncFileWrite.txt UNIT IGNORE)
  add_fdb_test(TEST_FILES BackupContainers.txt IGNORE)
  add_fdb_test(TEST_FILES s3VersionHeaders.txt IGNORE)
  add_fdb_test(TEST_FILES BandwidthThrottle.txt IGNORE)
  add_fdb_test(TEST_FILES BigInsert.txt IGNORE)
  add_fdb_test(TEST_FILES BGServerCommonUnit.toml)
  add_fdb_test(TEST_FILES BlobGranuleFileUnit.toml)
  add_fdb_test(TEST_FILES BlobManagerUnit.toml)
  add_fdb_test(TEST_FILES ConsistencyCheck.txt IGNORE)
  add_fdb_test(TEST_FILES DDMetricsExclude.txt IGNORE)
  add_fdb_test(TEST_FILES DDSketch.txt IGNORE)
  add_fdb_test(TEST_FILES DataDistributionMetrics.txt IGNORE)
  add_fdb_test(TEST_FILES DiskDurability.txt IGNORE)
  add_fdb_test(TEST_FILES FileSystem.txt IGNORE)
  add_fdb_test(TEST_FILES Happy.txt IGNORE)
  add_fdb_test(TEST_FILES Mako.txt IGNORE)
  add_fdb_test(TEST_FILES IncrementalDelete.txt IGNORE)
  add_fdb_test(TEST_FILES KVStoreMemTest.txt UNIT IGNORE)
  add_fdb_test(TEST_FILES KVStoreReadMostly.txt UNIT IGNORE)
  add_fdb_test(TEST_FILES KVStoreTest.txt UNIT IGNORE)
  add_fdb_test(TEST_FILES KVStoreTestRead.txt UNIT IGNORE)
  add_fdb_test(TEST_FILES KVStoreTestWrite.txt UNIT IGNORE)
  add_fdb_test(TEST_FILES KVStoreValueSize.txt UNIT IGNORE)
  add_fdb_test(TEST_FILES LayerStatusMerge.txt IGNORE)
  add_fdb_test(TEST_FILES ParallelRestoreApiCorrectnessAtomicRestore.txt IGNORE)
  add_fdb_test(TEST_FILES PureNetwork.txt IGNORE)
  add_fdb_test(TEST_FILES RRW2500.txt IGNORE)
  add_fdb_test(TEST_FILES RandomRead.txt IGNORE)
  add_fdb_test(TEST_FILES RandomRangeRead.txt IGNORE)
  add_fdb_test(TEST_FILES RandomReadWrite.txt IGNORE)
  add_fdb_test(TEST_FILES ReadAbsent.txt IGNORE)
  add_fdb_test(TEST_FILES ReadAfterWrite.txt IGNORE)
  add_fdb_test(TEST_FILES ReadHalfAbsent.txt IGNORE)
  add_fdb_test(TEST_FILES RedwoodCorrectnessUnits.txt IGNORE)
  add_fdb_test(TEST_FILES RedwoodCorrectnessBTree.txt IGNORE)
  add_fdb_test(TEST_FILES RedwoodCorrectnessPager.txt IGNORE)
  add_fdb_test(TEST_FILES RedwoodCorrectness.txt IGNORE)
  add_fdb_test(TEST_FILES RedwoodPerfTests.txt IGNORE)
  add_fdb_test(TEST_FILES RedwoodPerfSet.txt IGNORE)
  add_fdb_test(TEST_FILES RedwoodPerfPrefixCompression.txt IGNORE)
  add_fdb_test(TEST_FILES RedwoodPerfSequentialInsert.txt IGNORE)
  add_fdb_test(TEST_FILES RedwoodPerfRandomRangeScans.txt IGNORE)
  add_fdb_test(TEST_FILES RocksDBTest.txt IGNORE)
  add_fdb_test(TEST_FILES S3BlobStore.txt IGNORE)
  add_fdb_test(TEST_FILES SampleNoSimAttrition.txt IGNORE)

  if(NOT USE_UBSAN) # TODO re-enable in UBSAN after https://github.com/apple/foundationdb/issues/2410 is resolved
    add_fdb_test(TEST_FILES SimpleExternalTest.txt)
  else()
    add_fdb_test(TEST_FILES SimpleExternalTest.txt IGNORE)
  endif()

  add_fdb_test(TEST_FILES noSim/SlowTask.txt IGNORE)
  add_fdb_test(TEST_FILES SpecificUnitTest.txt IGNORE)
  add_fdb_test(TEST_FILES StorageMetricsSampleTests.txt IGNORE)
  add_fdb_test(TEST_FILES WorkerTests.txt IGNORE)
  add_fdb_test(TEST_FILES ClusterControllerTests.txt IGNORE)
  add_fdb_test(TEST_FILES StorageServerInterface.txt)
  add_fdb_test(TEST_FILES StreamingWrite.txt IGNORE)
  add_fdb_test(TEST_FILES SystemData.txt)
  add_fdb_test(TEST_FILES ThreadSafety.txt IGNORE)
  add_fdb_test(TEST_FILES TraceEventMetrics.txt IGNORE)
  add_fdb_test(TEST_FILES default.txt IGNORE)
  add_fdb_test(TEST_FILES errors.txt IGNORE)
  add_fdb_test(TEST_FILES fail.txt IGNORE)
  add_fdb_test(TEST_FILES killall.txt IGNORE)
  add_fdb_test(TEST_FILES latency.txt IGNORE)
  add_fdb_test(TEST_FILES performance-fs.txt IGNORE)
  add_fdb_test(TEST_FILES performance.txt IGNORE)
  add_fdb_test(TEST_FILES ping.TXT IGNORE)
  add_fdb_test(TEST_FILES pingServers.TXT IGNORE)
  add_fdb_test(TEST_FILES pt.TXT IGNORE)
  add_fdb_test(TEST_FILES randomSelector.txt IGNORE)
  add_fdb_test(TEST_FILES selectorCorrectness.txt IGNORE)
  add_fdb_test(TEST_FILES IThreadPool.txt IGNORE)
  add_fdb_test(TEST_FILES PerfUnitTests.toml IGNORE)
  add_fdb_test(TEST_FILES fast/AtomicBackupCorrectness.toml)
  add_fdb_test(TEST_FILES fast/AtomicBackupToDBCorrectness.toml)
  add_fdb_test(TEST_FILES fast/AtomicOps.toml)
  add_fdb_test(TEST_FILES fast/AtomicOpsApiCorrectness.toml)
  add_fdb_test(TEST_FILES fast/AuthzSecurity.toml)
  add_fdb_test(TEST_FILES fast/AuthzSecurityWithBlobGranules.toml)
  add_fdb_test(TEST_FILES fast/AutomaticIdempotency.toml)
  add_fdb_test(TEST_FILES fast/BackupAzureBlobCorrectness.toml IGNORE)
  add_fdb_test(TEST_FILES fast/BackupS3BlobCorrectness.toml IGNORE)
  add_fdb_test(TEST_FILES fast/BackupCorrectness.toml)
  add_fdb_test(TEST_FILES fast/BackupCorrectnessWithEKPKeyFetchFailures.toml)
  add_fdb_test(TEST_FILES fast/BackupCorrectnessWithTenantDeletion.toml)
  add_fdb_test(TEST_FILES fast/EncryptedBackupCorrectness.toml)
  add_fdb_test(TEST_FILES fast/BackupCorrectnessClean.toml)
  add_fdb_test(TEST_FILES fast/BackupToDBCorrectness.toml)
  add_fdb_test(TEST_FILES fast/BackupToDBCorrectnessClean.toml)
  add_fdb_test(TEST_FILES rare/BlobGranuleVerifyAtomicOps.toml)
  add_fdb_test(TEST_FILES rare/BlobGranuleVerifyCycle.toml)
  add_fdb_test(TEST_FILES rare/BlobGranuleVerifySmall.toml)
  add_fdb_test(TEST_FILES rare/BlobGranuleVerifySmallClean.toml)

  # TODO: test occasionally times out due to too many change feed shard parts
  add_fdb_test(TEST_FILES rare/BlobGranuleMoveVerifyCycle.toml IGNORE)
  add_fdb_test(TEST_FILES rare/BlobRestoreBasic.toml)
  add_fdb_test(TEST_FILES rare/BlobRestoreLarge.toml IGNORE)
  add_fdb_test(TEST_FILES rare/BlobRestoreToVersion.toml)
  add_fdb_test(TEST_FILES rare/BlobRestoreTenantMode.toml)
  add_fdb_test(TEST_FILES fast/BulkDumping.toml)
  add_fdb_test(TEST_FILES fast/BulkLoading.toml)
  add_fdb_test(TEST_FILES fast/CacheTest.toml)
  add_fdb_test(TEST_FILES fast/CloggedSideband.toml)
  add_fdb_test(TEST_FILES fast/CompressionUtilsUnit.toml IGNORE)
  add_fdb_test(TEST_FILES fast/ConfigureLocked.toml)
  add_fdb_test(TEST_FILES fast/ConfigIncrement.toml)
  add_fdb_test(TEST_FILES fast/ConfigIncrementChangeCoordinators.toml)
  add_fdb_test(TEST_FILES fast/ConfigIncrementWithKills.toml)
  add_fdb_test(TEST_FILES fast/ConstrainedRandomSelector.toml)
  add_fdb_test(TEST_FILES fast/CycleAndLock.toml)
  add_fdb_test(TEST_FILES fast/CycleTest.toml)
  add_fdb_test(TEST_FILES fast/NondeterministicTest.toml)
  add_fdb_test(TEST_FILES rare/ChangeFeeds.toml)
  add_fdb_test(TEST_FILES rare/ChangeFeedOperations.toml)
  add_fdb_test(TEST_FILES rare/ChangeFeedOperationsMove.toml)
  add_fdb_test(TEST_FILES fast/DataLossRecovery.toml)
  add_fdb_test(TEST_FILES fast/EncryptionOps.toml)
  # EncryptionUnitTests is only a convenience file to run the different types of encryption tests at once. Do not enable in general ensembles
  add_fdb_test(TEST_FILES fast/EncryptionUnitTests.toml IGNORE)
  add_fdb_test(TEST_FILES fast/EncryptKeyProxyTest.toml)
  add_fdb_test(TEST_FILES fast/FuzzApiCorrectness.toml)
  add_fdb_test(TEST_FILES fast/FuzzApiCorrectnessClean.toml)
  add_fdb_test(TEST_FILES fast/HTTPKeyValueStore.toml)
  add_fdb_test(TEST_FILES fast/IncrementalBackup.toml)
  add_fdb_test(TEST_FILES fast/IncrementalBackupWithEKPKeyFetchFailures.toml)
  add_fdb_test(TEST_FILES fast/IncrementalBackupWithTenantDeletion.toml)
  add_fdb_test(TEST_FILES fast/IncrementTest.toml)
  add_fdb_test(TEST_FILES fast/InventoryTestAlmostReadOnly.toml)
  add_fdb_test(TEST_FILES fast/InventoryTestSomeWrites.toml)
  add_fdb_test(TEST_FILES fast/KillRegionCycle.toml)
  add_fdb_test(TEST_FILES fast/LocalRatekeeper.toml)
  add_fdb_test(TEST_FILES fast/LongStackWriteDuringRead.toml)
  add_fdb_test(TEST_FILES fast/LowLatency.toml)

  # TODO: Fix failures and reenable this test:
  add_fdb_test(TEST_FILES fast/LowLatencySingleClog.toml IGNORE)
  add_fdb_test(TEST_FILES fast/MemoryLifetime.toml)
  add_fdb_test(TEST_FILES fast/MoveKeysCycle.toml)
  add_fdb_test(TEST_FILES fast/MutationLogReaderCorrectness.toml)

  add_fdb_test(TEST_FILES fast/GetEstimatedRangeSize.toml)
  add_fdb_test(TEST_FILES fast/GetMappedRange.toml)

  add_fdb_test(TEST_FILES fast/PerpetualWiggleStats.toml)
  add_fdb_test(TEST_FILES fast/PrivateEndpoints.toml)
  add_fdb_test(TEST_FILES fast/ProtocolVersion.toml)
  add_fdb_test(TEST_FILES fast/RandomSelector.toml)
  add_fdb_test(TEST_FILES fast/RandomUnitTests.toml)
  add_fdb_test(TEST_FILES fast/RangeLocking.toml)
  add_fdb_test(TEST_FILES fast/RangeLockCycle.toml)
  add_fdb_test(TEST_FILES fast/ReadHotDetectionCorrectness.toml IGNORE) # TODO re-enable once read hot detection is enabled.
  add_fdb_test(TEST_FILES fast/ReportConflictingKeys.toml)
  add_fdb_test(TEST_FILES fast/RESTUnit.toml IGNORE)
  add_fdb_test(TEST_FILES fast/SelectorCorrectness.toml)
  add_fdb_test(TEST_FILES fast/Sideband.toml)
  add_fdb_test(TEST_FILES fast/SidebandSingle.toml)
  add_fdb_test(TEST_FILES fast/SidebandWithStatus.toml)
  add_fdb_test(TEST_FILES fast/SimpleAtomicAdd.toml)
  add_fdb_test(TEST_FILES fast/SpecialKeySpaceCorrectness.toml)
  add_fdb_test(TEST_FILES fast/SpecialKeySpaceRobustness.toml)
  add_fdb_test(TEST_FILES fast/StreamingRangeRead.toml)
  add_fdb_test(TEST_FILES fast/SwizzledRollbackSideband.toml)
  add_fdb_test(TEST_FILES fast/SystemRebootTestCycle.toml)
  add_fdb_test(TEST_FILES fast/TaskBucketCorrectness.toml)
  add_fdb_test(TEST_FILES fast/TenantCycle.toml)
  add_fdb_test(TEST_FILES fast/TenantCycleTokenless.toml)
  add_fdb_test(TEST_FILES fast/TenantEntryCache.toml)
  add_fdb_test(TEST_FILES fast/TenantLock.toml)
  add_fdb_test(TEST_FILES fast/RawTenantAccessClean.toml)
  add_fdb_test(TEST_FILES fast/TimeKeeperCorrectness.toml)
  add_fdb_test(TEST_FILES fast/TxnStateStoreCycleTest.toml)
  add_fdb_test(TEST_FILES fast/UDP.toml)
  add_fdb_test(TEST_FILES fast/Unreadable.toml)
  add_fdb_test(TEST_FILES fast/VersionStamp.toml)
  add_fdb_test(TEST_FILES fast/Watches.toml)
  add_fdb_test(TEST_FILES fast/WriteDuringRead.toml)
  add_fdb_test(TEST_FILES fast/WriteDuringReadClean.toml)
  add_fdb_test(TEST_FILES noSim/RandomUnitTests.toml IGNORE)

  if(WITH_ROCKSDB)
    add_fdb_test(TEST_FILES fast/ValidateStorage.toml)
    add_fdb_test(TEST_FILES noSim/KeyValueStoreRocksDBTest.toml UNIT)
    add_fdb_test(TEST_FILES noSim/ShardedRocksDBTest.toml UNIT)
    if (USE_VALGRIND)
      add_fdb_test(TEST_FILES noSim/PerfShardedRocksDBTest.toml IGNORE)
    else()
      add_fdb_test(TEST_FILES noSim/PerfShardedRocksDBTest.toml UNIT)
    endif()
    add_fdb_test(TEST_FILES fast/PhysicalShardMove.toml)
    add_fdb_test(TEST_FILES fast/StorageServerCheckpointRestore.toml IGNORE)

    # Mock DD Tests
    add_fdb_test(TEST_FILES fast/IDDTxnProcessorMoveKeys.toml IGNORE)
    add_fdb_test(TEST_FILES fast/MockDDReadWrite.toml IGNORE)
    add_fdb_test(TEST_FILES rare/PerpetualWiggleStorageMigration.toml)
  else()
    add_fdb_test(TEST_FILES fast/ValidateStorage.toml IGNORE)
    add_fdb_test(TEST_FILES noSim/KeyValueStoreRocksDBTest.toml IGNORE)
    add_fdb_test(TEST_FILES noSim/ShardedRocksDBTest.toml IGNORE)
    add_fdb_test(TEST_FILES noSim/PerfShardedRocksDBTest.toml IGNORE)
    add_fdb_test(TEST_FILES fast/PhysicalShardMove.toml IGNORE)
    add_fdb_test(TEST_FILES fast/StorageServerCheckpointRestore.toml IGNORE)
    add_fdb_test(TEST_FILES rare/PerpetualWiggleStorageMigration.toml IGNORE)

    # Mock DD Tests
    add_fdb_test(TEST_FILES fast/IDDTxnProcessorMoveKeys.toml)
    add_fdb_test(TEST_FILES fast/MockDDReadWrite.toml)
  endif()

  add_fdb_test(TEST_FILES rare/BlobGranuleApiCorrectness.toml)
  add_fdb_test(TEST_FILES rare/BlobGranuleCorrectnessClean.toml)
  add_fdb_test(TEST_FILES rare/BlobGranuleCorrectness.toml IGNORE)
  add_fdb_test(TEST_FILES rare/BlobGranuleMergeBoundaries.toml)
  add_fdb_test(TEST_FILES rare/BlobGranuleRanges.toml)
  add_fdb_test(TEST_FILES rare/BlobGranuleRangesChangeLog.toml)
  add_fdb_test(TEST_FILES rare/BlobGranuleVerifyBalance.toml)
  add_fdb_test(TEST_FILES rare/BlobGranuleVerifyBalanceClean.toml)
  add_fdb_test(TEST_FILES rare/BlobGranuleVerifyLarge.toml IGNORE)
  add_fdb_test(TEST_FILES rare/BlobGranuleVerifyLargeClean.toml IGNORE)
  add_fdb_test(TEST_FILES rare/CheckRelocation.toml)
  add_fdb_test(TEST_FILES rare/ClogTlog.toml)
  add_fdb_test(TEST_FILES rare/ClogRemoteTLog.toml)
  add_fdb_test(TEST_FILES rare/ClogUnclog.toml)
  add_fdb_test(TEST_FILES rare/CloggedCycleWithKills.toml)
  add_fdb_test(TEST_FILES rare/ConfigDBUnitTest.toml)
  add_fdb_test(TEST_FILES rare/ConflictRangeCheck.toml)
  add_fdb_test(TEST_FILES rare/ConflictRangeRYOWCheck.toml)
  add_fdb_test(TEST_FILES rare/CycleRollbackClogged.toml)
  add_fdb_test(TEST_FILES rare/CycleWithKills.toml)
  add_fdb_test(TEST_FILES rare/CycleWithDeadHall.toml)
  add_fdb_test(TEST_FILES rare/DataDistributionMetrics.toml)
  add_fdb_test(TEST_FILES rare/DcLag.toml)
  add_fdb_test(TEST_FILES rare/ExpectStableThroughput.toml)
  add_fdb_test(TEST_FILES rare/FailoverWithSSLag.toml)
  add_fdb_test(TEST_FILES rare/HighContentionPrefixAllocator.toml)
  add_fdb_test(TEST_FILES rare/InventoryTestHeavyWrites.toml)
  add_fdb_test(TEST_FILES rare/LargeApiCorrectness.toml)
  add_fdb_test(TEST_FILES rare/LargeApiCorrectnessStatus.toml)
  add_fdb_test(TEST_FILES rare/MetaclusterManagement.toml)
  add_fdb_test(TEST_FILES rare/MetaclusterManagementConcurrency.toml)
  add_fdb_test(TEST_FILES rare/MetaclusterRecovery.toml)
  add_fdb_test(TEST_FILES rare/RYWDisable.toml)
  add_fdb_test(TEST_FILES rare/RandomReadWriteTest.toml)
  add_fdb_test(TEST_FILES rare/ReadSkewReadWrite.toml)
  add_fdb_test(TEST_FILES rare/RestoreMultiRanges.toml)
  add_fdb_test(TEST_FILES rare/SpecificUnitTests.toml)
  add_fdb_test(TEST_FILES rare/StorageQuotaTest.toml)
  add_fdb_test(TEST_FILES rare/SwizzledLargeApiCorrectness.toml)
  add_fdb_test(TEST_FILES rare/RedwoodCorrectnessBTree.toml)
  add_fdb_test(TEST_FILES rare/RedwoodDeltaTree.toml)
  add_fdb_test(TEST_FILES rare/Throttling.toml)
  add_fdb_test(TEST_FILES rare/ThroughputQuota.toml)
  add_fdb_test(TEST_FILES rare/TransactionCost.toml)
  add_fdb_test(TEST_FILES rare/TransactionTagApiCorrectness.toml)
  add_fdb_test(TEST_FILES rare/TransactionTagSwizzledApiCorrectness.toml)
  add_fdb_test(TEST_FILES rare/WriteTagThrottling.toml)
  add_fdb_test(TEST_FILES rare/AllSimUnitTests.toml IGNORE)
  add_fdb_test(TEST_FILES rare/StatusBuilderPerf.toml)
  add_fdb_test(TEST_FILES rare/TLogVersionMessagesOverheadFactor.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.1.0_until_7.3.0/ClientTransactionProfilingCorrectness-1.txt
    restarting/from_7.1.0_until_7.3.0/ClientTransactionProfilingCorrectness-2.txt)
  add_fdb_test(
    TEST_FILES restarting/from_7.1.0_until_7.3.0/CycleTestRestart-1.txt
    restarting/from_7.1.0_until_7.3.0/CycleTestRestart-2.txt)
  add_fdb_test(
    TEST_FILES restarting/from_7.1.0_until_7.3.0/StorefrontTestRestart-1.txt
    restarting/from_7.1.0_until_7.3.0/StorefrontTestRestart-2.txt)
  add_fdb_test(
    TEST_FILES restarting/from_7.1.0_until_7.2.0/DrUpgradeRestart-1.txt
    restarting/from_7.1.0_until_7.2.0/DrUpgradeRestart-2.txt)
  add_fdb_test(
    TEST_FILES restarting/from_7.1.0_until_7.2.0/ConfigureTestRestart-1.toml
    restarting/from_7.1.0_until_7.2.0/ConfigureTestRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.1.0_until_7.2.0/SnapIncrementalRestore-1.txt
    restarting/from_7.1.0_until_7.2.0/SnapIncrementalRestore-2.txt)
  add_fdb_test(
    TEST_FILES restarting/from_7.1.0_until_7.2.0/ConfigureStorageMigrationTestRestart-1.toml
    restarting/from_7.1.0_until_7.2.0/ConfigureStorageMigrationTestRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.1.0_until_7.2.0/UpgradeAndBackupRestore-1.toml
    restarting/from_7.1.0_until_7.2.0/UpgradeAndBackupRestore-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.1.63_until_7.2.0/ClientMetricRestart-1.toml
    restarting/from_7.1.63_until_7.2.0/ClientMetricRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.2.0_until_7.3.0/ConfigureTestRestart-1.toml
    restarting/from_7.2.0_until_7.3.0/ConfigureTestRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.2.0_until_7.3.0/ConfigureStorageMigrationTestRestart-1.toml
    restarting/from_7.2.0_until_7.3.0/ConfigureStorageMigrationTestRestart-2.toml)
    add_fdb_test(
      TEST_FILES restarting/from_7.2.0_until_7.3.0/DrUpgradeRestart-1.toml
      restarting/from_7.2.0_until_7.3.0/DrUpgradeRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.2.4_until_7.3.0/UpgradeAndBackupRestore-1.toml
    restarting/from_7.2.4_until_7.3.0/UpgradeAndBackupRestore-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.0/ConfigureTestRestart-1.toml
    restarting/from_7.3.0/ConfigureTestRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.0/ConfigureStorageMigrationTestRestart-1.toml
    restarting/from_7.3.0/ConfigureStorageMigrationTestRestart-2.toml)
    add_fdb_test(
    TEST_FILES restarting/from_7.3.0/DrUpgradeRestart-1.toml
    restarting/from_7.3.0/DrUpgradeRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.0/UpgradeAndBackupRestore-1.toml
    restarting/from_7.3.0/UpgradeAndBackupRestore-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.0/VersionVectorDisableRestart-1.toml
    restarting/from_7.3.0/VersionVectorDisableRestart-2.toml IGNORE)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.0/VersionVectorEnableRestart-1.toml
    restarting/from_7.3.0/VersionVectorEnableRestart-2.toml IGNORE)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.0/BlobGranuleRestartCycle-1.toml
    restarting/from_7.3.0/BlobGranuleRestartCycle-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.0/BlobGranuleRestartLarge-1.toml
    restarting/from_7.3.0/BlobGranuleRestartLarge-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.0/BlobGranuleRestartCorrectness-1.toml
    restarting/from_7.3.0/BlobGranuleRestartCorrectness-2.toml IGNORE)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.0/SnapCycleRestart-1.toml
    restarting/from_7.3.0/SnapCycleRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.0/SnapTestAttrition-1.toml
    restarting/from_7.3.0/SnapTestAttrition-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.0/SnapTestSimpleRestart-1.toml
    restarting/from_7.3.0/SnapTestSimpleRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.0/SnapTestRestart-1.toml
    restarting/from_7.3.0/SnapTestRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.49/ClientMetricRestart-1.toml
    restarting/from_7.3.49/ClientMetricRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.5_until_7.3.29/ClientTransactionProfilingCorrectness-1.toml
    restarting/from_7.3.5_until_7.3.29/ClientTransactionProfilingCorrectness-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.29/ClientTransactionProfilingCorrectness-1.toml
    restarting/from_7.3.29/ClientTransactionProfilingCorrectness-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.5_until_7.3.29/CycleTestRestart-1.toml
    restarting/from_7.3.5_until_7.3.29/CycleTestRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.29/CycleTestRestart-1.toml
    restarting/from_7.3.29/CycleTestRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/from_7.3.0/StorefrontTestRestart-1.toml
    restarting/from_7.3.0/StorefrontTestRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/to_7.1.0_until_7.2.0/ConfigureStorageMigrationTestRestart-1.toml
    restarting/to_7.1.0_until_7.2.0/ConfigureStorageMigrationTestRestart-2.toml IGNORE)
  add_fdb_test(
    TEST_FILES restarting/to_7.1.0_until_7.2.0/CycleTestRestart-1.toml
    restarting/to_7.1.0_until_7.2.0/CycleTestRestart-2.toml IGNORE)
  add_fdb_test(
    TEST_FILES restarting/to_7.2.0_until_7.3.0/ConfigureStorageMigrationTestRestart-1.toml
    restarting/to_7.2.0_until_7.3.0/ConfigureStorageMigrationTestRestart-2.toml IGNORE)
  add_fdb_test(
    TEST_FILES restarting/to_7.2.0_until_7.3.0/CycleTestRestart-1.toml
    restarting/to_7.2.0_until_7.3.0/CycleTestRestart-2.toml IGNORE)
  add_fdb_test(
    TEST_FILES restarting/to_7.3.0_until_7.3.39/BlobGranuleRestartCorrectness-1.toml
    restarting/to_7.3.0_until_7.3.39/BlobGranuleRestartCorrectness-2.toml IGNORE)
  add_fdb_test(
    TEST_FILES restarting/to_7.3.0_until_7.3.39/ConfigureStorageMigrationTestRestart-1.toml
    restarting/to_7.3.0_until_7.3.39/ConfigureStorageMigrationTestRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/to_7.3.0_until_7.3.39/CycleTestRestart-1.toml
    restarting/to_7.3.0_until_7.3.39/CycleTestRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/to_7.3.39_until_7.4.0/BlobGranuleRestartCorrectness-1.toml
    restarting/to_7.3.39_until_7.4.0/BlobGranuleRestartCorrectness-2.toml IGNORE)
  add_fdb_test(
    TEST_FILES restarting/to_7.3.39_until_7.4.0/ConfigureStorageMigrationTestRestart-1.toml
    restarting/to_7.3.39_until_7.4.0/ConfigureStorageMigrationTestRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/to_7.3.39_until_7.4.0/CycleTestRestart-1.toml
    restarting/to_7.3.39_until_7.4.0/CycleTestRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/to_7.4.0/BlobGranuleRestartCorrectness-1.toml
    restarting/to_7.4.0/BlobGranuleRestartCorrectness-2.toml)
  add_fdb_test(
    TEST_FILES restarting/to_7.4.0/ConfigureStorageMigrationTestRestart-1.toml
    restarting/to_7.4.0/ConfigureStorageMigrationTestRestart-2.toml)
  add_fdb_test(
    TEST_FILES restarting/to_7.4.0/CycleTestRestart-1.toml
    restarting/to_7.4.0/CycleTestRestart-2.toml)

  add_fdb_test(TEST_FILES slow/ApiCorrectness.toml)
  add_fdb_test(TEST_FILES slow/ApiCorrectnessAtomicRestore.toml)
  add_fdb_test(TEST_FILES slow/ApiCorrectnessSwitchover.toml)
  add_fdb_test(TEST_FILES slow/ApiCorrectnessWithConsistencyCheck.toml)
  add_fdb_test(TEST_FILES slow/ClogWithRollbacks.toml)
  add_fdb_test(TEST_FILES slow/CloggedCycleTest.toml)
  add_fdb_test(TEST_FILES slow/CloggedStorefront.toml)
  add_fdb_test(TEST_FILES slow/CommitBug.toml)
  add_fdb_test(TEST_FILES slow/ConfigureTest.toml)
  add_fdb_test(TEST_FILES slow/ConfigureStorageMigrationTest.toml)
  add_fdb_test(TEST_FILES slow/CycleRollbackPlain.toml)
  add_fdb_test(TEST_FILES slow/DDBalanceAndRemove.toml)
  add_fdb_test(TEST_FILES slow/DDBalanceAndRemoveStatus.toml)
  add_fdb_test(TEST_FILES slow/DifferentClustersSameRV.toml)
  add_fdb_test(TEST_FILES slow/DiskFailureCycle.toml)
  add_fdb_test(TEST_FILES slow/ExcludeIncludeStorageServers.toml)
  add_fdb_test(TEST_FILES slow/FastTriggeredWatches.toml)
  add_fdb_test(TEST_FILES slow/GcGenerations.toml)
  add_fdb_test(TEST_FILES slow/LongRunning.toml LONG_RUNNING)
  add_fdb_test(TEST_FILES slow/LowLatencyWithFailures.toml)
  add_fdb_test(TEST_FILES slow/MoveKeysClean.toml)
  add_fdb_test(TEST_FILES slow/MoveKeysSideband.toml)
  add_fdb_test(TEST_FILES slow/RyowCorrectness.toml)
  add_fdb_test(TEST_FILES slow/Serializability.toml)
  add_fdb_test(TEST_FILES slow/SharedBackupCorrectness.toml)
  add_fdb_test(TEST_FILES slow/SharedBackupToDBCorrectness.toml)
  add_fdb_test(TEST_FILES slow/SharedDefaultBackupCorrectness.toml)
  add_fdb_test(TEST_FILES slow/StorefrontTest.toml)
  add_fdb_test(TEST_FILES slow/SwizzledApiCorrectness.toml)
  add_fdb_test(TEST_FILES slow/SwizzledCycleTest.toml)
  add_fdb_test(TEST_FILES slow/SwizzledDdBalance.toml)
  add_fdb_test(TEST_FILES slow/SwizzledRollbackTimeLapse.toml)
  add_fdb_test(TEST_FILES slow/SwizzledRollbackTimeLapseIncrement.toml)
  add_fdb_test(TEST_FILES slow/SwizzledTenantManagement.toml)
  add_fdb_test(TEST_FILES slow/SwizzledTenantManagementMetacluster.toml)
  add_fdb_test(TEST_FILES slow/TenantCapacityLimits.toml)
  add_fdb_test(TEST_FILES slow/TenantManagement.toml)
  add_fdb_test(TEST_FILES slow/TenantManagementConcurrency.toml)
  add_fdb_test(TEST_FILES slow/VersionStampBackupToDB.toml)
  add_fdb_test(TEST_FILES slow/VersionStampSwitchover.toml)
  add_fdb_test(TEST_FILES slow/WriteDuringReadAtomicRestore.toml)
  add_fdb_test(TEST_FILES slow/WriteDuringReadSwitchover.toml)
  add_fdb_test(TEST_FILES slow/ddbalance.toml)
  add_fdb_test(TEST_FILES slow/ParallelRestoreNewBackupCorrectnessAtomicOp.toml)
  add_fdb_test(TEST_FILES slow/ParallelRestoreNewBackupCorrectnessCycle.toml)
  add_fdb_test(TEST_FILES slow/ParallelRestoreNewBackupCorrectnessMultiCycles.toml)
  add_fdb_test(TEST_FILES slow/ParallelRestoreNewBackupWriteDuringReadAtomicRestore.toml)
  add_fdb_test(TEST_FILES slow/ParallelRestoreOldBackupCorrectnessAtomicOp.toml)
  add_fdb_test(TEST_FILES slow/ParallelRestoreOldBackupCorrectnessCycle.toml)
  add_fdb_test(TEST_FILES slow/ParallelRestoreOldBackupCorrectnessMultiCycles.toml)
  add_fdb_test(TEST_FILES slow/ParallelRestoreOldBackupWriteDuringReadAtomicRestore.toml)
  add_fdb_test(TEST_FILES negative/ResolverIgnoreTooOld.toml)
  add_fdb_test(TEST_FILES negative/ResolverIgnoreReads.toml)
  add_fdb_test(TEST_FILES negative/ResolverIgnoreWrites.toml)
  add_fdb_test(TEST_FILES negative/StorageCorruption.toml)
  add_fdb_test(TEST_FILES ParallelRestoreOldBackupApiCorrectnessAtomicRestore.toml IGNORE)

  # Note that status tests are not deterministic.
  add_fdb_test(TEST_FILES status/invalid_proc_addresses.txt)
  add_fdb_test(TEST_FILES status/local_6_machine_no_replicas_remain.txt)
  add_fdb_test(TEST_FILES status/separate_1_of_3_coordinators_remain.txt)
  add_fdb_test(TEST_FILES status/separate_2_of_3_coordinators_remain.txt)
  add_fdb_test(TEST_FILES status/separate_cannot_write_cluster_file.txt)
  add_fdb_test(TEST_FILES status/separate_idle.txt)
  add_fdb_test(TEST_FILES status/separate_initializing.txt)
  add_fdb_test(TEST_FILES status/separate_no_coordinators.txt)
  add_fdb_test(TEST_FILES status/separate_no_database.txt)
  add_fdb_test(TEST_FILES status/separate_no_servers.txt)
  add_fdb_test(TEST_FILES status/separate_not_enough_servers.txt)
  add_fdb_test(TEST_FILES status/single_process_too_many_config_params.txt)

  if(NOT OPEN_FOR_IDE)
    add_test(
      NAME multiversion_client/unit_tests
      COMMAND $<TARGET_FILE:fdbserver> -r unittests -f /fdbclient/multiversionclient/
    )
    set_tests_properties("multiversion_client/unit_tests" PROPERTIES ENVIRONMENT "${SANITIZER_OPTIONS}")
    add_test(
      NAME threadsafe_threadfuture_to_future/unit_tests
      COMMAND $<TARGET_FILE:fdbserver> -r unittests -f /flow/safeThreadFutureToFuture/
    )
    set_tests_properties("threadsafe_threadfuture_to_future/unit_tests" PROPERTIES ENVIRONMENT "${SANITIZER_OPTIONS}")
  endif()

  if(NOT OPEN_FOR_IDE)
    add_test(
      NAME command_line_argument_test
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/argument_parsing/test_argument_parsing.py ${CMAKE_BINARY_DIR}
    )
    set_tests_properties(command_line_argument_test PROPERTIES ENVIRONMENT "FDB_CLUSTER_FILE=${CMAKE_BINARY_DIR}/fdb.cluster")
  endif()

  verify_testing()

  if(NOT OPEN_FOR_IDE AND NOT WIN32)
    create_correctness_package()

    if(USE_VALGRIND)
      create_valgrind_correctness_package()
    endif()

    if(ENABLE_LONG_RUNNING_TESTS)
      create_long_running_correctness_package()
    endif()
  endif()

  if(NOT OPEN_FOR_IDE AND NOT WIN32 AND NOT USE_SANITIZER)
    # setup venv for testing token-based authorization
    if(APPLE)
      set(ld_env_name "DYLD_LIBRARY_PATH")
    else()
      set(ld_env_name "LD_LIBRARY_PATH")
    endif()

    set(authz_venv_dir ${CMAKE_CURRENT_BINARY_DIR}/authorization_test_venv)
    set(authz_venv_activate ". ${authz_venv_dir}/bin/activate")
    set(authz_venv_stamp_file ${authz_venv_dir}/venv.ready)
    set(authz_venv_cmd "")
    string(APPEND authz_venv_cmd "[[ ! -f ${authz_venv_stamp_file} ]] && ")
    string(APPEND authz_venv_cmd "${Python3_EXECUTABLE} -m venv ${authz_venv_dir} ")
    string(APPEND authz_venv_cmd "&& ${authz_venv_activate} ")
    string(APPEND authz_venv_cmd "&& pip install --upgrade pip ")
    string(APPEND authz_venv_cmd "&& pip install -r ${CMAKE_SOURCE_DIR}/tests/authorization/requirements.txt ")
	# NOTE: Here we are already in the venv so Python3_EXECUTABLE is not available anymore
    string(APPEND authz_venv_cmd "&& (cd ${CMAKE_BINARY_DIR}/bindings/python && python3 -m pip install .) ")
    string(APPEND authz_venv_cmd "&& touch ${authz_venv_stamp_file} ")
    string(APPEND authz_venv_cmd "|| echo 'venv already set up'")
    add_test(
      NAME authorization_venv_setup
      COMMAND bash -c ${authz_venv_cmd}
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    set_tests_properties(authorization_venv_setup PROPERTIES FIXTURES_SETUP authz_virtual_env TIMEOUT 60)

    set(authz_script_dir ${CMAKE_SOURCE_DIR}/tests/authorization)
    set(enable_grv_cache 0 1)
    set(force_mvc 0 1)

    foreach(is_grv_cache_enabled IN LISTS enable_grv_cache)
      foreach(is_mvc_forced IN LISTS force_mvc)
        if(NOT is_mvc_forced AND is_grv_cache_enabled)
          continue() # grv cache requires setting up of shared database state which is only available in MVC mode
        endif()

        set(authz_test_name "authz")
        set(test_opt "")

        if(is_grv_cache_enabled)
          string(APPEND test_opt " --use-grv-cache")
          string(APPEND authz_test_name "_with_grv_cache")
        else()
          string(APPEND authz_test_name "_no_grv_cache")
        endif()

        if(is_mvc_forced)
          string(APPEND test_opt " --force-multi-version-client")
          string(APPEND authz_test_name "_with_forced_mvc")
        else()
          string(APPEND authz_test_name "_no_forced_mvc")
        endif()

        set(authz_test_cmd "${authz_venv_activate} && pytest ${authz_script_dir}/authz_test.py -rA --build-dir ${CMAKE_BINARY_DIR} -vvv${test_opt}")
        add_test(
          NAME ${authz_test_name}
          WORKING_DIRECTORY ${authz_venv_dir}
          COMMAND bash -c ${authz_test_cmd})
        set_tests_properties(${authz_test_name} PROPERTIES ENVIRONMENT "PYTHONPATH=${CMAKE_SOURCE_DIR}/tests/TestRunner;${ld_env_name}=${CMAKE_BINARY_DIR}/lib")
        set_tests_properties(${authz_test_name} PROPERTIES FIXTURES_REQUIRED authz_virtual_env)
        set_tests_properties(${authz_test_name} PROPERTIES TIMEOUT 120)
      endforeach()
    endforeach()
  endif()
else()
  message(WARNING "Python not found, won't configure ctest")
endif()

# Add all unit tests as ctests
if (AUTO_DISCOVER_UNIT_TESTS)
  collect_unit_tests(${CMAKE_SOURCE_DIR})
endif()
