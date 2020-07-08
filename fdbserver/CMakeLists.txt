set(FDBSERVER_SRCS
  ApplyMetadataMutation.h
  ApplyMetadataMutation.cpp
  BackupInterface.h
  BackupProgress.actor.cpp
  BackupProgress.actor.h
  BackupWorker.actor.cpp
  ClusterController.actor.cpp
  ConflictSet.h
  CoordinatedState.actor.cpp
  CoordinatedState.h
  Coordination.actor.cpp
  CoordinationInterface.h
  CoroFlow.actor.cpp
  CoroFlow.h
  DataDistribution.actor.cpp
  DataDistribution.actor.h
  DataDistributionQueue.actor.cpp
  DataDistributionTracker.actor.cpp
  DataDistributorInterface.h
  DBCoreState.h
  DiskQueue.actor.cpp
  fdbserver.actor.cpp
  FDBExecHelper.actor.cpp
  FDBExecHelper.actor.h
  IDiskQueue.h
  IKeyValueStore.h
  IPager.h
  IVersionedStore.h
  KeyValueStoreCompressTestData.actor.cpp
  KeyValueStoreMemory.actor.cpp
  KeyValueStoreRocksDB.actor.cpp
  KeyValueStoreSQLite.actor.cpp
  Knobs.cpp
  Knobs.h
  LatencyBandConfig.cpp
  LatencyBandConfig.h
  LeaderElection.actor.cpp
  LeaderElection.h
  LogProtocolMessage.h
  LogRouter.actor.cpp
  LogSystem.h
  LogSystemConfig.h
  LogSystemDiskQueueAdapter.actor.cpp
  LogSystemDiskQueueAdapter.h
  LogSystemPeekCursor.actor.cpp
  MasterInterface.h
  MasterProxyServer.actor.cpp
  masterserver.actor.cpp
  MutationTracking.h
  MutationTracking.cpp
  MoveKeys.actor.h
  MoveKeys.actor.cpp
  networktest.actor.cpp
  NetworkTest.h
  OldTLogServer_4_6.actor.cpp
  OldTLogServer_6_0.actor.cpp
  OldTLogServer_6_2.actor.cpp
  Orderer.actor.h
  pubsub.actor.cpp
  pubsub.h
  QuietDatabase.actor.cpp
  QuietDatabase.h
  Ratekeeper.actor.cpp
  RatekeeperInterface.h
  RecoveryState.h
  RestoreCommon.actor.h
  RestoreCommon.actor.cpp
  RestoreUtil.h
  RestoreUtil.actor.cpp
  RestoreRoleCommon.actor.h
  RestoreRoleCommon.actor.cpp
  RestoreMaster.actor.h
  RestoreMaster.actor.cpp
  RestoreApplier.actor.h
  RestoreApplier.actor.cpp
  RestoreLoader.actor.h
  RestoreLoader.actor.cpp
  RestoreWorker.actor.h
  RestoreWorker.actor.cpp
  Resolver.actor.cpp
  ResolverInterface.h
  ServerDBInfo.actor.h
  ServerDBInfo.h
  SimulatedCluster.actor.cpp
  SimulatedCluster.h
  SkipList.cpp
  Status.actor.cpp
  Status.h
  StorageCache.actor.cpp
  StorageMetrics.actor.h
  StorageMetrics.h
  storageserver.actor.cpp
  TagPartitionedLogSystem.actor.cpp
  template_fdb.h
  tester.actor.cpp
  TesterInterface.actor.h
  TLogInterface.h
  TLogServer.actor.cpp
  VersionedBTree.actor.cpp
  VFSAsync.cpp
  WaitFailure.actor.cpp
  WaitFailure.h
  worker.actor.cpp
  WorkerInterface.actor.h
  workloads/ApiCorrectness.actor.cpp
  workloads/ApiWorkload.actor.cpp
  workloads/ApiWorkload.h
  workloads/AsyncFile.actor.h
  workloads/AsyncFile.cpp
  workloads/AsyncFileCorrectness.actor.cpp
  workloads/AsyncFileRead.actor.cpp
  workloads/AsyncFileWrite.actor.cpp
  workloads/AtomicOps.actor.cpp
  workloads/ReadHotDetection.actor.cpp
  workloads/AtomicOpsApiCorrectness.actor.cpp
  workloads/AtomicRestore.actor.cpp
  workloads/AtomicSwitchover.actor.cpp
  workloads/BackgroundSelectors.actor.cpp
  workloads/BackupCorrectness.actor.cpp
  workloads/BackupAndParallelRestoreCorrectness.actor.cpp
  workloads/ParallelRestore.actor.cpp
  workloads/BackupToDBAbort.actor.cpp
  workloads/BackupToDBCorrectness.actor.cpp
  workloads/BackupToDBUpgrade.actor.cpp
  workloads/BulkLoad.actor.cpp
  workloads/BulkSetup.actor.h
  workloads/Cache.actor.cpp
  workloads/ChangeConfig.actor.cpp
  workloads/ClientTransactionProfileCorrectness.actor.cpp
  workloads/TriggerRecovery.actor.cpp
  workloads/SuspendProcesses.actor.cpp
  workloads/CommitBugCheck.actor.cpp
  workloads/ConfigureDatabase.actor.cpp
  workloads/ConflictRange.actor.cpp
  workloads/ConsistencyCheck.actor.cpp
  workloads/CpuProfiler.actor.cpp
  workloads/Cycle.actor.cpp
  workloads/DataDistributionMetrics.actor.cpp
  workloads/DDBalance.actor.cpp
  workloads/DDMetrics.actor.cpp
  workloads/DDMetricsExclude.actor.cpp
  workloads/DiskDurability.actor.cpp
  workloads/DiskDurabilityTest.actor.cpp
  workloads/DummyWorkload.actor.cpp
  workloads/ExternalWorkload.actor.cpp
  workloads/FastTriggeredWatches.actor.cpp
  workloads/FileSystem.actor.cpp
  workloads/Fuzz.cpp
  workloads/FuzzApiCorrectness.actor.cpp
  workloads/Increment.actor.cpp
  workloads/IndexScan.actor.cpp
  workloads/Inventory.actor.cpp
  workloads/KVStoreTest.actor.cpp
  workloads/KillRegion.actor.cpp
  workloads/LockDatabase.actor.cpp
  workloads/LocalRatekeeper.actor.cpp
  workloads/LogMetrics.actor.cpp
  workloads/LowLatency.actor.cpp
  workloads/MachineAttrition.actor.cpp
  workloads/Mako.actor.cpp
  workloads/MemoryKeyValueStore.cpp
  workloads/MemoryKeyValueStore.h
  workloads/MemoryLifetime.actor.cpp
  workloads/MetricLogging.actor.cpp
  workloads/Performance.actor.cpp
  workloads/Ping.actor.cpp
  workloads/PopulateTPCC.actor.cpp
  workloads/PubSubMultiples.actor.cpp
  workloads/QueuePush.actor.cpp
  workloads/RandomClogging.actor.cpp
  workloads/RandomMoveKeys.actor.cpp
  workloads/RandomSelector.actor.cpp
  workloads/ReadAfterWrite.actor.cpp
  workloads/ReadWrite.actor.cpp
  workloads/RemoveServersSafely.actor.cpp
  workloads/ReportConflictingKeys.actor.cpp
  workloads/Rollback.actor.cpp
  workloads/RyowCorrectness.actor.cpp
  workloads/RYWDisable.actor.cpp
  workloads/RYWPerformance.actor.cpp
  workloads/SaveAndKill.actor.cpp
  workloads/SelectorCorrectness.actor.cpp
  workloads/Serializability.actor.cpp
  workloads/Sideband.actor.cpp
  workloads/SlowTaskWorkload.actor.cpp
  workloads/SnapTest.actor.cpp
  workloads/SpecialKeySpaceCorrectness.actor.cpp
  workloads/StatusWorkload.actor.cpp
  workloads/Storefront.actor.cpp
  workloads/StreamingRead.actor.cpp
  workloads/TagThrottleApi.actor.cpp
  workloads/TargetedKill.actor.cpp
  workloads/TaskBucketCorrectness.actor.cpp
  workloads/ThreadSafety.actor.cpp
  workloads/Throttling.actor.cpp
  workloads/Throughput.actor.cpp
  workloads/TimeKeeperCorrectness.actor.cpp
  workloads/TPCC.actor.cpp
  workloads/TPCCWorkload.h
  workloads/DifferentClustersSameRV.actor.cpp
  workloads/UnitPerf.actor.cpp
  workloads/UnitTests.actor.cpp
  workloads/Unreadable.actor.cpp
  workloads/VersionStamp.actor.cpp
  workloads/WatchAndWait.actor.cpp
  workloads/Watches.actor.cpp
  workloads/WorkerErrors.actor.cpp
  workloads/workloads.actor.h
  workloads/WriteBandwidth.actor.cpp
  workloads/WriteDuringRead.actor.cpp)

add_library(fdb_sqlite STATIC
  sqlite/btree.h
  sqlite/hash.h
  sqlite/sqlite3.h
  sqlite/sqlite3ext.h
  sqlite/sqliteInt.h
  sqlite/sqliteLimit.h
  sqlite/sqlite3.amalgamation.c)

if (WITH_ROCKSDB_EXPERIMENTAL)
  add_definitions(-DSSD_ROCKSDB_EXPERIMENTAL)
  # Set this to 0 if you want to compile RocksDB with `-march=native`.
  set(PORTABLE_ROCKSDB 1)

  include(CompileRocksDB)
endif()

# Suppress warnings in sqlite since it's third party
if(NOT WIN32)
  target_compile_definitions(fdb_sqlite PRIVATE $<$<CONFIG:Debug>:NDEBUG>)
  target_compile_options(fdb_sqlite BEFORE PRIVATE -w) # disable warnings for third party
endif()

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/workloads)

add_flow_target(EXECUTABLE NAME fdbserver SRCS ${FDBSERVER_SRCS})

target_include_directories(fdbserver PRIVATE
  ${CMAKE_SOURCE_DIR}/bindings/c
  ${CMAKE_BINARY_DIR}/bindings/c
  ${CMAKE_CURRENT_BINARY_DIR}/workloads
  ${CMAKE_CURRENT_SOURCE_DIR}/workloads)
if (WITH_ROCKSDB_EXPERIMENTAL)
  add_dependencies(fdbserver rocksdb)
  target_include_directories(fdbserver PRIVATE ${ROCKSDB_INCLUDE_DIR})
  target_link_libraries(fdbserver PRIVATE fdbclient fdb_sqlite ${ROCKSDB_LIBRARIES})
else()
  target_link_libraries(fdbserver PRIVATE fdbclient fdb_sqlite)
endif()

if (GPERFTOOLS_FOUND)
  add_compile_definitions(USE_GPERFTOOLS)
  target_link_libraries(fdbserver PRIVATE gperftools)
endif()

if(GENERATE_DEBUG_PACKAGES)
  fdb_install(TARGETS fdbserver DESTINATION sbin COMPONENT server)
else()
  add_custom_target(prepare_fdbserver_install ALL DEPENDS strip_only_fdbserver)
  fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbserver DESTINATION sbin COMPONENT server)
endif()
