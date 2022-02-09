set(FDBSERVER_SRCS
  ApplyMetadataMutation.h
  ApplyMetadataMutation.cpp
  BackupInterface.h
  BackupProgress.actor.cpp
  BackupProgress.actor.h
  BackupWorker.actor.cpp
  BlobManager.actor.cpp
  BlobManagerInterface.h
  BlobWorker.actor.cpp
  ClusterController.actor.h
  ClusterController.actor.cpp
  ClusterRecovery.actor.h
  ClusterRecovery.actor.cpp
  ConfigBroadcaster.actor.cpp
  ConfigBroadcaster.h
  ConfigDatabaseUnitTests.actor.cpp
  ConfigFollowerInterface.cpp
  ConfigFollowerInterface.h
  ConfigNode.actor.cpp
  ConfigNode.h
  ConflictSet.h
  CoordinatedState.actor.cpp
  CoordinatedState.h
  Coordination.actor.cpp
  CoordinationInterface.h
  CoroFlow.h
  DataDistribution.actor.cpp
  DataDistribution.actor.h
  DataDistributionQueue.actor.cpp
  DataDistributionTracker.actor.cpp
  DataDistributorInterface.h
  DBCoreState.h
  DiskQueue.actor.cpp
  EncryptKeyProxyInterface.h
  EncryptKeyProxy.actor.cpp
  fdbserver.actor.cpp
  FDBExecHelper.actor.cpp
  FDBExecHelper.actor.h
  GrvProxyServer.actor.cpp
  IConfigConsumer.cpp
  IConfigConsumer.h
  IDiskQueue.h
  IKeyValueContainer.h
  IKeyValueStore.h
  IPager.h
  KeyValueStoreCompressTestData.actor.cpp
  KeyValueStoreMemory.actor.cpp
  KeyValueStoreRocksDB.actor.cpp
  KeyValueStoreSQLite.actor.cpp
  Knobs.h
  LatencyBandConfig.cpp
  LatencyBandConfig.h
  LeaderElection.actor.cpp
  LeaderElection.h
  LocalConfiguration.actor.cpp
  LocalConfiguration.h
  LogProtocolMessage.h
  LogRouter.actor.cpp
  LogSystem.cpp
  LogSystem.h
  LogSystemConfig.cpp
  LogSystemConfig.h
  LogSystemDiskQueueAdapter.actor.cpp
  LogSystemDiskQueueAdapter.h
  LogSystemPeekCursor.actor.cpp
  MasterInterface.h
  MetricLogger.actor.cpp
  MetricLogger.actor.h
  CommitProxyServer.actor.cpp
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
  OnDemandStore.actor.cpp
  OnDemandStore.h
  PaxosConfigConsumer.actor.cpp
  PaxosConfigConsumer.h
  ProxyCommitData.actor.h
  pubsub.actor.cpp
  pubsub.h
  QuietDatabase.actor.cpp
  QuietDatabase.h
  RadixTree.h
  Ratekeeper.actor.cpp
  RatekeeperInterface.h
  RecoveryState.h
  RestoreCommon.actor.h
  RestoreCommon.actor.cpp
  RestoreUtil.h
  RestoreUtil.actor.cpp
  RestoreRoleCommon.actor.h
  RestoreRoleCommon.actor.cpp
  RestoreController.actor.h
  RestoreController.actor.cpp
  RestoreApplier.actor.h
  RestoreApplier.actor.cpp
  RestoreLoader.actor.h
  RestoreLoader.actor.cpp
  RestoreWorker.actor.h
  RestoreWorker.actor.cpp
  RestoreWorkerInterface.actor.cpp
  RestoreWorkerInterface.actor.h
  Resolver.actor.cpp
  ResolverInterface.h
  RoleLineage.actor.h
  RoleLineage.actor.cpp
  ServerDBInfo.actor.h
  ServerDBInfo.h
  SigStack.cpp
  SimpleConfigConsumer.actor.cpp
  SimpleConfigConsumer.h
  SimulatedCluster.actor.cpp
  SimulatedCluster.h
  SkipList.cpp
  SpanContextMessage.h
  Status.actor.cpp
  Status.h
  StorageCache.actor.cpp
  StorageMetrics.actor.h
  StorageMetrics.h
  storageserver.actor.cpp
  TagPartitionedLogSystem.actor.cpp
  TagPartitionedLogSystem.actor.h
  template_fdb.h
  tester.actor.cpp
  TesterInterface.actor.h
  TLogInterface.h
  TLogServer.actor.cpp
  TSSMappingUtil.actor.h
  TSSMappingUtil.actor.cpp
  VersionedBTree.actor.cpp
  VFSAsync.h
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
  workloads/AtomicOpsApiCorrectness.actor.cpp
  workloads/AtomicRestore.actor.cpp
  workloads/AtomicSwitchover.actor.cpp
  workloads/BackgroundSelectors.actor.cpp
  workloads/BackupCorrectness.actor.cpp
  workloads/BackupAndParallelRestoreCorrectness.actor.cpp
  workloads/ClogSingleConnection.actor.cpp
  workloads/ConfigIncrement.actor.cpp
  workloads/BackupToBlob.actor.cpp
  workloads/BackupToDBAbort.actor.cpp
  workloads/BackupToDBCorrectness.actor.cpp
  workloads/BackupToDBUpgrade.actor.cpp
  workloads/BlobStoreWorkload.h
  workloads/BlobGranuleVerifier.actor.cpp
  workloads/BulkLoad.actor.cpp
  workloads/BulkSetup.actor.h
  workloads/Cache.actor.cpp
  workloads/ChangeConfig.actor.cpp
  workloads/ClearSingleRange.actor.cpp
  workloads/ClientLibManagementWorkload.actor.cpp
  workloads/ClientTransactionProfileCorrectness.actor.cpp
  workloads/TriggerRecovery.actor.cpp
  workloads/SuspendProcesses.actor.cpp
  workloads/CommitBugCheck.actor.cpp
  workloads/ConfigureDatabase.actor.cpp
  workloads/ConflictRange.actor.cpp
  workloads/ConsistencyCheck.actor.cpp
  workloads/CpuProfiler.actor.cpp
  workloads/Cycle.actor.cpp
  workloads/ChangeFeeds.actor.cpp
  workloads/DataDistributionMetrics.actor.cpp
  workloads/DataLossRecovery.actor.cpp
  workloads/DDBalance.actor.cpp
  workloads/DDMetrics.actor.cpp
  workloads/DDMetricsExclude.actor.cpp
  workloads/DiskDurability.actor.cpp
  workloads/DiskDurabilityTest.actor.cpp
  workloads/DiskFailureInjection.actor.cpp
  workloads/DummyWorkload.actor.cpp
  workloads/EncryptionOps.actor.cpp
  workloads/ExternalWorkload.actor.cpp
  workloads/FastTriggeredWatches.actor.cpp
  workloads/FileSystem.actor.cpp
  workloads/Fuzz.cpp
  workloads/FuzzApiCorrectness.actor.cpp
  workloads/GetRangeStream.actor.cpp
  workloads/HealthMetricsApi.actor.cpp
  workloads/HighContentionPrefixAllocatorWorkload.actor.cpp
  workloads/IncrementalBackup.actor.cpp
  workloads/Increment.actor.cpp
  workloads/IndexScan.actor.cpp
  workloads/Inventory.actor.cpp
  workloads/KVStoreTest.actor.cpp
  workloads/KillRegion.actor.cpp
  workloads/LockDatabase.actor.cpp
  workloads/LockDatabaseFrequently.actor.cpp
  workloads/LocalRatekeeper.actor.cpp
  workloads/LogMetrics.actor.cpp
  workloads/LowLatency.actor.cpp
  workloads/MachineAttrition.actor.cpp
  workloads/Mako.actor.cpp
  workloads/MemoryKeyValueStore.cpp
  workloads/MemoryKeyValueStore.h
  workloads/MemoryLifetime.actor.cpp
  workloads/MetricLogging.actor.cpp
  workloads/MiniCycle.actor.cpp
  workloads/MutationLogReaderCorrectness.actor.cpp
  workloads/GetRangeAndMap.actor.cpp
  workloads/ParallelRestore.actor.cpp
  workloads/Performance.actor.cpp
  workloads/Ping.actor.cpp
  workloads/PopulateTPCC.actor.cpp
  workloads/ProtocolVersion.actor.cpp
  workloads/PubSubMultiples.actor.cpp
  workloads/QueuePush.actor.cpp
  workloads/RandomClogging.actor.cpp
  workloads/RandomMoveKeys.actor.cpp
  workloads/RandomSelector.actor.cpp
  workloads/ReadAfterWrite.actor.cpp
  workloads/ReadHotDetection.actor.cpp
  workloads/ReadWrite.actor.cpp
  workloads/RemoveServersSafely.actor.cpp
  workloads/ReportConflictingKeys.actor.cpp
  workloads/RestoreBackup.actor.cpp
  workloads/RestoreFromBlob.actor.cpp
  workloads/Rollback.actor.cpp
  workloads/RyowCorrectness.actor.cpp
  workloads/RYWDisable.actor.cpp
  workloads/RYWPerformance.actor.cpp
  workloads/SaveAndKill.actor.cpp
  workloads/SelectorCorrectness.actor.cpp
  workloads/Serializability.actor.cpp
  workloads/Sideband.actor.cpp
  workloads/SimpleAtomicAdd.actor.cpp
  workloads/SlowTaskWorkload.actor.cpp
  workloads/SnapTest.actor.cpp
  workloads/SpecialKeySpaceCorrectness.actor.cpp
  workloads/StreamingRangeRead.actor.cpp
  workloads/StatusWorkload.actor.cpp
  workloads/Storefront.actor.cpp
  workloads/StreamingRead.actor.cpp
  workloads/SubmitBackup.actor.cpp
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
  workloads/UDPWorkload.actor.cpp
  workloads/VersionStamp.actor.cpp
  workloads/WatchAndWait.actor.cpp
  workloads/Watches.actor.cpp
  workloads/WatchesSameKeyCorrectness.actor.cpp
  workloads/WorkerErrors.actor.cpp
  workloads/workloads.actor.h
  workloads/WriteBandwidth.actor.cpp
  workloads/WriteDuringRead.actor.cpp
  workloads/WriteTagThrottling.actor.cpp
)

if(${COROUTINE_IMPL} STREQUAL libcoro)
  list(APPEND FDBSERVER_SRCS CoroFlowCoro.actor.cpp)
else()
  list(APPEND FDBSERVER_SRCS CoroFlow.actor.cpp)
endif()

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

  include(CompileRocksDB)
  # CompileRocksDB sets `lz4_LIBRARIES` to be the shared lib, we want to link
  # statically, so find the static library here.
  find_library(lz4_STATIC_LIBRARIES
    NAMES liblz4.a REQUIRED)
  if (WITH_LIBURING)
    find_package(uring)
  endif()
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
  if(WITH_LIBURING)
    target_include_directories(fdbserver PRIVATE ${ROCKSDB_INCLUDE_DIR} ${uring_INCLUDE_DIR})
    target_link_libraries(fdbserver PRIVATE fdbclient fdb_sqlite ${ROCKSDB_LIBRARIES} ${uring_LIBRARIES} ${lz4_STATIC_LIBRARIES})
    target_compile_definitions(fdbserver PRIVATE BOOST_ASIO_HAS_IO_URING=1 BOOST_ASIO_DISABLE_EPOLL=1)
  else()
    target_include_directories(fdbserver PRIVATE ${ROCKSDB_INCLUDE_DIR})
    target_link_libraries(fdbserver PRIVATE fdbclient fdb_sqlite ${ROCKSDB_LIBRARIES} ${lz4_STATIC_LIBRARIES})
    target_compile_definitions(fdbserver PRIVATE)
  endif()
else()
  target_link_libraries(fdbserver PRIVATE fdbclient fdb_sqlite)
endif()

target_link_libraries(fdbserver PRIVATE toml11_target jemalloc)
# target_compile_definitions(fdbserver PRIVATE -DENABLE_SAMPLING)

if (GPERFTOOLS_FOUND)
  target_link_libraries(fdbserver PRIVATE gperftools)
endif()

if(NOT OPEN_FOR_IDE)
  if(GENERATE_DEBUG_PACKAGES)
    fdb_install(TARGETS fdbserver DESTINATION sbin COMPONENT server)
  else()
    add_custom_target(prepare_fdbserver_install ALL DEPENDS strip_only_fdbserver)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbserver DESTINATION sbin COMPONENT server)
  endif()
endif()
