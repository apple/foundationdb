set(FDBSERVER_SRCS
  ApplyMetadataMutation.h
  ClusterController.actor.cpp
  ClusterRecruitmentInterface.h
  ConflictSet.h
  CoordinatedState.actor.cpp
  CoordinatedState.h
  Coordination.actor.cpp
  CoordinationInterface.h
  CoroFlow.actor.cpp
  CoroFlow.h
  DataDistribution.actor.cpp
  DataDistribution.h
  DataDistributionQueue.actor.cpp
  DataDistributionTracker.actor.cpp
  DBCoreState.h
  DiskQueue.actor.cpp
  fdbserver.actor.cpp
  IDiskQueue.h
  IKeyValueStore.h
  IPager.h
  IVersionedStore.h
  IndirectShadowPager.actor.cpp
  IndirectShadowPager.h
  KeyValueStoreCompressTestData.actor.cpp
  KeyValueStoreMemory.actor.cpp
  KeyValueStoreSQLite.actor.cpp
  Knobs.cpp
  Knobs.h
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
  MemoryPager.actor.cpp
  MemoryPager.h
  MoveKeys.actor.cpp
  MoveKeys.h
  networktest.actor.cpp
  NetworkTest.h
  OldTLogServer.actor.cpp
  Orderer.actor.h
  pubsub.actor.cpp
  pubsub.h
  QuietDatabase.actor.cpp
  QuietDatabase.h
  Ratekeeper.actor.cpp
  Ratekeeper.h
  RecoveryState.h
  Restore.actor.cpp
  RestoreInterface.h
  Resolver.actor.cpp
  ResolverInterface.h
  ServerDBInfo.h
  SimulatedCluster.actor.cpp
  SimulatedCluster.h
  SkipList.cpp
  sqlite/btree.h
  sqlite/hash.h
  sqlite/sqlite3.h
  sqlite/sqlite3ext.h
  sqlite/sqliteInt.h
  sqlite/sqliteLimit.h
  sqlite/sqlite3.amalgamation.c
  Status.actor.cpp
  Status.h
  StorageMetrics.actor.h
  StorageMetrics.h
  storageserver.actor.cpp
  TagPartitionedLogSystem.actor.cpp
  template_fdb.h
  tester.actor.cpp
  TesterInterface.h
  TLogInterface.h
  TLogServer.actor.cpp
  VersionedBTree.actor.cpp
  VFSAsync.cpp
  WaitFailure.actor.cpp
  WaitFailure.h
  worker.actor.cpp
  WorkerInterface.h
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
  workloads/BackupToDBAbort.actor.cpp
  workloads/BackupToDBCorrectness.actor.cpp
  workloads/BackupToDBUpgrade.actor.cpp
  workloads/BulkLoad.actor.cpp
  workloads/BulkSetup.actor.h
  workloads/ChangeConfig.actor.cpp
  workloads/ClientTransactionProfileCorrectness.actor.cpp
  workloads/CommitBugCheck.actor.cpp
  workloads/ConfigureDatabase.actor.cpp
  workloads/ConflictRange.actor.cpp
  workloads/ConsistencyCheck.actor.cpp
  workloads/CpuProfiler.actor.cpp
  workloads/Cycle.actor.cpp
  workloads/DDBalance.actor.cpp
  workloads/DDMetrics.actor.cpp
  workloads/DiskDurability.actor.cpp
  workloads/DiskDurabilityTest.actor.cpp
  workloads/DummyWorkload.actor.cpp
  workloads/FastTriggeredWatches.actor.cpp
  workloads/FileSystem.actor.cpp
  workloads/Fuzz.cpp
  workloads/FuzzApiCorrectness.actor.cpp
  workloads/Increment.actor.cpp
  workloads/IndexScan.actor.cpp
  workloads/Inventory.actor.cpp
  workloads/KVStoreTest.actor.cpp
  workloads/LockDatabase.actor.cpp
  workloads/LogMetrics.actor.cpp
  workloads/LowLatency.actor.cpp
  workloads/MachineAttrition.actor.cpp
  workloads/MemoryKeyValueStore.cpp
  workloads/MemoryKeyValueStore.h
  workloads/MemoryLifetime.actor.cpp
  workloads/MetricLogging.actor.cpp
  workloads/Performance.actor.cpp
  workloads/Ping.actor.cpp
  workloads/PubSubMultiples.actor.cpp
  workloads/QueuePush.actor.cpp
  workloads/RandomClogging.actor.cpp
  workloads/RandomMoveKeys.actor.cpp
  workloads/RandomSelector.actor.cpp
  workloads/ReadWrite.actor.cpp
  workloads/RemoveServersSafely.actor.cpp
  workloads/Rollback.actor.cpp
  workloads/RyowCorrectness.actor.cpp
  workloads/RYWDisable.actor.cpp
  workloads/RYWPerformance.actor.cpp
  workloads/SaveAndKill.actor.cpp
  workloads/SelectorCorrectness.actor.cpp
  workloads/Serializability.actor.cpp
  workloads/Sideband.actor.cpp
  workloads/SlowTaskWorkload.actor.cpp
  workloads/StatusWorkload.actor.cpp
  workloads/Storefront.actor.cpp
  workloads/StreamingRead.actor.cpp
  workloads/TargetedKill.actor.cpp
  workloads/TaskBucketCorrectness.actor.cpp
  workloads/ThreadSafety.actor.cpp
  workloads/Throughput.actor.cpp
  workloads/TimeKeeperCorrectness.actor.cpp
  workloads/UnitPerf.actor.cpp
  workloads/UnitTests.actor.cpp
  workloads/Unreadable.actor.cpp
  workloads/VersionStamp.actor.cpp
  workloads/WatchAndWait.actor.cpp
  workloads/Watches.actor.cpp
  workloads/WorkerErrors.actor.cpp
  workloads/workloads.h
  workloads/WriteBandwidth.actor.cpp
  workloads/WriteDuringRead.actor.cpp)

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/workloads)

actor_set(FDBSERVER_BUILD "${FDBSERVER_SRCS}")
add_executable(fdbserver ${FDBSERVER_BUILD})
actor_compile(fdbserver "${FDBSERVER_SRCS}")
target_include_directories(fdbserver PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/workloads
  ${CMAKE_CURRENT_SOURCE_DIR}/workloads)
target_link_libraries(fdbserver PRIVATE fdbclient)

install(TARGETS fdbserver DESTINATION ${FDB_SBIN_DIR} COMPONENT server)

################################################################################
# Testing
################################################################################

set(ENABLE_BUGGIFY OFF CACHE BOOL "Enable buggify for tests")

set(TestRunner "${PROJECT_SOURCE_DIR}/tests/TestRunner/TestRunner.py")

configure_file(${PROJECT_SOURCE_DIR}/tests/CTestCustom.ctest ${PROJECT_BINARY_DIR}/CTestCustom.ctest @ONLY)

# This will add a test that can be run by ctest. This macro can be called
# with the following arguments:
#
# - UNIT will run the test as a unit test (it won't bring up a whole simulated system)
# - TEST_NAME followed the name of the test
# - TIMEOUT followed by a timeout - reaching the timeout makes the test fail (default is
#   3600 seconds). The timeout will be reached whenever it ran either too long in simulated
#   time or in real time - whatever is smaller.
# - TEST_FILES followed by typically one test file. The test runner will run
#   all these tests in serialized order and within the same directory. This is
#   useful for restart tests
function(add_fdb_test)
  set(options UNIT)
  set(oneValueArgs TEST_NAME TIMEOUT)
  set(multiValueArgs TEST_FILES)
  cmake_parse_arguments(ADD_FDB_TEST "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")
  set(this_test_timeout ${ADD_FDB_TEST_TIMEOUT})
  if(NOT this_test_timeout)
    set(this_test_timeout 3600)
  endif()
  set(test_type "simulation")
  list(LENGTH ADD_FDB_TEST_TEST_FILES NUM_TEST_FILES)
  if(ADD_FDB_TEST_UNIT)
    set(test_type "test")
  endif()
  list(GET ADD_FDB_TEST_TEST_FILES 0 first_file)
  get_filename_component(test_name ${first_file} NAME_WE)
  if (NOT "${ADD_FDB_TEST_TEST_NAME}" STREQUAL "")
    set(test_name ${ADD_FDB_TEST_TEST_NAME})
  endif()
  if(ADD_FDB_TEST_UNIT)
    message(STATUS
      "ADDING UNIT TEST ${test_name}")
  else()
    message(STATUS
      "ADDING SIMULATOR TEST ${test_name}")
  endif()
  set(test_files "")
  foreach(curr_test_file ${ADD_FDB_TEST_TEST_FILES})
    set(test_files "${test_files} ${curr_test_file}")
  endforeach()
  set(BUGGIFY_OPTION "")
  if (ENABLE_BUGGIFY)
    set(BUGGIFY_OPTION "-B")
  endif()
  add_test(NAME ${test_name}
    COMMAND ${PYTHON_EXECUTABLE} ${TestRunner}
    -n ${test_name}
    -b ${PROJECT_BINARY_DIR}
    -t ${test_type}
    --seed ${SEED}
    ${BUGGIFY_OPTION}
    ${ADD_FDB_TEST_TEST_FILES}
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR})
	get_filename_component(test_dir_full ${first_file} DIRECTORY)
	get_filename_component(test_dir ${test_dir_full} NAME)
	set_tests_properties(${test_name} PROPERTIES TIMEOUT ${this_test_timeout} LABELS "${test_dir}")
endfunction()

#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/AsyncFileCorrectness.txt UNIT)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/AsyncFileMix.txt UNIT)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/AsyncFileRead.txt UNIT)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/AsyncFileReadRandom.txt UNIT)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/AsyncFileWrite.txt UNIT)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/BackupContainers.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/BandwidthThrottle.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/BigInsert.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/BlobStore.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/ConsistencyCheck.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/DiskDurability.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/FileSystem.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/Happy.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/IncrementalDelete.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/KVStoreMemTest.txt UNIT)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/KVStoreReadMostly.txt UNIT)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/KVStoreTest.txt UNIT)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/KVStoreTestRead.txt UNIT)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/KVStoreTestWrite.txt UNIT)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/KVStoreValueSize.txt UNIT)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/LayerStatusMerge.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/PureNetwork.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/RRW2500.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/RandomRead.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/RandomReadWrite.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/ReadAbsent.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/ReadHalfAbsent.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/SlowTask.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/SpecificUnitTest.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/StreamingWrite.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/ThreadSafety.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/TraceEventMetrics.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/default.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/errors.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fail.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/killall.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/latency.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/performance-fs.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/performance.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/ping.TXT)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/pingServers.TXT)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/pt.TXT)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/randomSelector.txt)
#add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/selectorCorrectness.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/AtomicBackupCorrectness.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/AtomicBackupToDBCorrectness.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/AtomicOps.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/AtomicOpsApiCorrectness.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/BackupCorrectness.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/BackupCorrectnessClean.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/BackupToDBCorrectness.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/BackupToDBCorrectnessClean.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/CloggedSideband.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/ConstrainedRandomSelector.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/CycleAndLock.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/CycleTest.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/FuzzApiCorrectness.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/FuzzApiCorrectnessClean.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/IncrementTest.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/InventoryTestAlmostReadOnly.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/InventoryTestSomeWrites.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/LongStackWriteDuringRead.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/LowLatency.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/MemoryLifetime.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/MoveKeysCycle.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/RandomSelector.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/RandomUnitTests.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/SelectorCorrectness.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/Sideband.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/SidebandWithStatus.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/SwizzledRollbackSideband.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/SystemRebootTestCycle.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/TaskBucketCorrectness.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/TimeKeeperCorrectness.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/Unreadable.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/VersionStamp.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/Watches.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/WriteDuringRead.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/fast/WriteDuringReadClean.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/rare/CheckRelocation.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/rare/ClogUnclog.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/rare/CloggedCycleWithKills.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/rare/ConflictRangeCheck.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/rare/ConflictRangeRYOWCheck.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/rare/CycleRollbackClogged.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/rare/CycleWithKills.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/rare/FuzzTest.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/rare/InventoryTestHeavyWrites.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/rare/LargeApiCorrectness.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/rare/LargeApiCorrectnessStatus.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/rare/RYWDisable.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/rare/RandomReadWriteTest.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/rare/SwizzledLargeApiCorrectness.txt)
add_fdb_test(
  TEST_FILES ${PROJECT_SOURCE_DIR}/tests/restarting/CycleTestRestart-1.txt
             ${PROJECT_SOURCE_DIR}/tests/restarting/CycleTestRestart-2.txt
  TEST_NAME CycleTestRestart)
add_fdb_test(
  TEST_FILES ${PROJECT_SOURCE_DIR}/tests/restarting/StorefrontTestRestart-1.txt
             ${PROJECT_SOURCE_DIR}/tests/restarting/StorefrontTestRestart-2.txt
  TEST_NAME StorefrontTestRestart)
add_fdb_test(
  TEST_FILES ${PROJECT_SOURCE_DIR}/tests/restarting/from_5.1.7/DrUpgradeRestart-1.txt
             ${PROJECT_SOURCE_DIR}/tests/restarting/from_5.1.7/DrUpgradeRestart-2.txt
  TEST_NAME DrUpgradeRestart)
add_fdb_test(
  TEST_FILES ${PROJECT_SOURCE_DIR}/tests/restarting/from_5.2.0/ClientTransactionProfilingCorrectness-1.txt
             ${PROJECT_SOURCE_DIR}/tests/restarting/from_5.2.0/ClientTransactionProfilingCorrectness-2.txt
  TEST_NAME ClientTransactionProfilingCorrectness)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/ApiCorrectness.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/ApiCorrectnessAtomicRestore.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/ApiCorrectnessSwitchover.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/ClogWithRollbacks.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/CloggedCycleTest.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/CloggedStorefront.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/CommitBug.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/ConfigureTest.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/CycleRollbackPlain.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/DDBalanceAndRemove.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/DDBalanceAndRemoveStatus.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/FastTriggeredWatches.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/LowLatencyWithFailures.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/MoveKeysClean.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/MoveKeysSideband.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/RyowCorrectness.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/Serializability.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/SharedBackupCorrectness.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/SharedBackupToDBCorrectness.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/StorefrontTest.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/SwizzledApiCorrectness.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/SwizzledCycleTest.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/SwizzledDdBalance.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/SwizzledRollbackTimeLapse.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/SwizzledRollbackTimeLapseIncrement.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/VersionStampBackupToDB.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/VersionStampSwitchover.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/WriteDuringReadAtomicRestore.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/WriteDuringReadSwitchover.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/slow/ddbalance.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/status/invalid_proc_addresses.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/status/local_6_machine_no_replicas_remain.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/status/separate_1_of_3_coordinators_remain.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/status/separate_2_of_3_coordinators_remain.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/status/separate_cannot_write_cluster_file.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/status/separate_idle.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/status/separate_initializing.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/status/separate_no_coordinators.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/status/separate_no_database.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/status/separate_no_servers.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/status/separate_not_enough_servers.txt)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/status/single_process_too_many_config_params.txt)
