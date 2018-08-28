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
macro(add_fdb_test)
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
    COMMAND python3 ${TestRunner}
    -n ${test_name}
    -b ${PROJECT_BINARY_DIR}
    -t ${test_type}
    --seed ${SEED}
    ${BUGGIFY_OPTION}
    ${ADD_FDB_TEST_TEST_FILES}
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR})
  set_tests_properties(${test_name} PROPERTIES TIMEOUT ${this_test_timeout})
endmacro()

add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/AsyncFileCorrectness.txt UNIT)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/restarting/CycleTestRestart-1.txt
                        ${PROJECT_SOURCE_DIR}/tests/restarting/CycleTestRestart-2.txt
             TEST_NAME CycleTestRestart)
add_fdb_test(TEST_FILES ${PROJECT_SOURCE_DIR}/tests/BigInsert.txt)

