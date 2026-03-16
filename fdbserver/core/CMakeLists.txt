fdb_find_sources(FDBSERVER_CORE_LOCAL_SRCS)

set(FDBSERVER_CORE_TO_KVSTORE_SRCS
  DiskQueue.actor.cpp
  FDBExecHelper.actor.cpp
  IKeyValueStore.cpp
  IPager.cpp
  KeyValueStoreCompressTestData.actor.cpp
  KeyValueStoreMemory.actor.cpp
  KeyValueStoreRocksDB.actor.cpp
  KeyValueStoreShardedRocksDB.actor.cpp
  KeyValueStoreSQLite.actor.cpp
  RemoteIKeyValueStore.actor.cpp
  RocksDBCommon.cpp
  RocksDBLogForwarder.cpp
  TransactionStoreMutationTracking.cpp
  VFSAsync.cpp
  VersionedBTree.actor.cpp
  VersionedBTreeDebug.cpp)
list(REMOVE_ITEM FDBSERVER_CORE_LOCAL_SRCS ${FDBSERVER_CORE_TO_KVSTORE_SRCS})

set(FDBSERVER_CORE_TO_LOGSYSTEM_SRCS
  LogSystemDiskQueueAdapter.cpp
  LogSystemFactory.actor.cpp
  LogSystemPeekCursor.actor.cpp
  TagPartitionedLogSystem.actor.cpp
  TagPartitionedLogSystemRecoveryTests.cpp)
list(REMOVE_ITEM FDBSERVER_CORE_LOCAL_SRCS ${FDBSERVER_CORE_TO_LOGSYSTEM_SRCS})

set(FDBSERVER_CORE_SRCS ${FDBSERVER_CORE_LOCAL_SRCS})
if(${COROUTINE_IMPL} STREQUAL libcoro)
  list(APPEND FDBSERVER_CORE_SRCS ../coroimpl/CoroFlowCoro.actor.cpp)
else()
  list(APPEND FDBSERVER_CORE_SRCS ../coroimpl/CoroFlow.actor.cpp)
endif()

add_flow_target(STATIC_LIBRARY NAME fdbserver_core SRCS ${FDBSERVER_CORE_SRCS})

configure_fdbserver_common_includes(fdbserver_core)
target_include_directories(fdbserver_core
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
  PRIVATE
    ${CMAKE_SOURCE_DIR}/fdbserver/include
    ${CMAKE_BINARY_DIR}/fdbserver/include)
target_link_libraries(fdbserver_core PRIVATE fdbclient)
