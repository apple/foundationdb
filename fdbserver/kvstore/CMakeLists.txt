set(FDBSERVER_KVSTORE_SRCS
  DiskQueue.actor.cpp
  FDBExecHelper.cpp
  IKeyValueStore.cpp
  IPager.cpp
  KeyValueStoreCompressTestData.cpp
  KeyValueStoreMemory.actor.cpp
  KeyValueStoreRocksDB.actor.cpp
  KeyValueStoreShardedRocksDB.actor.cpp
  KeyValueStoreSQLite.cpp
  RocksDBCommon.cpp
  RocksDBLogForwarder.cpp
  TransactionStoreMutationTracking.cpp
  VFSAsync.cpp
  VersionedBTree.actor.cpp
  VersionedBTreeDebug.cpp)

add_flow_target(STATIC_LIBRARY NAME fdbserver_kvstore SRCS ${FDBSERVER_KVSTORE_SRCS})
add_fdbserver_link_test(fdbserver_kvstorelinktest
  fdbserver_kvstore
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_kvstore)
target_include_directories(fdbserver_kvstore
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/contrib/sqlite
    ${CMAKE_SOURCE_DIR}/fdbserver/include
    ${CMAKE_BINARY_DIR}/fdbserver/include)
target_link_libraries(fdbserver_kvstore PUBLIC fdbserver_core sqlite)

if(WITH_ROCKSDB)
  add_dependencies(fdbserver_kvstore rocksdb)
  if(WITH_LIBURING)
    target_include_directories(fdbserver_kvstore PRIVATE ${ROCKSDB_INCLUDE_DIR} ${uring_INCLUDE_DIR})
    target_link_libraries(fdbserver_kvstore PUBLIC ${ROCKSDB_LIBRARIES} ${uring_LIBRARIES} ${LZ4_LIBRARY})
  else()
    target_include_directories(fdbserver_kvstore PRIVATE ${ROCKSDB_INCLUDE_DIR})
    target_link_libraries(fdbserver_kvstore PUBLIC ${ROCKSDB_LIBRARIES} ${LZ4_LIBRARY})
  endif()
  target_compile_definitions(fdbserver_kvstore PRIVATE WITH_ROCKSDB)
endif()
