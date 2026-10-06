fdb_find_sources(FDBSERVER_CHECKPOINT_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_checkpoint SRCS ${FDBSERVER_CHECKPOINT_SRCS})
add_fdbserver_link_test(fdbserver_checkpointlinktest
  fdbserver_checkpoint
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_checkpoint)
target_include_directories(fdbserver_checkpoint
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  PRIVATE
    ${CMAKE_SOURCE_DIR}/fdbserver/include)
target_link_libraries(fdbserver_checkpoint PUBLIC fdbserver_core)

if(WITH_ROCKSDB)
  add_dependencies(fdbserver_checkpoint rocksdb)
  if(WITH_LIBURING)
    target_include_directories(fdbserver_checkpoint PRIVATE ${ROCKSDB_INCLUDE_DIR} ${uring_INCLUDE_DIR})
    target_link_libraries(fdbserver_checkpoint PRIVATE ${ROCKSDB_LIBRARIES} ${uring_LIBRARIES} ${LZ4_LIBRARY})
  else()
    target_include_directories(fdbserver_checkpoint PRIVATE ${ROCKSDB_INCLUDE_DIR})
    target_link_libraries(fdbserver_checkpoint PRIVATE ${ROCKSDB_LIBRARIES} ${LZ4_LIBRARY})
  endif()
  target_compile_definitions(fdbserver_checkpoint PRIVATE WITH_ROCKSDB)
endif()
