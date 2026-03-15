fdb_find_sources(FDBSERVER_CORE_LOCAL_SRCS)
add_flow_target(STATIC_LIBRARY NAME fdbserver_core SRCS ${FDBSERVER_CORE_LOCAL_SRCS})

configure_fdbserver_common_includes(fdbserver_core)
target_include_directories(fdbserver_core
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
  PRIVATE
    ${CMAKE_SOURCE_DIR}/contrib/sqlite
    ${CMAKE_SOURCE_DIR}/fdbserver/include
    ${CMAKE_BINARY_DIR}/fdbserver/include)
target_link_libraries(fdbserver_core PRIVATE fdbclient PUBLIC sqlite)

if(WITH_ROCKSDB)
  add_dependencies(fdbserver_core rocksdb)
  if(WITH_LIBURING)
    target_include_directories(fdbserver_core PRIVATE ${ROCKSDB_INCLUDE_DIR} ${uring_INCLUDE_DIR})
    target_link_libraries(fdbserver_core PUBLIC ${ROCKSDB_LIBRARIES} ${uring_LIBRARIES} ${LZ4_LIBRARY})
  else()
    target_include_directories(fdbserver_core PRIVATE ${ROCKSDB_INCLUDE_DIR})
    target_link_libraries(fdbserver_core PUBLIC ${ROCKSDB_LIBRARIES} ${LZ4_LIBRARY})
  endif()
  target_compile_definitions(fdbserver_core PRIVATE WITH_ROCKSDB)
endif()
