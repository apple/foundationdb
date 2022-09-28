fdb_find_sources(FDBSERVER_SRCS)

if(${COROUTINE_IMPL} STREQUAL libcoro)
  list(APPEND FDBSERVER_SRCS coroimpl/CoroFlowCoro.actor.cpp)
else()
  list(APPEND FDBSERVER_SRCS coroimpl/CoroFlow.actor.cpp)
endif()

if(WITH_ROCKSDB_EXPERIMENTAL)
  find_package(RocksDB REQUIRED)
  if(WITH_LIBURING)
    find_package(uring)
  endif()
endif()

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/workloads)

add_flow_target(EXECUTABLE NAME fdbserver SRCS ${FDBSERVER_SRCS})

target_include_directories(fdbserver PRIVATE
  ${CMAKE_SOURCE_DIR}/bindings/c
  ${CMAKE_BINARY_DIR}/bindings/c
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_BINARY_DIR}/include)
if (WITH_ROCKSDB_EXPERIMENTAL)
  if(WITH_LIBURING)
    target_compile_definitions(fdbserver PRIVATE BOOST_ASIO_HAS_IO_URING=1 BOOST_ASIO_DISABLE_EPOLL=1)
  endif()
  target_compile_definitions(fdbserver PRIVATE SSD_ROCKSDB_EXPERIMENTAL)
  target_link_libraries(fdbserver PRIVATE RocksDB::rocksdb)
endif()
target_link_libraries(fdbserver PRIVATE fdbclient sqlite toml11_target jemalloc rapidjson)

# target_compile_definitions(fdbserver PRIVATE -DENABLE_SAMPLING)

if(GPERFTOOLS_FOUND)
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
