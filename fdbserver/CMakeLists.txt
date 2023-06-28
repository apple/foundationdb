fdb_find_sources(FDBSERVER_SRCS)

if(${COROUTINE_IMPL} STREQUAL libcoro)
  list(APPEND FDBSERVER_SRCS coroimpl/CoroFlowCoro.actor.cpp)
else()
  list(APPEND FDBSERVER_SRCS coroimpl/CoroFlow.actor.cpp)
endif()

if(WITH_ROCKSDB_EXPERIMENTAL)
  include(CompileRocksDB)
  # CompileRocksDB sets `lz4_LIBRARIES` to be the shared lib, we want to link
  # statically, so find the static library here.
  find_library(lz4_STATIC_LIBRARIES
    NAMES liblz4.a REQUIRED)
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
  add_dependencies(fdbserver rocksdb)
  if(WITH_LIBURING)
    target_include_directories(fdbserver PRIVATE ${ROCKSDB_INCLUDE_DIR} ${uring_INCLUDE_DIR})
    target_link_libraries(fdbserver PRIVATE fdbclient metacluster sqlite ${ROCKSDB_LIBRARIES} ${uring_LIBRARIES} ${lz4_STATIC_LIBRARIES})
    target_compile_definitions(fdbserver PRIVATE BOOST_ASIO_HAS_IO_URING=1 BOOST_ASIO_DISABLE_EPOLL=1)
  else()
    target_include_directories(fdbserver PRIVATE ${ROCKSDB_INCLUDE_DIR})
    target_link_libraries(fdbserver PRIVATE fdbclient metacluster sqlite ${ROCKSDB_LIBRARIES} ${lz4_STATIC_LIBRARIES})
    target_compile_definitions(fdbserver PRIVATE)
  endif()
else()
  target_link_libraries(fdbserver PRIVATE fdbclient metacluster sqlite)
endif()

if(USE_JEMALLOC)
  target_include_directories(fdbserver INTERFACE $<TARGET_PROPERTY:jemalloc::jemalloc, INTERFACE_INCLUDE_DIRECTORIES>)
  target_link_libraries(fdbserver PRIVATE jemalloc::jemalloc)
endif()

target_link_libraries(fdbserver PRIVATE toml11_target rapidjson)
if(WITH_ROCKSDB_EXPERIMENTAL)
  target_compile_definitions(fdbserver PRIVATE SSD_ROCKSDB_EXPERIMENTAL)
endif()
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

## This is taken from the the google benchmark documentation.
## It breaks if you attempt to re-use a build of googlebenchmark across FDB
## builds.

# include the configurations from benchmark.cmake
configure_file(benchmark.cmake googlebenchmark-download/CMakeLists.txt)
# executing the configuration step
execute_process(
  COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE results
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-download
)
# checking if the configuration step passed
if(results)
  message(FATAL_ERROR "Configuration step for Benchmark has Failed. ${results}")
endif()
# executing the build step
execute_process(
  COMMAND ${CMAKE_COMMAND} --build . --config Release
  RESULT_VARIABLE results
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-download
)
# checking if the build step passed
if(results)
  message(FATAL_ERROR "Build step for Benchmark has Failed. ${results}")
endif()
set(BENCHMARK_ENABLE_TESTING OFF)
add_subdirectory(
  ${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-src
  ${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-build
  EXCLUDE_FROM_ALL
)

set_target_properties(benchmark PROPERTIES COMPILE_OPTIONS "")
set_target_properties(benchmark_main PROPERTIES COMPILE_OPTIONS "")

target_include_directories(fdbserver SYSTEM PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-src/include)
target_link_libraries(fdbserver PRIVATE benchmark)
if(FLOW_USE_ZSTD)
   target_include_directories(fdbserver PRIVATE ${ZSTD_LIB_INCLUDE_DIR})
endif()
