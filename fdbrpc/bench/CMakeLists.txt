include(FDBBenchmark)

fdb_find_sources(FDBRPC_BENCH_SRCS)
add_flow_target(EXECUTABLE NAME fdbrpc_bench SRCS ${FDBRPC_BENCH_SRCS})

fdb_setup_googlebenchmark()

target_include_directories(
  fdbrpc_bench
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}"
          "${CMAKE_CURRENT_BINARY_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/.."
          "${CMAKE_CURRENT_SOURCE_DIR}/include"
          "${CMAKE_CURRENT_BINARY_DIR}/include")

target_link_libraries(fdbrpc_bench PRIVATE Threads::Threads fdb_google_benchmark flow fdbrpc)
