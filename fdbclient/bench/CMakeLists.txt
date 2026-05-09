include(FDBBenchmark)

fdb_find_sources(FDBCLIENT_BENCH_SRCS)
add_flow_target(EXECUTABLE NAME fdbclient_bench SRCS ${FDBCLIENT_BENCH_SRCS})

fdb_setup_googlebenchmark()

target_include_directories(
  fdbclient_bench
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")

target_link_libraries(fdbclient_bench PRIVATE Threads::Threads fdb_google_benchmark fdbclient)
