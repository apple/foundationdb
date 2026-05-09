include(FDBBenchmark)

fdb_find_sources(FLOW_BENCH_SRCS)
add_flow_target(EXECUTABLE NAME flow_bench SRCS ${FLOW_BENCH_SRCS})

fdb_setup_googlebenchmark()

target_include_directories(
  flow_bench
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/.."
          "${CMAKE_CURRENT_SOURCE_DIR}/include")

if(FLOW_USE_ZSTD)
  target_include_directories(flow_bench PRIVATE ${ZSTD_LIB_INCLUDE_DIR})
endif()

target_link_libraries(flow_bench PRIVATE Threads::Threads fdb_google_benchmark flow)
