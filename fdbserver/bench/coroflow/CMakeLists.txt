include(FDBBenchmark)

add_flow_target(EXECUTABLE NAME fdbserver_coroflow_bench
  SRCS BenchMain.cpp BenchCoroFlow.cpp)

fdb_setup_googlebenchmark()

target_include_directories(fdbserver_coroflow_bench
  PRIVATE "${CMAKE_SOURCE_DIR}/fdbserver/include")
target_link_libraries(fdbserver_coroflow_bench
  PRIVATE Threads::Threads fdb_google_benchmark fdbserver_core)
