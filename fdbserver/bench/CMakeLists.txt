include(FDBBenchmark)

fdb_find_sources(FDBSERVER_BENCH_SRCS)

# Compile fdbserver/GlobalNewDelete.cpp directly into this benchmark executable.
# It defines the global operator new/delete overrides that route through the
# memory tracker; as a strong definition in the executable link it wins over
# libc++'s weak one and is active process-wide here, exactly as in fdbserver.
# Without it the bench would link libc++'s operator new and the operator-new
# microbench would measure the unhooked allocator (its Arg(100)/Arg(1) rows
# would equal Arg(0)). It only needs flow (the tracker hooks it calls), not the
# rest of the fdbserver dependency graph.
add_flow_target(EXECUTABLE NAME fdbserver_bench
  SRCS ${FDBSERVER_BENCH_SRCS}
  ADDL_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/../GlobalNewDelete.cpp")

fdb_setup_googlebenchmark()

target_include_directories(
  fdbserver_bench
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/.."
          "${CMAKE_CURRENT_SOURCE_DIR}/include")

target_link_libraries(fdbserver_bench PRIVATE Threads::Threads fdb_google_benchmark flow)
