set(PERF_SRCS
  bench.cpp)

add_executable(bench ${PERF_SRCS})
target_link_libraries(bench benchmark)
