set(PERF_SRCS
  bench.cpp
  BenchPopulate.cpp)

find_package(benchmark REQUIRED)

add_executable(bench ${PERF_SRCS})
target_link_libraries(bench atomic flow benchmark::benchmark)
