project (flowbench)

fdb_find_sources(FLOWBENCH_SRCS)

find_package(benchmark)

set(BENCHMARK_ENABLE_TESTING OFF)
add_flow_target(EXECUTABLE NAME flowbench SRCS ${FLOWBENCH_SRCS})
target_link_libraries(flowbench PRIVATE benchmark::benchmark pthread flow fdbclient)
