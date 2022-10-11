project (flowbench)

fdb_find_sources(FLOWBENCH_SRCS)

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
add_flow_target(EXECUTABLE NAME flowbench SRCS ${FLOWBENCH_SRCS})
target_include_directories(flowbench PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include"  ${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-src/include)
if(FLOW_USE_ZSTD)
   target_include_directories(flowbench PRIVATE ${ZSTD_LIB_INCLUDE_DIR})
endif()
target_link_libraries(flowbench benchmark pthread flow fdbclient)
