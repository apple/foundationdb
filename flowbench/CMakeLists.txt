set(FLOWBENCH_SRCS
  flowbench.actor.cpp
  BenchCallback.actor.cpp
  BenchHash.cpp
  BenchIterate.cpp
  BenchIONet2.actor.cpp
  BenchMem.cpp
  BenchMetadataCheck.cpp
  BenchNet2.actor.cpp
  BenchPopulate.cpp
  BenchRandom.cpp
  BenchRef.cpp
  BenchStream.actor.cpp
  BenchTimer.cpp
  BenchVersionVector.cpp
  GlobalData.h
  GlobalData.cpp)

if(WITH_TLS AND NOT WIN32)
  set(FLOWBENCH_SRCS
    ${FLOWBENCH_SRCS}
    BenchEncrypt.cpp)
endif()

project (flowbench)
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
include_directories(${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-src/include)
add_flow_target(EXECUTABLE NAME flowbench SRCS ${FLOWBENCH_SRCS})
target_link_libraries(flowbench benchmark pthread flow fdbclient)
