set(FDBBENCH_SRCS
  fdbbench.cpp
  BenchIterate.cpp
  BenchPopulate.cpp
  GlobalData.h)

project (fdbbench)
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
add_subdirectory(
  ${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-src
  ${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-build
  EXCLUDE_FROM_ALL
)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-src/include)
add_executable(fdbbench ${FDBBENCH_SRCS})
target_link_libraries(fdbbench benchmark pthread flow)
