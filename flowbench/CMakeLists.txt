project (flowbench)

fdb_find_sources(FLOWBENCH_SRCS)

# There is no good way to incorporate the recommended googlebenchmark download + build
# process with one that checks to see if googlebenchmark has already been downloaded
# and built.
if(EXISTS /opt/googlebenchmark-f91b6b AND CLANG)
  add_flow_target(EXECUTABLE NAME flowbench SRCS ${FLOWBENCH_SRCS})
  target_include_directories(flowbench PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include" /opt/googlebenchmark-f91b6b/include)
  target_link_directories(flowbench PRIVATE /opt/googlebenchmark-f91b6b/lib64)
elseif(EXISTS /opt/googlebenchmark-f91b6b-g++ AND NOT CLANG)
  add_flow_target(EXECUTABLE NAME flowbench SRCS ${FLOWBENCH_SRCS})
  target_include_directories(flowbench PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include" /opt/googlebenchmark-f91b6b-g++/include)
  target_link_directories(flowbench PRIVATE /opt/googlebenchmark-f91b6b-g++/lib64)
else()
  ## This seems to be copy-pasted from the the google benchmark documentation.
  ## It breaks if you attempt to re-use a build of googlebenchmark across FDB
  ## builds.

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
endif()
if(FLOW_USE_ZSTD)
   target_include_directories(flowbench PRIVATE ${ZSTD_LIB_INCLUDE_DIR})
endif()
target_link_libraries(flowbench benchmark pthread flow fdbclient)
