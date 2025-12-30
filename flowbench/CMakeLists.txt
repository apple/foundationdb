project(flowbench)
# This stub is kept to maintain the backward compatibility with the existing build
# environment. It should be removed when a reasonable environment is established.
if(NOT benchmark_ROOT)
  if(EXISTS /opt/googlebenchmark-f91b6b AND CLANG AND USE_LIBCXX)
    set(benchmark_ROOT /opt/googlebenchmark-f91b6b)
  elseif(EXISTS /opt/googlebenchmark-f91b6b-g++ AND NOT USE_LIBCXX)
    set(benchmark_ROOT /opt/googlebenchmark-f91b6b-g++)
  endif()
endif()

find_package(benchmark REQUIRED)

fdb_find_sources(FLOWBENCH_SRCS)
add_flow_target(EXECUTABLE NAME flowbench SRCS ${FLOWBENCH_SRCS})

if(FLOW_USE_ZSTD)
  target_include_directories(flowbench PRIVATE ${ZSTD_LIB_INCLUDE_DIR})
endif()
target_link_libraries(flowbench PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include")
target_include_directories(flowbench INTERFACE $<TARGET_PROPERTY:benchmark::benchmark, INTERFACE_INCLUDE_DIRECTORIES>)
target_link_libraries(flowbench benchmark::benchmark pthread flow fdbclient)
