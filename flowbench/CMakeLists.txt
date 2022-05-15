set(FLOWBENCH_SRCS
  flowbench.actor.cpp
  BenchCallback.actor.cpp
  BenchEncrypt.cpp
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

project(flowbench)
find_package(benchmark)
if(benchmark_FOUND)
  add_flow_target(EXECUTABLE NAME flowbench SRCS ${FLOWBENCH_SRCS})
  target_link_libraries(flowbench benchmark::benchmark flow fdbclient)
endif()
