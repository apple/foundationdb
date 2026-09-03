fdb_find_sources(FDBSERVER_WORKLOADS_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_workloads SRCS ${FDBSERVER_WORKLOADS_SRCS})
add_fdbserver_unit_test(fdbserver_workloads_test workloads
  fdbserver_workloads
  fdbserver_core)
# UnitTests.cpp references the server-owned allocator tests even though this
# runner selects only cases defined in the workloads directory.
target_sources(fdbserver_workloads_test PRIVATE ../MemoryTrackerTest.cpp ../GlobalNewDelete.cpp)

configure_fdbserver_common_includes(fdbserver_workloads)
target_include_directories(fdbserver_workloads PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(fdbserver_workloads PRIVATE
  fdbserver_consistencyscan
  fdbserver_core
  fdbserver_kvstore
  fdbserver_worker
  fdbserver_tester
  fdbserver_datadistributor
  fdbserver_resolver
  fdbserver_storageserver
  fdbserver_mocks3)
