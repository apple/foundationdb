fdb_find_sources(FDBSERVER_WORKLOADS_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_workloads SRCS ${FDBSERVER_WORKLOADS_SRCS})

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
