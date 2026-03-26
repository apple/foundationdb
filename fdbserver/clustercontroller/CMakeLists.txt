fdb_find_sources(FDBSERVER_CLUSTERCONTROLLER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_clustercontroller SRCS ${FDBSERVER_CLUSTERCONTROLLER_SRCS})
add_fdbserver_link_test(fdbserver_clustercontrollerlinktest
  fdbserver_clustercontroller
  fdbserver_logsystem
  fdbserver_kvstore
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_clustercontroller)
target_include_directories(fdbserver_clustercontroller
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_SOURCE_DIR}/fdbserver/datadistributor/include
    ${CMAKE_BINARY_DIR}/fdbserver/datadistributor/include)
target_link_libraries(fdbserver_clustercontroller PRIVATE fdbserver_core fdbserver_logsystem)
