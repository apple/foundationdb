fdb_find_sources(FDBSERVER_CDCPROXY_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_cdcproxy SRCS ${FDBSERVER_CDCPROXY_SRCS})
add_fdbserver_link_test(fdbserver_cdcproxylinktest
  fdbserver_cdcproxy
  fdbserver_logsystem
  fdbserver_core)
add_fdbserver_unit_test(fdbserver_cdcproxy_test cdcproxy
  fdbserver_cdcproxy
  fdbserver_logsystem
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_cdcproxy)
target_include_directories(fdbserver_cdcproxy
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  PRIVATE
    ${CMAKE_SOURCE_DIR}/fdbclient)
target_link_libraries(fdbserver_cdcproxy PUBLIC fdbserver_logsystem fdbserver_core)
