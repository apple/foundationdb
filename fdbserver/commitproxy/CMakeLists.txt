fdb_find_sources(FDBSERVER_COMMITPROXY_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_commitproxy SRCS ${FDBSERVER_COMMITPROXY_SRCS})
add_fdbserver_link_test(fdbserver_commitproxylinktest
  fdbserver_commitproxy
  fdbserver_logsystem
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_commitproxy)
target_include_directories(fdbserver_commitproxy
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(fdbserver_commitproxy
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(fdbserver_commitproxy PRIVATE fdbserver_core fdbserver_kvstore fdbserver_logsystem)
