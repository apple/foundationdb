fdb_find_sources(FDBSERVER_TLOG_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_tlog SRCS ${FDBSERVER_TLOG_SRCS})
add_fdbserver_link_test(fdbserver_tloglinktest
  fdbserver_tlog
  fdbserver_logsystem
  fdbserver_kvstore
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_tlog)
target_include_directories(fdbserver_tlog
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(fdbserver_tlog PRIVATE fdbserver_core fdbserver_kvstore fdbserver_logsystem)
