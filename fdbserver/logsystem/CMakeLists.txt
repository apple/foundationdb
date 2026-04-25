fdb_find_sources(FDBSERVER_LOGSYSTEM_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_logsystem SRCS ${FDBSERVER_LOGSYSTEM_SRCS})
add_fdbserver_link_test(fdbserver_logsystemlinktest
  fdbserver_logsystem
  fdbserver_kvstore
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_logsystem)
target_include_directories(fdbserver_logsystem
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(fdbserver_logsystem PUBLIC fdbserver_core fdbserver_kvstore)
