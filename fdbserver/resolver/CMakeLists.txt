fdb_find_sources(FDBSERVER_RESOLVER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_resolver SRCS ${FDBSERVER_RESOLVER_SRCS})
add_fdbserver_link_test(fdbserver_resolverlinktest
  fdbserver_resolver
  fdbserver_logsystem
  fdbserver_kvstore
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_resolver)
target_include_directories(fdbserver_resolver
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(fdbserver_resolver PRIVATE fdbserver_core fdbserver_logsystem)
