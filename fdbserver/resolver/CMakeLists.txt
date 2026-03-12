fdb_find_sources(FDBSERVER_RESOLVER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_resolver SRCS ${FDBSERVER_RESOLVER_SRCS})

configure_fdbserver_common_includes(fdbserver_resolver)
target_include_directories(fdbserver_resolver
  PUBLIC
    ${CMAKE_SOURCE_DIR}/fdbserver/include
    ${CMAKE_BINARY_DIR}/fdbserver/include)
target_link_libraries(fdbserver_resolver PRIVATE fdbclient fdbserver_core)
