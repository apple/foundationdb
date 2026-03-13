fdb_find_sources(FDBSERVER_RESOLVER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_resolver SRCS ${FDBSERVER_RESOLVER_SRCS})

configure_fdbserver_common_includes(fdbserver_resolver)
target_include_directories(fdbserver_resolver
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include)
target_link_libraries(fdbserver_resolver PRIVATE fdbclient fdbserver_core)
