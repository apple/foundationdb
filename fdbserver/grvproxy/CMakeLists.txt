fdb_find_sources(FDBSERVER_GRVPROXY_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_grvproxy SRCS ${FDBSERVER_GRVPROXY_SRCS})

configure_fdbserver_common_includes(fdbserver_grvproxy)
target_include_directories(fdbserver_grvproxy
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(fdbserver_grvproxy PRIVATE fdbclient fdbserver_core)
