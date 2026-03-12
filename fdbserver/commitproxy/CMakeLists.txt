fdb_find_sources(FDBSERVER_COMMITPROXY_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_commitproxy SRCS ${FDBSERVER_COMMITPROXY_SRCS})

configure_fdbserver_common_includes(fdbserver_commitproxy)
target_include_directories(fdbserver_commitproxy
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include)
target_link_libraries(fdbserver_commitproxy PRIVATE fdbclient fdbserver_core)
