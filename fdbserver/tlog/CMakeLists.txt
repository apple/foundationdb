fdb_find_sources(FDBSERVER_TLOG_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_tlog SRCS ${FDBSERVER_TLOG_SRCS})

configure_fdbserver_common_includes(fdbserver_tlog)
target_include_directories(fdbserver_tlog
  PUBLIC
    ${CMAKE_SOURCE_DIR}/fdbserver/include
    ${CMAKE_BINARY_DIR}/fdbserver/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include)
target_link_libraries(fdbserver_tlog PRIVATE fdbclient fdbserver_core)
