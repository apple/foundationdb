fdb_find_sources(FDBSERVER_LOGROUTER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_logrouter SRCS ${FDBSERVER_LOGROUTER_SRCS})

configure_fdbserver_common_includes(fdbserver_logrouter)
target_include_directories(fdbserver_logrouter
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include)
target_link_libraries(fdbserver_logrouter PRIVATE fdbclient fdbserver_core fdbserver_logsystem)
