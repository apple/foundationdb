fdb_find_sources(FDBSERVER_TESTER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_tester SRCS ${FDBSERVER_TESTER_SRCS})

configure_fdbserver_common_includes(fdbserver_tester)
target_include_directories(fdbserver_tester
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(fdbserver_tester PRIVATE fdbclient fdbserver_core)
