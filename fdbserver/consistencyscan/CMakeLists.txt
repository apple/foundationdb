fdb_find_sources(FDBSERVER_CONSISTENCYSCAN_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_consistencyscan SRCS ${FDBSERVER_CONSISTENCYSCAN_SRCS})

configure_fdbserver_common_includes(fdbserver_consistencyscan)
target_include_directories(fdbserver_consistencyscan
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include)
target_link_libraries(fdbserver_consistencyscan PRIVATE fdbclient fdbserver_core fdbserver_tester)
