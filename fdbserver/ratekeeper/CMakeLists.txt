fdb_find_sources(FDBSERVER_RATEKEEPER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_ratekeeper SRCS ${FDBSERVER_RATEKEEPER_SRCS})

configure_fdbserver_common_includes(fdbserver_ratekeeper)
target_include_directories(fdbserver_ratekeeper
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include)
target_link_libraries(fdbserver_ratekeeper PRIVATE fdbclient fdbserver_core)
