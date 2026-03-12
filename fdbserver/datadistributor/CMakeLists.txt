fdb_find_sources(FDBSERVER_DATADISTRIBUTOR_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_datadistributor SRCS ${FDBSERVER_DATADISTRIBUTOR_SRCS})

configure_fdbserver_common_includes(fdbserver_datadistributor)
target_include_directories(fdbserver_datadistributor
  PUBLIC
    ${CMAKE_SOURCE_DIR}/fdbserver/include
    ${CMAKE_BINARY_DIR}/fdbserver/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include)
target_link_libraries(fdbserver_datadistributor PRIVATE fdbclient fdbserver_core)
