fdb_find_sources(FDBSERVER_DATADISTRIBUTOR_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_datadistributor SRCS ${FDBSERVER_DATADISTRIBUTOR_SRCS})
add_fdbserver_link_test(fdbserver_datadistributorlinktest
  fdbserver_datadistributor
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_datadistributor)
target_include_directories(fdbserver_datadistributor
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(fdbserver_datadistributor PRIVATE fdbserver_core)
