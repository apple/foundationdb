fdb_find_sources(FDBSERVER_SEQUENCER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_sequencer SRCS ${FDBSERVER_SEQUENCER_SRCS})

configure_fdbserver_common_includes(fdbserver_sequencer)
target_include_directories(fdbserver_sequencer
  PUBLIC
    ${CMAKE_SOURCE_DIR}/fdbserver/include
    ${CMAKE_BINARY_DIR}/fdbserver/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include)
target_link_libraries(fdbserver_sequencer PRIVATE fdbclient fdbserver_core)
