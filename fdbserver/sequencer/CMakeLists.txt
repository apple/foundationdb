fdb_find_sources(FDBSERVER_SEQUENCER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_sequencer SRCS ${FDBSERVER_SEQUENCER_SRCS})
add_fdbserver_link_test(fdbserver_sequencerlinktest
  fdbserver_sequencer
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_sequencer)
target_include_directories(fdbserver_sequencer
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(fdbserver_sequencer PRIVATE fdbserver_core)
