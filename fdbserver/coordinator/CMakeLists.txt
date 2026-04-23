fdb_find_sources(FDBSERVER_COORDINATOR_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_coordinator SRCS ${FDBSERVER_COORDINATOR_SRCS})
add_fdbserver_link_test(fdbserver_coordinatorlinktest
  fdbserver_coordinator
  fdbserver_kvstore
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_coordinator)
target_include_directories(fdbserver_coordinator
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(fdbserver_coordinator PRIVATE fdbserver_core fdbserver_kvstore)
