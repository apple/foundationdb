fdb_find_sources(FDBSERVER_RESTOREWORKER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_restoreworker SRCS ${FDBSERVER_RESTOREWORKER_SRCS})
add_fdbserver_link_test(fdbserver_restoreworkerlinktest
  fdbserver_restoreworker
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_restoreworker)
target_include_directories(fdbserver_restoreworker
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(fdbserver_restoreworker PRIVATE fdbserver_core)
