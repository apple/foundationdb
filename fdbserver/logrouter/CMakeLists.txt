fdb_find_sources(FDBSERVER_LOGROUTER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_logrouter SRCS ${FDBSERVER_LOGROUTER_SRCS})
add_fdbserver_link_test(fdbserver_logrouterlinktest
  fdbserver_logrouter
  fdbserver_logsystem
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_logrouter)
target_include_directories(fdbserver_logrouter
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(fdbserver_logrouter PRIVATE fdbserver_core fdbserver_logsystem)
