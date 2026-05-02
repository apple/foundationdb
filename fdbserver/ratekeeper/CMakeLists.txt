fdb_find_sources(FDBSERVER_RATEKEEPER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_ratekeeper SRCS ${FDBSERVER_RATEKEEPER_SRCS})
add_fdbserver_link_test(fdbserver_ratekeeperlinktest
  fdbserver_ratekeeper
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_ratekeeper)
target_include_directories(fdbserver_ratekeeper
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(fdbserver_ratekeeper PRIVATE fdbserver_core)
