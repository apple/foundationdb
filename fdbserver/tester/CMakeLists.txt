fdb_find_sources(FDBSERVER_TESTER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_tester SRCS ${FDBSERVER_TESTER_SRCS})
add_fdbserver_link_test(fdbserver_testerlinktest
  fdbserver_tester
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_tester)
target_include_directories(fdbserver_tester
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(fdbserver_tester PRIVATE fdbclient fdbserver_core toml11_target)

# Ensure toml11 external project is built before this target if needed
if(NOT toml11_FOUND AND TARGET toml11Project)
  add_dependencies(fdbserver_tester toml11Project)
endif()
