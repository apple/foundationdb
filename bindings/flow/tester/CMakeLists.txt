set(TEST_SRCS
  DirectoryTester.actor.cpp
  Tester.actor.cpp
  Tester.actor.h)
add_flow_target(NAME fdb_flow_tester EXECUTABLE SRCS ${TEST_SRCS})
target_link_libraries(fdb_flow_tester fdb_flow)
