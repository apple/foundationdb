set(TEST_SRCS
  DirectoryTester.actor.cpp
  Tester.actor.cpp
  Tester.actor.h)
add_flow_target(EXECUTABLE NAME fdb_flow_tester SRCS ${TEST_SRCS})
target_link_libraries(fdb_flow_tester fdb_flow)
