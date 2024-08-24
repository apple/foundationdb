if(NOT WIN32)
  add_flow_target(EXECUTABLE NAME authz_tls_unittest SRCS AuthzTlsTest.actor.cpp)
  target_link_libraries(authz_tls_unittest PRIVATE flow fdbrpc fmt::fmt)
  if(NOT OPEN_FOR_IDE)
    add_test(NAME authorization_tls_unittest
      COMMAND $<TARGET_FILE:authz_tls_unittest>)
    set_tests_properties(authorization_tls_unittest PROPERTIES TIMEOUT 120)
  endif()
endif()

add_flow_target(EXECUTABLE NAME fdbrpc_bench SRCS fdbrpc_bench.actor.cpp)
target_link_libraries(fdbrpc_bench PUBLIC flow fdbrpc)