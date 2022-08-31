if(NOT WIN32)
  add_flow_target(EXECUTABLE NAME authz_tls_unittest SRCS AuthzTlsTest.actor.cpp)
  target_link_libraries(authz_tls_unittest PRIVATE flow fdbrpc fmt::fmt)
  add_test(NAME authorization_tls_unittest
           COMMAND $<TARGET_FILE:authz_tls_unittest>)
endif()
