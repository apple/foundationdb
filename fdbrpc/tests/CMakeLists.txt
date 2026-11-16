if(NOT WIN32)
  add_flow_target(EXECUTABLE NAME authz_tls_unittest SRCS AuthzTlsTest.cpp)
  target_link_libraries(authz_tls_unittest PRIVATE flow fdbrpc fmt::fmt)
  if(NOT OPEN_FOR_IDE)
    add_test(NAME authorization_tls_unittest
      COMMAND $<TARGET_FILE:authz_tls_unittest>)
    set_tests_properties(authorization_tls_unittest PROPERTIES TIMEOUT 120)
  endif()
endif()

if(WITH_GRPC)
  generate_grpc_protobuf(fdbrpc.test protos/echo.proto)
endif()
add_flow_target(EXECUTABLE NAME fdbrpc_transport_bench SRCS fdbrpc_bench.cpp)
target_link_libraries(fdbrpc_transport_bench PUBLIC flow fdbrpc boost_target_program_options)

add_flow_target(EXECUTABLE NAME fdbrpc_network_test SRCS networktest_main.cpp networktest.cpp NetworkTest.h)
target_link_libraries(fdbrpc_network_test PRIVATE flow fdbrpc fmt::fmt)

if(FDB_REGISTER_UNIT_TESTS AND Python3_EXECUTABLE)
  add_dependencies(unit_tests fdbrpc_network_test)
  add_test(NAME unit/fdbrpc_network_test/native
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/networktest_smoke.py $<TARGET_FILE:fdbrpc_network_test>)
  set_tests_properties(unit/fdbrpc_network_test/native PROPERTIES
    LABELS "unit;native;fdbrpc_network_test"
    TIMEOUT 90
    ENVIRONMENT "${SANITIZER_OPTIONS}")
endif()
