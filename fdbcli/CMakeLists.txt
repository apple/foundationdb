include(AddFdbTest)
fdb_find_sources(FDBCLI_SRCS)

add_flow_target(EXECUTABLE NAME fdbcli SRCS ${FDBCLI_SRCS})
target_include_directories(fdbcli PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include")
target_link_libraries(fdbcli PRIVATE fdbclient metacluster SimpleOpt)
if (USE_UBSAN)
  # The intent is to put typeinfo symbols in the dynamic symbol table so that
  # the types in fdbcli and external libfdb_c clients agree for ubsan's vptr
  # check. This would not be a good idea for the normal build, or if we ever
  # start testing old libfdb_c's that are ubsan-instrumented.
  target_link_options(fdbcli PRIVATE "-rdynamic")
endif()

if(NOT WIN32)
  target_link_libraries(fdbcli PRIVATE linenoise)
endif()

if(NOT OPEN_FOR_IDE)
  if(GENERATE_DEBUG_PACKAGES)
    fdb_install(TARGETS fdbcli DESTINATION bin COMPONENT clients)
  else()
    add_custom_target(prepare_fdbcli_install ALL DEPENDS strip_only_fdbcli)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbcli DESTINATION bin COMPONENT clients)
  endif()
endif()

if (NOT WIN32 AND NOT OPEN_FOR_IDE)
  add_dependencies(fdbcli external_client)

  add_fdbclient_test(
    NAME single_process_fdbcli_tests
    COMMAND ${CMAKE_SOURCE_DIR}/fdbcli/tests/fdbcli_tests.py
            ${CMAKE_BINARY_DIR}
            @CLUSTER_FILE@
            )
  add_fdbclient_test(
    NAME multi_process_fdbcli_tests
    PROCESS_NUMBER 5
    COMMAND ${CMAKE_SOURCE_DIR}/fdbcli/tests/fdbcli_tests.py
            ${CMAKE_BINARY_DIR}
            @CLUSTER_FILE@
            5
            )
  add_fdbclient_test(
  NAME single_process_external_client_fdbcli_tests
  COMMAND ${CMAKE_SOURCE_DIR}/fdbcli/tests/fdbcli_tests.py
          ${CMAKE_BINARY_DIR}
          @CLUSTER_FILE@
          --external-client-library ${CMAKE_BINARY_DIR}/bindings/c/libfdb_c_external.so
          )
  add_fdbclient_test(
  NAME multi_process_external_client_fdbcli_tests
  PROCESS_NUMBER 5
  COMMAND ${CMAKE_SOURCE_DIR}/fdbcli/tests/fdbcli_tests.py
          ${CMAKE_BINARY_DIR}
          @CLUSTER_FILE@
          5
          --external-client-library ${CMAKE_BINARY_DIR}/bindings/c/libfdb_c_external.so
          )

  add_multi_fdbclient_test(
    NAME metacluster_fdbcli_tests
    COMMAND ${CMAKE_SOURCE_DIR}/fdbcli/tests/metacluster_fdbcli_tests.py
            ${CMAKE_BINARY_DIR}
            )
endif()
