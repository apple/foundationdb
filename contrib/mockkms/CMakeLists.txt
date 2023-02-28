if(BUILD_GO_BINDING)
  set(MOCK_KMS_SRC fault_injection.go get_encryption_keys.go mock_kms.go utils.go)
  set(MOCK_KMS_TEST_SRC ${MOCK_KMS_SRC} mockkms_test.go)
  add_custom_command(OUTPUT ${CMAKE_BINARY_DIR}/bin/mockkms
    COMMAND ${GO_EXECUTABLE} build -o ${CMAKE_BINARY_DIR}/bin/mockkms ${MOCK_KMS_SRC}
    DEPENDS ${MOCK_KMS_SRC}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  add_custom_target(mockkms ALL DEPENDS ${CMAKE_BINARY_DIR}/bin/mockkms)
  fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/bin/mockkms DESTINATION bin COMPONENT server)

  add_custom_command(OUTPUT ${CMAKE_BINARY_DIR}/bin/mockkms_test
    COMMAND ${GO_EXECUTABLE} test -c -o ${CMAKE_BINARY_DIR}/bin/mockkms_test ${MOCK_KMS_TEST_SRC}
    DEPENDS ${MOCK_KMS_TEST_SRC}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  add_custom_target(mockkms_test ALL DEPENDS ${CMAKE_BINARY_DIR}/bin/mockkms_test)
  add_test(NAME mockkms COMMAND ${CMAKE_BINARY_DIR}/bin/mockkms_test)

endif()
