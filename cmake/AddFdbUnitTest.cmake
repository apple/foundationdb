include_guard(GLOBAL)

option(ENABLE_UNIT_TESTS "Build and register per-library unit tests with CTest" ${BUILD_TESTING})
set(UNIT_TEST_SEED "1" CACHE STRING "Decimal random seed for CTest unit tests (0 chooses a random seed)")
set(UNIT_TEST_TIMEOUT "600" CACHE STRING "Wall-clock timeout in seconds for each CTest unit suite")

if(AUTO_DISCOVER_UNIT_TESTS)
  message(DEPRECATION "AUTO_DISCOVER_UNIT_TESTS has been replaced by ENABLE_UNIT_TESTS; source-based test discovery is no longer used")
endif()

set(FDB_REGISTER_UNIT_TESTS OFF)
if(BUILD_TESTING AND ENABLE_UNIT_TESTS AND NOT OPEN_FOR_IDE AND NOT FOUNDATIONDB_CROSS_COMPILING)
  set(FDB_REGISTER_UNIT_TESTS ON)
endif()

if(FDB_REGISTER_UNIT_TESTS)
  if(NOT UNIT_TEST_SEED MATCHES "^[0-9]+$")
    message(FATAL_ERROR "UNIT_TEST_SEED must be a decimal integer")
  endif()
  if(NOT UNIT_TEST_TIMEOUT MATCHES "^[1-9][0-9]*$")
    message(FATAL_ERROR "UNIT_TEST_TIMEOUT must be a positive integer")
  endif()
  add_custom_target(unit_tests ALL)
else()
  add_custom_target(unit_tests)
endif()

if(FDB_REGISTER_UNIT_TESTS AND Python3_EXECUTABLE)
  add_test(NAME cmake/unit_test_registration
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_LIST_DIR}/tests/test_unit_test_registration.py
      ${CMAKE_COMMAND} ${CMAKE_CTEST_COMMAND} ${CMAKE_CURRENT_LIST_FILE})
  set_tests_properties(cmake/unit_test_registration PROPERTIES LABELS cmake TIMEOUT 60)
endif()

function(register_fdb_unit_tests target_name)
  cmake_parse_arguments(UNIT "SIMULATION" "" "" ${ARGN})
  set_target_properties(${target_name} PROPERTIES EXCLUDE_FROM_ALL TRUE)
  add_dependencies(unit_tests ${target_name})
  if(NOT FDB_REGISTER_UNIT_TESTS)
    return()
  endif()

  set(modes native)
  if(UNIT_SIMULATION)
    list(APPEND modes simulation)
  endif()
  foreach(mode IN LISTS modes)
    set(mode_args)
    if(mode STREQUAL "simulation")
      list(APPEND mode_args --simulation)
    endif()
    set(test_name "unit/${target_name}/${mode}")
    add_test(NAME ${test_name}
      COMMAND ${target_name} --seed ${UNIT_TEST_SEED} ${mode_args})
    set_tests_properties(${test_name} PROPERTIES
      LABELS "unit;${mode};${target_name}"
      TIMEOUT ${UNIT_TEST_TIMEOUT}
      ENVIRONMENT "${SANITIZER_OPTIONS}")
  endforeach()
endfunction()
