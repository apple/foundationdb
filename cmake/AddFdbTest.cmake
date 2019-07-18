# This configures the fdb testing system in cmake. Currently this simply means
# that it will get a list of all test files and store this list in a parent scope
# so that we can later verify that all of them were assigned to a test.
#
# - TEST_DIRECTORY The directory where all the tests are
# - ERROR_ON_ADDITIONAL_FILES if this is passed verify_fdb_tests will print
#   an error if there are any .txt files in the test directory that do not
#   correspond to a test or are not ignore by a pattern
# - IGNORE_PATTERNS regular expressions. All files that match any of those
#   experessions don't need to be associated with a test
function(configure_testing)
  set(options ERROR_ON_ADDITIONAL_FILES)
  set(oneValueArgs TEST_DIRECTORY)
  set(multiValueArgs IGNORE_PATTERNS)
  cmake_parse_arguments(CONFIGURE_TESTING "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")
  set(no_tests YES)
  if(CONFIGURE_TESTING_ERROR_ON_ADDITIONAL_FILES)
    file(GLOB_RECURSE candidates "${CONFIGURE_TESTING_TEST_DIRECTORY}/*.txt")
    foreach(candidate IN LISTS candidates)
      set(candidate_is_test YES)
      foreach(pattern IN LISTS CONFIGURE_TESTING_IGNORE_PATTERNS)
        if("${candidate}" MATCHES "${pattern}")
          set(candidate_is_test NO)
        endif()
      endforeach()
      if(candidate_is_test)
        if(no_tests)
          set(no_tests NO)
          set(fdb_test_files "${candidate}")
        else()
          set(fdb_test_files "${fdb_test_files};${candidate}")
        endif()
      endif()
    endforeach()
    set(fdb_test_files "${fdb_test_files}" PARENT_SCOPE)
  endif()
endfunction()

function(verify_testing)
  foreach(test_file IN LISTS fdb_test_files)
    message(SEND_ERROR "${test_file} found but it is not associated with a test")
  endforeach()
endfunction()

# This will add a test that can be run by ctest. This macro can be called
# with the following arguments:
#
# - UNIT will run the test as a unit test (it won't bring up a whole simulated system)
# - TEST_NAME followed the name of the test
# - TIMEOUT followed by a timeout - reaching the timeout makes the test fail (default is
#   3600 seconds). The timeout will be reached whenever it ran either too long in simulated
#   time or in real time - whatever is smaller.
# - TEST_FILES followed by typically one test file. The test runner will run
#   all these tests in serialized order and within the same directory. This is
#   useful for restart tests
function(add_fdb_test)
  set(options UNIT IGNORE)
  set(oneValueArgs TEST_NAME TIMEOUT)
  set(multiValueArgs TEST_FILES)
  cmake_parse_arguments(ADD_FDB_TEST "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")
  set(this_test_timeout ${ADD_FDB_TEST_TIMEOUT})
  if(NOT this_test_timeout)
    set(this_test_timeout 3600)
  endif()
  set(test_type "simulation")
  set(fdb_test_files_ "${fdb_test_files}")
  foreach(test_file IN LISTS ADD_FDB_TEST_TEST_FILES)
    list(REMOVE_ITEM fdb_test_files_ "${CMAKE_CURRENT_SOURCE_DIR}/${test_file}")
  endforeach()
  set(fdb_test_files "${fdb_test_files_}" PARENT_SCOPE)
  list(LENGTH ADD_FDB_TEST_TEST_FILES NUM_TEST_FILES)
  if(ADD_FDB_TEST_IGNORE AND NOT RUN_IGNORED_TESTS)
    return()
  endif()
  if(ADD_FDB_TEST_UNIT)
    set(test_type "test")
  endif()
  list(GET ADD_FDB_TEST_TEST_FILES 0 first_file)
  string(REGEX REPLACE "^(.*)\\.txt$" "\\1" test_name ${first_file})
  if("${test_name}" MATCHES "(-\\d)$")
    string(REGEX REPLACE "(.*)(-\\d)$" "\\1" test_name_1 ${test_name})
    message(STATUS "new testname ${test_name_1}")
  endif()
  if (NOT "${ADD_FDB_TEST_TEST_NAME}" STREQUAL "")
    set(test_name ${ADD_FDB_TEST_TEST_NAME})
  endif()
  math(EXPR test_idx "${CURRENT_TEST_INDEX} + ${NUM_TEST_FILES}")
  set(CURRENT_TEST_INDEX "${test_idx}" PARENT_SCOPE)
  # set(<var> <value> PARENT_SCOPE) doesn't set the
  # value in this scope (only in the parent scope). So
  # if the value was undefined before, it will still be
  # undefined.
  math(EXPR assigned_id "${test_idx} - ${NUM_TEST_FILES}")
  if(ADD_FDB_TEST_UNIT)
    message(STATUS
      "ADDING UNIT TEST ${assigned_id} ${test_name}")
  else()
    message(STATUS
      "ADDING SIMULATOR TEST ${assigned_id} ${test_name}")
  endif()
  set(test_files "")
  foreach(curr_test_file ${ADD_FDB_TEST_TEST_FILES})
    set(test_files "${test_files} ${curr_test_file}")
  endforeach()
  set(BUGGIFY_OPTION "")
  if (ENABLE_BUGGIFY)
    set(BUGGIFY_OPTION "-B")
  endif()
  set(VALGRIND_OPTION "")
  if (USE_VALGRIND)
    set(VALGRIND_OPTION "--use-valgrind")
  endif()
  list(TRANSFORM ADD_FDB_TEST_TEST_FILES PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")
  add_test(NAME ${test_name}
    COMMAND $<TARGET_FILE:Python::Interpreter> ${TestRunner}
    -n ${test_name}
    -b ${PROJECT_BINARY_DIR}
    -t ${test_type}
    -O ${OLD_FDBSERVER_BINARY}
    --aggregate-traces ${TEST_AGGREGATE_TRACES}
    --log-format ${TEST_LOG_FORMAT}
    --keep-logs ${TEST_KEEP_LOGS}
    --keep-simdirs ${TEST_KEEP_SIMDIR}
    --seed ${SEED}
    --test-number ${assigned_id}
    ${BUGGIFY_OPTION}
    ${VALGRIND_OPTION}
    ${ADD_FDB_TEST_TEST_FILES}
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR})
	get_filename_component(test_dir_full ${first_file} DIRECTORY)
	if(NOT ${test_dir_full} STREQUAL "")
		get_filename_component(test_dir ${test_dir_full} NAME)
		set_tests_properties(${test_name} PROPERTIES TIMEOUT ${this_test_timeout} LABELS "${test_dir}")
	endif()
endfunction()
