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
    if(USE_VALGRIND_FOR_CTEST)
      set(this_test_timeout 36000)
    endif()
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
  if (USE_VALGRIND_FOR_CTEST)
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
  # set variables used for generating test packages
  set(TEST_NAMES ${TEST_NAMES} ${test_name} PARENT_SCOPE)
  set(TEST_FILES_${test_name} ${ADD_FDB_TEST_TEST_FILES} PARENT_SCOPE)
  set(TEST_TYPE_${test_name} ${test_type} PARENT_SCOPE)
endfunction()

if(NOT WIN32)
  set(TEST_PACKAGE_INCLUDE ".*" CACHE STRING "A regex of all tests that should be included in the test package")
  set(TEST_PACKAGE_EXCLUDE ".^" CACHE STRING "A regex of all tests that shouldn't be added to the test package")
  set(TEST_PACKAGE_ADD_DIRECTORIES "" CACHE STRING "A ;-separated list of directories. All files within each directory will be added to the test package")
endif()

function(create_test_package)
  if(WIN32)
    return()
  endif()
  string(LENGTH "${CMAKE_SOURCE_DIR}/tests/" base_length)
  foreach(test IN LISTS TEST_NAMES)
    if(("${TEST_TYPE_${test}}" STREQUAL "simulation") AND
        (${test} MATCHES ${TEST_PACKAGE_INCLUDE}) AND
        (NOT ${test} MATCHES ${TEST_PACKAGE_EXCLUDE}))
      foreach(file IN LISTS TEST_FILES_${test})
        string(SUBSTRING ${file} ${base_length} -1 rel_out_file)
        set(out_file ${CMAKE_BINARY_DIR}/packages/tests/${rel_out_file})
        list(APPEND out_files ${out_file})
        get_filename_component(test_dir ${out_file} DIRECTORY)
        file(MAKE_DIRECTORY packages/tests/${test_dir})
        add_custom_command(
          OUTPUT ${out_file}
          DEPENDS ${file}
          COMMAND ${CMAKE_COMMAND} -E copy ${file} ${out_file})
      endforeach()
    endif()
  endforeach()
  foreach(dir IN LISTS TEST_PACKAGE_ADD_DIRECTORIES)
    file(GLOB_RECURSE files ${dir}/*)
    string(LENGTH ${dir} dir_len)
    foreach(file IN LISTS files)
      get_filename_component(src_dir ${file} DIRECTORY)
      # We need to make sure that ${src_dir} is at least
      # as long as ${dir}. Otherwise the later call to
      # SUBSTRING will fail
      set(src_dir "${src_dir}/")
      string(SUBSTRING ${src_dir} ${dir_len} -1 dest_dir)
      string(SUBSTRING ${file} ${dir_len} -1 out_file)
      list(APPEND external_files ${CMAKE_BINARY_DIR}/packages/${out_file})
      file(COPY ${file} DESTINATION ${CMAKE_BINARY_DIR}/packages/${dest_dir})
    endforeach()
  endforeach()
  if(NOT USE_VALGRIND)
    set(tar_file ${CMAKE_BINARY_DIR}/packages/correctness-${CMAKE_PROJECT_VERSION}.tar.gz)
    add_custom_command(
      OUTPUT ${tar_file}
      DEPENDS ${out_files}
      COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/correctnessTest.sh ${CMAKE_BINARY_DIR}/packages/joshua_test
      COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/correctnessTimeout.sh ${CMAKE_BINARY_DIR}/packages/joshua_timeout
      COMMAND ${CMAKE_COMMAND} -E tar cfz ${tar_file} ${CMAKE_BINARY_DIR}/packages/bin/fdbserver
      ${CMAKE_BINARY_DIR}/packages/bin/TestHarness.exe
      ${CMAKE_BINARY_DIR}/packages/bin/TraceLogHelper.dll
      ${CMAKE_BINARY_DIR}/packages/joshua_test
      ${CMAKE_BINARY_DIR}/packages/joshua_timeout
      ${out_files} ${external_files}
      COMMAND ${CMAKE_COMMAND} -E remove ${CMAKE_BINARY_DIR}/packages/joshua_test ${CMAKE_BINARY_DIR}/packages/joshua_timeout
      WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/packages
      COMMENT "Package correctness archive"
      )
    add_custom_target(package_tests ALL DEPENDS ${tar_file})
    add_dependencies(package_tests strip_only_fdbserver TestHarness)
  endif()

  if(USE_VALGRIND)
    set(tar_file ${CMAKE_BINARY_DIR}/packages/valgrind-${CMAKE_PROJECT_VERSION}.tar.gz)
    add_custom_command(
      OUTPUT ${tar_file}
      DEPENDS ${out_files}
      COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/valgrindTest.sh ${CMAKE_BINARY_DIR}/packages/joshua_test
      COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/valgrindTimeout.sh ${CMAKE_BINARY_DIR}/packages/joshua_timeout
      COMMAND ${CMAKE_COMMAND} -E tar cfz ${tar_file} ${CMAKE_BINARY_DIR}/packages/bin/fdbserver
      ${CMAKE_BINARY_DIR}/packages/bin/TestHarness.exe
      ${CMAKE_BINARY_DIR}/packages/bin/TraceLogHelper.dll
      ${CMAKE_BINARY_DIR}/packages/joshua_test
      ${CMAKE_BINARY_DIR}/packages/joshua_timeout
      ${out_files} ${external_files}
      COMMAND ${CMAKE_COMMAND} -E remove ${CMAKE_BINARY_DIR}/packages/joshua_test ${CMAKE_BINARY_DIR}/packages/joshua_timeout
      WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/packages
      COMMENT "Package correctness archive"
      )
    add_custom_target(package_valgrind_tests ALL DEPENDS ${tar_file})
    add_dependencies(package_valgrind_tests strip_only_fdbserver TestHarness)
  endif()
endfunction()

function(package_bindingtester)
  if(WIN32 OR OPEN_FOR_IDE)
    return()
  elseif(APPLE)
    set(fdbcName "libfdb_c.dylib")
  else()
    set(fdbcName "libfdb_c.so")
  endif()
  set(bdir ${CMAKE_BINARY_DIR}/bindingtester)
  file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/bindingtester)
  set(outfiles ${bdir}/fdbcli ${bdir}/fdbserver ${bdir}/${fdbcName} ${bdir}/joshua_test ${bdir}/joshua_timeout)
  add_custom_command(
    OUTPUT ${outfiles}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/packages/bin/fdbcli
            ${CMAKE_BINARY_DIR}/packages/bin/fdbserver
            ${CMAKE_BINARY_DIR}/packages/lib/${fdbcName}
            ${bdir}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/bindingTest.sh ${bdir}/joshua_test
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/bindingTimeout.sh ${bdir}/joshua_timeout
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/localClusterStart.sh ${bdir}/localClusterStart.sh
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/bindingTestScript.sh ${bdir}/bindingTestScript.ksh
    COMMENT "Copy executes to bindingtester dir")
  file(GLOB_RECURSE test_files ${CMAKE_SOURCE_DIR}/bindings/*)
  add_custom_command(
    OUTPUT "${CMAKE_BINARY_DIR}/bindingtester.touch"
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_BINARY_DIR}/bindingtester/tests
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/bindingtester/tests
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/bindings ${CMAKE_BINARY_DIR}/bindingtester/tests
    COMMAND ${CMAKE_COMMAND} -E touch "${CMAKE_BINARY_DIR}/bindingtester.touch"
    COMMENT "Copy test files for bindingtester")
  add_custom_target(copy_bindingtester_binaries DEPENDS ${outfiles}  "${CMAKE_BINARY_DIR}/bindingtester.touch")
  add_dependencies(copy_bindingtester_binaries strip_only_fdbserver strip_only_fdbcli strip_only_fdb_c)
  set(tar_file ${CMAKE_BINARY_DIR}/packages/bindingtester-${CMAKE_PROJECT_VERSION}.tar.gz)
  add_custom_command(
    OUTPUT ${tar_file}
    COMMAND ${CMAKE_COMMAND} -E tar czf ${tar_file} *
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bindingtester
    COMMENT "Pack bindingtester")
  add_custom_target(bindingtester ALL DEPENDS ${tar_file})
  add_dependencies(bindingtester copy_bindingtester_binaries)
endfunction()
