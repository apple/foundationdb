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
    file(GLOB_RECURSE toml_candidates "${CONFIGURE_TESTING_TEST_DIRECTORY}/*.toml")
    list(APPEND candidates ${toml_candidates})
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
  if(NOT ENABLE_SIMULATION_TESTS)
    return()
  endif()
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
  string(REGEX REPLACE "^(.*)\\.(txt|toml)$" "\\1" test_name ${first_file})
  if("${test_name}" MATCHES "(-\\d)$")
    string(REGEX REPLACE "(.*)(-\\d)$" "\\1" test_name_1 ${test_name})
    message(STATUS "new testname ${test_name_1}")
  endif()
  if (NOT "${ADD_FDB_TEST_TEST_NAME}" STREQUAL "")
    set(test_name ${ADD_FDB_TEST_TEST_NAME})
  endif()
  if((NOT test_name MATCHES "${TEST_INCLUDE}") OR (test_name MATCHES "${TEST_EXCLUDE}"))
    return()
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
  if (ENABLE_SIMULATION_TESTS)
    add_test(NAME ${test_name}
      COMMAND $<TARGET_FILE:Python::Interpreter> ${TestRunner}
      -n ${test_name}
      -b ${PROJECT_BINARY_DIR}
      -t ${test_type}
      -O ${OLD_FDBSERVER_BINARY}
      --crash
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
    set_tests_properties("${test_name}" PROPERTIES ENVIRONMENT UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1)
    get_filename_component(test_dir_full ${first_file} DIRECTORY)
    if(NOT ${test_dir_full} STREQUAL "")
      get_filename_component(test_dir ${test_dir_full} NAME)
      set_tests_properties(${test_name} PROPERTIES TIMEOUT ${this_test_timeout} LABELS "${test_dir}")
    endif()
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

# This sets up a directory with the correctness files common to all correctness packages.
# This function should be called with the following arguments:
#
# - OUT_DIR the directory where files will be staged
# - CONTEXT the type of correctness package being built (e.g. 'valgrind correctness')
function(stage_correctness_package)
  set(oneValueArgs OUT_DIR CONTEXT OUT_FILES)
  cmake_parse_arguments(STAGE "" "${oneValueArgs}" "" "${ARGN}")
  file(MAKE_DIRECTORY ${STAGE_OUT_DIR}/bin)
  string(LENGTH "${CMAKE_SOURCE_DIR}/tests/" base_length)
  foreach(test IN LISTS TEST_NAMES)
    if(("${TEST_TYPE_${test}}" STREQUAL "simulation") AND
        (${test} MATCHES ${TEST_PACKAGE_INCLUDE}) AND
        (NOT ${test} MATCHES ${TEST_PACKAGE_EXCLUDE}))
      foreach(file IN LISTS TEST_FILES_${test})
        string(SUBSTRING ${file} ${base_length} -1 rel_out_file)
        set(out_file ${STAGE_OUT_DIR}/tests/${rel_out_file})
        list(APPEND test_files ${out_file})
        add_custom_command(
          OUTPUT ${out_file}
          DEPENDS ${file}
          COMMAND ${CMAKE_COMMAND} -E copy ${file} ${out_file}
          COMMENT "Copying ${STAGE_CONTEXT} test file ${rel_out_file}"
          )
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
      string(SUBSTRING ${file} ${dir_len} -1 rel_out_file)
	  set(out_file ${STAGE_OUT_DIR}/${rel_out_file})
      list(APPEND external_files ${out_file})
	  add_custom_command(
        OUTPUT ${out_file}
		DEPENDS ${file}
		COMMAND ${CMAKE_COMMAND} -E copy ${file} ${out_file}
		COMMENT "Copying ${STAGE_CONTEXT} external file ${file}"
		)
    endforeach()
  endforeach()
  list(APPEND package_files ${STAGE_OUT_DIR}/bin/fdbserver
                            ${STAGE_OUT_DIR}/bin/coverage.fdbserver.xml
                            ${STAGE_OUT_DIR}/bin/coverage.fdbclient.xml
                            ${STAGE_OUT_DIR}/bin/coverage.fdbrpc.xml
                            ${STAGE_OUT_DIR}/bin/coverage.flow.xml
                            ${STAGE_OUT_DIR}/bin/TestHarness.exe
                            ${STAGE_OUT_DIR}/bin/TraceLogHelper.dll
                            ${STAGE_OUT_DIR}/CMakeCache.txt
    )
  add_custom_command(
    OUTPUT ${package_files}
    DEPENDS ${CMAKE_BINARY_DIR}/CMakeCache.txt
            ${CMAKE_BINARY_DIR}/packages/bin/fdbserver
            ${CMAKE_BINARY_DIR}/bin/coverage.fdbserver.xml
            ${CMAKE_BINARY_DIR}/lib/coverage.fdbclient.xml
            ${CMAKE_BINARY_DIR}/lib/coverage.fdbrpc.xml
            ${CMAKE_BINARY_DIR}/lib/coverage.flow.xml
            ${CMAKE_BINARY_DIR}/packages/bin/TestHarness.exe
            ${CMAKE_BINARY_DIR}/packages/bin/TraceLogHelper.dll
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/CMakeCache.txt ${STAGE_OUT_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/packages/bin/fdbserver
                                     ${CMAKE_BINARY_DIR}/bin/coverage.fdbserver.xml
                                     ${CMAKE_BINARY_DIR}/lib/coverage.fdbclient.xml
                                     ${CMAKE_BINARY_DIR}/lib/coverage.fdbrpc.xml
                                     ${CMAKE_BINARY_DIR}/lib/coverage.flow.xml
                                     ${CMAKE_BINARY_DIR}/packages/bin/TestHarness.exe
                                     ${CMAKE_BINARY_DIR}/packages/bin/TraceLogHelper.dll
                                     ${STAGE_OUT_DIR}/bin
    COMMENT "Copying files for ${STAGE_CONTEXT} package"
    )
  list(APPEND package_files ${test_files} ${external_files})
  if(STAGE_OUT_FILES)
    set(${STAGE_OUT_FILES} ${package_files} PARENT_SCOPE)
  endif()
endfunction()

function(create_correctness_package)
  if(WIN32)
    return()
  endif()
  set(out_dir "${CMAKE_BINARY_DIR}/correctness")
  stage_correctness_package(OUT_DIR ${out_dir} CONTEXT "correctness" OUT_FILES package_files)
  set(tar_file ${CMAKE_BINARY_DIR}/packages/correctness-${CMAKE_PROJECT_VERSION}.tar.gz)
  add_custom_command(
    OUTPUT ${tar_file}
    DEPENDS ${package_files}
            ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/correctnessTest.sh
            ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/correctnessTimeout.sh
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/correctnessTest.sh
                                     ${out_dir}/joshua_test
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/correctnessTimeout.sh
                                     ${out_dir}/joshua_timeout
    COMMAND ${CMAKE_COMMAND} -E tar cfz ${tar_file} ${package_files}
                                                    ${out_dir}/joshua_test
                                                    ${out_dir}/joshua_timeout
    WORKING_DIRECTORY ${out_dir}
    COMMENT "Package correctness archive"
    )
  add_custom_target(package_tests ALL DEPENDS ${tar_file})
  add_dependencies(package_tests strip_only_fdbserver TestHarness)
  set(unversioned_tar_file "${CMAKE_BINARY_DIR}/packages/correctness.tar.gz")
  add_custom_command(
    OUTPUT "${unversioned_tar_file}"
    DEPENDS "${tar_file}"
    COMMAND ${CMAKE_COMMAND} -E copy "${tar_file}" "${unversioned_tar_file}"
    COMMENT "Copy correctness package to ${unversioned_tar_file}")
  add_custom_target(package_tests_u DEPENDS "${unversioned_tar_file}")
  add_dependencies(package_tests_u package_tests)
endfunction()

function(create_valgrind_correctness_package)
  if(WIN32)
    return()
  endif()
  if(USE_VALGRIND)
    set(out_dir "${CMAKE_BINARY_DIR}/valgrind_correctness")
    stage_correctness_package(OUT_DIR ${out_dir} CONTEXT "valgrind correctness" OUT_FILES package_files)
    set(tar_file ${CMAKE_BINARY_DIR}/packages/valgrind-${CMAKE_PROJECT_VERSION}.tar.gz)
    add_custom_command(
      OUTPUT ${tar_file}
      DEPENDS ${package_files}
              ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/valgrindTest.sh
              ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/valgrindTimeout.sh
      COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/valgrindTest.sh
                                       ${out_dir}/joshua_test
      COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/valgrindTimeout.sh
                                       ${out_dir}/joshua_timeout
      COMMAND ${CMAKE_COMMAND} -E tar cfz ${tar_file} ${package_files}
                                                      ${out_dir}/joshua_test
                                                      ${out_dir}/joshua_timeout
      WORKING_DIRECTORY ${out_dir}
      COMMENT "Package valgrind correctness archive"
      )
    add_custom_target(package_valgrind_tests ALL DEPENDS ${tar_file})
    add_dependencies(package_valgrind_tests strip_only_fdbserver TestHarness)
    set(unversioned_tar_file "${CMAKE_BINARY_DIR}/packages/valgrind.tar.gz")
    add_custom_command(
      OUTPUT "${unversioned_tar_file}"
      DEPENDS "${tar_file}"
      COMMAND ${CMAKE_COMMAND} -E copy "${tar_file}" "${unversioned_tar_file}"
      COMMENT "Copy valgrind package to ${unversioned_tar_file}")
    add_custom_target(package_valgrind_tests_u DEPENDS "${unversioned_tar_file}")
    add_dependencies(package_valgrind_tests_u package_valgrind_tests)
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
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/CMakeCache.txt
            ${CMAKE_BINARY_DIR}/packages/bin/fdbcli
            ${CMAKE_BINARY_DIR}/packages/bin/fdbserver
            ${CMAKE_BINARY_DIR}/packages/lib/${fdbcName}
            ${bdir}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/bindingTest.sh ${bdir}/joshua_test
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/bindingTimeout.sh ${bdir}/joshua_timeout
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/localClusterStart.sh ${bdir}/localClusterStart.sh
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/bindingTestScript.sh ${bdir}/bindingTestScript.sh
    COMMENT "Copy executables and scripts to bindingtester dir")
  file(GLOB_RECURSE test_files ${CMAKE_SOURCE_DIR}/bindings/*)
  add_custom_command(
    OUTPUT "${CMAKE_BINARY_DIR}/bindingtester.touch"
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_BINARY_DIR}/bindingtester/tests
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/bindingtester/tests
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/bindings ${CMAKE_BINARY_DIR}/bindingtester/tests
    COMMAND ${CMAKE_COMMAND} -E touch "${CMAKE_BINARY_DIR}/bindingtester.touch"
    COMMENT "Copy test files for bindingtester")

  add_custom_target(copy_binding_output_files DEPENDS ${CMAKE_BINARY_DIR}/bindingtester.touch python_binding fdb_flow_tester)
  add_custom_command(
    TARGET copy_binding_output_files
    COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:fdb_flow_tester> ${bdir}/tests/flow/bin/fdb_flow_tester
    COMMENT "Copy Flow tester for bindingtester")

  set(generated_binding_files python/fdb/fdboptions.py)
  if(WITH_JAVA_BINDING)
    if(NOT FDB_RELEASE)
      set(prerelease_string "-PRERELEASE")
    else()
      set(prerelease_string "")
    endif()
    add_custom_command(
      TARGET copy_binding_output_files
      COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/packages/fdb-java-${CMAKE_PROJECT_VERSION}${prerelease_string}.jar
        ${bdir}/tests/java/foundationdb-client.jar
      COMMENT "Copy Java bindings for bindingtester")
    add_dependencies(copy_binding_output_files fat-jar)
    add_dependencies(copy_binding_output_files foundationdb-tests)
    set(generated_binding_files ${generated_binding_files} java/foundationdb-tests.jar)
  endif()

  if(WITH_GO_BINDING AND NOT OPEN_FOR_IDE)
    add_dependencies(copy_binding_output_files fdb_go_tester fdb_go)
    add_custom_command(
      TARGET copy_binding_output_files
      COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/bindings/go/bin/_stacktester ${bdir}/tests/go/build/bin/_stacktester
      COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/bindings/go/src/github.com/apple/foundationdb/bindings/go/src/fdb/generated.go # SRC
        ${bdir}/tests/go/src/fdb/ # DEST
      COMMENT "Copy generated.go for bindingtester")
  endif()

  foreach(generated IN LISTS generated_binding_files)
    add_custom_command(
      TARGET copy_binding_output_files
      COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/bindings/${generated} ${bdir}/tests/${generated}
      COMMENT "Copy ${generated} to bindingtester")
  endforeach()

  add_custom_target(copy_bindingtester_binaries
    DEPENDS ${outfiles} "${CMAKE_BINARY_DIR}/bindingtester.touch" copy_binding_output_files)
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

# Creates a single cluster before running the specified command (usually a ctest test)
function(add_fdbclient_test)
  set(options DISABLED ENABLED)
  set(oneValueArgs NAME PROCESS_NUMBER TEST_TIMEOUT)
  set(multiValueArgs COMMAND)
  cmake_parse_arguments(T "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")
  if(OPEN_FOR_IDE)
    return()
  endif()
  if(NOT T_ENABLED AND T_DISABLED)
    return()
  endif()
  if(NOT T_NAME)
    message(FATAL_ERROR "NAME is a required argument for add_fdbclient_test")
  endif()
  if(NOT T_COMMAND)
    message(FATAL_ERROR "COMMAND is a required argument for add_fdbclient_test")
  endif()
  message(STATUS "Adding Client test ${T_NAME}")
  if (T_PROCESS_NUMBER)
    add_test(NAME "${T_NAME}"
    COMMAND ${Python_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/TestRunner/tmp_cluster.py
            --build-dir ${CMAKE_BINARY_DIR}
            --process-number ${T_PROCESS_NUMBER}
            --
            ${T_COMMAND})
  else()
    add_test(NAME "${T_NAME}"
    COMMAND ${Python_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/TestRunner/tmp_cluster.py
            --build-dir ${CMAKE_BINARY_DIR}
            --
            ${T_COMMAND})
  endif()
  if (T_TEST_TIMEOUT)
    set_tests_properties("${T_NAME}" PROPERTIES TIMEOUT ${T_TEST_TIMEOUT})
  else()
    # default timeout
    set_tests_properties("${T_NAME}" PROPERTIES TIMEOUT 60)
  endif()
  set_tests_properties("${T_NAME}" PROPERTIES ENVIRONMENT UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1)
endfunction()

# Creates 3 distinct clusters before running the specified command.
# This is useful for testing features that require multiple clusters (like the
# multi-cluster FDB client)
function(add_multi_fdbclient_test)
  set(options DISABLED ENABLED)
  set(oneValueArgs NAME)
  set(multiValueArgs COMMAND)
  cmake_parse_arguments(T "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")
  if(OPEN_FOR_IDE)
    return()
  endif()
  if(NOT T_ENABLED AND T_DISABLED)
    return()
  endif()
  if(NOT T_NAME)
    message(FATAL_ERROR "NAME is a required argument for add_multi_fdbclient_test")
  endif()
  if(NOT T_COMMAND)
    message(FATAL_ERROR "COMMAND is a required argument for add_multi_fdbclient_test")
  endif()
  message(STATUS "Adding Client test ${T_NAME}")
  add_test(NAME "${T_NAME}"
    COMMAND ${Python_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/TestRunner/tmp_multi_cluster.py
            --build-dir ${CMAKE_BINARY_DIR}
            --clusters 3
            --
            ${T_COMMAND})
  set_tests_properties("${T_NAME}" PROPERTIES TIMEOUT 60)
endfunction()

function(add_java_test)
  set(options DISABLED ENABLED)
  set(oneValueArgs NAME CLASS)
  set(multiValueArgs CLASS_PATH)
  cmake_parse_arguments(T "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")
  if(NOT T_ENABLED AND T_DISABLED)
    return()
  endif()
  if(NOT T_NAME)
    message(FATAL_ERROR "NAME is a required argument for add_java_test")
  endif()
  if(NOT T_CLASS)
    message(FATAL_ERROR "CLASS is a required argument for add_java_test")
  endif()
  set(cp "")
  set(separator ":")
  if (WIN32)
    set(separator ";")
  endif()
  message(STATUS "CLASSPATH ${T_CLASS_PATH}")
  foreach(path ${T_CLASS_PATH})
    if(cp)
      set(cp "${cp}${separator}${path}")
    else()
      set(cp "${path}")
    endif()
  endforeach()
  add_fdbclient_test(
    NAME ${T_NAME}
    COMMAND ${Java_JAVA_EXECUTABLE}
            -cp "${cp}"
            -Djava.library.path=${CMAKE_BINARY_DIR}/lib
            ${T_CLASS} "@CLUSTER_FILE@")
endfunction()
