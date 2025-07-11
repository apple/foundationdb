# This configures the fdb testing system in cmake. Currently this simply means
# that it will get a list of all test files and store this list in a parent scope
# so that we can later verify that all of them were assigned to a test.
#
# - TEST_DIRECTORY The directory where all the tests are
# - ERROR_ON_ADDITIONAL_FILES if this is passed verify_fdb_tests will print
#   an error if there are any .txt files in the test directory that do not
#   correspond to a test or are not ignore by a pattern
# - IGNORE_PATTERNS regular expressions. All files that match any of those
#   expressions don't need to be associated with a test
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
  set(options UNIT IGNORE LONG_RUNNING)
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
  # We shouldn't run downgrade tests under valgrind: https://github.com/apple/foundationdb/issues/6322
  if(USE_VALGRIND AND ${test_name} MATCHES .*to_.*)
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
  elseif(ADD_FDB_TEST_LONG_RUNNING)
    message(STATUS
      "ADDING LONG RUNNING TEST ${assigned_id} ${test_name}")
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
      COMMAND $<TARGET_FILE:Python3::Interpreter> ${TestRunner}
      -n ${test_name}
      -b ${PROJECT_BINARY_DIR}
      -t ${test_type}
      -O ${OLD_FDBSERVER_BINARY}
      --config "${CMAKE_BUILD_TYPE}"
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
    set_tests_properties("${test_name}" PROPERTIES ENVIRONMENT "${SANITIZER_OPTIONS}")
    get_filename_component(test_dir_full ${first_file} DIRECTORY)
    if(NOT ${test_dir_full} STREQUAL "")
      get_filename_component(test_dir ${test_dir_full} NAME)
      set_tests_properties(${test_name} PROPERTIES TIMEOUT ${this_test_timeout} LABELS "${test_dir}")
    endif()
  endif()
  # set variables used for generating test packages
  if(ADD_FDB_TEST_LONG_RUNNING)
    set(LONG_RUNNING_TEST_NAMES ${LONG_RUNNING_TEST_NAMES} ${test_name} PARENT_SCOPE)
    set(LONG_RUNNING_TEST_FILES_${test_name} ${ADD_FDB_TEST_TEST_FILES} PARENT_SCOPE)
    set(LONG_RUNNING_TEST_TYPE_${test_name} ${test_type} PARENT_SCOPE)
  else()
    set(TEST_NAMES ${TEST_NAMES} ${test_name} PARENT_SCOPE)
    set(TEST_FILES_${test_name} ${ADD_FDB_TEST_TEST_FILES} PARENT_SCOPE)
    set(TEST_TYPE_${test_name} ${test_type} PARENT_SCOPE)
  endif()
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
  set(options LONG_RUNNING)
  set(oneValueArgs OUT_DIR CONTEXT OUT_FILES)
  set(multiValueArgs TEST_LIST)
  cmake_parse_arguments(STAGE "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")
  file(MAKE_DIRECTORY ${STAGE_OUT_DIR}/bin)
  foreach(test IN LISTS STAGE_TEST_LIST)
    if((${test} MATCHES ${TEST_PACKAGE_INCLUDE}) AND
        (NOT ${test} MATCHES ${TEST_PACKAGE_EXCLUDE}))
      string(LENGTH "${CMAKE_SOURCE_DIR}/tests/" base_length)
      if(STAGE_LONG_RUNNING)
        set(TEST_FILES_PREFIX "LONG_RUNNING_TEST_FILES")
      else()
        set(TEST_FILES_PREFIX "TEST_FILES")
      endif()
      foreach(file IN LISTS ${TEST_FILES_PREFIX}_${test})
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
                            # ${STAGE_OUT_DIR}/bin/TestHarness.exe
                            # ${STAGE_OUT_DIR}/bin/TraceLogHelper.dll
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
            # ${CMAKE_BINARY_DIR}/packages/bin/TestHarness.exe
            # ${CMAKE_BINARY_DIR}/packages/bin/TraceLogHelper.dll
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/CMakeCache.txt ${STAGE_OUT_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/packages/bin/fdbserver
                                     ${CMAKE_BINARY_DIR}/bin/coverage.fdbserver.xml
                                     ${CMAKE_BINARY_DIR}/lib/coverage.fdbclient.xml
                                     ${CMAKE_BINARY_DIR}/lib/coverage.fdbrpc.xml
                                     ${CMAKE_BINARY_DIR}/lib/coverage.flow.xml
                                     # ${CMAKE_BINARY_DIR}/packages/bin/TestHarness.exe
                                     # ${CMAKE_BINARY_DIR}/packages/bin/TraceLogHelper.dll
                                     ${STAGE_OUT_DIR}/bin
    COMMENT "Copying files for ${STAGE_CONTEXT} package"
    )

  set(test_harness_dir "${CMAKE_SOURCE_DIR}/contrib/TestHarness2")
  file(GLOB_RECURSE test_harness2_files RELATIVE "${test_harness_dir}" CONFIGURE_DEPENDS "${test_harness_dir}/*.py")
  foreach(file IN LISTS test_harness2_files)
    set(src_file "${test_harness_dir}/${file}")
    set(out_file "${STAGE_OUT_DIR}/${file}")
    get_filename_component(dir "${out_file}" DIRECTORY)
    file(MAKE_DIRECTORY "${dir}")
    add_custom_command(OUTPUT ${out_file}
      COMMAND ${CMAKE_COMMAND} -E copy "${src_file}" "${out_file}"
      DEPENDS "${src_file}")
    list(APPEND package_files "${out_file}")
  endforeach()

  add_custom_command(
    OUTPUT "${STAGE_OUT_DIR}/joshua_logtool.py"
    COMMAND ${CMAKE_COMMAND} -E copy "${CMAKE_SOURCE_DIR}/contrib/joshua_logtool.py" "${STAGE_OUT_DIR}/joshua_logtool.py"
    DEPENDS "${CMAKE_SOURCE_DIR}/contrib/joshua_logtool.py"
  )

  list(APPEND package_files ${test_files} ${external_files} "${STAGE_OUT_DIR}/joshua_logtool.py")
  if(STAGE_OUT_FILES)
    set(${STAGE_OUT_FILES} ${package_files} PARENT_SCOPE)
  endif()
endfunction()

function(create_correctness_package)
  if(WIN32)
    return()
  endif()
  set(out_dir "${CMAKE_BINARY_DIR}/correctness")
  stage_correctness_package(OUT_DIR ${out_dir} CONTEXT "correctness" OUT_FILES package_files TEST_LIST "${TEST_NAMES}")
  set(tar_file ${CMAKE_BINARY_DIR}/packages/correctness-${FDB_VERSION}.tar.gz)
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

function(create_long_running_correctness_package)
  if(WIN32)
    return()
  endif()
  set(out_dir "${CMAKE_BINARY_DIR}/long_running_correctness")
  stage_correctness_package(OUT_DIR ${out_dir} CONTEXT "long running correctness" OUT_FILES package_files TEST_LIST "${LONG_RUNNING_TEST_NAMES}" LONG_RUNNING)
  set(tar_file ${CMAKE_BINARY_DIR}/packages/long-running-correctness-${FDB_VERSION}.tar.gz)
  add_custom_command(
    OUTPUT ${tar_file}
    DEPENDS ${package_files}
            ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/longRunningCorrectnessTest.sh
            ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/longRunningCorrectnessTimeout.sh
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/longRunningCorrectnessTest.sh
                                    ${out_dir}/joshua_test
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts/longRunningCorrectnessTimeout.sh
                                    ${out_dir}/joshua_timeout
    COMMAND ${CMAKE_COMMAND} -E tar cfz ${tar_file} ${package_files}
                                                    ${out_dir}/joshua_test
                                                    ${out_dir}/joshua_timeout
    WORKING_DIRECTORY ${out_dir}
    COMMENT "Package long running correctness archive"
    )
  add_custom_target(package_long_running_tests ALL DEPENDS ${tar_file})
  add_dependencies(package_long_running_tests strip_only_fdbserver TestHarness)
  set(unversioned_tar_file "${CMAKE_BINARY_DIR}/packages/long_running_correctness.tar.gz")
  add_custom_command(
    OUTPUT "${unversioned_tar_file}"
    DEPENDS "${tar_file}"
    COMMAND ${CMAKE_COMMAND} -E copy "${tar_file}" "${unversioned_tar_file}"
    COMMENT "Copy long running correctness package to ${unversioned_tar_file}")
  add_custom_target(package_long_running_tests_u DEPENDS "${unversioned_tar_file}")
  add_dependencies(package_long_running_tests_u package_long_running_tests)
endfunction()

function(create_valgrind_correctness_package)
  if(WIN32)
    return()
  endif()
  if(USE_VALGRIND)
    set(out_dir "${CMAKE_BINARY_DIR}/valgrind_correctness")
    stage_correctness_package(OUT_DIR ${out_dir} CONTEXT "valgrind correctness" OUT_FILES package_files TEST_LIST "${TEST_NAMES}")
    set(tar_file ${CMAKE_BINARY_DIR}/packages/valgrind-${FDB_VERSION}.tar.gz)
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

function(prepare_binding_test_files build_directory target_name target_dependency)
  add_custom_target(${target_name} DEPENDS ${target_dependency})
  add_custom_command(
    TARGET ${target_name}
    POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:fdb_flow_tester> ${build_directory}/tests/flow/bin/fdb_flow_tester
    COMMENT "Copy Flow tester for bindingtester")

  add_dependencies(${target_name} python_binding)
  set(generated_binding_files python/fdb/fdboptions.py python/fdb/apiversion.py)
  if(WITH_JAVA_BINDING)
    if(NOT FDB_RELEASE)
      set(not_fdb_release_string "-SNAPSHOT")
    else()
      set(not_fdb_release_string "")
    endif()
    add_custom_command(
      TARGET ${target_name}
      POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/packages/fdb-java-${FDB_VERSION}${not_fdb_release_string}.jar
        ${build_directory}/tests/java/foundationdb-client.jar
      COMMENT "Copy Java bindings for bindingtester")
    add_dependencies(${target_name} fat-jar)
    add_dependencies(${target_name} foundationdb-tests)
    set(generated_binding_files ${generated_binding_files} java/foundationdb-tests.jar)
  endif()

  if(WITH_GO_BINDING)
    add_dependencies(${target_name} fdb_go_tester fdb_go)
    add_custom_command(
      TARGET ${target_name}
      COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/bindings/go/bin/_stacktester ${build_directory}/tests/go/build/bin/_stacktester
      COMMAND ${CMAKE_COMMAND} -E make_directory ${build_directory}/tests/go/src/fdb/
      COMMAND ${CMAKE_COMMAND} -E copy
      ${CMAKE_BINARY_DIR}/bindings/go/src/github.com/apple/foundationdb/bindings/go/src/fdb/generated.go # SRC
      ${build_directory}/tests/go/src/fdb/ # DEST
      COMMENT "Copy generated.go for bindingtester")
  endif()

  foreach(generated IN LISTS generated_binding_files)
    add_custom_command(
      TARGET ${target_name}
      POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/bindings/${generated} ${build_directory}/tests/${generated}
      COMMENT "Copy ${generated} to bindingtester")
  endforeach()
endfunction(prepare_binding_test_files)

function(package_bindingtester2)
  if (WIN32 OR OPEN_FOR_IDE)
    message(WARNING "Binding tester is not built (WIN32/OPEN_FOR_IDE)")
    return()
  endif()

  set(fdbcName "libfdb_c.so")
  if (APPLE)
    set(fdbcName "libfdb_c.dylib")
  endif ()

  set(touch_file ${CMAKE_BINARY_DIR}/bindingtester2.touch)
  set(build_directory ${CMAKE_BINARY_DIR}/bindingtester2)
  set(tests_directory ${build_directory}/tests)
  add_custom_command(
    OUTPUT ${touch_file}
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${build_directory}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${build_directory}
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${tests_directory}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${tests_directory}
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/bindings ${tests_directory}
    COMMAND ${CMAKE_COMMAND} -E touch "${CMAKE_BINARY_DIR}/bindingtester2.touch"
    COMMENT "Setup scratch directory for bindingtester2")

  set(joshua_directory ${CMAKE_SOURCE_DIR}/contrib/Joshua/scripts)
  set(output_files
   ${build_directory}/joshua_test
   ${build_directory}/joshua_timeout
   ${build_directory}/fdbcli
   ${build_directory}/fdbserver
   ${build_directory}/${fdbcName}
  )

  add_custom_command(
    OUTPUT ${output_files}
    DEPENDS strip_only_fdbcli
            strip_only_fdbserver
            strip_only_fdb_c
            ${joshua_directory}/binding_test_start.sh
            ${joshua_directory}/binding_test_timeout.sh
            ${touch_file}
    COMMAND ${CMAKE_COMMAND} -E copy
            ${CMAKE_BINARY_DIR}/packages/bin/fdbcli
            ${CMAKE_BINARY_DIR}/packages/bin/fdbserver
            ${CMAKE_BINARY_DIR}/packages/lib/${fdbcName}
            ${build_directory}
    COMMAND ${CMAKE_COMMAND} -E copy ${joshua_directory}/binding_test_start.sh ${build_directory}/joshua_test
    COMMAND ${CMAKE_COMMAND} -E copy ${joshua_directory}/binding_test_timeout.sh ${build_directory}/joshua_timeout
    COMMENT "Copy executables and scripts to bindingtester2 dir")

  set(local_cluster_files ${build_directory}/local_cluster)
  set(local_cluster_directory ${CMAKE_SOURCE_DIR}/contrib/local_cluster)
  add_custom_command(
    OUTPUT ${local_cluster_files}
    COMMAND ${CMAKE_COMMAND} -E copy_directory
            ${local_cluster_directory}
            ${build_directory}
  )

  prepare_binding_test_files(${build_directory} copy_bindingtester2_test_files ${touch_file})

  set(tar_file ${CMAKE_BINARY_DIR}/packages/bindingtester2-${FDB_VERSION}.tar.gz)
  add_custom_command(
    OUTPUT ${tar_file}
    DEPENDS ${touch_file} ${output_files} ${local_cluster_files} copy_bindingtester2_test_files
    COMMAND ${CMAKE_COMMAND} -E tar czf ${tar_file} *
    WORKING_DIRECTORY ${build_directory}
    COMMENT "Pack bindingtester2"
  )

  add_custom_target(bindingtester2 ALL DEPENDS ${tar_file})
endfunction(package_bindingtester2)

function(package_bindingtester)
  if(WIN32 OR OPEN_FOR_IDE)
    message(WARNING "Binding tester is not built (WIN32/OPEN_FOR_IDE)")
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
  add_custom_command(
    OUTPUT "${CMAKE_BINARY_DIR}/bindingtester.touch"
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_BINARY_DIR}/bindingtester/tests
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/bindingtester/tests
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/bindings ${CMAKE_BINARY_DIR}/bindingtester/tests
    COMMAND ${CMAKE_COMMAND} -E touch "${CMAKE_BINARY_DIR}/bindingtester.touch"
    COMMENT "Copy test files for bindingtester")

  prepare_binding_test_files(${bdir} copy_binding_output_files ${CMAKE_BINARY_DIR}/bindingtester.touch)

  add_custom_target(copy_bindingtester_binaries
    DEPENDS ${outfiles} "${CMAKE_BINARY_DIR}/bindingtester.touch" copy_binding_output_files)
  add_dependencies(copy_bindingtester_binaries strip_only_fdbserver strip_only_fdbcli strip_only_fdb_c)

  set(tar_file ${CMAKE_BINARY_DIR}/packages/bindingtester-${FDB_VERSION}.tar.gz)
  add_custom_command(
    OUTPUT ${tar_file}
    COMMAND ${CMAKE_COMMAND} -E tar czf ${tar_file} *
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bindingtester
    COMMENT "Pack bindingtester")
  add_custom_target(bindingtester ALL DEPENDS ${tar_file} copy_bindingtester_binaries)
endfunction()

function(add_fdb_unit_test TEST_NAME PATTERN)
  add_test(NAME ${TEST_NAME}
           WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
           COMMAND ${CMAKE_BINARY_DIR}/bin/fdbserver -r unittests -f "${PATTERN}")
  set_tests_properties(${TEST_NAME} PROPERTIES
    FAIL_REGULAR_EXPRESSION "0 tests passed; 1 tests failed."
  )
endfunction()

function(collect_unit_tests SOURCE_DIR)
  message("Collecting unit_tests in ${SOURCE_DIR}")
  execute_process(
    COMMAND grep --include \*.h --include \*.cpp --include \*.hpp -rhoP "TEST_CASE\\(\\\"\\K[^\\\"]+(?=\\\"\\))" "${SOURCE_DIR}"
    OUTPUT_VARIABLE TEST_NAMES
  )
  string(REGEX REPLACE "\n" ";" TEST_NAMES "${TEST_NAMES}")

  foreach(TEST_NAME ${TEST_NAMES})
    message("ADDING DISCOVERED UNIT TEST: ${TEST_NAME}")
    add_fdb_unit_test(UnitTest_${TEST_NAME} ${TEST_NAME})
  endforeach()
endfunction()

# Test for setting up Python venv for client tests.
# Adding this test as a fixture to another test allows the use of non-native Python packages within client test scripts
# by installing dependencies from requirements.txt
set(test_venv_dir ${CMAKE_BINARY_DIR}/tests/test_venv)
if (WIN32)
  set(shell_cmd "cmd" CACHE INTERNAL "")
  set(shell_opt "/c" CACHE INTERNAL "")
  set(test_venv_activate "${test_venv_dir}/Scripts/activate.bat" CACHE INTERNAL "")
else()
  set(shell_cmd "bash" CACHE INTERNAL "")
  set(shell_opt "-c" CACHE INTERNAL "")
  set(test_venv_activate ". ${test_venv_dir}/bin/activate" CACHE INTERNAL "")
endif()
set(test_venv_cmd "")
string(APPEND test_venv_cmd "${Python3_EXECUTABLE} -m venv ${test_venv_dir} ")
string(APPEND test_venv_cmd "&& ${test_venv_activate} ")
string(APPEND test_venv_cmd "&& pip install --upgrade pip ")
string(APPEND test_venv_cmd "&& pip install -r ${CMAKE_SOURCE_DIR}/tests/TestRunner/requirements.txt")
# NOTE: At this stage we are in the virtual environment and Python3_EXECUTABLE is not available anymore
string(APPEND test_venv_cmd "&& (cd ${CMAKE_BINARY_DIR}/bindings/python && python3 -m pip install .) ")
add_test(
  NAME test_venv_setup
  COMMAND bash -c ${test_venv_cmd}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
set_tests_properties(test_venv_setup PROPERTIES FIXTURES_SETUP test_virtual_env_setup TIMEOUT 120)
set_tests_properties(test_venv_setup PROPERTIES RESOURCE_LOCK TEST_VENV_SETUP)

# Run the test command under Python venv as a cmd (Windows) or bash (Linux/Apple) script, which allows && or || chaining.
function(add_python_venv_test)
  set(oneValueArgs NAME WORKING_DIRECTORY TEST_TIMEOUT)
  set(multiValueArgs COMMAND)
  cmake_parse_arguments(T "" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")
  if(OPEN_FOR_IDE)
    return()
  endif()
  if(NOT T_NAME)
    message(FATAL_ERROR "NAME is a required argument for add_fdbclient_test")
  endif()
  if(NOT T_COMMAND)
    message(FATAL_ERROR "COMMAND is a required argument for add_fdbclient_test")
  endif()
  if(NOT T_WORKING_DIRECTORY)
    set(T_WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
  endif()
  if(NOT T_TEST_TIMEOUT)
    if(USE_SANITIZER)
      set(T_TEST_TIMEOUT 1200)
    else()
      set(T_TEST_TIMEOUT 300)
    endif()
  endif()
  # expand list of command arguments to space-separated string so that we can pass to shell
  string(REPLACE ";" " " T_COMMAND "${T_COMMAND}")
  add_test(
    NAME ${T_NAME}
    WORKING_DIRECTORY ${T_WORKING_DIRECTORY}
    COMMAND ${shell_cmd} ${shell_opt} "${test_venv_activate} && ${T_COMMAND}")
  set_tests_properties(${T_NAME} PROPERTIES FIXTURES_REQUIRED test_virtual_env_setup TIMEOUT ${T_TEST_TIMEOUT})
  set(test_env_vars "PYTHONPATH=${CMAKE_SOURCE_DIR}/tests/TestRunner:${CMAKE_BINARY_DIR}/tests/TestRunner")
  if(APPLE)
    set(ld_env_name "DYLD_LIBRARY_PATH")
  else()
    set(ld_env_name "LD_LIBRARY_PATH")
  endif()
  set(test_env_vars PROPERTIES ENVIRONMENT "${test_env_vars};${ld_env_name}=${CMAKE_BINARY_DIR}/lib:$ENV{${ld_env_name}}")
  if(USE_SANITIZER)
    set(test_env_vars "${test_env_vars};${SANITIZER_OPTIONS}")
  endif()
  set_tests_properties("${T_NAME}" PROPERTIES ENVIRONMENT "${test_env_vars}")
endfunction()

# Creates a single cluster before running the specified command (usually a ctest test)
function(add_fdbclient_test)
  set(options DISABLED ENABLED DISABLE_TENANTS DISABLE_LOG_DUMP API_TEST_BLOB_GRANULES_ENABLED TLS_ENABLED)
  set(oneValueArgs NAME PROCESS_NUMBER TEST_TIMEOUT WORKING_DIRECTORY)
  set(multiValueArgs COMMAND)
  cmake_parse_arguments(T "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")
  if(OPEN_FOR_IDE)
    return()
  endif()
  if(NOT T_ENABLED AND T_DISABLED)
    return()
  endif()
  if(NOT T_WORKING_DIRECTORY)
    set(T_WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
  endif()
  if(NOT T_NAME)
    message(FATAL_ERROR "NAME is a required argument for add_fdbclient_test")
  endif()
  if(NOT T_COMMAND)
    message(FATAL_ERROR "COMMAND is a required argument for add_fdbclient_test")
  endif()
  set(TMP_CLUSTER_CMD python ${CMAKE_SOURCE_DIR}/tests/TestRunner/tmp_cluster.py --build-dir ${CMAKE_BINARY_DIR})
  if(T_PROCESS_NUMBER)
    list(APPEND TMP_CLUSTER_CMD --process-number ${T_PROCESS_NUMBER})
  endif()
  if(T_DISABLE_LOG_DUMP)
    list(APPEND TMP_CLUSTER_CMD --disable-log-dump)
  endif()
  if(T_DISABLE_TENANTS)
    list(APPEND TMP_CLUSTER_CMD --disable-tenants)
  endif()
  if(T_API_TEST_BLOB_GRANULES_ENABLED)
    list(APPEND TMP_CLUSTER_CMD --blob-granules-enabled)
  endif()
  if(T_TLS_ENABLED)
    list(APPEND TMP_CLUSTER_CMD --tls-enabled)
  endif()
  list(APPEND TMP_CLUSTER_CMD -- ${T_COMMAND})
  message(STATUS "Adding Client test ${T_NAME}")
  if (NOT T_TEST_TIMEOUT)
    # default timeout
    if(USE_SANITIZER)
      set(T_TEST_TIMEOUT 1200)
    else()
      set(T_TEST_TIMEOUT 300)
    endif()
  endif()
  add_python_venv_test(
    NAME ${T_NAME}
    WORKING_DIRECTORY ${T_WORKING_DIRECTORY}
    COMMAND ${TMP_CLUSTER_CMD}
    TEST_TIMEOUT ${T_TEST_TIMEOUT})
endfunction()

# Creates a cluster file for a nonexistent cluster before running the specified command
# (usually a ctest test)
function(add_unavailable_fdbclient_test)
  set(options DISABLED ENABLED)
  set(oneValueArgs NAME TEST_TIMEOUT)
  set(multiValueArgs COMMAND)
  cmake_parse_arguments(T "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")
  if(OPEN_FOR_IDE)
    return()
  endif()
  if(NOT T_ENABLED AND T_DISABLED)
    return()
  endif()
  if(NOT T_NAME)
    message(FATAL_ERROR "NAME is a required argument for add_unavailable_fdbclient_test")
  endif()
  if(NOT T_COMMAND)
    message(FATAL_ERROR "COMMAND is a required argument for add_unavailable_fdbclient_test")
  endif()
  if (NOT T_TEST_TIMEOUT)
    # default timeout
    set(T_TEST_TIMEOUT 60)
  endif()
  message(STATUS "Adding unavailable client test ${T_NAME}")
  add_python_venv_test(
    NAME ${T_NAME}
    COMMAND python ${CMAKE_SOURCE_DIR}/tests/TestRunner/fake_cluster.py
            --output-dir ${CMAKE_BINARY_DIR} -- ${T_COMMAND}
    TEST_TIMEOUT ${T_TEST_TIMEOUT})
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
  add_python_venv_test(
    NAME ${T_NAME}
    COMMAND python ${CMAKE_SOURCE_DIR}/tests/TestRunner/tmp_multi_cluster.py
            --build-dir ${CMAKE_BINARY_DIR}
            --clusters 3 -- ${T_COMMAND}
    TEST_TIMEOUT 60)
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
