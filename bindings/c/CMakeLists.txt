set(FDB_C_SRCS
  fdb_c.cpp
  foundationdb/fdb_c.h
  foundationdb/fdb_c_internal.h
  foundationdb/fdb_c_types.h)

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/foundationdb)

set(asm_file ${CMAKE_CURRENT_BINARY_DIR}/fdb_c.g.S)

set(os "linux")
set(cpu "intel")
if(APPLE)
  set(os "osx")
elseif(WIN32)
  set(os "windows")
  set(asm_file ${CMAKE_CURRENT_BINARY_DIR}/fdb_c.g.asm)
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64" OR CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
  set(cpu "aarch64")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(ppc64le|powerpc64le)")
  set(cpu "ppc64le")
endif()

set(IS_ARM_MAC NO)
if(APPLE AND CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
  set(IS_ARM_MAC YES)
endif()

add_custom_command(OUTPUT ${asm_file} ${CMAKE_CURRENT_BINARY_DIR}/fdb_c_function_pointers.g.h
  COMMAND $<TARGET_FILE:Python3::Interpreter> ${CMAKE_CURRENT_SOURCE_DIR}/generate_asm.py ${os} ${cpu}
  ${CMAKE_CURRENT_SOURCE_DIR}/fdb_c.cpp
  ${asm_file}
  ${CMAKE_CURRENT_BINARY_DIR}/fdb_c_function_pointers.g.h
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/generate_asm.py ${CMAKE_CURRENT_SOURCE_DIR}/fdb_c.cpp
  COMMENT "Generate C bindings")
add_custom_target(fdb_c_generated DEPENDS ${asm_file}
  ${CMAKE_CURRENT_BINARY_DIR}/fdb_c_function_pointers.g.h)

vexillographer_compile(TARGET fdb_c_options LANG c OUT ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/fdb_c_options.g.h
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/fdb_c_options.g.h)

if(NOT DEFINED FDB_API_VERSION_FILE)
  message(FATAL_ERROR "Missing definitions of API versions")
endif()
include(${FDB_API_VERSION_FILE})
set(fdb_c_apiversion_file ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/fdb_c_apiversion.g.h)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/foundationdb/fdb_c_apiversion.h.cmake ${fdb_c_apiversion_file})

include(GenerateExportHeader)

if(OPEN_FOR_IDE)
  add_library(fdb_c OBJECT ${FDB_C_SRCS} ${fdb_c_apiversion_file} ${asm_file})
else()
  add_library(fdb_c SHARED ${FDB_C_SRCS} ${fdb_c_apiversion_file} ${asm_file})
  strip_debug_symbols(fdb_c)
endif()
add_dependencies(fdb_c fdb_c_generated fdb_c_options)
add_dependencies(fdbclient fdb_c_options)
add_dependencies(fdbclient_sampling fdb_c_options)
target_link_libraries(fdb_c PRIVATE $<BUILD_INTERFACE:fdbclient>)
if(USE_UBSAN)
  # The intent of this hack is to force c targets that depend on fdb_c to use
  # c++ as their linker language. Otherwise you see undefined references to c++
  # specific ubsan symbols.
  add_library(force_cxx_linker STATIC IMPORTED)
  set_property(TARGET force_cxx_linker PROPERTY IMPORTED_LOCATION /dev/null)
  set_target_properties(force_cxx_linker PROPERTIES IMPORTED_LINK_INTERFACE_LANGUAGES CXX)
  target_link_libraries(fdb_c PUBLIC $<BUILD_INTERFACE:force_cxx_linker>)
endif()
if(APPLE)
  set(symbols ${CMAKE_CURRENT_BINARY_DIR}/fdb_c.symbols)
  add_custom_command(OUTPUT ${symbols}
    COMMAND $<TARGET_FILE:Python3::Interpreter> ${CMAKE_CURRENT_SOURCE_DIR}/symbolify.py
    ${CMAKE_CURRENT_SOURCE_DIR}/foundationdb/fdb_c.h
    ${CMAKE_CURRENT_SOURCE_DIR}/foundationdb/fdb_c_internal.h
    ${symbols}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/symbolify.py ${CMAKE_CURRENT_SOURCE_DIR}/foundationdb/fdb_c.h ${CMAKE_CURRENT_SOURCE_DIR}/foundationdb/fdb_c_internal.h
    COMMENT "Generate exported_symbols_list")
  add_custom_target(exported_symbols_list DEPENDS ${symbols})
  add_dependencies(fdb_c exported_symbols_list)
  target_link_options(fdb_c PRIVATE "LINKER:-no_weak_exports,-exported_symbols_list,${symbols}")
elseif(WIN32)
else()
  if(NOT USE_UBSAN)
    # For ubsan we need to export type information for the vptr check to work.
    # Otherwise we only want to export fdb symbols in the fdb c api.
    target_link_options(fdb_c PRIVATE "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/fdb_c.map")
  endif()
  target_link_options(fdb_c PRIVATE "LINKER:-z,nodelete,-z,noexecstack")
endif()
target_include_directories(fdb_c PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/foundationdb>)
if(WIN32)
  enable_language(ASM_MASM)
  set_property(SOURCE ${asm_file} PROPERTY LANGUAGE ASM_MASM)
endif()

# The tests don't build on windows
if(NOT WIN32)
  set(MAKO_SRCS
    test/mako/admin_server.hpp
    test/mako/admin_server.cpp
    test/mako/async.hpp
    test/mako/async.cpp
    test/mako/blob_granules.hpp
    test/mako/blob_granules.cpp
    test/mako/future.hpp
    test/mako/limit.hpp
    test/mako/logger.hpp
    test/mako/mako.cpp
    test/mako/mako.hpp
    test/mako/operations.hpp
    test/mako/operations.cpp
    test/mako/process.hpp
    test/mako/shm.hpp
    test/mako/stats.hpp
    test/mako/tenant.cpp
    test/mako/tenant.hpp
    test/mako/time.hpp
    test/mako/utils.cpp
    test/mako/utils.hpp)
  add_subdirectory(test/unit/third_party)
  find_package(Threads REQUIRED)
  set(UNIT_TEST_SRCS
    test/unit/unit_tests.cpp
    test/unit/fdb_api.cpp
    test/unit/fdb_api.hpp)

  set(UNIT_TEST_VERSION_510_SRCS test/unit/unit_tests_version_510.cpp)
  set(TRACE_PARTIAL_FILE_SUFFIX_TEST_SRCS test/unit/trace_partial_file_suffix_test.cpp)
  set(DISCONNECTED_TIMEOUT_UNIT_TEST_SRCS
    test/unit/disconnected_timeout_tests.cpp
    test/unit/fdb_api.cpp
    test/unit/fdb_api.hpp)

  add_library(fdb_cpp INTERFACE test/fdb_api.hpp)
  target_sources(fdb_cpp INTERFACE)
  target_include_directories(fdb_cpp INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/test)
  target_link_libraries(fdb_cpp INTERFACE fdb_c fmt::fmt)

  set(API_TESTER_SRCS
    test/apitester/fdb_c_api_tester.cpp
    test/apitester/TesterAtomicOpsCorrectnessWorkload.cpp
    test/apitester/TesterApiWorkload.cpp
    test/apitester/TesterApiWorkload.h
    test/apitester/TesterTestSpec.cpp
    test/apitester/TesterTestSpec.h
    test/apitester/TesterBlobGranuleCorrectnessWorkload.cpp
    test/apitester/TesterBlobGranuleErrorsWorkload.cpp
    test/apitester/TesterBlobGranuleUtil.cpp
    test/apitester/TesterBlobGranuleUtil.h
    test/apitester/TesterCancelTransactionWorkload.cpp
    test/apitester/TesterCorrectnessWorkload.cpp
    test/apitester/TesterExampleWorkload.cpp
    test/apitester/TesterKeyValueStore.cpp
    test/apitester/TesterKeyValueStore.h
    test/apitester/TesterOptions.h
    test/apitester/TesterScheduler.cpp
    test/apitester/TesterScheduler.h
    test/apitester/TesterTransactionExecutor.cpp
    test/apitester/TesterTransactionExecutor.h
    test/apitester/TesterUtil.cpp
    test/apitester/TesterUtil.h
    test/apitester/TesterWatchAndWaitWorkload.cpp
    test/apitester/TesterWorkload.cpp
    test/apitester/TesterWorkload.h
    )

  add_library(fdb_c_unit_tests_impl OBJECT ${UNIT_TEST_SRCS})
  add_library(fdb_c_api_tester_impl OBJECT ${API_TESTER_SRCS})

  if(OPEN_FOR_IDE)
    add_library(fdb_c_performance_test OBJECT test/performance_test.c test/test.h)
    add_library(fdb_c_ryw_benchmark OBJECT test/ryw_benchmark.c test/test.h)
    add_library(fdb_c_txn_size_test OBJECT test/txn_size_test.c test/test.h)
    add_library(fdb_c_client_memory_test OBJECT test/client_memory_test.cpp test/unit/fdb_api.cpp test/unit/fdb_api.hpp)
    add_library(mako OBJECT ${MAKO_SRCS})
    add_library(fdb_c_setup_tests OBJECT test/unit/setup_tests.cpp)
    add_library(fdb_c_unit_tests_version_510 OBJECT ${UNIT_TEST_VERSION_510_SRCS})
    add_library(trace_partial_file_suffix_test OBJECT ${TRACE_PARTIAL_FILE_SUFFIX_TEST_SRCS})
    add_library(disconnected_timeout_unit_tests OBJECT ${DISCONNECTED_TIMEOUT_UNIT_TEST_SRCS})
    add_library(fdb_c_client_config_tester OBJECT test/client_config_tester.cpp)
  else()
    add_executable(fdb_c_performance_test test/performance_test.c test/test.h)
    add_executable(fdb_c_ryw_benchmark test/ryw_benchmark.c test/test.h)
    add_executable(fdb_c_txn_size_test test/txn_size_test.c test/test.h)
    add_executable(fdb_c_client_memory_test test/client_memory_test.cpp test/unit/fdb_api.cpp test/unit/fdb_api.hpp)
    add_executable(mako ${MAKO_SRCS})
    add_executable(fdb_c_setup_tests test/unit/setup_tests.cpp)
    add_executable(fdb_c_unit_tests)
    target_link_libraries(fdb_c_unit_tests PRIVATE fdb_c fdb_c_unit_tests_impl)
    add_executable(fdb_c_unit_tests_version_510 ${UNIT_TEST_VERSION_510_SRCS})
    add_executable(trace_partial_file_suffix_test ${TRACE_PARTIAL_FILE_SUFFIX_TEST_SRCS})
    add_executable(disconnected_timeout_unit_tests ${DISCONNECTED_TIMEOUT_UNIT_TEST_SRCS})
    add_executable(fdb_c_api_tester)
    target_link_libraries(fdb_c_api_tester PRIVATE fdb_c fdb_c_api_tester_impl)
    strip_debug_symbols(fdb_c_performance_test)
    strip_debug_symbols(fdb_c_ryw_benchmark)
    strip_debug_symbols(fdb_c_txn_size_test)
    strip_debug_symbols(fdb_c_client_memory_test)
    add_executable(fdb_c_client_config_tester test/client_config_tester.cpp)
  endif()

  target_link_libraries(fdb_c_performance_test PRIVATE fdb_c Threads::Threads)
  target_link_libraries(fdb_c_ryw_benchmark PRIVATE fdb_c Threads::Threads)
  target_link_libraries(fdb_c_txn_size_test PRIVATE fdb_c Threads::Threads)
  target_link_libraries(fdb_c_client_memory_test PRIVATE fdb_c Threads::Threads)

  target_include_directories(fdb_c_api_tester_impl PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/ ${CMAKE_SOURCE_DIR}/flow/include ${CMAKE_BINARY_DIR}/flow/include)
  target_link_libraries(fdb_c_api_tester_impl PRIVATE fdb_cpp toml11_target Threads::Threads fmt::fmt boost_target)
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    target_link_libraries(fdb_c_api_tester_impl PRIVATE stdc++fs)
  endif()
  target_link_libraries(fdb_c_api_tester_impl PRIVATE SimpleOpt)

  target_include_directories(fdb_c_unit_tests_impl PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/)
  target_include_directories(fdb_c_unit_tests_version_510 PUBLIC ${CMAKE_BINARY_DIR}/flow/include)
  target_link_libraries(fdb_c_setup_tests PRIVATE fdb_c Threads::Threads doctest)
  target_link_libraries(fdb_c_unit_tests_impl PRIVATE fdb_c Threads::Threads fdbclient rapidjson doctest)
  target_link_libraries(fdb_c_unit_tests_version_510 PRIVATE fdb_c Threads::Threads doctest)
  target_link_libraries(trace_partial_file_suffix_test PRIVATE fdb_c Threads::Threads flow doctest)
  target_link_libraries(disconnected_timeout_unit_tests PRIVATE fdb_c Threads::Threads doctest)
  target_link_libraries(fdb_c_client_config_tester PRIVATE SimpleOpt fdb_cpp fdb_c fdbclient Threads::Threads fmt::fmt)
  target_include_directories(fdb_c_client_config_tester PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/)

  # do not set RPATH for mako
  set_property(TARGET mako PROPERTY SKIP_BUILD_RPATH TRUE)
  target_link_libraries(mako PRIVATE fdb_c tokensign fdbclient fmt::fmt Threads::Threads fdb_cpp boost_target rapidjson)

  if(NOT OPEN_FOR_IDE)
    # Make sure that fdb_c.h is compatible with c90
    add_executable(fdb_c90_test test/fdb_c90_test.c)
    set_property(TARGET fdb_c90_test PROPERTY C_STANDARD 90)
	if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
	  target_compile_options(fdb_c90_test PRIVATE -Wall -Wextra -Wpedantic -Wno-gnu-line-marker -Werror)
	else ()
	  target_compile_options(fdb_c90_test PRIVATE -Wall -Wextra -Wpedantic -Werror)
	endif ()
    target_link_libraries(fdb_c90_test PRIVATE fdb_c)
  endif()

  if(OPEN_FOR_IDE)
    set(FDB_C_TARGET $<TARGET_OBJECTS:fdb_c>)
  else()
    set(FDB_C_TARGET $<TARGET_FILE:fdb_c>)
    add_custom_command(
      OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libfdb_c_external.so
      COMMAND ${CMAKE_COMMAND} -E copy ${FDB_C_TARGET} ${CMAKE_CURRENT_BINARY_DIR}/libfdb_c_external.so
      DEPENDS fdb_c
      COMMENT "Copy libfdb_c to use as external client for test")
    add_custom_target(external_client DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/libfdb_c_external.so)
    add_dependencies(fdb_c_unit_tests_impl external_client)
    add_dependencies(disconnected_timeout_unit_tests external_client)
    add_dependencies(fdb_c_api_tester_impl external_client)

    add_fdbclient_test(
      NAME fdb_c_setup_tests
      COMMAND $<TARGET_FILE:fdb_c_setup_tests>)
    add_fdbclient_test(
      NAME fdb_c_unit_tests
      COMMAND $<TARGET_FILE:fdb_c_unit_tests>
      @CLUSTER_FILE@
      fdb)
    add_fdbclient_test(
      NAME fdb_c_unit_tests_version_510
      COMMAND $<TARGET_FILE:fdb_c_unit_tests_version_510>
      @CLUSTER_FILE@
      fdb)
    add_fdbclient_test(
      NAME trace_partial_file_suffix_test
      COMMAND $<TARGET_FILE:trace_partial_file_suffix_test>
      @CLUSTER_FILE@
      fdb)
    add_fdbclient_test(
      NAME fdb_c_external_client_unit_tests
      COMMAND $<TARGET_FILE:fdb_c_unit_tests>
      @CLUSTER_FILE@
      fdb
      ${CMAKE_CURRENT_BINARY_DIR}/libfdb_c_external.so
    )
    add_unavailable_fdbclient_test(
      NAME disconnected_timeout_unit_tests
      COMMAND $<TARGET_FILE:disconnected_timeout_unit_tests>
      @CLUSTER_FILE@
    )
    add_unavailable_fdbclient_test(
      NAME disconnected_timeout_external_client_unit_tests
      COMMAND $<TARGET_FILE:disconnected_timeout_unit_tests>
      @CLUSTER_FILE@
      ${CMAKE_CURRENT_BINARY_DIR}/libfdb_c_external.so
    )

    file(GLOB API_TEST_FILES "${CMAKE_CURRENT_SOURCE_DIR}/test/apitester/tests/*.toml")
    set(PATTERNS_TO_SKIP "XXXX")
    if (USE_ASAN)
      set(PATTERNS_TO_SKIP
        ".*BlobGranule.*"  # skip BlobGranule tests because we don't use
        ".*Tenant.*"       # skip Tenant tests because we don't use
      )
    endif()
    foreach(test_file ${API_TEST_FILES})
      get_filename_component(file_name "${test_file}" NAME_WE)
      set(test_name "fdb_c_api_test_${file_name}")

      # skip tests that match the patterns in PATTERNS_TO_SKIP
      set(SKIP_FILE FALSE)
      foreach(PATTERN ${PATTERNS_TO_SKIP})
          if(file_name MATCHES "${PATTERN}")
              set(SKIP_FILE TRUE)
              break()
          endif()
      endforeach()
      if(SKIP_FILE)
        continue()
      endif()

      add_python_venv_test(NAME "${test_name}"
        COMMAND python ${CMAKE_SOURCE_DIR}/bindings/c/test/apitester/run_c_api_tests.py
          --build-dir ${CMAKE_BINARY_DIR}
          --api-tester-bin $<TARGET_FILE:fdb_c_api_tester>
          --external-client-library ${CMAKE_CURRENT_BINARY_DIR}/libfdb_c_external.so
          --test-file ${test_file}
          --retain-client-lib-copies
        TEST_TIMEOUT 300
      )
    endforeach()

    add_python_venv_test(NAME fdb_c_upgrade_to_future_version
      COMMAND python ${CMAKE_SOURCE_DIR}/tests/TestRunner/upgrade_test.py
      --build-dir ${CMAKE_BINARY_DIR}
      --test-file ${CMAKE_SOURCE_DIR}/bindings/c/test/apitester/tests/upgrade/MixedApiWorkloadMultiThr.toml
      --upgrade-path "${FDB_CURRENT_VERSION}" "${FDB_FUTURE_VERSION}" "${FDB_CURRENT_VERSION}"
      --process-number 3
      )

    add_python_venv_test(NAME fdb_c_upgrade_to_future_version_blob_granules
      COMMAND python ${CMAKE_SOURCE_DIR}/tests/TestRunner/upgrade_test.py
      --build-dir ${CMAKE_BINARY_DIR}
      --test-file ${CMAKE_SOURCE_DIR}/bindings/c/test/apitester/tests/upgrade/ApiBlobGranulesCorrectness.toml
      --upgrade-path "${FDB_CURRENT_VERSION}" "${FDB_FUTURE_VERSION}" "${FDB_CURRENT_VERSION}"
      --blob-granules-enabled
      --process-number 3
      )

    if(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" AND NOT USE_SANITIZER)
      add_python_venv_test(NAME fdb_c_client_config_tests
        COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/test/fdb_c_client_config_tests.py
        --build-dir ${CMAKE_BINARY_DIR}
        --client-config-tester-bin $<TARGET_FILE:fdb_c_client_config_tester>
        )

      add_python_venv_test(NAME fdb_c_upgrade_from_prev2_gradual
        COMMAND python ${CMAKE_SOURCE_DIR}/tests/TestRunner/upgrade_test.py
        --build-dir ${CMAKE_BINARY_DIR}
        --test-file ${CMAKE_SOURCE_DIR}/bindings/c/test/apitester/tests/upgrade/MixedApiWorkloadMultiThr.toml
        --upgrade-path "${FDB_PREV2_RELEASE_VERSION}" "${FDB_PREV_RELEASE_VERSION}" "${FDB_CURRENT_VERSION}" "${FDB_PREV_RELEASE_VERSION}"
        --process-number 3
        )

      add_python_venv_test(NAME fdb_c_upgrade_from_prev2_direct
        COMMAND python ${CMAKE_SOURCE_DIR}/tests/TestRunner/upgrade_test.py
        --build-dir ${CMAKE_BINARY_DIR}
        --test-file ${CMAKE_SOURCE_DIR}/bindings/c/test/apitester/tests/upgrade/MixedApiWorkloadMultiThr.toml
        --upgrade-path "${FDB_PREV2_RELEASE_VERSION}" "${FDB_CURRENT_VERSION}" "${FDB_PREV_RELEASE_VERSION}"
        --process-number 3
        )

      add_python_venv_test(NAME fdb_c_upgrade_from_prev
        COMMAND python ${CMAKE_SOURCE_DIR}/tests/TestRunner/upgrade_test.py
        --build-dir ${CMAKE_BINARY_DIR}
        --test-file ${CMAKE_SOURCE_DIR}/bindings/c/test/apitester/tests/upgrade/MixedApiWorkloadMultiThr.toml
        --upgrade-path "${FDB_PREV_RELEASE_VERSION}" "${FDB_CURRENT_VERSION}" "${FDB_PREV_RELEASE_VERSION}"
        --process-number 3
        )

      add_python_venv_test(NAME fdb_c_wiggle_only
        COMMAND python ${CMAKE_SOURCE_DIR}/tests/TestRunner/upgrade_test.py
        --build-dir ${CMAKE_BINARY_DIR}
        --test-file ${CMAKE_SOURCE_DIR}/bindings/c/test/apitester/tests/upgrade/MixedApiWorkloadMultiThr.toml
        --upgrade-path "${FDB_CURRENT_VERSION}" "wiggle"
        --disable-log-dump
        --process-number 3
        --redundancy double
        )

      add_python_venv_test(NAME fdb_c_wiggle_and_upgrade
        COMMAND python ${CMAKE_SOURCE_DIR}/tests/TestRunner/upgrade_test.py
        --build-dir ${CMAKE_BINARY_DIR}
        --test-file ${CMAKE_SOURCE_DIR}/bindings/c/test/apitester/tests/upgrade/MixedApiWorkloadMultiThr.toml
        --upgrade-path "${FDB_PREV_RELEASE_VERSION}" "wiggle" "${FDB_CURRENT_VERSION}"
        --disable-log-dump
        --process-number 3
        --redundancy double
        )

    endif()
  endif()

endif()

set(cpp_workloads_srcs
  test/workloads/workloads.cpp
  test/workloads/workloads.h
  test/workloads/SimpleWorkload.cpp)

set(c_workloads_srcs
  test/workloads/CWorkload.c)

if(OPEN_FOR_IDE)
  add_library(cpp_workloads OBJECT ${cpp_workloads_srcs})
  add_library(c_workloads OBJECT ${c_workloads_srcs})
else()
  add_library(cpp_workloads SHARED ${cpp_workloads_srcs})
  add_library(c_workloads SHARED ${c_workloads_srcs})
endif()
set_target_properties(cpp_workloads PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/share/foundationdb")
set_target_properties(c_workloads PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/share/foundationdb")
target_link_libraries(cpp_workloads PUBLIC fdb_c)
target_include_directories(c_workloads PUBLIC ${CMAKE_SOURCE_DIR}/bindings/c)
target_link_libraries(c_workloads PUBLIC fdb_c)

if(NOT WIN32 AND NOT APPLE AND NOT OPEN_FOR_IDE)
  # Add the -Wl,--undefined-version flag to the linker command to allow
  # undefined symbols in version scripts. Clang 19 doesn't allow this and would
  # complain with "symbol not defined" errors.
  if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "19.0.0")
    target_link_options(cpp_workloads PRIVATE
      "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/external_workload.map"
      "LINKER:-z,nodelete"
      "LINKER:--undefined-version"
    )
    target_link_options(c_workloads PRIVATE
      "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/external_workload.map"
      "LINKER:-z,nodelete"
      "LINKER:--undefined-version"
    )
  else()
    target_link_options(cpp_workloads PRIVATE
      "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/external_workload.map"
      "LINKER:-z,nodelete"
    )
    target_link_options(c_workloads PRIVATE
      "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/external_workload.map"
      "LINKER:-z,nodelete"
    )
  endif()
endif()

# Generate shim library in Linux builds
if(OPEN_FOR_IDE)

  add_library(fdb_c_shim OBJECT foundationdb/fdb_c_shim.h fdb_c_shim.cpp)
  target_link_libraries(fdb_c_shim PUBLIC dl)
  target_include_directories(fdb_c_shim PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/foundationdb>)

  add_library(fdb_c_shim_lib_tester OBJECT test/shim_lib_tester.cpp)
  target_link_libraries(fdb_c_shim_lib_tester PRIVATE fdb_c_shim SimpleOpt fdb_cpp Threads::Threads)
  target_include_directories(fdb_c_shim_lib_tester PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/ ${CMAKE_SOURCE_DIR}/flow/include)

elseif(NOT WIN32 AND NOT APPLE) # Linux Only

  set(SHIM_LIB_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR})

  set(SHIM_LIB_GEN_SRC
    ${SHIM_LIB_OUTPUT_DIR}/libfdb_c.so.init.cpp
    ${SHIM_LIB_OUTPUT_DIR}/libfdb_c.so.tramp.S)

  set(IMPLIBSO_SRC_DIR ${CMAKE_SOURCE_DIR}/contrib/Implib.so)

  if(CMAKE_SYSTEM_PROCESSOR STREQUAL "amd64")
    set(IMPLIBSO_ARCH "x86_64")
  else()
    set(IMPLIBSO_ARCH ${CMAKE_SYSTEM_PROCESSOR})
  endif()

  set(IMPLIBSO_SRC
    ${IMPLIBSO_SRC_DIR}/implib-gen.py
    ${IMPLIBSO_SRC_DIR}/arch/common/init.cpp.tpl
    ${IMPLIBSO_SRC_DIR}/arch/${IMPLIBSO_ARCH}/config.ini
    ${IMPLIBSO_SRC_DIR}/arch/${IMPLIBSO_ARCH}/table.S.tpl
    ${IMPLIBSO_SRC_DIR}/arch/${IMPLIBSO_ARCH}/trampoline.S.tpl
    )

  add_custom_command(OUTPUT ${SHIM_LIB_GEN_SRC}
    COMMAND $<TARGET_FILE:Python3::Interpreter> ${IMPLIBSO_SRC_DIR}/implib-gen.py
    --target ${IMPLIBSO_ARCH}
    --outdir ${SHIM_LIB_OUTPUT_DIR}
    --dlopen-callback=fdb_shim_dlopen_callback
    --symbol-filter='^fdb_.*$$'
    $<TARGET_FILE:fdb_c>
    DEPENDS ${IMPLIBSO_SRC} fdb_c
    COMMENT "Generating source code for C shim library")

  add_library(fdb_c_shim SHARED ${SHIM_LIB_GEN_SRC} foundationdb/fdb_c_shim.h fdb_c_shim.cpp)
  target_link_options(fdb_c_shim PRIVATE "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/fdb_c.map,-z,nodelete,-z,noexecstack")
  target_link_libraries(fdb_c_shim PUBLIC dl)
  target_include_directories(fdb_c_shim PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/foundationdb>)

  add_executable(fdb_c_shim_unit_tests)
  target_link_libraries(fdb_c_shim_unit_tests PRIVATE fdb_c_shim fdb_c_unit_tests_impl)

  add_executable(fdb_c_shim_api_tester)
  target_link_libraries(fdb_c_shim_api_tester PRIVATE fdb_c_shim fdb_c_api_tester_impl)

  add_executable(fdb_c_shim_lib_tester test/shim_lib_tester.cpp)
  target_link_libraries(fdb_c_shim_lib_tester PRIVATE fdb_c_shim SimpleOpt fdb_cpp Threads::Threads)
  target_include_directories(fdb_c_shim_lib_tester PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/ ${CMAKE_SOURCE_DIR}/flow/include)

  set(SHIM_LIB_TEST_EXTRA_OPTIONS "")
  if(NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR USE_SANITIZER)
    list(APPEND SHIM_LIB_TEST_EXTRA_OPTIONS --disable-prev-version-tests)
  endif()  

  add_python_venv_test(NAME fdb_c_shim_library_tests
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/test/fdb_c_shim_tests.py
    --build-dir ${CMAKE_BINARY_DIR}
    --unit-tests-bin $<TARGET_FILE:fdb_c_shim_unit_tests>
    --api-tester-bin $<TARGET_FILE:fdb_c_shim_api_tester>
    --shim-lib-tester-bin $<TARGET_FILE:fdb_c_shim_lib_tester>
    --api-test-dir ${CMAKE_SOURCE_DIR}/bindings/c/test/apitester/tests
    ${SHIM_LIB_TEST_EXTRA_OPTIONS}
    )

endif() # End Linux only

# TODO: re-enable once the old vcxproj-based build system is removed.
#generate_export_header(fdb_c EXPORT_MACRO_NAME "DLLEXPORT"
#  EXPORT_FILE_NAME ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/fdb_c_export.h)

set(targets_export_name "FoundationDB-Client")
set(generated_dir "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(version_config "${generated_dir}/${targets_export_name}ConfigVersion.cmake")
set(project_config "${generated_dir}/${targets_export_name}Config.cmake")
include(CMakePackageConfigHelpers)
write_basic_package_version_file(
  "${version_config}" VERSION ${GENERIC_LIB_VERSION} COMPATIBILITY AnyNewerVersion
)
configure_file("${PROJECT_SOURCE_DIR}/cmake/Config.cmake.in" "${project_config}" @ONLY)

fdb_install(
  TARGETS fdb_c
  EXPORT ${targets_export_name}
  DESTINATION lib
  COMPONENT clients)
fdb_install(
  FILES foundationdb/fdb_c.h
  ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/fdb_c_options.g.h
  ${fdb_c_apiversion_file}
  ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options
  ${CMAKE_SOURCE_DIR}/bindings/c/foundationdb/fdb_c_types.h
  DESTINATION include
  DESTINATION_SUFFIX /foundationdb
  COMPONENT clients)
fdb_install(
  FILES "${project_config}" "${version_config}"
  DESTINATION lib
  DESTINATION_SUFFIX "/cmake/${targets_export_name}"
  COMPONENT clients)
fdb_configure_and_install(
  FILE "${PROJECT_SOURCE_DIR}/cmake/foundationdb-client.pc.in"
  DESTINATION lib
  DESTINATION_SUFFIX "/pkgconfig"
  COMPONENT clients)
fdb_install(
  EXPORT ${targets_export_name}
  DESTINATION lib
  DESTINATION_SUFFIX "/cmake/${targets_export_name}"
  COMPONENT clients)

if(NOT WIN32 AND NOT APPLE) # Linux Only

  fdb_install(
    FILES foundationdb/fdb_c_shim.h
    DESTINATION include
    DESTINATION_SUFFIX /foundationdb
    COMPONENT clients)

  fdb_install(
    TARGETS fdb_c_shim
    EXPORT ${targets_export_name}
    DESTINATION lib
    COMPONENT clients)

endif() # End Linux only
