option(RUN_RUST_INTEGRATION_TESTS "Compile and run Rust tests against a temporary cluster" OFF)

# bindgen and the Rust option generator consume the same headers and options as
# the other bindings. Packaged historical headers remain available to Cargo users.
set(rust_include_dir ${CMAKE_CURRENT_BINARY_DIR}/include)
set(rust_header_inputs
  ${CMAKE_SOURCE_DIR}/bindings/c/foundationdb/fdb_c.h
  ${CMAKE_SOURCE_DIR}/bindings/c/foundationdb/fdb_c_types.h
  ${CMAKE_BINARY_DIR}/bindings/c/foundationdb/fdb_c_options.g.h
  ${CMAKE_BINARY_DIR}/bindings/c/foundationdb/fdb_c_apiversion.g.h
  ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options)
set(rust_headers)
foreach(header IN LISTS rust_header_inputs)
  get_filename_component(name ${header} NAME)
  list(APPEND rust_headers ${rust_include_dir}/${name})
endforeach()
add_custom_command(
  OUTPUT ${rust_headers}
  COMMAND ${CMAKE_COMMAND} -E make_directory ${rust_include_dir}
  COMMAND ${CMAKE_COMMAND} -E copy_if_different ${rust_header_inputs} ${rust_include_dir}
  DEPENDS ${rust_header_inputs} fdb_c_options
  COMMENT "Stage current FoundationDB headers and options for Rust"
  VERBATIM)
add_custom_target(fdb_rust_headers DEPENDS ${rust_headers})

set(rust_target_dir ${CMAKE_CURRENT_BINARY_DIR}/target)
set(rust_env
  FDB_INCLUDE_DIR=${rust_include_dir}
  FDB_CLIENT_LIB_PATH=$<TARGET_FILE_DIR:fdb_c>
  CARGO_TARGET_DIR=${rust_target_dir})
set(rust_profile $<IF:$<CONFIG:Debug>,debug,release>)
# API 740 is the newest API supported by the imported Rust binding. Building
# against current headers does not imply support for every current C API.
set(rust_cargo_base_args
  --manifest-path ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml
  --locked
  --profile $<IF:$<CONFIG:Debug>,dev,release>)
set(rust_cargo_args ${rust_cargo_base_args}
  --features foundationdb/fdb-7_4,foundationdb-gen/fdb-7_4)

# Cargo owns Rust dependency tracking, including changes to build-script inputs.
# Always invoking Cargo avoids maintaining a second Rust source list in CMake.
add_custom_target(fdb_rust ALL
  COMMAND ${CMAKE_COMMAND} -E env ${rust_env}
    ${CARGO_EXECUTABLE} build ${rust_cargo_args}
    --package foundationdb --package bindingtester
  DEPENDS fdb_c fdb_rust_headers
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Build Rust client and binding tester"
  COMMAND_EXPAND_LISTS
  VERBATIM)

set(rust_tester ${CMAKE_CURRENT_BINARY_DIR}/bin/bindingtester)
add_custom_target(fdb_rust_tester ALL
  COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/bin
  COMMAND ${CMAKE_COMMAND} -E copy_if_different
    ${rust_target_dir}/${rust_profile}/bindingtester ${rust_tester}
  DEPENDS fdb_rust
  BYPRODUCTS ${rust_tester}
  COMMENT "Stage Rust binding tester"
  VERBATIM)

set(rust_unit_packages
  --package foundationdb
  --package foundationdb-tuple
  --package foundationdb-gen
  --package foundationdb-macros
  --package foundationdb-simulation)
set(rust_unit_args ${rust_cargo_args}
  --features foundationdb-simulation/fdb-7_4)
add_custom_target(fdb_rust_tests ALL
  COMMAND ${CMAKE_COMMAND} -E env ${rust_env}
    ${CARGO_EXECUTABLE} test ${rust_unit_args} ${rust_unit_packages} --lib --no-run
  DEPENDS fdb_rust
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Compile Rust library unit tests"
  COMMAND_EXPAND_LISTS
  VERBATIM)

if(APPLE)
  set(rust_library_path DYLD_LIBRARY_PATH)
else()
  set(rust_library_path LD_LIBRARY_PATH)
endif()
set(rust_test_env ${rust_env}
  "${rust_library_path}=$<TARGET_FILE_DIR:fdb_c>:$ENV{${rust_library_path}}")

add_test(NAME rust_unit_tests
  COMMAND ${CMAKE_COMMAND} -E env ${rust_test_env}
    ${CARGO_EXECUTABLE} test ${rust_unit_args} ${rust_unit_packages} --lib --offline
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND_EXPAND_LISTS)
set_tests_properties(rust_unit_tests PROPERTIES TIMEOUT 300 LABELS "rust")

# This compile-fail doctest checks the public raw-pointer ownership boundary.
# Other client doctests may require a running cluster.
add_test(NAME rust_future_safety_tests
  COMMAND ${CMAKE_COMMAND} -E env ${rust_test_env}
    ${CARGO_EXECUTABLE} test ${rust_cargo_args} --package foundationdb --doc --offline
    future::FdbFuture
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND_EXPAND_LISTS)
set_tests_properties(rust_future_safety_tests PROPERTIES TIMEOUT 300 LABELS "rust")

foreach(symbol str_from_c WorkloadContext::new WorkloadContext::set_process_id register_workload_context WrappedWorkload Metrics)
  string(REPLACE "::" "_" test_suffix ${symbol})
  add_test(NAME rust_simulation_${test_suffix}_safety_tests
    COMMAND ${CMAKE_COMMAND} -E env ${rust_test_env}
      ${CARGO_EXECUTABLE} test ${rust_cargo_base_args}
      --features foundationdb-simulation/fdb-7_4
      --package foundationdb-simulation --doc --offline ${symbol}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND_EXPAND_LISTS)
  set_tests_properties(rust_simulation_${test_suffix}_safety_tests PROPERTIES
    TIMEOUT 300 LABELS "rust")
endforeach()

add_test(NAME rust_simulation_wake_order_tests
  COMMAND ${CMAKE_COMMAND} -E env ${rust_test_env}
    "RUSTFLAGS=$ENV{RUSTFLAGS} --cfg fdb_simulation_loom"
    CARGO_TARGET_DIR=${rust_target_dir}-loom
    ${CARGO_EXECUTABLE} test ${rust_cargo_base_args}
    --features foundationdb-simulation/fdb-7_4
    --package foundationdb-simulation --lib --offline
    fdb_rt::loom_tests::coalesced_wake_publishes_before_poll -- --exact
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND_EXPAND_LISTS)
set_tests_properties(rust_simulation_wake_order_tests PROPERTIES
  TIMEOUT 300 LABELS "rust")

if(RUN_RUST_INTEGRATION_TESTS)
  add_custom_target(fdb_rust_integration_tests ALL
    COMMAND ${CMAKE_COMMAND} -E env ${rust_env}
      ${CARGO_EXECUTABLE} test ${rust_cargo_args} --package foundationdb --tests --no-run
    DEPENDS fdb_rust_tests
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Compile Rust client integration tests"
    COMMAND_EXPAND_LISTS
    VERBATIM)
  add_fdbclient_test(NAME rust_integration_tests
    COMMAND ${CMAKE_COMMAND} -E env ${rust_test_env}
      ${CARGO_EXECUTABLE} test ${rust_cargo_args} --package foundationdb --tests --offline -- --test-threads=1
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    TEST_TIMEOUT 1200)
  # tmp_cluster otherwise preserves an inherited default cluster. These tests
  # must always use the disposable cluster created by the test runner.
  set_tests_properties(rust_integration_tests PROPERTIES
    LABELS "rust;integration"
    ENVIRONMENT_MODIFICATION "FDB_CLUSTER_FILE=unset:")
endif()
