# Generate the canonical C header inputs for compile-only Rust CI without
# configuring or linking the FoundationDB server.
if(NOT DEFINED OUTPUT_DIR)
  message(FATAL_ERROR "OUTPUT_DIR is required")
endif()
get_filename_component(fdb_source_dir "${CMAKE_CURRENT_LIST_DIR}/../../.." ABSOLUTE)
include(${fdb_source_dir}/flow/ApiVersions.cmake)
find_package(Python3 REQUIRED COMPONENTS Interpreter)

file(MAKE_DIRECTORY ${OUTPUT_DIR})
file(COPY
  ${fdb_source_dir}/bindings/c/foundationdb/fdb_c.h
  ${fdb_source_dir}/bindings/c/foundationdb/fdb_c_types.h
  ${fdb_source_dir}/bindings/c/foundationdb/CWorkload.h
  ${fdb_source_dir}/fdbclient/vexillographer/fdb.options
  DESTINATION ${OUTPUT_DIR})
configure_file(
  ${fdb_source_dir}/bindings/c/foundationdb/fdb_c_apiversion.h.cmake
  ${OUTPUT_DIR}/fdb_c_apiversion.g.h @ONLY)
execute_process(
  COMMAND ${Python3_EXECUTABLE}
    ${fdb_source_dir}/fdbclient/vexillographer/vexillographer.py
    ${fdb_source_dir}/fdbclient/vexillographer/fdb.options c
    ${OUTPUT_DIR}/fdb_c_options.g.h
  COMMAND_ERROR_IS_FATAL ANY)
