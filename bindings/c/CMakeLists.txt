set(FDB_C_SRCS
  fdb_c.cpp
  foundationdb/fdb_c.h
  ThreadCleanup.cpp)

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/foundationdb)

set(asm_file ${CMAKE_CURRENT_BINARY_DIR}/fdb_c.g.S)

set(platform "linux")
if(APPLE)
  set(platform "osx")
elseif(WIN32)
  set(platform "windows")
  set(asm_file ${CMAKE_CURRENT_BINARY_DIR}/fdb_c.g.asm)
endif()

add_custom_command(OUTPUT ${asm_file} ${CMAKE_CURRENT_BINARY_DIR}/fdb_c_function_pointers.g.h
  COMMAND $<TARGET_FILE:Python::Interpreter> ${CMAKE_CURRENT_SOURCE_DIR}/generate_asm.py ${platform}
      ${CMAKE_CURRENT_SOURCE_DIR}/fdb_c.cpp
      ${asm_file}
      ${CMAKE_CURRENT_BINARY_DIR}/fdb_c_function_pointers.g.h
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/generate_asm.py ${CMAKE_CURRENT_SOURCE_DIR}/fdb_c.cpp
  COMMENT "Generate C bindings")
add_custom_target(fdb_c_generated DEPENDS ${asm_file}
      ${CMAKE_CURRENT_BINARY_DIR}/fdb_c_function_pointers.g.h)

vexillographer_compile(TARGET fdb_c_options LANG c OUT ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/fdb_c_options.g.h
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/fdb_c_options.g.h)

include(GenerateExportHeader)

if(OPEN_FOR_IDE)
  add_library(fdb_c OBJECT ${FDB_C_SRCS} ${asm_file})
else()
  add_library(fdb_c SHARED ${FDB_C_SRCS} ${asm_file})
  strip_debug_symbols(fdb_c)
endif()
add_dependencies(fdb_c fdb_c_generated fdb_c_options)
target_link_libraries(fdb_c PUBLIC fdbclient)
target_include_directories(fdb_c PUBLIC
  ${CMAKE_CURRENT_BINARY_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}/foundationdb)
if(WIN32)
  enable_language(ASM_MASM)
  set_property(SOURCE ${asm_file} PROPERTY LANGUAGE ASM_MASM)
endif()

# The tests don't build on windows
if(NOT WIN32)
  if(OPEN_FOR_IDE)
    add_library(fdb_c_performance_test OBJECT test/performance_test.c test/test.h)
    add_library(fdb_c_ryw_benchmark OBJECT test/ryw_benchmark.c test/test.h)
  else()
    add_executable(fdb_c_performance_test test/performance_test.c test/test.h)
    add_executable(fdb_c_ryw_benchmark test/ryw_benchmark.c test/test.h)
    strip_debug_symbols(fdb_c_performance_test)
    strip_debug_symbols(fdb_c_ryw_benchmark)
  endif()
  target_link_libraries(fdb_c_performance_test PRIVATE fdb_c)
  target_link_libraries(fdb_c_ryw_benchmark PRIVATE fdb_c)
endif()

# TODO: re-enable once the old vcxproj-based build system is removed.
#generate_export_header(fdb_c EXPORT_MACRO_NAME "DLLEXPORT"
#  EXPORT_FILE_NAME ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/fdb_c_export.h)
fdb_install(TARGETS fdb_c
  DESTINATION lib
  COMPONENT clients)
fdb_install(
  FILES foundationdb/fdb_c.h
  ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/fdb_c_options.g.h
  ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options
  DESTINATION include COMPONENT clients)
#install(EXPORT fdbc DESTINATION ${FDB_LIB_DIR}/foundationdb COMPONENT clients)
