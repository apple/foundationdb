set(FDB_C_SRCS
  fdb_c.cpp
  foundationdb/fdb_c.h
  ThreadCleanup.cpp)

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/foundationdb)

set(platform)
if(APPLE)
  set(platform "osx")
else()
  set(platform "linux")
endif()

add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/fdb_c.g.S
      ${CMAKE_CURRENT_BINARY_DIR}/fdb_c_function_pointers.g.h
  COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/generate_asm.py ${platform}
      ${CMAKE_CURRENT_SOURCE_DIR}/fdb_c.cpp
	  ${CMAKE_CURRENT_BINARY_DIR}/fdb_c.g.S
      ${CMAKE_CURRENT_BINARY_DIR}/fdb_c_function_pointers.g.h
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/generate_asm.py ${CMAKE_CURRENT_SOURCE_DIR}/fdb_c.cpp
  COMMENT "Generate C bindings")
add_custom_target(fdb_c_generated DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/fdb_c.g.S
      ${CMAKE_CURRENT_BINARY_DIR}/fdb_c_function_pointers.g.h)

add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/fdb_c_options.g.h
  COMMAND ${MONO_EXECUTABLE} ${VEXILLOGRAPHER_EXE} ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options c ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/fdb_c_options.g.h
  DEPENDS ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options vexillographer
  COMMENT "Generate C options")
add_custom_target(fdb_c_options DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/fdb_c_options.g.h)

include(GenerateExportHeader)

add_library(fdb_c SHARED ${FDB_C_SRCS} ${CMAKE_CURRENT_BINARY_DIR}/fdb_c.g.S)
add_dependencies(fdb_c fdb_c_generated fdb_c_options)
target_link_libraries(fdb_c PUBLIC fdbclient)
target_include_directories(fdb_c PUBLIC
  ${CMAKE_CURRENT_BINARY_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}/foundationdb)
# TODO: re-enable once the old vcxproj-based build system is removed.
#generate_export_header(fdb_c EXPORT_MACRO_NAME "DLLEXPORT"
#  EXPORT_FILE_NAME ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/fdb_c_export.h)
install(TARGETS fdb_c
  EXPORT fdbc
  DESTINATION ${FDB_LIB_DIR}
  COMPONENT clients)
install(
  FILES foundationdb/fdb_c.h
        ${CMAKE_CURRENT_BINARY_DIR}/foundationdb/fdb_c_options.g.h
        ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options
  DESTINATION ${FDB_INCLUDE_INSTALL_DIR}/foundationdb COMPONENT clients)
#install(EXPORT fdbc DESTINATION ${FDB_LIB_DIR}/foundationdb COMPONENT clients)
