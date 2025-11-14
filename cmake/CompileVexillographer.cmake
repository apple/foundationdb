# Python vexillographer source files
set(VEXILLOGRAPHER_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/__init__.py
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/__main__.py
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/models.py
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/parser.py
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/utils.py
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/generators/__init__.py
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/generators/base.py
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/generators/c.py
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/generators/cpp.py
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/generators/java.py
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/generators/python_gen.py
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/generators/ruby.py
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/templates/c.h.j2
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/templates/cpp_header.h.j2
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/templates/cpp_impl.cpp.j2
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/templates/java_enum.java.j2
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/templates/java_exception.java.j2
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/templates/java_options.java.j2
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/templates/python.py.j2
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/templates/ruby.rb.j2)

# Find Python3
find_package(Python3 COMPONENTS Interpreter REQUIRED)

# Create a dummy target for vexillographer (for dependency tracking)
add_custom_target(vexillographer DEPENDS ${VEXILLOGRAPHER_SRCS})

function(vexillographer_compile)
  set(CX_OPTIONS ALL)
  set(CX_ONE_VALUE_ARGS TARGET LANG OUT)
  set(CX_MULTI_VALUE_ARGS OUTPUT)
  cmake_parse_arguments(VX "${CX_OPTIONS}" "${CX_ONE_VALUE_ARGS}" "${CX_MULTI_VALUE_ARGS}" "${ARGN}")
  if(NOT VX_OUTPUT)
    set(VX_OUTPUT ${VX_OUT})
  endif()

  # Use Python vexillographer
  add_custom_command(
    OUTPUT ${VX_OUTPUT}
    COMMAND ${Python3_EXECUTABLE} -m fdbclient.vexillographer ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options ${VX_LANG} ${VX_OUT}
    DEPENDS ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options ${VEXILLOGRAPHER_SRCS}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Generate FDBOptions ${VX_LANG} files")

  if(VX_ALL)
    add_custom_target(${VX_TARGET} ALL DEPENDS ${VX_OUTPUT})
  else()
    add_custom_target(${VX_TARGET} DEPENDS ${VX_OUTPUT})
  endif()
endfunction()
