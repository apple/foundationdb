set(SRCS

  fdb/__init__.py
  fdb/directory_impl.py
  fdb/impl.py
  fdb/locality.py
  fdb/subspace_impl.py
  fdb/tenant_management.py
  fdb/tuple.py

  fdb/apiversion.py.cmake

  README.rst
  MANIFEST.in

  pyproject.toml
)

# FIXME Windows Python support at fdb/fdb_c.dll.pth
if(APPLE)
  list(APPEND SRCS fdb/libfdb_c.dylib.pth)
else()
  list(APPEND SRCS fdb/libfdb_c.so.pth)
endif()

set(out_files "")
foreach(src ${SRCS})
  get_filename_component(dirname ${src} DIRECTORY)
  get_filename_component(extname ${src} EXT)
  if(NOT EXISTS ${dirname})
    file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/bindings/python/${dirname})
  endif()
  set(from_path ${CMAKE_CURRENT_SOURCE_DIR}/${src})
  set(to_path ${CMAKE_CURRENT_BINARY_DIR}/${src})
  add_custom_command(OUTPUT ${PROJECT_BINARY_DIR}/bindings/python/${src}
    COMMAND ${CMAKE_COMMAND} -E copy ${from_path} ${to_path}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${src}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "copy ${src}")
  set(out_files "${out_files};${PROJECT_BINARY_DIR}/bindings/python/${src}")
endforeach()
add_custom_target(python_binding ALL DEPENDS ${out_files})

file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/bindings/python/fdb)
set(options_file ${PROJECT_BINARY_DIR}/bindings/python/fdb/fdboptions.py)
vexillographer_compile(TARGET fdb_python_options LANG python OUT ${options_file}
  OUTPUT ${options_file})

if(NOT DEFINED FDB_API_VERSION_FILE)
  message(FATAL_ERROR "Missing definitions of API versions")
endif()
include(${FDB_API_VERSION_FILE})
set(apiversion_file ${CMAKE_CURRENT_BINARY_DIR}/fdb/apiversion.py)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/fdb/apiversion.py.cmake ${apiversion_file})

add_dependencies(python_binding fdb_python_options)

set(out_files "${out_files};${options_file};${apiversion_file}")

# Create Python package in tar.gz format
configure_file(${CMAKE_SOURCE_DIR}/LICENSE ${CMAKE_CURRENT_BINARY_DIR}/LICENSE COPYONLY)
find_program(pycodestyle pycodestyle)
if (pycodestyle)
  add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/check_py_code_style
    COMMAND ${pycodestyle} bindings/python --config=${CMAKE_CURRENT_SOURCE_DIR}/pycodestyle.cfg &&
            ${CMAKE_COMMAND} -E ${CMAKE_CURRENT_BINARY_DIR}/check_py_code_style
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${out_files}
    COMMENT "Check python code style")
  add_custom_target(fdb_python_check DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/check_py_code_style)
else()
  add_custom_target(fdb_python_check COMMAND ${CMAKE_COMMAND} -E echo "Skipped Python style check! Missing: pycodestyle")
endif()
if(NOT FDB_RELEASE)
  set(not_fdb_release_string "a1")
else()
  set(not_fdb_release_string "")
endif()


set(setup_file_name foundationdb-${FDB_VERSION}.tar.gz)
set(package_file ${CMAKE_BINARY_DIR}/packages/foundationdb-${FDB_VERSION}${not_fdb_release_string}.tar.gz)
add_custom_command(OUTPUT ${package_file}
  COMMAND $<TARGET_FILE:Python3::Interpreter> -m ensurepip
  COMMAND $<TARGET_FILE:Python3::Interpreter> -m pip install --upgrade build
  COMMAND $<TARGET_FILE:Python3::Interpreter> -m build &&
          ${CMAKE_COMMAND} -E copy dist/${setup_file_name} ${package_file}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  COMMENT "Create Python package")
add_custom_target(python_package DEPENDS ${package_file})
add_dependencies(python_package python_binding)
add_dependencies(packages python_package)

if(NOT OPEN_FOR_IDE AND NOT USE_SANITIZER)
  add_fdbclient_test(
    NAME python_unit_tests
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tests/unit_tests.py
            --cluster-file @CLUSTER_FILE@ --verbose
    DISABLE_LOG_DUMP
  )
endif() # NOT OPEN_FOR_IDE AND NOT USE_SANITIZER
