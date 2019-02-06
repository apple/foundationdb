set(SRCS
  fdb/__init__.py
  fdb/directory_impl.py
  fdb/impl.py
  fdb/locality.py
  fdb/six.py
  fdb/subspace_impl.py
  fdb/tuple.py)

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
  set(copy_command "cp")
  set(from_path ${CMAKE_CURRENT_SOURCE_DIR}/${src})
  set(to_path ${CMAKE_CURRENT_BINARY_DIR}/${src})
  if (WIN32)
    set(copy_command "copy")
    # copy on Windows doesn't understand '/' separators
    string(REPLACE "/" "\\" from_path "${from_path}")
    string(REPLACE "/" "\\" to_path "${to_path}")
  endif()
  add_custom_command(OUTPUT ${PROJECT_BINARY_DIR}/bindings/python/${src}
    COMMAND ${copy_command} ${from_path} ${to_path}
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

add_dependencies(python_binding fdb_python_options)

set(out_files "${out_files};${options_file}")
install(FILES ${out_files} DESTINATION ${FDB_PYTHON_INSTALL_DIR} COMPONENT clients)
