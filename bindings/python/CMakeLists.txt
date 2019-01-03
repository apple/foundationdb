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
  add_custom_command(OUTPUT ${PROJECT_BINARY_DIR}/bindings/python/${src}
	COMMAND mkdir -p ${PROJECT_BINARY_DIR}/bindings/python/${dirname}
	COMMAND cp ${src} ${PROJECT_BINARY_DIR}/bindings/python/${dirname}/
	DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${src}
	WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
	COMMENT "copy ${src}")
  set(out_files "${out_files};${PROJECT_BINARY_DIR}/bindings/python/${src}")
endforeach()
add_custom_target(python_binding ALL DEPENDS ${out_files})

file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/bindings/python/fdb)
set(options_file ${PROJECT_BINARY_DIR}/bindings/python/fdb/fdboptions.py)
add_custom_command(OUTPUT ${options_file}
  COMMAND ${MONO_EXECUTABLE} ${VEXILLOGRAPHER_EXE} ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options python ${options_file}
  DEPENDS ${PROJECT_SOURCE_DIR}/fdbclient/vexillographer/fdb.options vexillographer
  COMMENT "Generate Python options")
add_custom_target(fdb_python_options DEPENDS
  ${options_file}
  ${PROJECT_SOURCE_DIR}/fdbclient/vexillographer/fdb.options
  vexillographer)

add_dependencies(python_binding fdb_python_options)

set(out_files "${out_files};${options_file}")
install(FILES ${out_files} DESTINATION ${FDB_PYTHON_INSTALL_DIR} COMPONENT clients)
