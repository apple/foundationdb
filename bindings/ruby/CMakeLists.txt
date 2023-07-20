# we put this generated file into the src dir, as it
# greatly simplifies debugging
vexillographer_compile(TARGET ruby_options LANG ruby
  OUT ${CMAKE_CURRENT_SOURCE_DIR}/lib/fdboptions.rb ALL)
configure_file(fdb.gemspec.cmake fdb.gemspec)
configure_file(${CMAKE_SOURCE_DIR}/LICENSE ${CMAKE_CURRENT_BINARY_DIR}/LICENSE COPYONLY)

set(SRCS
  lib/fdb.rb
  lib/fdbdirectory.rb
  lib/fdbimpl.rb
  lib/fdbimpl_v609.rb
  lib/fdblocality.rb
  lib/fdboptions.rb
  lib/fdbsubspace.rb
  lib/fdbtuple.rb)

set(out_files "")
foreach(src ${SRCS})
  get_filename_component(dirname ${src} DIRECTORY)
  get_filename_component(extname ${src} EXT)
  if(NOT EXISTS ${dirname})
    file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/bindings/ruby/${dirname})
  endif()
  set(from_path ${CMAKE_CURRENT_SOURCE_DIR}/${src})
  set(to_path ${CMAKE_CURRENT_BINARY_DIR}/${src})
  add_custom_command(OUTPUT ${PROJECT_BINARY_DIR}/bindings/ruby/${src}
    COMMAND ${CMAKE_COMMAND} -E copy ${from_path} ${to_path}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${src}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "copy ${src}")
  set(out_files "${out_files};${PROJECT_BINARY_DIR}/bindings/ruby/${src}")
endforeach()
add_custom_target(ruby_binding ALL DEPENDS ${out_files})

if(NOT FDB_RELEASE)
  set(not_fdb_release_string "SNAPSHOT")
else()
  set(not_fdb_release_string "")
endif()

set(setup_file fdb-${FDB_VERSION}.gem)
set(gem_target ${CMAKE_BINARY_DIR}/packages/fdb-${FDB_VERSION}${not_fdb_release_string}.gem)
add_custom_command(OUTPUT ${gem_target}
  COMMAND ${GEM_COMMAND} build fdb.gemspec --output ${setup_file} &&
          ${CMAKE_COMMAND} -E copy ${setup_file} ${gem_target}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  COMMENT "Building ruby gem")
add_custom_target(gem_package DEPENDS ${gem_target})
add_dependencies(gem_package ruby_options)
add_dependencies(gem_package ruby_binding)
add_dependencies(packages gem_package)
