# we put this generated file into the src dir, as it
# greatly simplifies debugging
vexillographer_compile(TARGET ruby_options LANG ruby
  OUT ${CMAKE_CURRENT_SOURCE_DIR}/lib/fdboptions.rb ALL)
configure_file(fdb.gemspec.cmake fdb.gemspec)

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
add_dependencies(packages gem_package)
