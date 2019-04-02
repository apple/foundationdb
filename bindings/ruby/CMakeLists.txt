# we put this generated file into the src dir, as it
# greatly simplifies debugging
vexillographer_compile(TARGET ruby_options LANG ruby
  OUT ${CMAKE_CURRENT_SOURCE_DIR}/lib/fdboptions.rb ALL)
configure_file(fdb.gemspec.cmake fdb.gemspec)

set(gem_file fdb-${FDB_VERSION}.gem)
set(gem_target ${CMAKE_BINARY_DIR}/packages/${gem_file})
add_custom_command(OUTPUT ${gem_target}
  COMMAND ${GEM_COMMAND} build fdb.gemspec &&
          ${CMAKE_COMMAND} -E copy ${gem_file} ${gem_target}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  COMMENT "Building ruby gem")
add_custom_target(gem_package DEPENDS ${gem_target})
add_dependencies(gem_package ruby_options)
add_dependencies(packages gem_package)
