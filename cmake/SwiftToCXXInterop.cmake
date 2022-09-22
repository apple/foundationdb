include(CompilerChecks)
include(FindSwiftLibs)

# Generates a C++ compatibility header for the given target.
# FIXME: Can we remove the headerName and match the Swift module name?
function(generate_cxx_compat_header target headerName)
  check_swift_source_compiles("@_expose(Cxx) public func test() {}" REVERSE_INTEROP_SUPPORTED)
  if (NOT ${REVERSE_INTEROP_SUPPORTED})
    message(FATAL_ERROR "Swift: reverse interop is required, but not supported.  Update your toolcain.")
  endif()

  set(destpath ${CMAKE_CURRENT_BINARY_DIR}/include/SwiftModules)
  message(STATUS "Swift: C++ compatibility headers for ${target} will be generated in ${destpath}")

  # Generate a directory into which the C++ headers will go.
  file(MAKE_DIRECTORY ${destpath})
 
  # Copy the shim file for C++ to find it.
  file(MAKE_DIRECTORY ${destpath}/../shims)
  swift_get_resource_path(SWIFT_RESOURCE_PATH)
  add_custom_command(TARGET ${target} PRE_BUILD
                     COMMAND ${CMAKE_COMMAND} -E
                         copy ${SWIFT_RESOURCE_PATH}/shims/_SwiftCxxInteroperability.h ${destpath}/../shims/_SwiftCxxInteroperability.h)

  # Generate the C++ compatibility header for the Swift module.
  target_compile_options(${target} PRIVATE "$<$<COMPILE_LANGUAGE:Swift>:SHELL: -Xfrontend -emit-clang-header-path -Xfrontend ${destpath}/${headerName}>")
endfunction()
