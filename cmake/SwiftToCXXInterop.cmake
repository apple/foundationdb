include(CompilerChecks)
include(FindSwiftLibs)

# Generates a C++ compatibility header for the given target.
# FIXME: Can we remove the headerName and match the Swift module name?
function(generate_cxx_compat_header target headerName)
  # Verify toolchain support.
  get_filename_component(SwiftBinPath ${CMAKE_Swift_COMPILER} DIRECTORY)
  set (SwiftInteropVersionFile ${SwiftBinPath}/../lib/swift/swiftToCxx/experimental-interoperability-version.json)
  if (EXISTS ${SwiftInteropVersionFile})
    file(READ ${SwiftInteropVersionFile} SwiftInteropVersion)
    message(STATUS "Swift: Experimental C++ interop version is ${SwiftInteropVersion}")
    if (${SwiftInteropVersion} VERSION_LESS 2)
      message(FATAL_ERROR "Swift: reverse interop support is too old.")
    endif()
    set(LegacySwiftToolchain NO) # FIXME: remove
  else()
    # FIXME: Once toolchain upgrades to Sept 30th, Always fail here.
    check_swift_source_compiles("@_expose(Cxx) public func test() {}" REVERSE_INTEROP_SUPPORTED)
     if (NOT ${REVERSE_INTEROP_SUPPORTED})
       message(FATAL_ERROR "Swift: reverse interop is required, but not supported.  Update your toolcain.")
     endif()
    set(LegacySwiftToolchain YES)
  endif()

  set(destpath ${CMAKE_CURRENT_BINARY_DIR}/include/SwiftModules)
  message(STATUS "Swift: C++ compatibility headers for ${target} will be generated in ${destpath}")

  # Generate a directory into which the C++ headers will go.
  file(MAKE_DIRECTORY ${destpath})
 
  if (LegacySwiftToolchain)
    # FIXME: Remove.
    # Copy the shim file for C++ to find it.
    file(MAKE_DIRECTORY ${destpath}/../shims)
    swift_get_resource_path(SWIFT_RESOURCE_PATH)
    add_custom_command(TARGET ${target} PRE_BUILD
                       COMMAND ${CMAKE_COMMAND} -E
                         copy ${SWIFT_RESOURCE_PATH}/shims/_SwiftCxxInteroperability.h ${destpath}/../shims/_SwiftCxxInteroperability.h)
  endif()

  # Generate the C++ compatibility header for the Swift module.
  target_compile_options(${target} PRIVATE "$<$<COMPILE_LANGUAGE:Swift>:SHELL: -Xfrontend -emit-clang-header-path -Xfrontend ${destpath}/${headerName}>")
  if (NOT LegacySwiftToolchain)
    # Note: do not generate Stdlib bindings yet (C++20 only).
    target_compile_options(${target} PRIVATE "$<$<COMPILE_LANGUAGE:Swift>:SHELL: -Xfrontend -clang-header-expose-decls=has-expose-attr>")
  endif()
endfunction()
