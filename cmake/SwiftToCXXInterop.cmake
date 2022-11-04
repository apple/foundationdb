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
    if (${SwiftInteropVersion} VERSION_LESS 6)
      message(FATAL_ERROR "Swift: reverse interop support is too old. Update your toolchain.")
    endif()
  else()
    message(FATAL_ERROR "Swift: reverse interop is required, but not supported. Update your toolcain.")
  endif()

  set(destpath ${CMAKE_CURRENT_BINARY_DIR}/include/SwiftModules)
  message(STATUS "Swift: C++ compatibility headers for ${target} will be generated in ${destpath}")

  # Generate a directory into which the C++ headers will go.
  file(MAKE_DIRECTORY ${destpath})
 
  # Generate the C++ compatibility header for the Swift module.
  target_compile_options(${target} PRIVATE "$<$<COMPILE_LANGUAGE:Swift>:SHELL: -Xfrontend -emit-clang-header-path -Xfrontend ${destpath}/${headerName}>")
  # Note: do not generate Stdlib bindings yet (C++20 only).
  target_compile_options(${target} PRIVATE "$<$<COMPILE_LANGUAGE:Swift>:SHELL: -Xfrontend -clang-header-expose-decls=has-expose-attr>")
endfunction()
