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
    message(FATAL_ERROR "Swift: reverse interop is required, but not supported. Update your toolchain.")
  endif()

  set(destpath ${CMAKE_CURRENT_BINARY_DIR}/include/SwiftModules)
  message(STATUS "Swift: C++ compatibility headers for ${target} will be generated in ${destpath}")

  # Generate a directory into which the C++ headers will go.
  file(MAKE_DIRECTORY ${destpath})
 
  # Generate the C++ compatibility header for the Swift module.
  target_compile_options(${target} PRIVATE "$<$<COMPILE_LANGUAGE:Swift>:SHELL: -Xfrontend -emit-clang-header-path -Xfrontend ${destpath}/${headerName}>")
  # Note: do not generate Stdlib bindings yet (C++20 only).
  if (${headerName} STREQUAL "Flow")
    target_compile_options(${target} PRIVATE "$<$<COMPILE_LANGUAGE:Swift>:SHELL: -Xfrontend -clang-header-expose-decls=has-expose-attr>")
  endif()
endfunction()

function(add_swift_to_cxx_header_gen_target target_name header_target_name header_path source_path)
  cmake_parse_arguments(ARG "" "" "FLAGS" ${ARGN})

  set(target_includes_expr "$<TARGET_PROPERTY:${target_name},INCLUDE_DIRECTORIES>")
  add_custom_command(
  OUTPUT
    "${header_path}"
  COMMAND
    ${CMAKE_Swift_COMPILER} -frontend -typecheck
    "${source_path}"
    -module-name "${target_name}"
    -emit-clang-header-path "${header_path}"
    "$<$<BOOL:${target_includes_expr}>:-I$<JOIN:${target_includes_expr},;-I>>"
    ${ARG_FLAGS}
  DEPENDS
    "${source_path}"
  COMMAND_EXPAND_LISTS
  COMMENT
    "Generating '${header_path}'"
  )

  add_custom_target(${header_target_name}
    DEPENDS
    "${header_path}"
  )
endfunction()
