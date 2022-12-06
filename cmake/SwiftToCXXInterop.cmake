include(CompilerChecks)
include(FindSwiftLibs)

function(add_swift_to_cxx_header_gen_target target_name header_target_name header_path)
  cmake_parse_arguments(ARG "" "" "SOURCES;FLAGS" ${ARGN})

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

  set(target_includes_expr "$<TARGET_PROPERTY:${target_name},INCLUDE_DIRECTORIES>")
  if(ARG_SOURCES)
    set(target_sources ${ARG_SOURCES})
  else()
    get_target_property(target_sources ${target_name} SOURCES)
    get_target_property(target_source_dir ${target_name} SOURCE_DIR)
    list(TRANSFORM target_sources PREPEND "${target_source_dir}/")
  endif()
  add_custom_command(
  OUTPUT
    "${header_path}"
  COMMAND
    ${CMAKE_Swift_COMPILER} -frontend -typecheck
    ${target_sources}
    -enable-experimental-cxx-interop
    -module-name "${target_name}"
    -emit-clang-header-path "${header_path}"
    "$<$<BOOL:${target_includes_expr}>:-I$<JOIN:${target_includes_expr},;-I>>"
    ${ARG_FLAGS}
  DEPENDS
    "${target_sources}"
  COMMAND_EXPAND_LISTS
  COMMENT
    "Generating '${header_path}'"
  )

  add_custom_target(${header_target_name}
    DEPENDS
    "${header_path}"
  )
endfunction()
