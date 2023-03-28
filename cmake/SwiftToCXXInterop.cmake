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
    if (${SwiftInteropVersion} VERSION_LESS 16)
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

  set (SwiftFrontendOpts )

  string(REGEX MATCHALL "-Xcc [-=/a-zA-Z0-9_.]+" SwiftXccOptionsFlags "${CMAKE_Swift_FLAGS}")
  string(REGEX MATCHALL "-target [-=/a-zA-Z0-9_.]+" SwiftTargetFlags "${CMAKE_Swift_FLAGS}")
  string(REGEX MATCHALL "-sdk [-=/a-zA-Z0-9_.]+" SwiftSDKFlags "${CMAKE_Swift_FLAGS}")
  string(REGEX MATCHALL "-module-cache-path [-=/a-zA-Z0-9_.]+" SwiftMCFlags "${CMAKE_Swift_FLAGS}")
  foreach (flag ${SwiftXccOptionsFlags})
    string(SUBSTRING ${flag} 5 -1 clangFlag)
    list(APPEND SwiftFrontendOpts "-Xcc")
    list(APPEND SwiftFrontendOpts "${clangFlag}")
  endforeach()

  set(FlagName "-cxx-interoperability-mode")
  string(REGEX MATCHALL "${FlagName}=[-_/a-zA-Z0-9.]+" SwiftEqFlags "${CMAKE_Swift_FLAGS}")
  foreach (flag ${SwiftEqFlags})
    list(APPEND SwiftFrontendOpts "${flag}")
  endforeach()
  foreach (flag ${SwiftTargetFlags})
    string(SUBSTRING ${flag} 8 -1 clangFlag)
    list(APPEND SwiftFrontendOpts "-target")
    list(APPEND SwiftFrontendOpts "${clangFlag}")
  endforeach()
  foreach (flag ${SwiftSDKFlags})
    string(SUBSTRING ${flag} 5 -1 clangFlag)
    list(APPEND SwiftFrontendOpts "-sdk")
    list(APPEND SwiftFrontendOpts "${clangFlag}")
  endforeach()
  foreach (flag ${SwiftMCFlags})
    string(SUBSTRING ${flag} 19 -1 clangFlag)
    list(APPEND SwiftFrontendOpts "-module-cache-path")
    list(APPEND SwiftFrontendOpts "${clangFlag}")
  endforeach()

  add_custom_command(
  OUTPUT
    "${header_path}"
  COMMAND
    ${CMAKE_Swift_COMPILER} -frontend -typecheck
    ${target_sources}
    -module-name "${target_name}"
    -emit-clang-header-path "${header_path}"
    "$<$<BOOL:${target_includes_expr}>:-I$<JOIN:${target_includes_expr},;-I>>"
    ${ARG_FLAGS}
    ${SwiftFrontendOpts}
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
