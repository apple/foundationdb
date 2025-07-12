# Try to find fmt using pkg-config first (for homebrew installations)
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(FMT QUIET fmt)
  if(FMT_FOUND)
    # Create an imported target for fmt
    add_library(fmt::fmt INTERFACE IMPORTED)
    target_link_libraries(fmt::fmt INTERFACE ${FMT_LIBRARIES})
    target_link_directories(fmt::fmt INTERFACE ${FMT_LIBRARY_DIRS})
    target_include_directories(fmt::fmt INTERFACE ${FMT_INCLUDE_DIRS})
    target_compile_options(fmt::fmt INTERFACE ${FMT_CFLAGS_OTHER})
    set(fmt_FOUND TRUE)
  endif()
endif()

# Fall back to standard find_package
if(NOT fmt_FOUND)
  find_package(fmt 11.0.2 EXACT QUIET CONFIG)
endif()

# Fall back to FetchContent if still not found
if(NOT fmt_FOUND)
  include(FetchContent)
  FetchContent_Declare(
    fmt
    GIT_REPOSITORY https://github.com/fmtlib/fmt
    GIT_TAG 11.0.2
  )
  FetchContent_MakeAvailable(fmt)
endif()
