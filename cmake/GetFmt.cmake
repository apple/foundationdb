find_package(fmt 11.1.4 EXACT QUIET CONFIG)

if(NOT fmt_FOUND)
  include(FetchContent)
  FetchContent_Declare(
    fmt
    GIT_REPOSITORY https://github.com/fmtlib/fmt
    GIT_TAG 11.1.4
  )
  FetchContent_MakeAvailable(fmt)
endif()
