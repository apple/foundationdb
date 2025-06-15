# find_package(fmt 11.0.2 EXACT QUIET CONFIG)

# if(NOT fmt_FOUND) # Always true now effectively
include(FetchContent)
FetchContent_Declare(
  fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt
  GIT_TAG 11.0.2
)
FetchContent_MakeAvailable(fmt)
# endif()
