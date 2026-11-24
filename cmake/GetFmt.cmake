find_package(fmt 12.2.0 EXACT CONFIG QUIET)

if(fmt_FOUND)
  message(STATUS "Found fmt ${fmt_VERSION} at ${fmt_DIR}")
else()
  include(FetchContent)
  FetchContent_Declare(
    fmt
    URL https://github.com/fmtlib/fmt/archive/refs/tags/12.2.0.tar.gz
    URL_HASH SHA256=8b852bb5aa6e7d8564f9e81394055395dd1d1936d38dfd3a17792a02bebd7af0
  )
  FetchContent_MakeAvailable(fmt)
endif()
