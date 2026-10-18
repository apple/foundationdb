find_package(fmt 11.1.4 EXACT CONFIG QUIET)

if(fmt_FOUND)
  message(STATUS "Found fmt ${fmt_VERSION} at ${fmt_DIR}")
else()
  include(FetchContent)
  FetchContent_Declare(
    fmt
    URL      "https://github.com/fmtlib/fmt/archive/11.1.4.tar.gz"
    URL_HASH SHA256=ac366b7b4c2e9f0dde63a59b3feb5ee59b67974b14ee5dc9ea8ad78aa2c1ee1e
  )
  FetchContent_MakeAvailable(fmt)
endif()
