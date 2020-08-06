find_path(TOML11_INCLUDE_DIR
  NAMES
    toml.hpp
  PATH_SUFFIXES
    include
    toml11
    include/toml11
   HINTS
    "${_TOML11_HINTS}"
)

if (NOT TOML11_INCLUDE_DIR)
  include(ExternalProject)

  ExternalProject_add(toml11
    URL "https://github.com/ToruNiina/toml11/archive/v3.4.0.tar.gz"
    URL_HASH SHA256=bc6d733efd9216af8c119d8ac64a805578c79cc82b813e4d1d880ca128bd154d
    CMAKE_CACHE_ARGS
      -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/toml11
      -Dtoml11_BUILD_TEST:BOOL=OFF)

  set(TOML11_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/toml11/include")
endif()

find_package_handle_standard_args(TOML11
  REQUIRED_VARS
    TOML11_INCLUDE_DIR
)
