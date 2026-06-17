# Compile zstd
# Only used by flow, only if FLOW_USE_ZSTD
# see flow/CMakeLists.txt

function(compile_zstd)
  include(FetchContent)

  set(ZSTD_BUILD_STATIC ON)
  set(ZSTD_BUILD_SHARED OFF)
  set(ZSTD_BUILD_PROGRAMS OFF)
  set(ZSTD_BUILD_TESTS OFF)
  set(ZSTD_BUILD_CONTRIB OFF)

  set(CMAKE_POLICY_VERSION_MINIMUM "3.10")

  FetchContent_Declare(zstd
    URL "https://github.com/facebook/zstd/releases/download/v1.5.2/zstd-1.5.2.tar.gz"
    URL_HASH "SHA256=7c42d56fac126929a6a85dbc73ff1db2411d04f104fae9bdea51305663a83fd0"
    SOURCE_SUBDIR "build/cmake"
  )
  FetchContent_MakeAvailable(zstd)
  target_compile_options(libzstd_static PRIVATE -Wno-error)

  unset(CMAKE_POLICY_VERSION_MINIMUM)

  set(ZSTD_LIB_INCLUDE_DIR ${zstd_SOURCE_DIR}/lib CACHE INTERNAL ZSTD_LIB_INCLUDE_DIR)
endfunction(compile_zstd)
