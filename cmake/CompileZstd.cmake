# Compile zstd

function(compile_zstd)

  include(FetchContent)

  FetchContent_Declare(ZSTD
    GIT_REPOSITORY https://github.com/facebook/zstd.git
    GIT_TAG v1.5.2
    SOURCE_SUBDIR "build/cmake"
  )

  FetchContent_GetProperties(ZSTD)
  if (NOT zstd_POPULATED)
    FetchContent_Populate(ZSTD)

    add_subdirectory(${zstd_SOURCE_DIR}/build/cmake ${zstd_BINARY_DIR})

    if (CLANG)
      target_compile_options(zstd PRIVATE -Wno-array-bounds -Wno-tautological-compare)
      target_compile_options(libzstd_static PRIVATE -Wno-array-bounds -Wno-tautological-compare)
      target_compile_options(zstd-frugal PRIVATE -Wno-array-bounds -Wno-tautological-compare)
    endif()
  endif()

  set(ZSTD_LIB_INCLUDE_DIR ${zstd_SOURCE_DIR}/lib CACHE INTERNAL ZSTD_LIB_INCLUDE_DIR)
endfunction(compile_zstd)
