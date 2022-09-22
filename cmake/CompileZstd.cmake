# Compile zstd

function(compile_zstd)

  include(FetchContent)

  set(ZSTD_SOURCE_DIR ${CMAKE_BINARY_DIR}/zstd)

  FetchContent_Declare(
    ZSTD
    GIT_REPOSITORY https://github.com/facebook/zstd.git
    GIT_TAG        v1.5.2
    SOURCE_DIR     ${ZSTD_SOURCE_DIR}
    BINARY_DIR     ${ZSTD_SOURCE_DIR}
    SOURCE_SUBDIR  "build/cmake"
  )

  FetchContent_MakeAvailable(ZSTD)

  add_library(ZSTD::ZSTD STATIC IMPORTED)
  set_target_properties(ZSTD::ZSTD PROPERTIES IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/lib/libzstd.a")
  target_include_directories(ZSTD::ZSTD PUBLIC ${ZSTD_INCLUDE_DIRS})
endfunction(compile_zstd)
