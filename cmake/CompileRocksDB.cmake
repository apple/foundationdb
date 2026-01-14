# FindRocksDB

# Load RocksDB version configuration from source-controlled file
include(${CMAKE_CURRENT_LIST_DIR}/RocksDBVersion.cmake)

# Validate configuration - must use exactly one of the two options
if(ROCKSDB_GIT_HASH AND ROCKSDB_VERSION)
  message(FATAL_ERROR
    "Both ROCKSDB_GIT_HASH and ROCKSDB_VERSION are set in cmake/RocksDBVersion.cmake. "
    "Please choose only ONE option: comment out the one you don't want to use.")
endif()

if(NOT ROCKSDB_GIT_HASH AND NOT ROCKSDB_VERSION)
  message(FATAL_ERROR
    "Neither ROCKSDB_GIT_HASH nor ROCKSDB_VERSION is set in cmake/RocksDBVersion.cmake. "
    "Please configure at least one option.")
endif()

# Determine version string for header generation
if(ROCKSDB_GIT_HASH)
  # Auto-fetch version from GitHub
  message(STATUS "Fetching RocksDB version for commit ${ROCKSDB_GIT_HASH}...")
  set(ROCKSDB_VERSION_URL "https://raw.githubusercontent.com/facebook/rocksdb/${ROCKSDB_GIT_HASH}/include/rocksdb/version.h")
  file(DOWNLOAD
    ${ROCKSDB_VERSION_URL}
    "${CMAKE_BINARY_DIR}/rocksdb_version_check.h"
    STATUS ROCKSDB_VERSION_DOWNLOAD_STATUS
    TIMEOUT 30)
  list(GET ROCKSDB_VERSION_DOWNLOAD_STATUS 0 ROCKSDB_VERSION_DOWNLOAD_ERROR)
  if(NOT ROCKSDB_VERSION_DOWNLOAD_ERROR EQUAL 0)
    list(GET ROCKSDB_VERSION_DOWNLOAD_STATUS 1 ROCKSDB_VERSION_DOWNLOAD_ERROR_MSG)
    message(FATAL_ERROR
      "Failed to fetch RocksDB version.h from ${ROCKSDB_VERSION_URL}: ${ROCKSDB_VERSION_DOWNLOAD_ERROR_MSG}\n"
      "Network access is required when using ROCKSDB_GIT_HASH.")
  endif()

  # Parse version from downloaded file
  file(READ "${CMAKE_BINARY_DIR}/rocksdb_version_check.h" ROCKSDB_VERSION_H_CONTENT)
  string(REGEX MATCH "#define ROCKSDB_MAJOR ([0-9]+)" _ ${ROCKSDB_VERSION_H_CONTENT})
  set(FDB_ROCKSDB_MAJOR ${CMAKE_MATCH_1})
  string(REGEX MATCH "#define ROCKSDB_MINOR ([0-9]+)" _ ${ROCKSDB_VERSION_H_CONTENT})
  set(FDB_ROCKSDB_MINOR ${CMAKE_MATCH_1})
  string(REGEX MATCH "#define ROCKSDB_PATCH ([0-9]+)" _ ${ROCKSDB_VERSION_H_CONTENT})
  set(FDB_ROCKSDB_PATCH ${CMAKE_MATCH_1})

  if(NOT FDB_ROCKSDB_MAJOR OR NOT FDB_ROCKSDB_MINOR OR NOT FDB_ROCKSDB_PATCH)
    message(FATAL_ERROR "Failed to parse version from RocksDB version.h")
  endif()

  message(STATUS "Detected RocksDB version: ${FDB_ROCKSDB_MAJOR}.${FDB_ROCKSDB_MINOR}.${FDB_ROCKSDB_PATCH}")
  set(FDB_ROCKSDB_GIT_HASH ${ROCKSDB_GIT_HASH})
else()
  set(ROCKSDB_VERSION_FOR_HEADER ${ROCKSDB_VERSION})
  # Parse version string into MAJOR.MINOR.PATCH
  string(REPLACE "." ";" ROCKSDB_VERSION_LIST ${ROCKSDB_VERSION_FOR_HEADER})
  list(LENGTH ROCKSDB_VERSION_LIST ROCKSDB_VERSION_LIST_LENGTH)
  if(NOT ROCKSDB_VERSION_LIST_LENGTH EQUAL 3)
    message(FATAL_ERROR
      "Invalid RocksDB version format: ${ROCKSDB_VERSION_FOR_HEADER}. Expected MAJOR.MINOR.PATCH (e.g., 9.7.3)")
  endif()
  list(GET ROCKSDB_VERSION_LIST 0 FDB_ROCKSDB_MAJOR)
  list(GET ROCKSDB_VERSION_LIST 1 FDB_ROCKSDB_MINOR)
  list(GET ROCKSDB_VERSION_LIST 2 FDB_ROCKSDB_PATCH)
  set(FDB_ROCKSDB_GIT_HASH "")
endif()

# Generate FDBRocksDBVersion.h from template into build directory
# This file is NOT source-controlled - it's regenerated at cmake configure time
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/fdbserver/include/fdbserver)
configure_file(
  ${CMAKE_CURRENT_LIST_DIR}/FDBRocksDBVersion.h.in
  ${CMAKE_BINARY_DIR}/fdbserver/include/fdbserver/FDBRocksDBVersion.h
  @ONLY)
message(STATUS "Generated FDBRocksDBVersion.h with version ${FDB_ROCKSDB_MAJOR}.${FDB_ROCKSDB_MINOR}.${FDB_ROCKSDB_PATCH}")

# Only try to find system RocksDB if not using a specific commit hash
if(NOT ROCKSDB_GIT_HASH)
  find_package(RocksDB ${ROCKSDB_VERSION})
endif()

include(ExternalProject)

set(RocksDB_CMAKE_ARGS
  -DUSE_RTTI=1
  -DPORTABLE=${PORTABLE_ROCKSDB}
  -DFORCE_SSE42=${ROCKSDB_SSE42}
  -DFORCE_AVX=${ROCKSDB_AVX}
  -DFORCE_AVX2=${ROCKSDB_AVX2}
  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
  -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
  -DCMAKE_CXX_STANDARD=${CMAKE_CXX_STANDARD}
  -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
  -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
  -DCMAKE_SHARED_LINKER_FLAGS=${CMAKE_SHARED_LINKER_FLAGS}
  -DCMAKE_STATIC_LINKER_FLAGS=${CMAKE_STATIC_LINKER_FLAGS}
  -DCMAKE_EXE_LINKER_FLAGS=${CMAKE_EXE_LINKER_FLAGS}
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
  -DCMAKE_EXPORT_COMPILE_COMMANDS=${CMAKE_EXPORT_COMPILE_COMMANDS}
  -DFAIL_ON_WARNINGS=OFF
  -DWITH_GFLAGS=OFF
  -DWITH_TESTS=OFF
  -DWITH_TOOLS=${ROCKSDB_TOOLS}
  -DWITH_CORE_TOOLS=${ROCKSDB_TOOLS}
  -DWITH_BENCHMARK_TOOLS=OFF
  -DWITH_BZ2=OFF
  -DWITH_LZ4=ON
  -DWITH_SNAPPY=OFF
  -DWITH_ZLIB=OFF
  -DWITH_ZSTD=OFF
  -DWITH_LIBURING=${WITH_LIBURING}
  -DWITH_TSAN=${USE_TSAN}
  -DWITH_ASAN=${USE_ASAN}
  -DWITH_UBSAN=${USE_UBSAN}
  -DROCKSDB_BUILD_SHARED=OFF
  -DCMAKE_POSITION_INDEPENDENT_CODE=True
)

if(ROCKSDB_FOUND)
  ExternalProject_Add(rocksdb
    SOURCE_DIR "${RocksDB_ROOT}"
    DOWNLOAD_COMMAND ""
    CMAKE_ARGS ${RocksDB_CMAKE_ARGS}
    BUILD_BYPRODUCTS <BINARY_DIR>/librocksdb.a
    INSTALL_COMMAND ""
  )

  ExternalProject_Get_Property(rocksdb BINARY_DIR)
  set(ROCKSDB_LIBRARIES ${BINARY_DIR}/librocksdb.a)
else()
  # Determine download URL and hash based on whether using commit hash or version
  if(ROCKSDB_GIT_HASH)
    if(NOT ROCKSDB_GIT_HASH_SHA256)
      message(FATAL_ERROR "ROCKSDB_GIT_HASH_SHA256 is required when using ROCKSDB_GIT_HASH for security. "
        "Get the SHA256 with: curl -sL https://github.com/facebook/rocksdb/archive/${ROCKSDB_GIT_HASH}.tar.gz | sha256sum")
    endif()
    set(ROCKSDB_DOWNLOAD_URL "https://github.com/facebook/rocksdb/archive/${ROCKSDB_GIT_HASH}.tar.gz")
    set(ROCKSDB_URL_HASH_ARG URL_HASH SHA256=${ROCKSDB_GIT_HASH_SHA256})
    message(STATUS "Building RocksDB from commit: ${ROCKSDB_GIT_HASH}")
  else()
    set(ROCKSDB_DOWNLOAD_URL "https://github.com/facebook/rocksdb/archive/refs/tags/v${ROCKSDB_VERSION}.tar.gz")
    set(ROCKSDB_URL_HASH_ARG URL_HASH SHA256=${ROCKSDB_VERSION_SHA256})
    message(STATUS "Building RocksDB version: ${ROCKSDB_VERSION}")
  endif()

  ExternalProject_Add(rocksdb
    URL ${ROCKSDB_DOWNLOAD_URL}
    ${ROCKSDB_URL_HASH_ARG}
    CMAKE_ARGS ${RocksDB_CMAKE_ARGS}
    BUILD_BYPRODUCTS <BINARY_DIR>/librocksdb.a
    INSTALL_COMMAND ""
  )

  ExternalProject_Get_Property(rocksdb BINARY_DIR)
  set(ROCKSDB_LIBRARIES ${BINARY_DIR}/librocksdb.a)

  ExternalProject_Get_Property(rocksdb SOURCE_DIR)
  set(ROCKSDB_INCLUDE_DIR "${SOURCE_DIR}/include")

  set(ROCKSDB_FOUND TRUE)
endif()

message(STATUS "Found RocksDB library: ${ROCKSDB_LIBRARIES}")
message(STATUS "Found RocksDB includes: ${ROCKSDB_INCLUDE_DIR}")

mark_as_advanced(
    ROCKSDB_LIBRARIES
    ROCKSDB_INCLUDE_DIR
)