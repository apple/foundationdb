# Compiles a custom build of RocksDB
#
# If RocksDB_ROOT is defined, build RocksDB from the given RocksDB_ROOT;
# otherwise download RocksDB from official website and build from it.

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
    -DFAIL_ON_WARNINGS=OFF
    -DWITH_GFLAGS=OFF
    -DWITH_TESTS=OFF
    -DWITH_TOOLS=OFF
    -DWITH_CORE_TOOLS=OFF
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
    -DCMAKE_POSITION_INDEPENDENT_CODE=True)

if(NOT DEFINED RocksDB_ROOT)
  set(RocksDB_ROOT ENV{RocksDB_ROOT})
endif()
message(STATUS "Using RocksDB_ROOT: ${RocksDB_ROOT}")

# if(NOT "${RocksDB_ROOT}" STREQUAL "")
if(ROCKSDB_FOUND)
  # Build the RocksDB at a specific location
  ExternalProject_Add(
    rocksdb
    SOURCE_DIR "${RocksDB_ROOT}"
    CMAKE_ARGS ${RocksDB_CMAKE_ARGS}
    BUILD_BYPRODUCTS <BINARY_DIR>/librocksdb.a)

  ExternalProject_Get_Property(rocksdb BINARY_DIR)
  set(ROCKSDB_LIBRARIES ${BINARY_DIR}/librocksdb.a)

  set(ROCKSDB_INCLUDE_DIR "${RocksDB_ROOT}/include")
else()
  # Build the RocksDB from git repository
  ExternalProject_Add(
    rocksdb
	GIT_REPOSITORY https://github.com/facebook/rocksdb
	GIT_TAG 8.1.fb
	GIT_SHALLOW True
    CMAKE_ARGS ${RocksDB_CMAKE_ARGS}
    BUILD_BYPRODUCTS <BINARY_DIR>/librocksdb.a
    INSTALL_COMMAND "")

  ExternalProject_Get_Property(rocksdb BINARY_DIR)
  set(ROCKSDB_LIBRARIES ${BINARY_DIR}/librocksdb.a)

  ExternalProject_Get_Property(rocksdb SOURCE_DIR)
  set(ROCKSDB_INCLUDE_DIR "${SOURCE_DIR}/include")
  set(ROCKSDB_FOUND TRUE)
endif()

message(STATUS "Using RocksDB library: ${ROCKSDB_LIBRARIES}")
message(STATUS "Using RocksDB includes: ${ROCKSDB_INCLUDE_DIR}")

mark_as_advanced(ROCKSDB_LIBRARIES ROCKSDB_INCLUDE_DIR)
