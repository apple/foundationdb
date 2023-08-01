# FindRocksDB

find_package(RocksDB 7.10.2)

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
  set(ROCKSDB_LIBRARIES
      ${BINARY_DIR}/librocksdb.a)
else()
  ExternalProject_Add(rocksdb
     URL https://github.com/facebook/rocksdb/archive/refs/tags/v7.10.2.tar.gz
     URL_HASH SHA256=4619ae7308cd3d11cdd36f0bfad3fb03a1ad399ca333f192b77b6b95b08e2f78
    CMAKE_ARGS ${RocksDB_CMAKE_ARGS}
    BUILD_BYPRODUCTS <BINARY_DIR>/librocksdb.a
    INSTALL_COMMAND ""
  )

  ExternalProject_Get_Property(rocksdb BINARY_DIR)
  set(ROCKSDB_LIBRARIES
      ${BINARY_DIR}/librocksdb.a)

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
