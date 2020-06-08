# FindRocksDB

find_package(RocksDB)

include(ExternalProject)

if (RocksDB_FOUND)
  ExternalProject_Add(rocksdb
    SOURCE_DIR "${RocksDB_ROOT}"
    DOWNLOAD_COMMAND ""
    CMAKE_ARGS -DUSE_RTTI=1 -DPORTABLE=${PORTABLE_ROCKSDB}
               -DCMAKE_CXX_STANDARD=${CMAKE_CXX_STANDARD}
               -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
               -DWITH_GFLAGS=OFF
               -DWITH_TESTS=OFF
               -DWITH_TOOLS=OFF
               -DWITH_CORE_TOOLS=OFF
               -DWITH_BENCHMARK_TOOLS=OFF
               -DWITH_BZ2=OFF
               -DWITH_LZ4=OFF
               -DWITH_SNAPPY=OFF
               -DWITH_ZLIB=OFF
               -DWITH_ZSTD=OFF
               -DCMAKE_POSITION_INDEPENDENT_CODE=True
    INSTALL_COMMAND ""
  )

  ExternalProject_Get_Property(rocksdb BINARY_DIR)
  set(ROCKSDB_LIBRARIES
      ${BINARY_DIR}/librocksdb.a)
else()
  message("She MISSED it!")
  ExternalProject_Add(rocksdb
    URL        https://github.com/facebook/rocksdb/archive/v6.10.1.tar.gz
    URL_HASH   SHA256=d573d2f15cdda883714f7e0bc87b814a8d4a53a82edde558f08f940e905541ee
    CMAKE_ARGS -DUSE_RTTI=1 -DPORTABLE=${PORTABLE_ROCKSDB}
               -DCMAKE_CXX_STANDARD=${CMAKE_CXX_STANDARD}
               -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
               -DWITH_GFLAGS=OFF
               -DWITH_TESTS=OFF
               -DWITH_TOOLS=OFF
               -DWITH_CORE_TOOLS=OFF
               -DWITH_BENCHMARK_TOOLS=OFF
               -DWITH_BZ2=OFF
               -DWITH_LZ4=OFF
               -DWITH_SNAPPY=OFF
               -DWITH_ZLIB=OFF
               -DWITH_ZSTD=OFF
               -DCMAKE_POSITION_INDEPENDENT_CODE=True
    INSTALL_COMMAND ""
  )

  ExternalProject_Get_Property(rocksdb BINARY_DIR)
  set(ROCKSDB_LIBRARIES
      ${BINARY_DIR}/librocksdb.a)

  set(ROCKSDB_FOUND TRUE)
endif()

message(STATUS "Found RocksDB library: ${ROCKSDB_LIBRARIES}")
message(STATUS "Found RocksDB includes: ${ROCKSDB_INCLUDE_DIRS}")

mark_as_advanced(
    ROCKSDB_LIBRARIES
    ROCKSDB_INCLUDE_DIR
)
