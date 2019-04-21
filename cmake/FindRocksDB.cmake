# FindRocksDB

if(ROCKSDB_USE_STATIC_LIBS)
  set(_rocksdb_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
  if(WIN32)
    set(CMAKE_FIND_LIBRARY_SUFFIXES .lib .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
  elseif(APPLE)
    set(CMAKE_FIND_LIBRARY_SUFFIXES .a .dylib)
  else()
    set(CMAKE_FIND_LIBRARY_SUFFIXES .a .so)
  endif()
endif()

find_path(ROCKSDB_INCLUDE_DIR
  NAMES rocksdb/db.h
  PATH_SUFFIXES include)

find_library(ROCKSDB_LIBRARY
  NAMES rocksdb)

find_library(SNAPPY_LIBRARY
  NAMES snappy)

find_library(BZ2_LIBRARY
  NAMES bz2)

find_library(LZ4_LIBRARY
  NAMES lz4)

mark_as_advanced(ROCKSDB_INCLUDE_DIR ROCKSDB_LIBRARY SNAPPY_LIBRARY BZ2_LIBRARY LZ4_LIBRARY)

find_package_handle_standard_args(RocksDB
  REQUIRED_VARS ROCKSDB_INCLUDE_DIR ROCKSDB_LIBRARY SNAPPY_LIBRARY BZ2_LIBRARY LZ4_LIBRARY
  FAIL_MESSAGE "Could NOT find RocksDB, try to set the path to root folder in variable RocksDB_ROOT")

if(ROCKSDB_FOUND)
  add_library(RocksDB INTERFACE)
  target_include_directories(RocksDB INTERFACE "${ROCKSDB_INCLUDE_DIR}")
  target_link_libraries(RocksDB INTERFACE "${ROCKSDB_LIBRARY}" ${SNAPPY_LIBRARY} ${BZ2_LIBRARY} ${LZ4_LIBRARY})
endif()

if (ROCKSDB_USE_STATIC_LIBS)
  set(CMAKE_FIND_LIBRARY_SUFFIXES ${_rocksdb_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})
endif()
