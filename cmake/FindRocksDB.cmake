# FindRocksDB

find_path(ROCKSDB_INCLUDE_DIR
  NAMES rocksdb/db.h
  PATH_SUFFIXES include)

if(ROCKSDB_INCLUDE_DIR AND EXISTS "${ROCKSDB_INCLUDE_DIR}/rocksdb/version.h")
  foreach(ver "MAJOR" "MINOR" "PATCH")
    file(STRINGS "${ROCKSDB_INCLUDE_DIR}/rocksdb/version.h" ROCKSDB_VER_${ver}_LINE
      REGEX "^#define[ \t]+ROCKSDB_${ver}[ \t]+[0-9]+$")
    string(REGEX REPLACE "^#define[ \t]+ROCKSDB_${ver}[ \t]+([0-9]+)$"
      "\\1" ROCKSDB_VERSION_${ver} "${ROCKSDB_VER_${ver}_LINE}")
    unset(${ROCKSDB_VER_${ver}_LINE})
  endforeach()
  set(ROCKSDB_VERSION_STRING
    "${ROCKSDB_VERSION_MAJOR}.${ROCKSDB_VERSION_MINOR}.${ROCKSDB_VERSION_PATCH}")

  message(STATUS "Found RocksDB version: ${ROCKSDB_VERSION_STRING}")
endif()

find_package_handle_standard_args(RocksDB
  REQUIRED_VARS ROCKSDB_INCLUDE_DIR
  VERSION_VAR ROCKSDB_VERSION_STRING)
