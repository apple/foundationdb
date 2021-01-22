# FindRocksDB

find_path(ROCKSDB_INCLUDE_DIR
  NAMES rocksdb/db.h
  PATH_SUFFIXES include)

find_package_handle_standard_args(RocksDB
  DEFAULT_MSG ROCKSDB_INCLUDE_DIR)
