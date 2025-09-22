# Distributed under the OSI-approved Apache 2.0. See the LICENSE file in
# FoundationDB source code

#[=======================================================================[.rst:
Finddotnet
-------

Find dotnet, the free, open-source, cross-platform framework for building modern apps and powerful cloud services

dotnet_ROOT variable can be used for HINTS for different version of dotnet.

Result variables
^^^^^^^^^^^^^^^^

This module will set the following variables in your project:

``ROCKSDB_FOUND``
  If false, do not try to use dotnet.
``ROCKSDB_VERSION``
  the version of the dotnet found
``dotnet_EXECUTABLE``
  path to `dotnet` executable
#]=======================================================================]

if(NOT ROCKSDB_ROOT)
  set(ROCKSDB_ROOT $ENV{RocksDB_ROOT})
endif()

find_path(
  ROCKSDB_INCLUDE_DIR
  NAMES rocksdb/db.h
  HINTS ${ROCKSDB_ROOT}
  DOC "RocksDB include file")

if(ROCKSDB_INCLUDE_DIR AND EXISTS "${ROCKSDB_INCLUDE_DIR}/rocksdb/version.h")
  foreach(ver "MAJOR" "MINOR" "PATCH")
    file(STRINGS "${ROCKSDB_INCLUDE_DIR}/rocksdb/version.h"
         ROCKSDB_VER_${ver}_LINE
         REGEX "^#define[ \t]+ROCKSDB_${ver}[ \t]+[0-9]+$")
    string(REGEX REPLACE "^#define[ \t]+ROCKSDB_${ver}[ \t]+([0-9]+)$" "\\1"
                         ROCKSDB_VERSION_${ver} "${ROCKSDB_VER_${ver}_LINE}")
    unset(${ROCKSDB_VER_${ver}_LINE})
  endforeach()
  set(ROCKSDB_VERSION
      "${ROCKSDB_VERSION_MAJOR}.${ROCKSDB_VERSION_MINOR}.${ROCKSDB_VERSION_PATCH}"
  )

  message(STATUS "Found RocksDB version: ${ROCKSDB_VERSION}")
endif()

find_library(
  ROCKSDB_LIBRARY
  NAMES rocksdb librocksdb
  HINTS ${ROCKSDB_ROOT}
  DOC "RocksDB Library")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  RocksDB
  REQUIRED_VARS ROCKSDB_INCLUDE_DIR ROCKSDB_LIBRARY
  VERSION_VAR ROCKSDB_VERSION)

mark_as_advanced(ROCKSDB_INCLUDE_DIR ROCKSDB_LIBRARY ROCKSDB_VERSION)